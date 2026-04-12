package ingester

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	eventspb "github.com/reach-will/realtime-leaderboard/gen/events/v1"
	"github.com/reach-will/realtime-leaderboard/internal/rediskeys"
	"github.com/redis/go-redis/v9"
	kafka "github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

const (
	winDelta  = 3.0
	lossDelta = -1.0
	drawDelta = 1.0
)

// Consumer reads match outcomes from Kafka and updates scores in Redis.
// Call Close when done.
type Consumer struct {
	reader *kafka.Reader
	rdb    *redis.Client
	dlt    *kafka.Writer
}

// New creates a Consumer from cfg, establishing Kafka and Redis connections.
func New(cfg Config) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{cfg.KafkaAddr},
		Topic:    cfg.KafkaTopic,
		GroupID:  cfg.KafkaGroupID,
		Dialer:   &kafka.Dialer{KeepAlive: 30 * time.Second},
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		Protocol:     2,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})
	dlt := &kafka.Writer{
		Addr:     kafka.TCP(cfg.KafkaAddr),
		Topic:    cfg.KafkaDLTopic,
		Balancer: &kafka.LeastBytes{},
	}
	return &Consumer{reader: reader, rdb: rdb, dlt: dlt}
}

// Close releases the Kafka reader, Redis connection, and DLT writer.
func (c *Consumer) Close() {
	c.reader.Close()
	c.rdb.Close()
	c.dlt.Close()
}

// Run processes messages until ctx is cancelled.
//
// Error handling strategy:
//
// All processing errors — whether non-transient (malformed payload, unknown outcome) or
// transient (Redis timeouts, network blips) — are routed directly to the Dead Letter Topic
// (DLT) and committed, keeping the main consumer unblocked.
//
// A more resilient architecture would separate these two classes:
//   - Non-transient (poison pills): malformed or semantically invalid messages that will
//     never succeed regardless of retries. Route immediately to DLT.
//   - Transient (client errors): temporary downstream failures (e.g. Redis timeouts) that
//     may resolve on retry. Route to a dedicated retry topic consumed by a separate consumer,
//     with backoff between attempts, graduating to DLT after exhaustion. This avoids blocking
//     the main partition while still preserving the message for later reprocessing.
//
// See Confluent's Pattern 3 (retry topic) for the transient case:
// https://www.confluent.io/blog/error-handling-patterns-in-kafka/
// And Uber's tiered retry topic architecture for a production-grade version of the same idea:
// https://www.uber.com/ie/en/blog/reliable-reprocessing/
//
// TODO: introduce a retry topic for transient Redis errors once infrastructure supports it.
func (c *Consumer) Run(ctx context.Context) {
	slog.Info("ingester started")

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				slog.Info("ingester shutting down")
				return
			}
			slog.Error("failed to read message", "error", err)
			continue
		}

		start := time.Now()

		var outcome eventspb.MatchOutcome
		if err := proto.Unmarshal(msg.Value, &outcome); err != nil {
			// Non-transient: malformed payload will never deserialize successfully (poison pill).
			messagesProcessedCounter.Inc()
			processingErrorsCounter.Inc()
			slog.Error("failed to unmarshal message", "error", err)
			c.sendToDLT(ctx, msg, "unmarshal_error: "+err.Error())
			continue
		}

		var deltaA, deltaB float64
		switch outcome.Outcome {
		case eventspb.Outcome_OUTCOME_PLAYER_A_WINS:
			deltaA, deltaB = winDelta, lossDelta
		case eventspb.Outcome_OUTCOME_PLAYER_B_WINS:
			deltaA, deltaB = lossDelta, winDelta
		case eventspb.Outcome_OUTCOME_DRAW:
			deltaA, deltaB = drawDelta, drawDelta
		default:
			// Non-transient: unknown outcome type is a producer-side bug, not a transient failure.
			messagesProcessedCounter.Inc()
			processingErrorsCounter.Inc()
			slog.Warn("unknown outcome", "outcome", outcome.Outcome)
			c.sendToDLT(ctx, msg, "unknown_outcome: "+outcome.Outcome.String())
			continue
		}

		redisStart := time.Now()

		// ZIncrBy is atomic per call; two separate calls are acceptable here
		// because the worst case (crash between them) leaves one player updated.
		// Exact atomicity across both players requires a Lua script, added in a later phase.
		scoreA, err := c.rdb.ZIncrBy(ctx, rediskeys.LeaderboardGlobal, deltaA, outcome.PlayerA).Result()
		if err != nil {
			if ctx.Err() != nil {
				slog.Info("ingester shutting down")
				return
			}
			// Transient: Redis errors may be temporary, but we route directly to DLT rather
			// than retrying in-place to avoid blocking the partition.
			// TODO: route to a retry topic instead once infrastructure supports it.
			messagesProcessedCounter.Inc()
			processingErrorsCounter.Inc()
			redisUpdatesCounter.Inc()
			redisErrorsCounter.Inc()
			slog.Error("failed to update player score", "player", outcome.PlayerA, "error", err)
			c.sendToDLT(ctx, msg, "redis_error: "+err.Error())
			continue
		}
		redisUpdatesCounter.Inc()

		scoreB, err := c.rdb.ZIncrBy(ctx, rediskeys.LeaderboardGlobal, deltaB, outcome.PlayerB).Result()
		if err != nil {
			if ctx.Err() != nil {
				slog.Info("ingester shutting down")
				return
			}
			// Transient: same as above.
			// TODO: route to a retry topic instead once infrastructure supports it.
			messagesProcessedCounter.Inc()
			processingErrorsCounter.Inc()
			redisUpdatesCounter.Inc()
			redisErrorsCounter.Inc()
			slog.Error("failed to update player score", "player", outcome.PlayerB, "error", err)
			c.sendToDLT(ctx, msg, "redis_error: "+err.Error())
			continue
		}
		redisUpdatesCounter.Inc()
		redisUpdateDuration.Observe(time.Since(redisStart).Seconds())

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				slog.Info("ingester shutting down")
				return
			}
			// Commit failure after successful Redis writes means scores are updated but
			// the offset is not saved. The message will be reprocessed on restart, causing
			// a duplicate ZIncrBy — acceptable under at-least-once delivery semantics.
			// Idempotent score updates (e.g. via match_id deduplication) would prevent this.
			messagesProcessedCounter.Inc()
			processingErrorsCounter.Inc()
			slog.Error("failed to commit message", "error", err)
			continue
		}

		processingDuration.Observe(time.Since(start).Seconds())
		messagesProcessedCounter.Inc()

		slog.Info("match processed",
			"match_id", outcome.MatchId,
			"player_a", outcome.PlayerA,
			"score_a", scoreA,
			"player_b", outcome.PlayerB,
			"score_b", scoreB,
			"outcome", outcome.Outcome,
		)
	}
}

// sendToDLT forwards msg to the dead letter topic with a reason header, then commits
// the original offset so the main consumer can advance past the failed message.
// If the DLT write itself fails, the error is logged and the offset is left uncommitted —
// the message will be reprocessed on restart (at-least-once delivery).
func (c *Consumer) sendToDLT(ctx context.Context, msg kafka.Message, reason string) {
	if ctx.Err() != nil {
		return
	}
	err := c.dlt.WriteMessages(ctx, kafka.Message{
		Key:   msg.Key,
		Value: msg.Value,
		Headers: []kafka.Header{
			{Key: "error-reason", Value: []byte(reason)},
			{Key: "original-topic", Value: []byte(msg.Topic)},
			{Key: "original-partition", Value: []byte(strconv.Itoa(msg.Partition))},
			{Key: "original-offset", Value: []byte(strconv.FormatInt(msg.Offset, 10))},
		},
	})
	if err != nil {
		slog.Error("failed to write to DLT — message will be reprocessed on restart",
			"error", err,
			"reason", reason,
		)
		return
	}
	if err := c.reader.CommitMessages(ctx, msg); err != nil && ctx.Err() == nil {
		slog.Error("failed to commit after DLT write", "error", err)
	}
}
