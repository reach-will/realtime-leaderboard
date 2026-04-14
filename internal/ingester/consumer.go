package ingester

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
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

	// Batching tuning — adjust based on profiling.
	// batchSize caps the number of messages accumulated before flushing to Redis.
	// batchTimeout flushes a partial batch if no new messages arrive within the window,
	// preventing stale scores under low traffic.
	batchSize    = 100
	batchTimeout = 20 * time.Millisecond

	// processedMatchesTTL is the lifetime of the processed-match-IDs set in Redis.
	// It only needs to outlive the redelivery window: the gap between a partial pipeline
	// apply and the next successful commit after a consumer restart, which is seconds in
	// practice. 24 hours is deliberately conservative.
	processedMatchesTTL = 24 * 60 * 60 // seconds
)

// matchUpdate holds the parsed outcome of a single match ready to be applied to Redis.
type matchUpdate struct {
	msg     kafka.Message
	matchID string
	playerA string
	deltaA  float64
	playerB string
	deltaB  float64
}

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

// Run collects and flushes batches until ctx is cancelled.
//
// Collection and flushing are decoupled: a dedicated goroutine collects the next
// batch from Kafka while the main goroutine is flushing the previous one to Redis.
// A channel of size 1 acts as the handoff — the collector stays one batch ahead,
// so the flusher never waits for Kafka and the collector never waits for Redis.
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

	ch := make(chan []matchUpdate, 1)

	go func() {
		defer close(ch)
		for {
			updates := c.collectBatch(ctx)
			if ctx.Err() != nil {
				return
			}
			if len(updates) == 0 {
				continue
			}
			select {
			case ch <- updates:
			case <-ctx.Done():
				return
			}
		}
	}()

	for updates := range ch {
		c.flushBatch(ctx, updates)
	}
	slog.Info("ingester shutting down")
}

// collectBatch fetches up to batchSize messages within batchTimeout, parsing each
// into a matchUpdate. Non-transient errors (bad payload, unknown outcome) are
// forwarded to the DLT inline and excluded from the returned batch.
func (c *Consumer) collectBatch(ctx context.Context) (updates []matchUpdate) {
	timer := prometheus.NewTimer(batchCollectDuration)
	defer timer.ObserveDuration()
	deadline := time.Now().Add(batchTimeout)

	for len(updates) < batchSize {
		fetchCtx, cancel := context.WithDeadline(ctx, deadline)
		msg, err := c.reader.FetchMessage(fetchCtx)
		cancel()

		if ctx.Err() != nil {
			return
		}
		if fetchCtx.Err() != nil {
			return // batch window expired, flush what we have
		}
		if err != nil {
			slog.Error("failed to read message", "error", err)
			continue
		}

		messagesReceivedTotal.Inc()

		var outcome eventspb.MatchOutcome
		err = proto.Unmarshal(msg.Value, &outcome)
		if err != nil {
			// Non-transient: malformed payload will never deserialize successfully (poison pill).
			messagesErrorsTotal.Inc()
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
			messagesErrorsTotal.Inc()
			slog.Warn("unknown outcome", "outcome", outcome.Outcome)
			c.sendToDLT(ctx, msg, "unknown_outcome: "+outcome.Outcome.String())
			continue
		}

		updates = append(updates, matchUpdate{
			msg:     msg,
			matchID: outcome.MatchId,
			playerA: outcome.PlayerA,
			deltaA:  deltaA,
			playerB: outcome.PlayerB,
			deltaB:  deltaB,
		})
	}
	return
}

// flushBatch pipelines one Lua script call per match in a single Redis round-trip,
// then commits the batch offset.
//
// Each script call atomically updates both players' scores for a single match
// (see scripts.go). Pipelining preserves per-match atomicity while still batching
// all network I/O into one round-trip.
//
// If the pipeline fails, all messages in the batch are routed to the DLT.
// TODO: route to a retry topic instead once infrastructure supports it.
func (c *Consumer) flushBatch(ctx context.Context, updates []matchUpdate) {
	timer := prometheus.NewTimer(batchFlushDuration)
	defer timer.ObserveDuration()
	batchesTotal.Inc()
	batchSizeHistogram.Observe(float64(len(updates)))

	msgs := make([]kafka.Message, len(updates))
	for i, upd := range updates {
		msgs[i] = upd.msg
	}

	redisRequestsTotal.Inc()
	redisTimer := prometheus.NewTimer(redisRequestDuration)
	_, err := c.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, upd := range updates {
			updateScoresScript.Run(ctx, pipe,
				[]string{rediskeys.LeaderboardGlobal, rediskeys.ProcessedMatches},
				upd.deltaA, upd.playerA, upd.deltaB, upd.playerB, upd.matchID, processedMatchesTTL)
		}
		return nil
	})
	redisTimer.ObserveDuration()
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		// Transient: pipeline failure — route entire batch to DLT.
		// TODO: route to a retry topic instead once infrastructure supports it.
		redisErrorsTotal.Inc()
		batchesErrorsTotal.Inc()
		slog.Error("pipeline failed", "error", err, "batch_size", len(updates))
		for _, msg := range msgs {
			messagesErrorsTotal.Inc()
			c.sendToDLT(ctx, msg, "redis_pipeline_error: "+err.Error())
		}
		return
	}

	err = c.reader.CommitMessages(ctx, msgs...)
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		// Scores updated but offset not saved — will reprocess on restart (at-least-once delivery).
		// Idempotent score updates (e.g. via match_id deduplication) would prevent score corruption.
		batchesErrorsTotal.Inc()
		slog.Error("failed to commit batch", "error", err, "batch_size", len(updates))
		return
	}

	slog.Info("batch flushed", "messages", len(updates))
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
	err = c.reader.CommitMessages(ctx, msg)
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		slog.Error("failed to commit after DLT write", "error", err)
	}
}
