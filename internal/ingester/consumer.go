package ingester

import (
	"context"
	"errors"
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
	// Must outlive the redelivery window: the gap between a partial pipeline apply
	// and the next successful commit after a consumer restart, which is seconds in
	// practice. 5 minutes is a conservative bound that keeps SET size manageable
	// under high-throughput load.
	processedMatchesTTL = 5 * 60 // seconds
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
		// CommitInterval > 0 makes CommitMessages return immediately; kafka-go's
		// internal commit loop goroutine flushes offsets to the coordinator on
		// this interval. The trade-off is that on a crash the consumer may
		// re-read up to CommitInterval worth of already-processed messages —
		// safe here because score updates are idempotent via match-ID deduplication.
		CommitInterval: 500 * time.Millisecond,
	})
	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		Protocol:     2,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})
	dlt := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.KafkaAddr),
		Topic:                  cfg.KafkaDLTopic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
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
// Processing errors are split into two classes:
//   - Non-transient (poison pills): malformed or semantically invalid messages that will
//     never succeed regardless of retries. Routed immediately to the DLT.
//   - Transient (client errors): temporary downstream failures (e.g. Redis timeouts) that
//     may resolve on retry. Routed to the retry topic via sendBatchToRetryTopic, which
//     currently forwards to the DLT until retry infrastructure is in place.
//
// See Confluent's Pattern 3 (retry topic) for the intended transient-error architecture:
// https://www.confluent.io/blog/error-handling-patterns-in-kafka/
// And Uber's tiered retry topic architecture for a production-grade version of the same idea:
// https://www.uber.com/ie/en/blog/reliable-reprocessing/
func (c *Consumer) Run(ctx context.Context) {
	slog.Info("ingester started")

	if err := updateScoresScript.Load(ctx, c.rdb).Err(); err != nil {
		slog.Error("failed to load Redis Lua script", "error", err)
		return
	}

	activeInstances.Inc()
	defer activeInstances.Dec()

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
		if errors.Is(err, context.DeadlineExceeded) {
			return // batch window elapsed, flush what we have
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
// CommitMessages returns immediately because CommitInterval > 0 on the reader;
// the library's internal commit goroutine flushes offsets on the configured timer.
//
// If the pipeline fails, all messages in the batch are routed to the retry topic.
func (c *Consumer) flushBatch(ctx context.Context, updates []matchUpdate) {
	timer := prometheus.NewTimer(batchFlushDuration)
	defer timer.ObserveDuration()
	batchesTotal.Inc()
	batchSizeHistogram.Observe(float64(len(updates)))

	msgs := make([]kafka.Message, len(updates))
	for i, upd := range updates {
		msgs[i] = upd.msg
	}

	cmds := make([]*redis.Cmd, len(updates))
	redisRequestsTotal.Inc()
	redisTimer := prometheus.NewTimer(redisRequestDuration)
	_, err := c.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for i, upd := range updates {
			cmds[i] = updateScoresScript.Eval(ctx, pipe,
				[]string{rediskeys.ScoreGlobal, rediskeys.MatchesProcessed},
				upd.deltaA, upd.playerA, upd.deltaB, upd.playerB, upd.matchID, processedMatchesTTL)
		}
		return nil
	})
	redisTimer.ObserveDuration()
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		// Transient: pipeline failure — route entire batch to retry topic.
		redisErrorsTotal.Inc()
		batchesErrorsTotal.Inc()
		messagesErrorsTotal.Add(float64(len(updates)))
		slog.Error("pipeline failed", "error", err, "batch_size", len(updates))
		c.sendBatchToRetryTopic(ctx, msgs, "redis_pipeline_error: "+err.Error())
		return
	}

	for i, cmd := range cmds {
		result, err := cmd.Int()
		if err != nil {
			slog.Error("unexpected script result", "error", err, "match_id", updates[i].matchID)
			continue
		}
		if result == 0 {
			messagesDuplicatesTotal.Inc()
			slog.Debug("duplicate match skipped", "match_id", updates[i].matchID)
		}
	}

	err = c.reader.CommitMessages(ctx, msgs...)
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		batchesErrorsTotal.Inc()
		slog.Error("failed to commit batch", "error", err, "batch_size", len(updates))
		return
	}

	// Notify StreamTop subscribers that scores have changed. A missed publish
	// is non-fatal — subscribers simply skip one update and receive the next.
	err = c.rdb.Publish(ctx, rediskeys.ScoresUpdated, "").Err()
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		slog.Warn("failed to publish leaderboard update", "error", err)
	}

	slog.Info("batch flushed", "messages", len(updates))
}

// sendBatchToRetryTopic routes a batch of transiently-failed messages for reprocessing.
// Transient failures (e.g. Redis pipeline timeouts) may resolve on retry and should not
// be permanently discarded — routing them here keeps the main consumer unblocked while
// preserving the messages for a later attempt.
//
// TODO: replace the DLT fallback below with a dedicated retry topic consumed by a separate
// consumer with exponential backoff between attempts, graduating to the DLT after exhaustion.
// See Confluent's Pattern 3: https://www.confluent.io/blog/error-handling-patterns-in-kafka/
func (c *Consumer) sendBatchToRetryTopic(ctx context.Context, msgs []kafka.Message, reason string) {
	c.sendBatchToDLT(ctx, msgs, reason)
}

// sendBatchToDLT forwards a slice of messages to the dead letter topic in a single
// WriteMessages call, then commits all offsets together. This avoids the per-message
// kafka.Writer BatchTimeout overhead that would otherwise serialize each write
// sequentially (~1s each) and cause ~N×BatchTimeout idle time after a pipeline failure.
func (c *Consumer) sendBatchToDLT(ctx context.Context, msgs []kafka.Message, reason string) {
	if ctx.Err() != nil {
		return
	}
	dltMsgs := make([]kafka.Message, len(msgs))
	for i, msg := range msgs {
		dltMsgs[i] = kafka.Message{
			Key:   msg.Key,
			Value: msg.Value,
			Headers: []kafka.Header{
				{Key: "error-reason", Value: []byte(reason)},
				{Key: "original-topic", Value: []byte(msg.Topic)},
				{Key: "original-partition", Value: []byte(strconv.Itoa(msg.Partition))},
				{Key: "original-offset", Value: []byte(strconv.FormatInt(msg.Offset, 10))},
			},
		}
	}
	err := c.dlt.WriteMessages(ctx, dltMsgs...)
	if err != nil {
		slog.Error("failed to write batch to DLT: messages will be reprocessed on restart",
			"error", err,
			"reason", reason,
			"count", len(msgs),
		)
		return
	}
	err = c.reader.CommitMessages(ctx, msgs...)
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		slog.Error("failed to commit batch after DLT write", "error", err)
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
		slog.Error("failed to write to DLT: message will be reprocessed on restart",
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
