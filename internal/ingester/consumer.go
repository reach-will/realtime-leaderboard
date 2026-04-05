package ingester

import (
	"context"
	"fmt"
	"log"
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
type Consumer struct {
	reader *kafka.Reader
	rdb    *redis.Client
}

// New creates a Consumer with the given Kafka reader and Redis client.
func New(reader *kafka.Reader, rdb *redis.Client) *Consumer {
	return &Consumer{reader: reader, rdb: rdb}
}

// Run processes messages until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) {
	fmt.Println("Ingester started: consuming match outcomes. Ctrl+C to stop.")

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				fmt.Println("Ingester shutting down...")
				return
			}
			log.Println("read error:", err)
			continue
		}

		start := time.Now()

		var outcome eventspb.MatchOutcome
		if err := proto.Unmarshal(msg.Value, &outcome); err != nil {
			messagesProcessedCounter.Inc()
			processingErrorsCounter.Inc()
			log.Println("unmarshal error:", err)
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
			messagesProcessedCounter.Inc()
			processingErrorsCounter.Inc()
			log.Println("unknown outcome:", outcome.Outcome)
			continue
		}

		redisStart := time.Now()

		// ZIncrBy is atomic per call; two separate calls are acceptable here
		// because the worst case (crash between them) leaves one player updated.
		// Exact atomicity across both players requires a Lua script, added in a later phase.
		scoreA, err := c.rdb.ZIncrBy(ctx, rediskeys.LeaderboardGlobal, deltaA, outcome.PlayerA).Result()
		if err != nil {
			if ctx.Err() != nil {
				fmt.Println("Ingester shutting down...")
				return
			}
			messagesProcessedCounter.Inc()
			processingErrorsCounter.Inc()
			redisUpdatesCounter.Inc()
			redisErrorsCounter.Inc()
			log.Println("redis error (playerA):", err)
			continue
		}
		redisUpdatesCounter.Inc()

		scoreB, err := c.rdb.ZIncrBy(ctx, rediskeys.LeaderboardGlobal, deltaB, outcome.PlayerB).Result()
		if err != nil {
			if ctx.Err() != nil {
				fmt.Println("Ingester shutting down...")
				return
			}
			messagesProcessedCounter.Inc()
			processingErrorsCounter.Inc()
			redisUpdatesCounter.Inc()
			redisErrorsCounter.Inc()
			log.Println("redis error (playerB):", err)
			continue
		}
		redisUpdatesCounter.Inc()
		redisUpdateDuration.Observe(time.Since(redisStart).Seconds())

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				fmt.Println("Ingester shutting down...")
				return
			}
			messagesProcessedCounter.Inc()
			processingErrorsCounter.Inc()
			log.Println("commit error:", err)
			continue
		}

		processingDuration.Observe(time.Since(start).Seconds())
		messagesProcessedCounter.Inc()

		fmt.Printf("match_id=%s  playerA=%s(%.0f)  playerB=%s(%.0f)  outcome=%s\n",
			outcome.MatchId,
			outcome.PlayerA, scoreA,
			outcome.PlayerB, scoreB,
			outcome.Outcome,
		)
	}
}
