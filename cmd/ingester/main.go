package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/reach-will/realtime-leaderboard/internal/adminhttp"
	"github.com/reach-will/realtime-leaderboard/internal/events"
	"github.com/reach-will/realtime-leaderboard/internal/rediskeys"
	"github.com/redis/go-redis/v9"
	kafka "github.com/segmentio/kafka-go"
)

const (
	winDelta  = 3.0
	lossDelta = -1.0
	drawDelta = 1.0
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{os.Getenv("KAFKA_URL")},
		Topic:    os.Getenv("KAFKA_TOPIC"),
		GroupID:  os.Getenv("KAFKA_GROUP_ID"),
		Dialer:   &kafka.Dialer{KeepAlive: 30 * time.Second},
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_ADDR"),
		Protocol: 2,
	})
	defer rdb.Close()

	metricsAddr := os.Getenv("METRICS_ADDR")
	if metricsAddr == "" {
		metricsAddr = ":2112"
	}
	go adminhttp.Start(metricsAddr)

	fmt.Println("Ingester started: consuming match outcomes. Ctrl+C to stop.")

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				fmt.Println("Ingester shutting down...")
				return
			}
			log.Println("read error:", err)
			continue
		}

		start := time.Now()

		var outcome events.MatchOutcome
		if err := json.Unmarshal(msg.Value, &outcome); err != nil {
			messagesProcessedCounter.Inc()
			processingErrorsCounter.Inc()
			log.Println("unmarshal error:", err)
			continue
		}

		var deltaA, deltaB float64
		switch outcome.Outcome {
		case events.OutcomePlayerAWins:
			deltaA, deltaB = winDelta, lossDelta
		case events.OutcomePlayerBWins:
			deltaA, deltaB = lossDelta, winDelta
		case events.OutcomeDraw:
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
		scoreA, err := rdb.ZIncrBy(ctx, rediskeys.LeaderboardGlobal, deltaA, outcome.PlayerA).Result()
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

		scoreB, err := rdb.ZIncrBy(ctx, rediskeys.LeaderboardGlobal, deltaB, outcome.PlayerB).Result()
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

		if err := reader.CommitMessages(ctx, msg); err != nil {
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
			outcome.MatchID,
			outcome.PlayerA, scoreA,
			outcome.PlayerB, scoreB,
			outcome.Outcome,
		)
	}
}
