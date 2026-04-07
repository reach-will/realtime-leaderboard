package main

import (
	"context"
	"strconv"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	eventspb "github.com/reach-will/realtime-leaderboard/gen/events/v1"
	kafka "github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

func main() {
	cfg, err := loadConfig()
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Generate a fixed pool of player IDs using UUID v5 (deterministic, same IDs on every restart).
	// UUID v5 is SHA-1 hashed from a namespace + name, so player-0 always produces the same UUID.
	const playerCount = 100
	var playerNamespace = uuid.MustParse("a0000000-0000-0000-0000-000000000000")
	players := make([]string, playerCount)
	for i := range players {
		players[i] = uuid.NewSHA1(playerNamespace, []byte(strconv.Itoa(i))).String()
	}

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.KafkaAddr),
		Topic:                  cfg.KafkaTopic,
		Balancer:               &kafka.Hash{},
		AllowAutoTopicCreation: true,
	}
	defer writer.Close()

	slog.Info("simulator started")

	for {
		idx1 := rand.IntN(playerCount)
		idx2 := rand.IntN(playerCount - 1)
		if idx2 >= idx1 {
			idx2++
		}

		var outcome eventspb.Outcome
		switch rand.IntN(11) {
		case 0:
			outcome = eventspb.Outcome_OUTCOME_DRAW
		case 1, 2, 3, 4, 5:
			outcome = eventspb.Outcome_OUTCOME_PLAYER_B_WINS
		default:
			outcome = eventspb.Outcome_OUTCOME_PLAYER_A_WINS
		}

		event := &eventspb.MatchOutcome{
			MatchId:     uuid.New().String(),
			PlayerA:     players[idx1],
			PlayerB:     players[idx2],
			Outcome:     outcome,
			TimestampMs: time.Now().UnixMilli(),
		}

		payload, err := proto.Marshal(event)
		if err != nil {
			slog.Error("failed to marshal event", "error", err)
			continue
		}

		err = writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(event.MatchId),
			Value: payload,
		})
		if err != nil {
			slog.Error("failed to produce message", "error", err)
		} else {
			slog.Info("match produced",
				"match_id", event.MatchId,
				"player_a", event.PlayerA,
				"player_b", event.PlayerB,
				"outcome", event.Outcome,
			)
		}

		select {
		case <-ctx.Done():
			slog.Info("simulator shutting down")
			return
		case <-time.After(time.Second):
		}
	}
}
