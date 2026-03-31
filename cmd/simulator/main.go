package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"
	"time"

	eventspb "github.com/reach-will/realtime-leaderboard/gen/events/v1"
	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Generate a fixed pool of player IDs using UUID v5 (deterministic, same IDs on every restart).
	// UUID v5 is SHA-1 hashed from a namespace + name, so player-0 always produces the same UUID.
	const playerCount = 100
	var playerNamespace = uuid.MustParse("a0000000-0000-0000-0000-000000000000")
	players := make([]string, playerCount)
	for i := range players {
		players[i] = uuid.NewSHA1(playerNamespace, []byte(fmt.Sprintf("%d", i))).String()
	}

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(os.Getenv("KAFKA_URL")),
		Topic:                  os.Getenv("KAFKA_TOPIC"),
		Balancer:               &kafka.Hash{},
		AllowAutoTopicCreation: true,
	}
	defer writer.Close()

	fmt.Println("Simulator started: producing match outcomes every second. Ctrl+C to stop.")

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
			fmt.Println("marshal error:", err)
			continue
		}

		msg := kafka.Message{
			Key:   []byte(event.MatchId),
			Value: payload,
		}

		if err := writer.WriteMessages(ctx, msg); err != nil {
			fmt.Println("produce error:", err)
		} else {
			fmt.Printf("produced matchId=%s  playerA=%s  playerB=%s  outcome=%s\n",
				event.MatchId, event.PlayerA, event.PlayerB, event.Outcome)
		}

		select {
		case <-ctx.Done():
			fmt.Println("Simulator shutting down...")
			return
		case <-time.After(time.Second):
		}
	}
}
