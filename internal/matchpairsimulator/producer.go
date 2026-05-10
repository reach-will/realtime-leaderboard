package matchpairsimulator

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/google/uuid"
	matchmakingpb "github.com/reach-will/realtime-leaderboard/gen/matchmaking/v1"
	kafka "github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

const playerCount = 1000

// Producer simulates the matchmaking layer by generating random player pairs
// and publishing MatchFound events to Kafka.
// Call Close when done.
type Producer struct {
	writer  *kafka.Writer
	players []string
}

// newPlayers generates a deterministic pool of player UUIDs.
func newPlayers(count int) []string {
	var playerNamespace = uuid.MustParse("a0000000-0000-0000-0000-000000000000")
	players := make([]string, count)
	for i := range players {
		players[i] = uuid.NewSHA1(playerNamespace, []byte(strconv.Itoa(i))).String()
	}
	return players
}

// New creates a Producer from cfg, establishing a Kafka writer and a fixed player pool.
func New(cfg Config) *Producer {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(cfg.KafkaAddr),
		Topic:    cfg.KafkaTopic,
		Balancer: &kafka.Hash{},
	}
	return &Producer{writer: writer, players: newPlayers(playerCount)}
}

// randomPair picks two distinct players and returns a MatchFound event.
func (p *Producer) randomPair() *matchmakingpb.MatchFound {
	idx1 := rand.IntN(playerCount)
	idx2 := rand.IntN(playerCount - 1)
	if idx2 >= idx1 {
		idx2++
	}
	return &matchmakingpb.MatchFound{
		MatchId:     uuid.New().String(),
		PlayerA:     p.players[idx1],
		PlayerB:     p.players[idx2],
		TimestampMs: time.Now().UnixMilli(),
	}
}

// Run produces one MatchFound event per second until ctx is cancelled.
func (p *Producer) Run(ctx context.Context) {
	slog.Info("match pair simulator started")

	for {
		event := p.randomPair()

		payload, err := proto.Marshal(event)
		if err != nil {
			slog.Error("failed to marshal event", "error", err)
			continue
		}

		err = p.writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(event.MatchId),
			Value: payload,
		})
		if err != nil {
			slog.Error("failed to produce message", "error", err)
		} else {
			slog.Info("pair produced",
				"match_id", event.MatchId,
				"player_a", event.PlayerA,
				"player_b", event.PlayerB,
			)
		}

		select {
		case <-ctx.Done():
			slog.Info("match pair simulator shutting down")
			return
		case <-time.After(time.Second):
		}
	}
}

// Close releases the underlying Kafka writer.
func (p *Producer) Close() { p.writer.Close() }
