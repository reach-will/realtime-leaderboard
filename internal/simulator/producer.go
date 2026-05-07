package simulator

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/google/uuid"
	eventspb "github.com/reach-will/realtime-leaderboard/gen/events/v1"
	kafka "github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

const playerCount = 1000

// Producer generates random match outcomes and publishes them to Kafka.
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

// randomMatch generates a single random match outcome from the player pool.
func (p *Producer) randomMatch() *eventspb.MatchOutcome {
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

	return &eventspb.MatchOutcome{
		MatchId:     uuid.New().String(),
		PlayerA:     p.players[idx1],
		PlayerB:     p.players[idx2],
		Outcome:     outcome,
		TimestampMs: time.Now().UnixMilli(),
	}
}

// Run produces match outcome events until ctx is cancelled.
func (p *Producer) Run(ctx context.Context) {
	slog.Info("simulator started")

	for {
		event := p.randomMatch()

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

// Close releases the underlying Kafka writer.
func (p *Producer) Close() { p.writer.Close() }
