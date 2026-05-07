package loadgen

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	eventspb "github.com/reach-will/realtime-leaderboard/gen/events/v1"
	kafka "github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

const playerCount = 1000

// Producer generates random match outcomes at high throughput and publishes them to Kafka.
// Call Close when done.
type Producer struct {
	cfg     Config
	writer  *kafka.Writer
	players []string
}

func newPlayers(count int) []string {
	ns := uuid.MustParse("a0000000-0000-0000-0000-000000000000")
	players := make([]string, count)
	for i := range players {
		players[i] = uuid.NewSHA1(ns, []byte(strconv.Itoa(i))).String()
	}
	return players
}

// New creates a Producer from cfg. The underlying Kafka writer uses async delivery
// and writer-level batching to maximise throughput.
func New(cfg Config) *Producer {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(cfg.KafkaAddr),
		Topic:    cfg.KafkaTopic,
		Balancer: &kafka.Hash{},
		// Large internal batch so the writer aggregates across all worker goroutines.
		BatchSize:    1_000,
		BatchTimeout: 5 * time.Millisecond,
		// Async lets WriteMessages return immediately; errors surface via writer.Stats().
		Async: true,
	}
	return &Producer{cfg: cfg, writer: writer, players: newPlayers(playerCount)}
}

func (p *Producer) randomMatch() (kafka.Message, error) {
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

	id := uuid.New()
	payload, err := proto.Marshal(&eventspb.MatchOutcome{
		MatchId:     id.String(),
		PlayerA:     p.players[idx1],
		PlayerB:     p.players[idx2],
		Outcome:     outcome,
		TimestampMs: time.Now().UnixMilli(),
	})
	if err != nil {
		return kafka.Message{}, err
	}

	return kafka.Message{Key: id[:], Value: payload}, nil
}

// Run produces match outcome events at the configured rate until ctx is cancelled.
// It spawns cfg.Workers goroutines, each ticking at rate/workers per second.
func (p *Producer) Run(ctx context.Context) {
	msgsPerWorker := p.cfg.Rate / p.cfg.Workers
	if msgsPerWorker < 1 {
		msgsPerWorker = 1
	}
	// Clamp to 1ms minimum (OS timer resolution floor).
	interval := time.Second / time.Duration(msgsPerWorker)
	if interval < time.Millisecond {
		interval = time.Millisecond
	}

	slog.Info("load generator started",
		"target_rate_per_s", p.cfg.Rate,
		"workers", p.cfg.Workers,
		"interval_per_worker", interval,
	)

	var sent atomic.Int64

	// Periodic stats: log actual throughput and Kafka-level error counts.
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				n := sent.Swap(0)
				ws := p.writer.Stats()
				slog.Info("throughput",
					"sent_per_5s", n,
					"approx_rate_per_s", n/5,
					"kafka_write_errors", ws.Errors,
				)
			}
		}
	}()

	var wg sync.WaitGroup
	for range p.cfg.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t := time.NewTicker(interval)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					msg, err := p.randomMatch()
					if err != nil {
						slog.Error("marshal failed", "error", err)
						continue
					}
					// WriteMessages is non-blocking (Async: true); errors tracked via Stats().
					_ = p.writer.WriteMessages(ctx, msg)
					sent.Add(1)
				}
			}
		}()
	}

	wg.Wait()
	slog.Info("load generator stopped")
}

// Close flushes pending async writes and releases the Kafka writer.
func (p *Producer) Close() { p.writer.Close() }
