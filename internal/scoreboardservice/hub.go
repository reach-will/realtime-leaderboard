package scoreboardservice

import (
	"context"
	"log/slog"
	"sync"

	pb "github.com/reach-will/realtime-leaderboard/gen/leaderboard/v1"
	"github.com/reach-will/realtime-leaderboard/internal/rediskeys"
	"github.com/redis/go-redis/v9"
)

// Hub subscribes to the leaderboard:updates Redis pub/sub channel and
// fans out a fresh leaderboard snapshot to all registered StreamTop clients on
// each notification. One Redis read serves every connected client, regardless
// of how many streams are active.
type Hub struct {
	rdb  *redis.Client
	mu   sync.Mutex
	subs map[uint64]chan []*pb.Player
	next uint64
}

func newHub(rdb *redis.Client) *Hub {
	return &Hub{
		rdb:  rdb,
		subs: make(map[uint64]chan []*pb.Player),
	}
}

// Subscribe registers a new subscriber and returns a channel that receives
// leaderboard snapshots. The subscription is automatically removed when ctx
// is cancelled (i.e. when the stream's context is done).
func (h *Hub) Subscribe(ctx context.Context) <-chan []*pb.Player {
	h.mu.Lock()
	id := h.next
	h.next++
	ch := make(chan []*pb.Player, 1)
	h.subs[id] = ch
	h.mu.Unlock()

	go func() {
		<-ctx.Done()
		h.mu.Lock()
		delete(h.subs, id)
		h.mu.Unlock()
	}()

	return ch
}

// Run subscribes to leaderboard:updates and fans out snapshots until ctx is
// cancelled. Call this once in a goroutine when the server starts.
func (h *Hub) Run(ctx context.Context) {
	pubsub := h.rdb.Subscribe(ctx, rediskeys.ScoresUpdated)
	defer pubsub.Close()

	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-pubsub.Channel():
			if !ok {
				return
			}
			scores, err := h.rdb.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
				Key:   rediskeys.ScoresGlobal,
				Start: 0,
				Stop:  maxLimit - 1,
				Rev:   true,
			}).Result()
			if err != nil {
				slog.Error("hub: failed to fetch scoreboard", "error", err)
				continue
			}

			players := make([]*pb.Player, len(scores))
			for i, z := range scores {
				players[i] = &pb.Player{
					PlayerId: z.Member.(string),
					Score:    z.Score,
					Rank:     int32(i + 1),
				}
			}
			h.fanOut(players)
		}
	}
}

// fanOut sends the snapshot to every registered subscriber. Slow subscribers
// are skipped — their buffered channel already holds a pending update.
func (h *Hub) fanOut(players []*pb.Player) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, ch := range h.subs {
		select {
		case ch <- players:
		default:
		}
	}
}
