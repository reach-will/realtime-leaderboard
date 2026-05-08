package leaderboardservice

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	pb "github.com/reach-will/realtime-leaderboard/gen/leaderboard/v1"
	"github.com/reach-will/realtime-leaderboard/internal/rediskeys"
	"github.com/redis/go-redis/v9"
)

const heartbeatInterval = 15 * time.Second

type ssePlayer struct {
	PlayerID string  `json:"player_id"`
	Score    float64 `json:"score"`
	Rank     int32   `json:"rank"`
}

type ssePayload struct {
	Players []ssePlayer `json:"players"`
	AsOf    int64       `json:"as_of"` // Unix milliseconds; lets the UI show "updated X ago"
}

// ServeLeaderboard is the SSE endpoint for browser clients. It streams the
// current top-10 leaderboard as server-sent events.
//
// On connect the client receives a "snapshot" event with the current standings,
// followed by "update" events whenever scores change. A ": ping" comment is
// sent every 15 s to keep the connection alive through proxies and load balancers.
//
// Event types:
//
//	snapshot  full top-10 on initial connect
//	update    full top-10 after each score change
//
// Browser usage:
//
//	const es = new EventSource("/leaderboard/stream");
//	es.addEventListener("snapshot", e => render(JSON.parse(e.data)));
//	es.addEventListener("update",   e => render(JSON.parse(e.data)));
func (s *Server) ServeLeaderboard(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // disable nginx proxy buffering
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// Tell the browser to reconnect after 3 s if the connection drops.
	fmt.Fprintf(w, "retry: 3000\n\n")
	flusher.Flush()

	// Subscribe before fetching initial state so no update is lost in the gap.
	ch := s.hub.Subscribe(r.Context())

	players, err := s.fetchTop10(r.Context())
	if err != nil {
		slog.Error("sse: initial snapshot failed", "error", err)
		return
	}
	writeSSEEvent(w, flusher, "snapshot", players)

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case players := <-ch:
			writeSSEEvent(w, flusher, "update", players)
		case <-ticker.C:
			fmt.Fprintf(w, ": ping\n\n")
			flusher.Flush()
		}
	}
}

func (s *Server) fetchTop10(ctx context.Context) ([]*pb.Player, error) {
	scores, err := s.rdb.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
		Key:   rediskeys.LeaderboardGlobal,
		Start: 0,
		Stop:  -1,
		Rev:   true,
	}).Result()
	if err != nil {
		return nil, err
	}
	return toPlayers(scores), nil
}

func writeSSEEvent(w http.ResponseWriter, f http.Flusher, event string, players []*pb.Player) {
	payload := ssePayload{
		Players: make([]ssePlayer, len(players)),
		AsOf:    time.Now().UnixMilli(),
	}
	for i, p := range players {
		payload.Players[i] = ssePlayer{
			PlayerID: p.PlayerId,
			Score:    p.Score,
			Rank:     p.Rank,
		}
	}

	data, err := json.Marshal(payload)
	if err != nil {
		slog.Error("sse: failed to marshal payload", "error", err)
		return
	}

	fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
	f.Flush()
}
