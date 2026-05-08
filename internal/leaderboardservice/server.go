package leaderboardservice

import (
	"context"
	"errors"
	"time"

	pb "github.com/reach-will/realtime-leaderboard/gen/leaderboard/v1"
	"github.com/reach-will/realtime-leaderboard/internal/rediskeys"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxLimit = 1000

func toPlayers(scores []redis.Z) []*pb.Player {
	players := make([]*pb.Player, len(scores))
	for i, z := range scores {
		players[i] = &pb.Player{
			PlayerId: z.Member.(string),
			Score:    z.Score,
			Rank:     int32(i + 1),
		}
	}
	return players
}

// Server is the leaderboard gRPC service. Call Close when done.
type Server struct {
	pb.UnimplementedLeaderboardServiceServer
	rdb *redis.Client
	hub *Hub
}

// New returns a leaderboard gRPC service implementation backed by Redis.
func New(cfg Config) *Server {
	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})
	return &Server{rdb: rdb, hub: newHub(rdb)}
}

// Start runs the leaderboard hub until ctx is cancelled.
// Call this once in a goroutine after the server starts.
func (s *Server) Start(ctx context.Context) {
	s.hub.Run(ctx)
}

// Close releases the underlying Redis connection.
func (s *Server) Close() { s.rdb.Close() }

func (s *Server) GetPlayer(ctx context.Context, req *pb.GetPlayerRequest) (*pb.GetPlayerResponse, error) {
	if req.PlayerId == "" {
		return nil, status.Error(codes.InvalidArgument, "player_id is required")
	}

	result, err := s.rdb.ZRevRankWithScore(ctx, rediskeys.ScoresGlobal, req.PlayerId).Result()
	if errors.Is(err, redis.Nil) {
		return nil, status.Errorf(codes.NotFound, "player %q not found", req.PlayerId)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to fetch player: %v", err)
	}

	return &pb.GetPlayerResponse{
		Player: &pb.Player{
			PlayerId: req.PlayerId,
			Score:    result.Score,
			Rank:     int32(result.Rank + 1),
		},
	}, nil
}

func (s *Server) GetTop(ctx context.Context, req *pb.GetTopRequest) (*pb.GetTopResponse, error) {
	if req.Limit <= 0 {
		return nil, status.Error(codes.InvalidArgument, "limit must be greater than 0")
	}
	if req.Limit > maxLimit {
		return nil, status.Errorf(codes.InvalidArgument, "limit must be at most %d", maxLimit)
	}

	scores, err := s.rdb.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
		Key:   rediskeys.ScoresGlobal,
		Start: 0,
		Stop:  req.Limit - 1,
		Rev:   true,
	}).Result()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to fetch top players: %v", err)
	}

	return &pb.GetTopResponse{Players: toPlayers(scores)}, nil
}
