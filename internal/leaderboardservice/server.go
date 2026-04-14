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

// Server is the leaderboard gRPC service. Call Close when done.
type Server struct {
	pb.UnimplementedLeaderboardServiceServer
	rdb         *redis.Client
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

func (s *Server) GetTop(ctx context.Context, req *pb.GetTopRequest) (*pb.GetTopResponse, error) {
	if req.Limit <= 0 {
		return nil, status.Error(codes.InvalidArgument, "limit must be greater than 0")
	}
	if req.Limit > maxLimit {
		return nil, status.Errorf(codes.InvalidArgument, "limit must be at most %d", maxLimit)
	}

	topScores, err := s.rdb.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
		Key:   rediskeys.LeaderboardGlobal,
		Start: 0,
		Stop:  req.Limit - 1,
		Rev:   true,
	}).Result()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to fetch top players: %v", err)
	}

	players := make([]*pb.Player, len(topScores))
	for i, score := range topScores {
		players[i] = &pb.Player{
			PlayerId: score.Member.(string),
			Score:    score.Score,
			Rank:     int32(i + 1),
		}
	}
	return &pb.GetTopResponse{Players: players}, nil
}

func (s *Server) GetPlayer(ctx context.Context, req *pb.GetPlayerRequest) (*pb.GetPlayerResponse, error) {
	if req.PlayerId == "" {
		return nil, status.Error(codes.InvalidArgument, "player_id is required")
	}

	result, err := s.rdb.ZRevRankWithScore(ctx, rediskeys.LeaderboardGlobal, req.PlayerId).Result()
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

func (s *Server) StreamTop(req *pb.GetTopRequest, stream pb.LeaderboardService_StreamTopServer) error {
	if req.Limit <= 0 {
		return status.Error(codes.InvalidArgument, "limit must be greater than 0")
	}
	if req.Limit > maxLimit {
		return status.Errorf(codes.InvalidArgument, "limit must be at most %d", maxLimit)
	}

	// Subscribe before the initial fetch so we don't miss an update that arrives
	// between the fetch and the loop below.
	ch := s.hub.Subscribe(stream.Context())

	// Send an immediate snapshot so the client doesn't wait for the next ingester flush.
	topScores, err := s.rdb.ZRangeArgsWithScores(stream.Context(), redis.ZRangeArgs{
		Key:   rediskeys.LeaderboardGlobal,
		Start: 0,
		Stop:  req.Limit - 1,
		Rev:   true,
	}).Result()
	if err != nil {
		return status.Errorf(codes.Internal, "failed to fetch initial leaderboard: %v", err)
	}
	players := make([]*pb.Player, len(topScores))
	for i, score := range topScores {
		players[i] = &pb.Player{
			PlayerId: score.Member.(string),
			Score:    score.Score,
			Rank:     int32(i + 1),
		}
	}
	if err := stream.Send(&pb.GetTopResponse{Players: players}); err != nil {
		return err
	}

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case players := <-ch:
			if int32(len(players)) > req.Limit {
				players = players[:req.Limit]
			}
			if err := stream.Send(&pb.GetTopResponse{Players: players}); err != nil {
				return err
			}
		}
	}
}
