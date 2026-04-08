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

// Server is the leaderboard gRPC service. Call Close when done.
type Server struct {
	pb.LeaderboardServiceServer
	rdb *redis.Client
}

type server struct {
	pb.UnimplementedLeaderboardServiceServer
	rdb *redis.Client
}

// New returns a leaderboard gRPC service implementation backed by Redis.
func New(cfg Config) *Server {
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	return &Server{
		LeaderboardServiceServer: &server{rdb: rdb},
		rdb:                      rdb,
	}
}

// Close releases the underlying Redis connection.
func (s *Server) Close() { s.rdb.Close() }

func (s *server) GetTop(ctx context.Context, req *pb.GetTopRequest) (*pb.GetTopResponse, error) {
	if req.Limit <= 0 {
		return nil, status.Error(codes.InvalidArgument, "limit must be greater than 0")
	}

	topScores, err := s.rdb.ZRevRangeWithScores(ctx, rediskeys.LeaderboardGlobal, 0, int64(req.Limit-1)).Result()
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

func (s *server) GetPlayer(ctx context.Context, req *pb.GetPlayerRequest) (*pb.GetPlayerResponse, error) {
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

func (s *server) StreamTop(req *pb.GetTopRequest, stream pb.LeaderboardService_StreamTopServer) error {
	if req.Limit <= 0 {
		return status.Error(codes.InvalidArgument, "limit must be greater than 0")
	}

	var last []*pb.Player
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-ticker.C:
			topScores, err := s.rdb.ZRevRangeWithScores(stream.Context(), rediskeys.LeaderboardGlobal, 0, int64(req.Limit-1)).Result()
			if err != nil {
				return status.Errorf(codes.Internal, "failed to fetch top players: %v", err)
			}

			players := make([]*pb.Player, len(topScores))
			for i, score := range topScores {
				players[i] = &pb.Player{
					PlayerId: score.Member.(string),
					Score:    score.Score,
					Rank:     int32(i + 1),
				}
			}

			if !playersEqual(last, players) {
				if err := stream.Send(&pb.GetTopResponse{Players: players}); err != nil {
					return err
				}
				last = players
			}
		}
	}
}

func playersEqual(a, b []*pb.Player) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].PlayerId != b[i].PlayerId || a[i].Score != b[i].Score {
			return false
		}
	}
	return true
}
