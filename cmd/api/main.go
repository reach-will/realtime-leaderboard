package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	pb "github.com/reach-will/realtime-leaderboard/gen/leaderboard/v1"
	"github.com/reach-will/realtime-leaderboard/internal/adminhttp"
	"github.com/reach-will/realtime-leaderboard/internal/rediskeys"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type leaderboardServiceServer struct {
	pb.UnimplementedLeaderboardServiceServer
	rdb *redis.Client
}

func (s *leaderboardServiceServer) GetTop(ctx context.Context, req *pb.GetTopRequest) (*pb.GetTopResponse, error) {
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

func (s *leaderboardServiceServer) GetPlayer(ctx context.Context, req *pb.GetPlayerRequest) (*pb.GetPlayerResponse, error) {
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

func (s *leaderboardServiceServer) StreamTop(req *pb.GetTopRequest, stream pb.LeaderboardService_StreamTopServer) error {
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

func main() {
	port := flag.String("port", os.Getenv("PORT"), "Server port (env: PORT, default :50051)")
	redisAddr := flag.String("redis-addr", os.Getenv("REDIS_ADDR"), "Redis address (env: REDIS_ADDR, default localhost:6379)")
	flag.Parse()

	if *port == "" {
		*port = ":50051"
	}
	if *redisAddr == "" {
		*redisAddr = "localhost:6379"
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: *redisAddr,
	})
	defer rdb.Close()

	lis, err := net.Listen("tcp", *port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	srvMetrics := grpcprom.NewServerMetrics(
		grpcprom.WithServerHandlingTimeHistogram(
			grpcprom.WithHistogramBuckets([]float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120}),
		),
	)
	prometheus.MustRegister(srvMetrics)

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(srvMetrics.UnaryServerInterceptor()),
		grpc.ChainStreamInterceptor(srvMetrics.StreamServerInterceptor()),
	)
	pb.RegisterLeaderboardServiceServer(grpcServer, &leaderboardServiceServer{rdb: rdb})
	registerReflection(grpcServer)
	srvMetrics.InitializeMetrics(grpcServer)

	metricsAddr := os.Getenv("METRICS_ADDR")
	if metricsAddr == "" {
		metricsAddr = ":2113"
	}
	go adminhttp.Start(metricsAddr)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		fmt.Printf("API server listening on %s\n", *port)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	<-ctx.Done()
	fmt.Println("API server shutting down...")
	grpcServer.GracefulStop()
}
