package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	pb "github.com/reach-will/realtime-leaderboard/gen/leaderboard/v1"
	"github.com/reach-will/realtime-leaderboard/internal/adminhttp"
	"github.com/reach-will/realtime-leaderboard/internal/leaderboardservice"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

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

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	defer rdb.Close()

	lis, err := net.Listen("tcp", *port)
	if err != nil {
		slog.Error("failed to listen", "error", err)
		os.Exit(1)
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
	pb.RegisterLeaderboardServiceServer(grpcServer, leaderboardservice.New(rdb))
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
		slog.Info("API server listening", "port", *port)
		if err := grpcServer.Serve(lis); err != nil {
			slog.Error("failed to serve", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("API server shutting down")
	grpcServer.GracefulStop()
}
