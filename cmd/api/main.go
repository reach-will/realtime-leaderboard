package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"github.com/prometheus/client_golang/prometheus"
	pb "github.com/reach-will/realtime-leaderboard/gen/leaderboard/v1"
	"github.com/reach-will/realtime-leaderboard/internal/admin"
	"github.com/reach-will/realtime-leaderboard/internal/scoreboardservice"
	"google.golang.org/grpc"
)

func main() {
	cfg, err := scoreboardservice.Load()
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	svc := scoreboardservice.New(cfg)
	defer svc.Close()

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
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

	recoveryHandler := recovery.WithRecoveryHandler(func(p any) error {
		slog.Error("panic recovered", "panic", p)
		return nil
	})

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			srvMetrics.UnaryServerInterceptor(),
			recovery.UnaryServerInterceptor(recoveryHandler),
		),
		grpc.ChainStreamInterceptor(
			srvMetrics.StreamServerInterceptor(),
			recovery.StreamServerInterceptor(recoveryHandler),
		),
	)
	pb.RegisterScoreboardServiceServer(grpcServer, svc)
	registerReflection(grpcServer)
	srvMetrics.InitializeMetrics(grpcServer)

	go admin.Serve(cfg.AdminAddr)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go svc.Start(ctx)

	go func() {
		<-ctx.Done()
		slog.Info("API server shutting down")

		stopped := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(stopped)
		}()

		select {
		case <-stopped:
			slog.Info("graceful shutdown complete")
		case <-time.After(15 * time.Second):
			slog.Warn("graceful shutdown timed out, forcing stop")
			grpcServer.Stop()
		}
	}()

	slog.Info("API server listening", "addr", cfg.GRPCAddr)
	if err := grpcServer.Serve(lis); err != nil {
		slog.Error("failed to serve", "error", err)
		os.Exit(1)
	}
}
