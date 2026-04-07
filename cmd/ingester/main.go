package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/reach-will/realtime-leaderboard/internal/adminhttp"
	"github.com/reach-will/realtime-leaderboard/internal/ingester"
	"github.com/redis/go-redis/v9"
	kafka "github.com/segmentio/kafka-go"
)

func main() {
	cfg, err := loadConfig()
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{cfg.KafkaAddr},
		Topic:    cfg.KafkaTopic,
		GroupID:  cfg.KafkaGroupID,
		Dialer:   &kafka.Dialer{KeepAlive: 30 * time.Second},
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Protocol: 2,
	})
	defer rdb.Close()

	go adminhttp.Start(cfg.MetricsAddr)

	ingester.New(reader, rdb).Run(ctx)
}
