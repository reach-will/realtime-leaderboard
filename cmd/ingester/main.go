package main

import (
	"context"
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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{os.Getenv("KAFKA_URL")},
		Topic:    os.Getenv("KAFKA_TOPIC"),
		GroupID:  os.Getenv("KAFKA_GROUP_ID"),
		Dialer:   &kafka.Dialer{KeepAlive: 30 * time.Second},
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_ADDR"),
		Protocol: 2,
	})
	defer rdb.Close()

	metricsAddr := os.Getenv("METRICS_ADDR")
	if metricsAddr == "" {
		metricsAddr = ":2112"
	}
	go adminhttp.Start(metricsAddr)

	ingester.New(reader, rdb).Run(ctx)
}
