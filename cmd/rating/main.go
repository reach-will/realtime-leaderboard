package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/reach-will/realtime-leaderboard/internal/admin"
	"github.com/reach-will/realtime-leaderboard/internal/rating"
)

func main() {
	cfg, err := rating.Load()
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	consumer := rating.New(cfg)
	defer consumer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go admin.Serve(cfg.AdminAddr)

	consumer.Run(ctx)
}
