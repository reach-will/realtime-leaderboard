package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/reach-will/realtime-leaderboard/internal/matchpairsimulator"
)

func main() {
	cfg, err := matchpairsimulator.Load()
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	p := matchpairsimulator.New(cfg)
	defer p.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	p.Run(ctx)
}
