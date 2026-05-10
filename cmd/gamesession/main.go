package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/reach-will/realtime-leaderboard/internal/admin"
	"github.com/reach-will/realtime-leaderboard/internal/gamesession"
)

func main() {
	cfg, err := gamesession.Load()
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	session := gamesession.New(cfg)
	defer session.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go admin.Serve(cfg.AdminAddr)

	session.Run(ctx)
}
