package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/reach-will/realtime-leaderboard/internal/matchpairloadgen"
)

func main() {
	cfg, err := matchpairloadgen.Load()
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	p := matchpairloadgen.New(cfg)
	defer p.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	p.Run(ctx)
}
