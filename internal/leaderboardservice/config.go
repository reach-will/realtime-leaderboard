package leaderboardservice

import (
	"fmt"
	"os"

	"github.com/reach-will/realtime-leaderboard/internal/config"
)

type Config struct {
	GRPCAddr  string // e.g. :50051
	SSEAddr   string // e.g. :8080
	AdminAddr string // e.g. :2113
	RedisAddr string // e.g. redis:6379
}

func Load() (Config, error) {
	c := Config{
		GRPCAddr:  config.Get("GRPC_ADDR", ":50051"),
		SSEAddr:   config.Get("SSE_ADDR", ":8080"),
		AdminAddr: config.Get("ADMIN_ADDR", ":2113"),
		RedisAddr: os.Getenv("REDIS_ADDR"),
	}
	if c.RedisAddr == "" {
		return Config{}, fmt.Errorf("missing required env var: REDIS_ADDR")
	}
	return c, nil
}
