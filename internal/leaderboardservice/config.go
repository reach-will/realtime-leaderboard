package leaderboardservice

import "github.com/reach-will/realtime-leaderboard/internal/config"

// Config holds all configuration for the leaderboard API binary.
type Config struct {
	GRPCAddr  string // e.g. :50051
	AdminAddr string // e.g. :2113
	RedisAddr string // e.g. localhost:6379
}

// Load reads configuration from environment variables.
func Load() (Config, error) {
	return Config{
		GRPCAddr:  config.Get("GRPC_ADDR", ":50051"),
		AdminAddr: config.Get("ADMIN_ADDR", ":2113"),
		RedisAddr: config.Get("REDIS_ADDR", "localhost:6379"),
	}, nil
}
