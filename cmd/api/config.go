package main

import (
	"github.com/reach-will/realtime-leaderboard/internal/env"
)

type config struct {
	Port        string
	RedisAddr   string
	MetricsAddr string
}

func loadConfig() (config, error) {
	c := config{
		Port:        ":" + env.OrDefault("PORT", "50051"),
		RedisAddr:   env.OrDefault("REDIS_ADDR", "localhost:6379"),
		MetricsAddr: env.OrDefault("METRICS_ADDR", ":2113"),
	}

	return c, nil
}
