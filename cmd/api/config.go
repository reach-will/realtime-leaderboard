package main

import (
	"github.com/reach-will/realtime-leaderboard/internal/env"
)

type config struct {
	Port        string
	RedisAddr   string
	AdminAddr string
}

func loadConfig() (config, error) {
	c := config{
		Port:        ":" + env.OrDefault("PORT", "50051"),
		RedisAddr:   env.OrDefault("REDIS_ADDR", "localhost:6379"),
		AdminAddr: env.OrDefault("ADMIN_ADDR", ":2113"),
	}

	return c, nil
}
