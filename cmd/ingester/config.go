package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/reach-will/realtime-leaderboard/internal/env"
)

type config struct {
	KafkaURL     string
	KafkaTopic   string
	KafkaGroupID string
	RedisAddr    string
	MetricsAddr  string
}

func loadConfig() (config, error) {
	c := config{
		KafkaURL:     os.Getenv("KAFKA_URL"),
		KafkaTopic:   os.Getenv("KAFKA_TOPIC"),
		KafkaGroupID: os.Getenv("KAFKA_GROUP_ID"),
		RedisAddr:    env.OrDefault("REDIS_ADDR", "localhost:6379"),
		MetricsAddr:  env.OrDefault("METRICS_ADDR", ":2112"),
	}

	var missing []string
	if c.KafkaURL == "" {
		missing = append(missing, "KAFKA_URL")
	}
	if c.KafkaTopic == "" {
		missing = append(missing, "KAFKA_TOPIC")
	}
	if c.KafkaGroupID == "" {
		missing = append(missing, "KAFKA_GROUP_ID")
	}
	if len(missing) > 0 {
		return c, fmt.Errorf("missing required environment variables: %s", strings.Join(missing, ", "))
	}

	return c, nil
}
