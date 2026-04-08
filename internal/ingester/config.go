package ingester

import (
	"fmt"
	"os"
	"strings"

	"github.com/reach-will/realtime-leaderboard/internal/config"
)

// Config holds all configuration for the ingester binary.
type Config struct {
	KafkaAddr    string // e.g. localhost:9092
	KafkaTopic   string
	KafkaGroupID string
	RedisAddr    string // e.g. localhost:6379
	AdminAddr    string // e.g. :2112
}

// Load reads configuration from environment variables with fail-fast validation.
func Load() (Config, error) {
	c := Config{
		KafkaAddr:    os.Getenv("KAFKA_ADDR"),
		KafkaTopic:   os.Getenv("KAFKA_TOPIC"),
		KafkaGroupID: os.Getenv("KAFKA_GROUP_ID"),
		RedisAddr:    config.Get("REDIS_ADDR", "localhost:6379"),
		AdminAddr:    config.Get("ADMIN_ADDR", ":2112"),
	}
	var missing []string
	if c.KafkaAddr == "" {
		missing = append(missing, "KAFKA_ADDR")
	}
	if c.KafkaTopic == "" {
		missing = append(missing, "KAFKA_TOPIC")
	}
	if c.KafkaGroupID == "" {
		missing = append(missing, "KAFKA_GROUP_ID")
	}
	if len(missing) > 0 {
		return c, fmt.Errorf("missing required env vars: %s", strings.Join(missing, ", "))
	}
	return c, nil
}
