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
	RedisAddr    string // e.g. redis:6379
	AdminAddr    string // e.g. :2112
}

// Load reads configuration from environment variables with fail-fast validation.
func Load() (Config, error) {
	c := Config{
		KafkaAddr:    os.Getenv("KAFKA_ADDR"),
		KafkaTopic:   os.Getenv("KAFKA_TOPIC"),
		KafkaGroupID: os.Getenv("KAFKA_GROUP_ID"),
		RedisAddr:    os.Getenv("REDIS_ADDR"),
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
	if c.RedisAddr == "" {
		missing = append(missing, "REDIS_ADDR")
	}
	if len(missing) > 0 {
		return Config{}, fmt.Errorf("missing required env vars: %s", strings.Join(missing, ", "))
	}
	return c, nil
}
