package loadgen

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	defaultRate    = 5_000
	defaultWorkers = 10
)

// Config holds all configuration for the load-generator binary.
type Config struct {
	KafkaAddr  string
	KafkaTopic string
	// Rate is the total target message throughput in messages per second.
	Rate int
	// Workers is the number of concurrent producer goroutines.
	// Effective max rate = Workers * 1000/s (limited by 1ms ticker resolution).
	Workers int
}

// Load reads configuration from environment variables with fail-fast validation.
func Load() (Config, error) {
	c := Config{
		KafkaAddr:  os.Getenv("KAFKA_ADDR"),
		KafkaTopic: os.Getenv("KAFKA_TOPIC"),
		Rate:       defaultRate,
		Workers:    defaultWorkers,
	}

	if v := os.Getenv("SIMULATOR_RATE"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return Config{}, fmt.Errorf("SIMULATOR_RATE must be a positive integer, got %q", v)
		}
		c.Rate = n
	}

	if v := os.Getenv("SIMULATOR_WORKERS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return Config{}, fmt.Errorf("SIMULATOR_WORKERS must be a positive integer, got %q", v)
		}
		c.Workers = n
	}

	var missing []string
	if c.KafkaAddr == "" {
		missing = append(missing, "KAFKA_ADDR")
	}
	if c.KafkaTopic == "" {
		missing = append(missing, "KAFKA_TOPIC")
	}
	if len(missing) > 0 {
		return Config{}, fmt.Errorf("missing required env vars: %s", strings.Join(missing, ", "))
	}
	return c, nil
}
