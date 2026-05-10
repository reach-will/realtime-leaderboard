package matchoutcomesimulator

import (
	"fmt"
	"os"
	"strings"
)

type Config struct {
	KafkaAddr  string
	KafkaTopic string
}

func Load() (Config, error) {
	c := Config{
		KafkaAddr:  os.Getenv("KAFKA_ADDR"),
		KafkaTopic: os.Getenv("KAFKA_TOPIC"),
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
