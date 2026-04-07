package main

import (
	"fmt"
	"os"
	"strings"
)

type config struct {
	KafkaAddr  string
	KafkaTopic string
}

func loadConfig() (config, error) {
	c := config{
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
		return c, fmt.Errorf("missing required environment variables: %s", strings.Join(missing, ", "))
	}

	return c, nil
}
