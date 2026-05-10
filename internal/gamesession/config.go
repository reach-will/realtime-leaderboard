package gamesession

import (
	"fmt"
	"os"
	"strings"

	"github.com/reach-will/realtime-leaderboard/internal/config"
)

type Config struct {
	KafkaAddr string
	InTopic   string // consumed: matchmaking.match.found
	OutTopic  string // produced: gamesession.match.completed
	GroupID   string
	DLTopic   string // dead letter for unprocessable MatchFound messages
	AdminAddr string
}

func Load() (Config, error) {
	inTopic := os.Getenv("MATCHMAKING_MATCH_FOUND_KAFKA_TOPIC")
	c := Config{
		KafkaAddr: os.Getenv("KAFKA_ADDR"),
		InTopic:   inTopic,
		OutTopic:  os.Getenv("GAMESESSION_MATCH_COMPLETED_KAFKA_TOPIC"),
		GroupID:   config.Get("GAMESESSION_GROUP_ID", "gamesession.consumer"),
		AdminAddr: config.Get("ADMIN_ADDR", ":2114"),
	}
	var missing []string
	if c.KafkaAddr == "" {
		missing = append(missing, "KAFKA_ADDR")
	}
	if c.InTopic == "" {
		missing = append(missing, "MATCHMAKING_MATCH_FOUND_KAFKA_TOPIC")
	}
	if c.OutTopic == "" {
		missing = append(missing, "GAMESESSION_MATCH_COMPLETED_KAFKA_TOPIC")
	}
	if len(missing) > 0 {
		return Config{}, fmt.Errorf("missing required env vars: %s", strings.Join(missing, ", "))
	}
	c.DLTopic = config.Get("GAMESESSION_DL_TOPIC", inTopic+".dlt")
	return c, nil
}
