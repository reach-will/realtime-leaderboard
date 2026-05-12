package gamesession

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"time"

	gamesessionpb "github.com/reach-will/realtime-leaderboard/gen/gamesession/v1"
	matchmakingpb "github.com/reach-will/realtime-leaderboard/gen/matchmaking/v1"
	kafka "github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

// Session consumes MatchFound events, resolves a random outcome, and produces
// MatchOutcome events for the rating service.
type Session struct {
	reader *kafka.Reader
	writer *kafka.Writer
	dlt    *kafka.Writer
}

// New creates a Session from cfg, establishing Kafka reader, writer, and DLT connections.
func New(cfg Config) *Session {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{cfg.KafkaAddr},
		Topic:          cfg.InTopic,
		GroupID:        cfg.GroupID,
		Dialer:         &kafka.Dialer{KeepAlive: 30 * time.Second},
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	})
	writer := &kafka.Writer{
		Addr:     kafka.TCP(cfg.KafkaAddr),
		Topic:    cfg.OutTopic,
		Balancer: &kafka.Hash{},
	}
	dlt := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.KafkaAddr),
		Topic:                  cfg.DLTopic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}
	return &Session{reader: reader, writer: writer, dlt: dlt}
}

// Close releases the Kafka reader, writer, and DLT writer.
func (s *Session) Close() {
	s.reader.Close()
	s.writer.Close()
	s.dlt.Close()
}

// Run processes MatchFound events until ctx is cancelled.
// Each event is resolved to a random outcome and forwarded to the output topic
// before its offset is committed. Poison pills are routed to the DLT.
func (s *Session) Run(ctx context.Context) {
	slog.Info("game session service started")
	for {
		msg, err := s.reader.FetchMessage(ctx)
		if ctx.Err() != nil {
			slog.Info("game session service shutting down")
			return
		}
		if err != nil {
			slog.Error("failed to fetch message", "error", err)
			continue
		}

		var found matchmakingpb.MatchFound
		if err := proto.Unmarshal(msg.Value, &found); err != nil {
			slog.Error("failed to unmarshal MatchFound", "error", err)
			s.sendToDLT(ctx, msg, "unmarshal_error: "+err.Error())
			continue
		}

		outcome := resolveOutcome(&found)
		payload, err := proto.Marshal(outcome)
		if err != nil {
			slog.Error("failed to marshal MatchOutcome", "match_id", found.MatchId, "error", err)
			s.sendToDLT(ctx, msg, "marshal_error: "+err.Error())
			continue
		}

		err = s.writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(found.MatchId),
			Value: payload,
		})
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			slog.Error("failed to produce MatchOutcome", "match_id", found.MatchId, "error", err)
			s.sendToDLT(ctx, msg, "produce_error: "+err.Error())
			continue
		}

		if err := s.reader.CommitMessages(ctx, msg); ctx.Err() != nil {
			return
		} else if err != nil {
			slog.Error("failed to commit offset", "match_id", found.MatchId, "error", err)
		}

		slog.Info("match resolved",
			"match_id", outcome.MatchId,
			"player_a", outcome.PlayerA,
			"player_b", outcome.PlayerB,
			"outcome", outcome.Outcome,
		)
	}
}

// resolveOutcome generates a random result for the matched pair.
// Distribution: ~9% draw, ~45.5% player A wins, ~45.5% player B wins.
func resolveOutcome(found *matchmakingpb.MatchFound) *gamesessionpb.MatchOutcome {
	var o gamesessionpb.Outcome
	switch rand.IntN(11) {
	case 0:
		o = gamesessionpb.Outcome_OUTCOME_DRAW
	case 1, 2, 3, 4, 5:
		o = gamesessionpb.Outcome_OUTCOME_PLAYER_B_WINS
	default:
		o = gamesessionpb.Outcome_OUTCOME_PLAYER_A_WINS
	}
	return &gamesessionpb.MatchOutcome{
		MatchId:     found.MatchId,
		PlayerA:     found.PlayerA,
		PlayerB:     found.PlayerB,
		Outcome:     o,
		TimestampMs: time.Now().UnixMilli(),
	}
}

// sendToDLT forwards a poison pill to the dead letter topic and commits its offset.
func (s *Session) sendToDLT(ctx context.Context, msg kafka.Message, reason string) {
	if ctx.Err() != nil {
		return
	}
	err := s.dlt.WriteMessages(ctx, kafka.Message{
		Key:   msg.Key,
		Value: msg.Value,
		Headers: []kafka.Header{
			{Key: "error-reason", Value: []byte(reason)},
			{Key: "original-topic", Value: []byte(msg.Topic)},
			{Key: "original-partition", Value: []byte(strconv.Itoa(msg.Partition))},
			{Key: "original-offset", Value: []byte(strconv.FormatInt(msg.Offset, 10))},
		},
	})
	if err != nil {
		slog.Error("failed to write to DLT: message will be reprocessed on restart",
			"error", err,
			"reason", reason,
		)
		return
	}
	if err := s.reader.CommitMessages(ctx, msg); ctx.Err() != nil {
		return
	} else if err != nil {
		slog.Error("failed to commit after DLT write", "error", err)
	}
}
