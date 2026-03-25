package events

import "time"

type Outcome string

const (
	OutcomePlayerAWins Outcome = "PLAYER_A_WINS"
	OutcomePlayerBWins Outcome = "PLAYER_B_WINS"
	OutcomeDraw        Outcome = "DRAW"
)

type MatchOutcome struct {
	MatchID   string  `json:"matchId"`
	PlayerA   string  `json:"playerA"`
	PlayerB   string  `json:"playerB"`
	Outcome   Outcome `json:"outcome"`
	Timestamp int64   `json:"timestamp"`
}

func NewMatchOutcome(matchID, playerA, playerB string, outcome Outcome) MatchOutcome {
	return MatchOutcome{
		MatchID:   matchID,
		PlayerA:   playerA,
		PlayerB:   playerB,
		Outcome:   outcome,
		Timestamp: time.Now().UnixMilli(),
	}
}
