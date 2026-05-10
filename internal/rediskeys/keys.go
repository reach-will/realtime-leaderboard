package rediskeys

const (
	// Write model — source of truth for all player scores.
	ScoresGlobal     = "scores:global"
	MatchesProcessed = "matches:processed"

	// Read model — pre-projected top-10 snapshot, updated after every rating service flush.
	LeaderboardGlobal  = "leaderboard:global"
	LeaderboardUpdated = "leaderboard:updated"
)
