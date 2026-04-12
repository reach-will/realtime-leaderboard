package ingester

import "github.com/redis/go-redis/v9"

// updateScoresScript atomically increments both players' scores in a single Redis operation.
//
// Redis executes Lua scripts atomically — no other command can interleave between the two
// ZINCRBY calls, eliminating the partial-update risk that exists with two separate calls
// (i.e. player A's score being updated while player B's is not, in the event of a crash).
//
// KEYS[1]  — sorted set key (e.g. leaderboard:global)
// ARGV[1]  — player A delta (float, as string)
// ARGV[2]  — player A ID
// ARGV[3]  — player B delta (float, as string)
// ARGV[4]  — player B ID
//
// Returns a two-element array: [newScoreA, newScoreB] as bulk strings.
var updateScoresScript = redis.NewScript(`
local scoreA = redis.call('ZINCRBY', KEYS[1], ARGV[1], ARGV[2])
local scoreB = redis.call('ZINCRBY', KEYS[1], ARGV[3], ARGV[4])
return {scoreA, scoreB}
`)
