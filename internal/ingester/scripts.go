package ingester

import "github.com/redis/go-redis/v9"

// updateScoresScript atomically checks for duplicate processing, records the match ID,
// and increments both players' scores in a single Redis operation.
//
// Redis executes Lua scripts atomically — no other command can interleave, which means
// the duplicate check and the score updates form a single indivisible operation. This
// guarantees that a match replayed after a consumer crash (Kafka at-least-once delivery)
// is a safe no-op rather than a double-count.
//
// KEYS[1]  — sorted set key (e.g. scores:global)
// KEYS[2]  — processed match IDs set key (e.g. ingester:processed_matches)
// ARGV[1]  — player A delta (float, as string)
// ARGV[2]  — player A ID
// ARGV[3]  — player B delta (float, as string)
// ARGV[4]  — player B ID
// ARGV[5]  — match ID
// ARGV[6]  — TTL for the processed-matches set in seconds
//
// Returns 1 if both scores were updated, or 0 if the match was already processed (duplicate).
var updateScoresScript = redis.NewScript(`
if redis.call('SISMEMBER', KEYS[2], ARGV[5]) == 1 then
  return 0
end
redis.call('SADD', KEYS[2], ARGV[5])
redis.call('EXPIRE', KEYS[2], ARGV[6], 'NX')
redis.call('ZINCRBY', KEYS[1], ARGV[1], ARGV[2])
redis.call('ZINCRBY', KEYS[1], ARGV[3], ARGV[4])
return 1
`)
