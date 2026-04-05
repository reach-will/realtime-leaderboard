package ingester

import "github.com/prometheus/client_golang/prometheus"

var (
	messagesProcessedCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "leaderboard",
		Subsystem: "ingester",
		Name:      "messages_processed_total",
		Help:      "Total number of messages successfully fetched and entered processing",
	})
	processingErrorsCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "leaderboard",
		Subsystem: "ingester",
		Name:      "processing_errors_total",
		Help:      "Total number of errors encountered while processing messages",
	})
	redisUpdatesCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "leaderboard",
		Subsystem: "ingester",
		Name:      "redis_updates_total",
		Help:      "Total number of Redis score updates attempted",
	})
	redisErrorsCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "leaderboard",
		Subsystem: "ingester",
		Name:      "redis_errors_total",
		Help:      "Total number of Redis score update errors",
	})
	redisUpdateDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "leaderboard",
		Subsystem: "ingester",
		Name:      "redis_update_duration_seconds",
		Help:      "Duration of successful Redis score updates in seconds",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 14),
	})
	processingDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "leaderboard",
		Subsystem: "ingester",
		Name:      "processing_duration_seconds",
		Help:      "Duration of successful message processing in seconds",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 14),
	})
)

func init() {
	prometheus.MustRegister(
		messagesProcessedCounter,
		processingErrorsCounter,
		redisUpdatesCounter,
		redisErrorsCounter,
		redisUpdateDuration,
		processingDuration,
	)
}
