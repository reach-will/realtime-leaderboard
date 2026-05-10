package rating

import "github.com/prometheus/client_golang/prometheus"

var (
	// Message-level metrics.
	messagesReceivedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "leaderboard",
		Subsystem: "rating",
		Name:      "messages_received_total",
		Help:      "Total number of messages successfully received from Kafka (includes messages later routed to the DLT).",
	})
	messagesErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "leaderboard",
		Subsystem: "rating",
		Name:      "messages_errors_total",
		Help:      "Total number of message-level failures: invalid payloads (poison pills) and messages lost to Redis pipeline failures.",
	})

	// Batch-level metrics.
	batchesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "leaderboard",
		Subsystem: "rating",
		Name:      "batches_total",
		Help:      "Total number of batch flushes attempted.",
	})
	batchesErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "leaderboard",
		Subsystem: "rating",
		Name:      "batches_errors_total",
		Help:      "Total number of batch flushes that failed (pipeline or commit error).",
	})
	batchSizeHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "leaderboard",
		Subsystem: "rating",
		Name:      "batch_size",
		Help:      "Number of messages per batch flush.",
		Buckets:   []float64{1, 5, 10, 25, 50, 75, 100},
	})
	batchCollectDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "leaderboard",
		Subsystem: "rating",
		Name:      "batch_collect_duration_seconds",
		Help:      "Time spent collecting a batch from Kafka (FetchMessage loop + proto unmarshal).",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 14),
	})
	batchFlushDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "leaderboard",
		Subsystem: "rating",
		Name:      "batch_flush_duration_seconds",
		Help:      "Time spent flushing a batch to Redis and committing offsets.",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 14),
	})

	// Instance-level metrics.
	activeInstances = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "leaderboard",
		Subsystem: "rating",
		Name:      "active_instances",
		Help:      "Number of rating service instances currently processing messages. Increments when Run starts, decrements after graceful shutdown completes.",
	})

	// Duplicate-level metrics.
	messagesDuplicatesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "leaderboard",
		Subsystem: "rating",
		Name:      "messages_duplicates_total",
		Help:      "Total number of match events skipped because the match ID was already processed (idempotency deduplication).",
	})

	// Redis-level metrics.
	redisRequestsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "leaderboard",
		Subsystem: "rating",
		Name:      "redis_requests_total",
		Help:      "Total number of Redis pipeline requests attempted.",
	})
	redisErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "leaderboard",
		Subsystem: "rating",
		Name:      "redis_errors_total",
		Help:      "Total number of Redis pipeline requests that failed.",
	})
	redisRequestDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "leaderboard",
		Subsystem: "rating",
		Name:      "redis_request_duration_seconds",
		Help:      "Duration of successful Redis pipeline round-trips in seconds.",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 14),
	})
)

func init() {
	prometheus.MustRegister(
		activeInstances,
		messagesReceivedTotal,
		messagesErrorsTotal,
		messagesDuplicatesTotal,
		batchesTotal,
		batchesErrorsTotal,
		batchSizeHistogram,
		batchCollectDuration,
		batchFlushDuration,
		redisRequestsTotal,
		redisErrorsTotal,
		redisRequestDuration,
	)
}
