package ingester

import "github.com/prometheus/client_golang/prometheus"

var (
	// Message-level metrics.
	messagesReceivedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "leaderboard",
		Subsystem: "ingester",
		Name:      "messages_received_total",
		Help:      "Total number of messages successfully received from Kafka (includes messages later routed to the DLT).",
	})
	messagesErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "leaderboard",
		Subsystem: "ingester",
		Name:      "messages_errors_total",
		Help:      "Total number of message-level failures: invalid payloads (poison pills) and messages lost to Redis pipeline failures.",
	})

	// Batch-level metrics.
	batchesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "leaderboard",
		Subsystem: "ingester",
		Name:      "batches_total",
		Help:      "Total number of batch flushes attempted.",
	})
	batchesErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "leaderboard",
		Subsystem: "ingester",
		Name:      "batches_errors_total",
		Help:      "Total number of batch flushes that failed (pipeline or commit error).",
	})
	batchSizeHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "leaderboard",
		Subsystem: "ingester",
		Name:      "batch_size",
		Help:      "Number of messages per batch flush.",
		Buckets:   []float64{1, 5, 10, 25, 50, 75, 100},
	})
	batchProcessingDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "leaderboard",
		Subsystem: "ingester",
		Name:      "batch_processing_duration_seconds",
		Help:      "Duration of successful batch flushes in seconds (collect + pipeline + commit).",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 14),
	})

	// Redis-level metrics.
	redisRequestsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "leaderboard",
		Subsystem: "ingester",
		Name:      "redis_requests_total",
		Help:      "Total number of Redis pipeline requests attempted.",
	})
	redisErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "leaderboard",
		Subsystem: "ingester",
		Name:      "redis_errors_total",
		Help:      "Total number of Redis pipeline requests that failed.",
	})
	redisRequestDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "leaderboard",
		Subsystem: "ingester",
		Name:      "redis_request_duration_seconds",
		Help:      "Duration of successful Redis pipeline round-trips in seconds.",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 14),
	})
)

func init() {
	prometheus.MustRegister(
		messagesReceivedTotal,
		messagesErrorsTotal,
		batchesTotal,
		batchesErrorsTotal,
		batchSizeHistogram,
		batchProcessingDuration,
		redisRequestsTotal,
		redisErrorsTotal,
		redisRequestDuration,
	)
}
