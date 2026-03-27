package adminhttp

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Start serves /metrics and /healthz on addr. It blocks, so call it in a goroutine.
func Start(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal("metrics server:", err)
	}
}
