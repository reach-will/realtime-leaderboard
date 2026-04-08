package admin

import (
	"log/slog"
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Serves /metrics and /healthz on addr. It blocks, so call it in a goroutine.
func Serve(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	if err := http.ListenAndServe(addr, mux); err != nil {
		slog.Error("admin server failed", "error", err)
		os.Exit(1)
	}
}
