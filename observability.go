package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

type ctxKey string

const reqIDKey ctxKey = "req_id"

func newRequestID() string {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

func requestIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(reqIDKey).(string); ok && v != "" {
		return v
	}
	return "-"
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (s *Server) withObservability(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		reqID := r.Header.Get("X-Request-ID")
		if reqID == "" {
			reqID = newRequestID()
		}
		w.Header().Set("X-Request-ID", reqID)

		ctx := context.WithValue(r.Context(), reqIDKey, reqID)
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(rec, r.WithContext(ctx))

		if r.URL.Path != "/health" && r.URL.Path != "/metrics" {
			log.Printf("req_id=%s method=%s path=%s status=%d duration=%s",
				reqID, r.Method, r.URL.Path, rec.status, time.Since(start).Round(time.Microsecond))
		}
	})
}

func (s *Server) statsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.metrics.RecordRequest(http.StatusMethodNotAllowed)
		http.Error(w, "Only GET method is supported on this handler", http.StatusMethodNotAllowed)
		return
	}

	s.metrics.RecordRequest(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.broker.Stats())
}

func (s *Server) metricsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.metrics.RecordRequest(http.StatusMethodNotAllowed)
		http.Error(w, "Only GET method is supported on this handler", http.StatusMethodNotAllowed)
		return
	}

	s.metrics.RecordRequest(http.StatusOK)
	stats := s.broker.Stats()
	httpReqs, httpErrs := s.metrics.Snapshot()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	fmt.Fprintf(w, "# HELP pubsub_active_rooms Current active chat rooms\n# TYPE pubsub_active_rooms gauge\npubsub_active_rooms %d\n", stats.ActiveRooms)
	fmt.Fprintf(w, "# HELP pubsub_total_subscribers Current active subscribers\n# TYPE pubsub_total_subscribers gauge\npubsub_total_subscribers %d\n", stats.TotalSubscribers)
	fmt.Fprintf(w, "# HELP pubsub_published_messages_total Total messages published\n# TYPE pubsub_published_messages_total counter\npubsub_published_messages_total %d\n", stats.PublishedMessages)
	fmt.Fprintf(w, "# HELP pubsub_dropped_messages_total Total messages dropped due to full subscriber buffers\n# TYPE pubsub_dropped_messages_total counter\npubsub_dropped_messages_total %d\n", stats.DroppedMessages)
	fmt.Fprintf(w, "# HELP pubsub_http_requests_total Total HTTP requests recorded\n# TYPE pubsub_http_requests_total counter\npubsub_http_requests_total %d\n", httpReqs)
	fmt.Fprintf(w, "# HELP pubsub_http_errors_total Total HTTP responses with status >= 400\n# TYPE pubsub_http_errors_total counter\npubsub_http_errors_total %d\n", httpErrs)
}