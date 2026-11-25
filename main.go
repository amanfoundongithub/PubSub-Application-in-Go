package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "For health controller, please use GET method only.", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, `{"status" : "ok", "service" : "pubsub-chat"}`)
}

func newMux(b *Broker) *http.ServeMux {
	s := &Server{broker: b}
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.indexHandler)
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/publish", s.publishHandler)
	mux.HandleFunc("/stream", s.streamHandler)
	mux.HandleFunc("/ws", s.wsHandler)
	return mux
}

func main() {
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8092"
	}

	broker := NewBroker()
	mux := newMux(broker)

	httpServer := &http.Server{
		Addr:        addr,
		Handler:     mux,
		ReadTimeout: 10 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("Starting server on http://localhost%s\n", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Server startup failed: %v", err)
		}
	}()

	sig := <-stop
	log.Printf("Received signal %s, initiating graceful shutdown...", sig)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("graceful shutdown failed: %v", err)
	}

	log.Println("Server stopped cleanly.")
}
