package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
	"os/signal"
	"syscall"
	"errors"
	"context"
)

// Health controller
func healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "For health controller, please use GET method only.", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, `{"status" : "ok", "service" : "pubsub-chat"}`)
}

func main() {

	broker := NewBroker()
	srv := NewServer(broker)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/publish", srv.publishHandler)
	mux.HandleFunc("/stream", srv.streamHandler)
	mux.HandleFunc("/ws", srv.wsHandler)
	mux.HandleFunc("/", srv.indexHandler)

	httpServer := &http.Server{
		Addr: ":8092",
		Handler: mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("Starting server on http://localhost%s\n", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Server startup failed: %v", err)
		}
	}()

	sig := <- stop
	log.Printf("Received signal %s, initiating graceful shutdown...", sig)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("graceful shutdown failed: %v", err)
	}

	log.Println("Server stopped cleanly.")

}