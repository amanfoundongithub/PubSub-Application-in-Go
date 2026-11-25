package main 

import (
	"fmt"
	"log"
	"net/http"
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

	addr := ":8092"
	log.Printf("Starting server on http://localhost%s\n", addr)

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Server startup failed: %v", err)
	}

}