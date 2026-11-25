package main 

import(
	"encoding/json"
	"fmt"
	"net/http"
)

type Server struct {
	broker *Broker
}

func NewServer(broker *Broker) *Server {
	return &Server{
		broker: broker,
	}
}

func(s* Server) publishHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		http.Error(w, "Publishing endpoint accepts only POST, please use that!", http.StatusMethodNotAllowed)
		return
	}

	var msg Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "Invalid JSON payload received, please ensure it is give properly!", http.StatusBadRequest)
		return
	}

	if msg.Topic == "" {
		http.Error(w, "Topic name is required", http.StatusBadRequest)
		return 
	}

	if msg.Body == "" {
		http.Error(w, "Message body is required", http.StatusBadRequest)
		return
	}

	s.broker.Publish(msg)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintln(w, `{"status" : "published"}`)

}