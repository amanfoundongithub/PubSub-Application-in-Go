package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

func (s *Server) publishHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Publishing endpoint accepts only POST, please use that!", http.StatusMethodNotAllowed)
		return
	}

	var req PublishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON payload received, please ensure it is give properly!", http.StatusBadRequest)
		return
	}

	topic := strings.TrimSpace(req.Topic)
	if topic == "" {
		http.Error(w, "Topic name is required", http.StatusBadRequest)
		return
	}

	body := strings.TrimSpace(req.Body)
	if body == "" {
		http.Error(w, "Message body is required", http.StatusBadRequest)
		return
	}
	if len(body) > maxMessageBodyLen {
		http.Error(w, "Message body exceeds maximum allowed length", http.StatusBadRequest)
		return
	}

	sender := strings.TrimSpace(req.Sender)
	s.broker.Publish(NewChatMessage(topic, sender, body))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintln(w, `{"status" : "published"}`)
}

func (s *Server) streamHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Streaming handler accepts only GET method", http.StatusMethodNotAllowed)
		return
	}

	topic := strings.TrimSpace(r.URL.Query().Get("topic"))
	if topic == "" {
		http.Error(w, "Topic name is required. Add it to URL param `topic`.", http.StatusBadRequest)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming is not supported", http.StatusInternalServerError)
		return
	}

	subID := fmt.Sprintf("%s-%p", r.RemoteAddr, r)
	sub, _ := s.broker.Subscribe(topic, subID, "sse-client", 16)
	defer s.broker.Unsubscribe(topic, subID)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case msg, open := <-sub.Ch:
			if !open {
				return
			}
			payload, err := json.Marshal(msg)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "event: message\ndata: %s\n\n", payload)
			flusher.Flush()
		}
	}
}
