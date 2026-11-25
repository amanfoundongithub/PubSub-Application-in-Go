package main 

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

type ChatMessage struct {
	Topic  string `json:"topic"`
	Body   string `json:"body"`
	Sender string `json:"sender"`
	SentAt string `json:"sent_at"`
}

type incomingChat struct {
	Body string `json:"body"`
}

func(s* Server) wsHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodGet {
		http.Error(w, "Only GET method is supported on this handler", http.StatusMethodNotAllowed)
		return
	}

	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "Please provide a topic parameter in the URL", http.StatusBadRequest)
		return 
	}

	sender := r.URL.Query().Get("sender")
	if sender == "" {
		sender = "anonymous"
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true,
	})
	if err != nil {
		log.Printf("WebSocket accept error: %v", err)
		return 
	}
	defer conn.CloseNow()

	subID := fmt.Sprintf("ws-%s-%p", sender, conn)
	sub := s.broker.Subscribe(topic, subID, 32)
	past := s.broker.History(topic)
	defer s.broker.Unsubscribe(topic, subID)

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)

		sendOne := func(m Message) bool {
			writeCtx, writeCancel := context.WithTimeout(ctx, 5*time.Second)
			err := wsjson.Write(writeCtx, conn, m)
			writeCancel()
			if err != nil {
				cancel()
				return false
			}
			return true
		}
		
		for _, histMsg := range past {
			if !sendOne(histMsg) {
				return
			}
		}

		for {
			select {
			case <- ctx.Done():
				return
			case msg, open := <- sub.Ch:
				if !open {
					return
				}
				if !sendOne(msg) {
					return
				}
			}
		}

	}()

	for {
		var in incomingChat
		err := wsjson.Read(ctx, conn, &in)
		if err != nil {
			cancel()
			return
		}
		if in.Body == "" {
			continue
		}
		s.broker.Publish(Message{
			Topic: topic,
			Sender: sender,
			Body: fmt.Sprintf("%s: %s", sender, in.Body),
		})
	}
	<- writeDone
	_ = conn.Close(websocket.StatusNormalClosure, "bye")

}
