package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func (s *Server) wsHandler(w http.ResponseWriter, r *http.Request) {
	reqID := requestIDFromContext(r.Context())

	if r.Method != http.MethodGet {
		s.metrics.RecordRequest(http.StatusMethodNotAllowed)
		http.Error(w, "Only GET method is supported on this handler", http.StatusMethodNotAllowed)
		return
	}

	topic := strings.TrimSpace(r.URL.Query().Get("topic"))
	if topic == "" {
		s.metrics.RecordRequest(http.StatusBadRequest)
		http.Error(w, "Please provide a topic parameter in the URL", http.StatusBadRequest)
		return
	}

	sender := strings.TrimSpace(r.URL.Query().Get("user"))
	if sender == "" {
		sender = "anonymous"
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true,
	})
	if err != nil {
		s.metrics.RecordRequest(http.StatusBadRequest)
		log.Printf("req_id=%s websocket accept error: %v", reqID, err)
		return
	}
	defer conn.CloseNow()

	s.metrics.RecordRequest(http.StatusSwitchingProtocols)

	subID := fmt.Sprintf("ws-%s-%p", sender, conn)
	sub, _ := s.broker.Subscribe(topic, subID, sender, 32)
	past := s.broker.History(topic)
	defer s.broker.Unsubscribe(topic, subID)

	log.Printf("req_id=%s ws connected topic=%s user=%s", reqID, topic, sender)

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
			case <-ctx.Done():
				return
			case msg, open := <-sub.Ch:
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
		var in WSIncomingMessage
		err := wsjson.Read(ctx, conn, &in)
		if err != nil {
			cancel()
			<-writeDone
			log.Printf("req_id=%s ws disconnected topic=%s user=%s", reqID, topic, sender)
			return
		}

		body := strings.TrimSpace(in.Body)
		if body == "" || len(body) > maxMessageBodyLen {
			continue
		}

		s.broker.Publish(NewChatMessage(topic, sender, body))
	}
}
