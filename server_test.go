package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestHealthEndpoint(t *testing.T) {
	srv := httptest.NewServer(newMux(NewBroker()))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatalf("GET /health failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}
}

func TestPublishEndpointValidationAndDelivery(t *testing.T) {
	broker := NewBroker()
	sub, _ := broker.Subscribe("general", "sub-1", "aman", 8)
	defer broker.Unsubscribe("general", "sub-1")

	// Drain join event.
	<-sub.Ch

	srv := httptest.NewServer(newMux(broker))
	defer srv.Close()

	// 1. Invalid JSON should return 400.
	badResp, err := http.Post(srv.URL+"/publish", "application/json", strings.NewReader("{bad-json"))
	if err != nil {
		t.Fatalf("POST /publish bad payload failed: %v", err)
	}
	badResp.Body.Close()
	if badResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid JSON, got %d", badResp.StatusCode)
	}

	// 2. Valid publish should deliver to subscriber on "general".
	payload := `{"topic":"general","sender":"aman","body":"hello via http"}`
	goodResp, err := http.Post(srv.URL+"/publish", "application/json", bytes.NewBufferString(payload))
	if err != nil {
		t.Fatalf("POST /publish valid payload failed: %v", err)
	}
	goodResp.Body.Close()

	if goodResp.StatusCode != http.StatusAccepted && goodResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 202 or 200, got %d", goodResp.StatusCode)
	}

	select {
	case msg := <-sub.Ch:
		if msg.Body != "hello via http" || msg.Sender != "aman" {
			t.Fatalf("unexpected message received: %+v", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for published message")
	}
}

func TestSSEStreamEndpoint(t *testing.T) {
	broker := NewBroker()
	srv := httptest.NewServer(newMux(broker))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/stream?topic=news", nil)
	if err != nil {
		t.Fatalf("create request failed: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /stream failed: %v", err)
	}
	defer resp.Body.Close()

	if !strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("expected text/event-stream, got %q", resp.Header.Get("Content-Type"))
	}

	// Give stream handler a moment to register its subscription.
	time.Sleep(30 * time.Millisecond)

	broker.Publish(Message{
		Kind:   "chat",
		Topic:  "news",
		Sender: "system",
		Body:   "breaking update",
	})

	scanner := bufio.NewScanner(resp.Body)
	foundData := false
	done := make(chan struct{})

	go func() {
		defer close(done)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "data:") && strings.Contains(line, "breaking update") {
				foundData = true
				cancel()
				return
			}
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		cancel()
		<-done
	}

	if !foundData {
		t.Fatal("expected SSE stream to emit published message")
	}
}

func TestWebSocketHistoryAndBroadcast(t *testing.T) {
	broker := NewBroker()
	broker.Publish(Message{
		Kind:   "chat",
		Topic:  "room1",
		Sender: "seed",
		Body:   "earlier message",
	})

	srv := httptest.NewServer(newMux(broker))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws?topic=room1&user=aman"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("websocket dial failed: %v", err)
	}
	defer conn.Close()

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	// Expect history replay first, then join notice.
	var received []Message
	for len(received) < 2 {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read failed: %v", err)
		}
		var m Message
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("unmarshal failed: %v", err)
		}
		received = append(received, m)
	}

	hasHistory := false
	for _, m := range received {
		if m.Body == "earlier message" {
			hasHistory = true
		}
	}
	if !hasHistory {
		t.Fatalf("expected history message 'earlier message' in initial frames, got %+v", received)
	}

	// Send a message over WebSocket and read it back.
	if err := conn.WriteJSON(map[string]string{"body": "live ping"}); err != nil {
		t.Fatalf("write json failed: %v", err)
	}

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read after send failed: %v", err)
		}
		var m Message
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("unmarshal failed: %v", err)
		}
		if m.Body == "live ping" && m.Sender == "aman" {
			break
		}
	}
}