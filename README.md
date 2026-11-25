# Go Real-Time Pub/Sub Chat

A high-concurrency, race-free real-time Pub/Sub chat server built in Go. It supports dynamic custom rooms, bidirectional WebSockets with chat history replay, unidirectional Server-Sent Events (SSE), HTTP publishing, presence tracking, request correlation IDs, and live observability endpoints (`/stats` and `/metrics`).

---

## Features

* **In-Memory Pub/Sub Engine (`Broker`)**: Thread-safe topic isolation using `sync.RWMutex`, bounded per-room chat history, live room member counts, and atomic publish/drop counters.
* **Race-Free Channel Synchronization**: `Subscriber` lifecycle managed via per-subscriber mutex locks (`Close()` and non-blocking `Send()`) to prevent data races during concurrent publishes and disconnects.
* **Multiple Transport Protocols**:
* **WebSocket (`/ws`)**: Full-duplex messaging with history replay on connect, live member counts, and system join/leave events.
* **Server-Sent Events (`/stream`)**: Lightweight unidirectional HTTP event stream for room subscribers.
* **HTTP POST (`/publish`)**: REST endpoint to publish messages into any topic with input trimming and payload length validation.

* **Observability & Diagnostics**:
* **Request Correlation (`X-Request-ID`)**: Propagates incoming `X-Request-ID` headers or generates a random hex ID per request, attaching it to response headers, context, and structured request/WebSocket logs.
* **Broker & Room Stats (`GET /stats`)**: JSON snapshot of active rooms, total subscribers, published/dropped message counters, and per-room member and history counts.
* **Prometheus Metrics (`GET /metrics`)**: Plain-text gauge and counter exposition (`text/plain; version=0.0.4`) for rooms, subscribers, published/dropped messages, HTTP requests, and HTTP errors (`status >= 400`).

* **Dynamic Custom Rooms**: Join any topic on the fly without preset room definitions.
* **Embedded Web UI**: Bootstrap 5 chat client embedded directly into the Go binary via `//go:embed`.
* **Graceful Shutdown**: Traps `SIGINT` and `SIGTERM` and drains active HTTP connections within a shutdown timeout.
* **Test Suite**: Unit tests, `-race` concurrency tests, and end-to-end HTTP, SSE, WebSocket, `/stats`, `/metrics`, and request ID integration tests.

---

## Project Structure

```text
.
├── handlers.go       # HTTP handlers: /publish and /stream (SSE)
├── index.html        # Embedded Bootstrap 5 web client with room switching
├── main.go           # Entrypoint, routing, server timeouts, and graceful shutdown
├── models.go         # Domain models, constants, Metrics, Broker.Stats(), constructors
├── observability.go  # Request ID middleware, logging, Hijack/Flush recorder, /stats, /metrics
├── pubsub.go         # In-memory Pub/Sub Broker implementation
├── pubsub_test.go    # Broker unit tests and concurrency race tests
├── server_test.go    # HTTP, SSE, WebSocket, and observability integration tests
├── go.mod
└── go.sum

```

---

## Getting Started

### Prerequisites

* Go 1.22+

### Running the Server

```bash
go run .

```

The server listens on `:8092` by default. Set `ADDR` to bind a different address or port:

```bash
ADDR=":9000" go run .

```

Open `http://localhost:8092` in your browser to use the chat UI.

---

## API & Observability Endpoints

### 1. Health Check

* **URL**: `GET /health`
* **Response**: `200 OK`

```json
{
  "status": "ok",
  "service": "pubsub-chat"
}

```

### 2. HTTP Publish

* **URL**: `POST /publish`
* **Headers**: `Content-Type: application/json` (optional `X-Request-ID: <id>`)
* **Request Body**:

```json
{
  "topic": "tech",
  "sender": "aman",
  "body": "Hello world from curl!"
}

```

* **Response**: `202 Accepted` (with `X-Request-ID` response header)

```json
{
  "status": "published"
}

```

### 3. Server-Sent Events (SSE) Stream

* **URL**: `GET /stream?topic=<room-name>`
* **Response Headers**: `Content-Type: text/event-stream`, `X-Request-ID: <id>`
* **Stream Frame**:

```text
event: message
data: {"kind":"chat","topic":"tech","body":"Hello world from curl!","sender":"aman","sent_at":"2026-11-26T01:48:00Z","member_count":1}

```

### 4. WebSocket Connection

* **URL**: `GET /ws?topic=<room-name>&user=<username>`
* **Client -> Server Frame**:

```json
{
  "body": "Hello room!"
}

```

* **Server -> Client Frame**:

```json
{
  "kind": "chat",
  "topic": "tech",
  "body": "Hello room!",
  "sender": "aman",
  "sent_at": "2026-11-26T01:48:00Z",
  "member_count": 2
}

```

### 5. Broker & Room Stats

* **URL**: `GET /stats`
* **Response**: `200 OK` (`application/json`)

```json
{
  "active_rooms": 1,
  "total_subscribers": 2,
  "published_messages": 14,
  "dropped_messages": 0,
  "rooms": [
    {
      "topic": "general",
      "members": 2,
      "history_count": 10
    }
  ]
}

```

### 6. Prometheus Metrics

* **URL**: `GET /metrics`
* **Response**: `200 OK` (`text/plain; version=0.0.4; charset=utf-8`)

```text
# HELP pubsub_active_rooms Current active chat rooms
# TYPE pubsub_active_rooms gauge
pubsub_active_rooms 1
# HELP pubsub_total_subscribers Current active subscribers
# TYPE pubsub_total_subscribers gauge
pubsub_total_subscribers 2
# HELP pubsub_published_messages_total Total messages published
# TYPE pubsub_published_messages_total counter
pubsub_published_messages_total 14
# HELP pubsub_dropped_messages_total Total messages dropped due to full subscriber buffers
# TYPE pubsub_dropped_messages_total counter
pubsub_dropped_messages_total 0
# HELP pubsub_http_requests_total Total HTTP requests recorded
# TYPE pubsub_http_requests_total counter
pubsub_http_requests_total 25
# HELP pubsub_http_errors_total Total HTTP responses with status >= 400
# TYPE pubsub_http_errors_total counter
pubsub_http_errors_total 1

```

---

## Testing & Verification

Format, vet, and run all tests with the race detector enabled:

```bash
gofmt -w .
go vet ./...
go test -v -race ./...

```

Build a standalone binary to verify compilation with embedded assets:

```bash
go build -o pubsub-chat .

```
