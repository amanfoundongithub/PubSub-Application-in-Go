# Go Real-Time Pub/Sub Chat

A high-concurrency, race-free real-time Pub/Sub chat server built in Go. It supports custom dynamic chat rooms, bidirectional WebSocket communication with chat history replay, unidirectional Server-Sent Events (SSE), HTTP publishing, and presence tracking.

---

## Features

* **In-Memory Pub/Sub Engine (`Broker`)**: Fully thread-safe topic isolation using `sync.RWMutex`, bounded chat history per room, and live room member counts.
* **Race-Free Channel Synchronization**: `Subscriber` lifecycle managed using per-subscriber mutex locks (`Close()` and non-blocking `Send()`) to guarantee zero data races during concurrent publish and disconnect events.
* **Multiple Transport Protocols**:
* **WebSocket (`/ws`)**: Full-duplex messaging with history replay on connect, live member counts, and system notifications.
* **Server-Sent Events (`/stream`)**: Lightweight, unidirectional HTTP streaming for subscribers.
* **HTTP POST (`/publish`)**: REST endpoint to publish messages directly into any room.

* **Dynamic Custom Rooms**: Join any topic on the fly without preset room configurations.
* **Zero External Assets**: Web UI embedded directly into the Go binary using `//go:embed`.
* **Graceful Shutdown**: Intercepts `SIGINT`/`SIGTERM` to allow in-flight connections to drain cleanly.
* **Comprehensive Test Suite**: Unit tests, race detector verification (`go test -race`), and end-to-end HTTP/SSE/WebSocket integration tests.

---

## Project Structure

```text
.
├── handlers.go       # HTTP handlers: /publish and /stream (SSE)
├── index.html        # Embedded web client with custom room switching
├── main.go           # Application entrypoint, routing (mux), and graceful shutdown
├── models.go         # Domain models, constants, payload schemas, and constructors
├── pubsub.go         # In-memory Pub/Sub Broker implementation
├── pubsub_test.go    # Broker unit tests and concurrency race tests
├── server_test.go    # HTTP, SSE, and WebSocket integration tests
├── go.mod
└── go.sum

```

---

## Getting Started

### Prerequisites

* Go 1.22+

### Running the Server

Start the application using:

```bash
go run .

```

By default, the server listens on port `8092`. To configure a custom port, set the `ADDR` environment variable:

```bash
ADDR=":9000" go run .

```

Open `http://localhost:8092` in your browser to launch the web client.

---

## API Endpoints

### 1. Health Check

Checks if the service is running.

* **URL**: `/health`
* **Method**: `GET`
* **Response**: `200 OK`

```json
{
  "status": "ok",
  "service": "pubsub-chat"
}

```

---

### 2. HTTP Publish

Publishes a chat message into a given topic.

* **URL**: `/publish`
* **Method**: `POST`
* **Headers**: `Content-Type: application/json`
* **Body**:

```json
{
  "topic": "tech",
  "sender": "aman",
  "body": "Hello world from curl!"
}

```

* **Response**: `202 Accepted`

```json
{
  "status": "published"
}

```

---

### 3. Server-Sent Events (SSE) Stream

Subscribes to live messages in a given topic over an SSE stream.

* **URL**: `/stream?topic=<room-name>`
* **Method**: `GET`
* **Headers Returned**: `Content-Type: text/event-stream`
* **Stream Format**:

```text
event: message
data: {"kind":"chat","topic":"tech","body":"Hello world from curl!","sender":"aman","sent_at":"2026-11-25T19:50:00Z","member_count":1}

```

---

### 4. WebSocket Connection

Full-duplex real-time chat with message replay and system announcements.

* **URL**: `/ws?topic=<room-name>&user=<username>`
* **Method**: `GET` (Upgraded to WebSocket)
* **Incoming Frame Format (Client -> Server)**:

```json
{
  "body": "Hello room!"
}

```

* **Outgoing Frame Format (Server -> Client)**:

```json
{
  "kind": "chat",
  "topic": "tech",
  "body": "Hello room!",
  "sender": "aman",
  "sent_at": "2026-11-25T19:50:00Z",
  "member_count": 2
}

```

---

## Running Tests

Run the complete test suite with the Go runtime race detector enabled:

```bash
go test -v -race ./...

```

Run static analysis and formatting checks:

```bash
gofmt -l .
go vet ./...

```
