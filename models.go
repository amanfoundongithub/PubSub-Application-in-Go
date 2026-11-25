package main

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type MessageKind string

const (
	KindChat   MessageKind = "chat"
	KindSystem MessageKind = "system"
)

const (
	defaultHistoryLimit = 20
	maxMessageBodyLen   = 2000
)

type Message struct {
	Kind        MessageKind `json:"kind"`
	Topic       string      `json:"topic"`
	Body        string      `json:"body"`
	Sender      string      `json:"sender"`
	SentAt      string      `json:"sent_at"`
	MemberCount int         `json:"member_count,omitempty"`
}

type Subscriber struct {
	ID     string
	User   string
	Ch     chan Message
	mu     sync.Mutex
	closed bool
}

func (s *Subscriber) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.Ch)
	}
}

func (s *Subscriber) Send(msg Message) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	select {
	case s.Ch <- msg:
		return true
	default:
		return false
	}
}

type PublishRequest struct {
	Topic  string `json:"topic"`
	Sender string `json:"sender"`
	Body   string `json:"body"`
}

type WSIncomingMessage struct {
	Body string `json:"body"`
}

func NewChatMessage(topic, sender, body string) Message {
	return Message{
		Kind:   KindChat,
		Topic:  topic,
		Sender: sender,
		Body:   body,
		SentAt: time.Now().UTC().Format(time.RFC3339),
	}
}

func NewSystemMessage(topic, body string, memberCount int) Message {
	return Message{
		Kind:        KindSystem,
		Topic:       topic,
		Sender:      string(KindSystem),
		Body:        body,
		SentAt:      time.Now().UTC().Format(time.RFC3339),
		MemberCount: memberCount,
	}
}

type Metrics struct {
	httpRequestsTotal atomic.Uint64
	httpErrorsTotal   atomic.Uint64
}

func (m *Metrics) RecordRequest(statusCode int) {
	m.httpRequestsTotal.Add(1)
	if statusCode >= 400 {
		m.httpErrorsTotal.Add(1)
	}
}

func (m *Metrics) Snapshot() (requests uint64, errors uint64) {
	return m.httpRequestsTotal.Load(), m.httpErrorsTotal.Load()
}

type Broker struct {
	mu             sync.RWMutex
	topics         map[string]map[string]*Subscriber
	members        map[string]map[string]string
	history        map[string][]Message
	historyLimit   int
	publishedTotal atomic.Uint64
	droppedTotal   atomic.Uint64
}

func NewBroker() *Broker {
	return &Broker{
		topics:       make(map[string]map[string]*Subscriber),
		members:      make(map[string]map[string]string),
		history:      make(map[string][]Message),
		historyLimit: defaultHistoryLimit,
	}
}

type RoomStats struct {
	Topic        string `json:"topic"`
	Members      int    `json:"members"`
	HistoryCount int    `json:"history_count"`
}

type BrokerStats struct {
	ActiveRooms       int         `json:"active_rooms"`
	TotalSubscribers  int         `json:"total_subscribers"`
	PublishedMessages uint64      `json:"published_messages"`
	DroppedMessages   uint64      `json:"dropped_messages"`
	Rooms             []RoomStats `json:"rooms"`
}

func (b *Broker) Stats() BrokerStats {
	b.mu.RLock()
	defer b.mu.RUnlock()

	topicSet := make(map[string]struct{})
	for t := range b.topics {
		topicSet[t] = struct{}{}
	}
	for t := range b.members {
		topicSet[t] = struct{}{}
	}
	for t := range b.history {
		topicSet[t] = struct{}{}
	}

	rooms := make([]RoomStats, 0, len(topicSet))
	totalSubs := 0

	for t := range topicSet {
		subCount := len(b.topics[t])
		memCount := len(b.members[t])
		histCount := len(b.history[t])
		if subCount == 0 && memCount == 0 && histCount == 0 {
			continue
		}
		totalSubs += subCount
		rooms = append(rooms, RoomStats{
			Topic:        t,
			Members:      memCount,
			HistoryCount: histCount,
		})
	}

	sort.Slice(rooms, func(i, j int) bool {
		return rooms[i].Topic < rooms[j].Topic
	})

	return BrokerStats{
		ActiveRooms:       len(rooms),
		TotalSubscribers:  totalSubs,
		PublishedMessages: b.publishedTotal.Load(),
		DroppedMessages:   b.droppedTotal.Load(),
		Rooms:             rooms,
	}
}

type Server struct {
	broker  *Broker
	metrics Metrics
}

func NewServer(broker *Broker) *Server {
	return &Server{
		broker: broker,
	}
}
