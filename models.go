package main

import (
	"sync"
	"time"
)

type MessageKind string

const (
	KindChat   MessageKind = "chat"
	KindSystem MessageKind = "system"
)

const maxMessageBodyLen = 2000

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

func (s *Subscriber) Send(msg Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	select {
	case s.Ch <- msg:
	default:
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

const defaultHistoryLimit = 20

type Broker struct {
	mu           sync.RWMutex
	topics       map[string]map[string]*Subscriber
	members      map[string]map[string]string
	history      map[string][]Message
	historyLimit int
}

func NewBroker() *Broker {
	return &Broker{
		topics:       make(map[string]map[string]*Subscriber),
		members:      make(map[string]map[string]string),
		history:      make(map[string][]Message),
		historyLimit: defaultHistoryLimit,
	}
}

type Server struct {
	broker *Broker
}

func NewServer(broker *Broker) *Server {
	return &Server{
		broker: broker,
	}
}
