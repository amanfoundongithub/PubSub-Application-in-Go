package main

import (
	"fmt"
	"sync"
	"time"
)

const defaultHistoryLimit = 20

type Message struct {
	Kind        string `json:"kind"`
	Topic       string `json:"topic"`
	Body        string `json:"body"`
	Sender      string `json:"sender"`
	SentAt      string `json:"sent_at"`
	MemberCount int    `json:"member_count,omitempty"`
}

type Subscriber struct {
	ID     string
	User   string
	Ch     chan Message
	mu     sync.Mutex
	closed bool
}

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

func (b *Broker) Subscribe(topic, id, user string, buffSize int) (*Subscriber, int) {
	sub := &Subscriber{
		ID:   id,
		User: user,
		Ch:   make(chan Message, buffSize),
	}

	b.mu.Lock()
	if b.topics == nil {
		b.topics = make(map[string]map[string]*Subscriber)
	}
	if b.topics[topic] == nil {
		b.topics[topic] = make(map[string]*Subscriber)
	}
	b.topics[topic][id] = sub
	if b.members == nil {
		b.members = make(map[string]map[string]string)
	}
	if b.members[topic] == nil {
		b.members[topic] = make(map[string]string)
	}
	b.members[topic][id] = user
	count := len(b.members[topic])
	b.mu.Unlock()

	b.Publish(Message{
		Kind:        "system",
		Topic:       topic,
		Sender:      "system",
		Body:        fmt.Sprintf("%s joined #%s", user, topic),
		MemberCount: count,
	})

	return sub, count
}

func (b *Broker) History(topic string) []Message {
	b.mu.RLock()
	defer b.mu.RUnlock()

	src := b.history[topic]
	out := make([]Message, len(src))
	copy(out, src)
	return out
}

func (b *Broker) MemberCount(topic string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.members[topic])
}

func (b *Broker) Unsubscribe(topic, id string) {
	b.mu.Lock()
	subs, ok := b.topics[topic]
	if !ok {
		b.mu.Unlock()
		return
	}
	sub, exists := subs[id]
	if !exists {
		b.mu.Unlock()
		return
	}
	delete(subs, id)
	sub.Close()
	if len(subs) == 0 {
		delete(b.topics, topic)
	}
	delete(b.members[topic], id)
	count := len(b.members[topic])
	if count == 0 {
		delete(b.members, topic)
	}
	b.mu.Unlock()
	if count > 0 {
		b.Publish(Message{
			Kind:        "system",
			Topic:       topic,
			Sender:      "system",
			Body:        fmt.Sprintf("%s left #%s", sub.User, topic),
			MemberCount: count,
		})
	}
}

func (b *Broker) Publish(msg Message) {
	if msg.Kind == "" {
		msg.Kind = "chat"
	}
	if msg.SentAt == "" {
		msg.SentAt = time.Now().UTC().Format(time.RFC3339)
	}
	if msg.Sender == "" {
		msg.Sender = "system"
	}

	b.mu.Lock()
	if msg.MemberCount == 0 {
		msg.MemberCount = len(b.members[msg.Topic])
	}
	if msg.Kind == "chat" {
		hist := append(b.history[msg.Topic], msg)
		if len(hist) > b.historyLimit {
			hist = hist[len(hist)-b.historyLimit:]
		}
		b.history[msg.Topic] = hist
	}
	subsMap := b.topics[msg.Topic]
	targets := make([]*Subscriber, 0, len(subsMap))
	for _, sub := range subsMap {
		targets = append(targets, sub)
	}
	b.mu.Unlock()

	for _, sub := range targets {
		sub.Send(msg)
	}
}