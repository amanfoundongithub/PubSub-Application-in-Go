package main

import (
	"sync"
	"time"
)

const defaultHistoryLimit = 20

type Message struct {
	Kind   string `json:"kind"`
	Topic  string `json:"topic"`
	Body   string `json:"body"`
	Sender string `json:"sender"`
	SentAt string `json:"sent_at"`
	MemberCount int `json:"member_count,omitempty"`
}

type Subscriber struct {
	ID string
	User string
	Ch chan Message
}

type Broker struct {
	mu           sync.RWMutex
	topics       map[string]map[string]*Subscriber
	members      map[string]map[string]string
	history      map[string][]Message
	historyLimit int
}

// Function to make new broker
func NewBroker() *Broker {
	return &Broker{
		topics:       make(map[string]map[string]*Subscriber),
		history:      make(map[string][]Message),
		historyLimit: defaultHistoryLimit,
	}
}

// Subscribe to broker
func (b *Broker) Subscribe(topic, id string, buffSize int) *Subscriber {

	// Create a subscriber
	sub := &Subscriber{
		ID: id,
		Ch: make(chan Message, buffSize),
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.topics[topic] == nil {
		b.topics[topic] = make(map[string]*Subscriber)
	}
	b.topics[topic][id] = sub
	return sub

}

// History
func (b *Broker) History(topic string) []Message {
	b.mu.RLock()
	defer b.mu.RUnlock()

	src := b.history[topic]
	out := make([]Message, len(src))
	copy(out, src)
	return out
}

// Unsubscribe to broker
func (b *Broker) Unsubscribe(topic, id string) {

	b.mu.Lock()
	defer b.mu.Unlock()

	subs, ok := b.topics[topic]
	if !ok {
		return
	}

	if sub, exists := subs[id]; exists {
		delete(subs, id)
		close(sub.Ch)
	}

	if len(subs) == 0 {
		delete(b.topics, topic)
	}

}

// Publish a message to the broker
func (b *Broker) Publish(msg Message) {

	if msg.SentAt == "" {
		msg.SentAt = time.Now().UTC().Format(time.RFC3339)
	}

	if msg.Sender == "" {
		msg.Sender = "system"
	}

	b.mu.Lock()
	hist := append(b.history[msg.Topic], msg)
	if len(hist) > b.historyLimit {
		hist = hist[len(hist)-b.historyLimit:]
	}
	b.history[msg.Topic] = hist

	subsMap := b.topics[msg.Topic]
	targets := make([]*Subscriber, 0, len(subsMap))
	for _, sub := range subsMap {
		targets = append(targets, sub)
	}
	b.mu.Unlock()

	for _, sub := range targets {
		select {
		case sub.Ch <- msg:
		default:
		}
	}

}
