package main

import (
	"fmt"
	"time"
)

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

	msg := NewSystemMessage(topic, fmt.Sprintf("%s joined #%s", user, topic), count)
	b.Publish(msg)

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
		msg := NewSystemMessage(topic, fmt.Sprintf("%s left #%s", sub.User, topic), count)
		b.Publish(msg)
	}
}

func (b *Broker) Publish(msg Message) {
	if msg.Kind == "" {
		msg.Kind = KindChat
	}
	if msg.SentAt == "" {
		msg.SentAt = time.Now().UTC().Format(time.RFC3339)
	}
	if msg.Sender == "" {
		msg.Sender = string(KindSystem)
	}

	b.mu.Lock()
	if msg.MemberCount == 0 {
		msg.MemberCount = len(b.members[msg.Topic])
	}
	if msg.Kind == KindChat {
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

	b.publishedTotal.Add(1)

	var dropped uint64
	for _, sub := range targets {
		if !sub.Send(msg) {
			dropped++
		}
	}
	if dropped > 0 {
		b.droppedTotal.Add(dropped)
	}
}
