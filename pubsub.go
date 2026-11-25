package main 

import "sync"

type Message struct {
	Topic string `json:"topic"`
	Body  string `json:"body"`
}

type Subscriber struct {
	ID string 
	Ch chan Message 
}

type Broker struct {
	mu sync.RWMutex
	topics map[string]map[string]*Subscriber
}

// Function to make new broker
func NewBroker() *Broker {
	return &Broker{
		topics : make(map[string]map[string]*Subscriber),
	}
}

// Subscribe to broker
func(b* Broker) Subscribe(topic, id string, buffSize int) *Subscriber {

	// Create a subscriber
	sub := &Subscriber{
		ID: id, 
		Ch : make(chan Message, buffSize),
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.topics[topic] == nil {
		b.topics[topic] = make(map[string]*Subscriber)
	}
	b.topics[topic][id] = sub
	return sub
	
}