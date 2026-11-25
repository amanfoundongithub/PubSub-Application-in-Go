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