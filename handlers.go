package main 

import(
	"encoding/json"
	"fmt"
	"net/http"
)

type Server struct {
	broker *Broker
}

func NewServer(broker *Broker) *Server {
	return &Server{
		broker: broker,
	}
}

