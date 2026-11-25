package main 

import(
	"encoding/json"
	"fmt"
	"net/http"
)

type Server struct {
	broker *Broker
}