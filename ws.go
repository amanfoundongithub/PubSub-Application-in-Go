package main 

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

type ChatMessage struct {
	Topic  string `json="topic"`
	Body   string `json="body"`
	Sender string `json="sender"`
	SentAt string `json="sent_at"`
}