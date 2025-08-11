package models

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// WebSocketMessage represents the structure of messages sent to clients
type WebSocketMessage struct {
	Type      string      `json:"type"`      // "token_info", "trade_data", "aggregate_data"
	Channel   string      `json:"channel"`   // "dashboard"
	Data      interface{} `json:"data"`      // Actual payload
	Timestamp string      `json:"timestamp"` // ISO 8601 format
}

// SubscriptionMessage represents client subscription requests
type SubscriptionMessage struct {
	Type    string `json:"type"`    // "subscribe" or "unsubscribe"
	Channel string `json:"channel"` // "dashboard"
}

// Client represents a WebSocket client connection
type Client struct {
	ID       string                 `json:"id"`
	Conn     *websocket.Conn        `json:"-"`
	Send     chan []byte            `json:"-"`
	Channels map[string]bool        `json:"channels"`
	Hub      *Hub                   `json:"-"`
}

// Hub maintains the set of active clients and broadcasts messages to them
type Hub struct {
	// Registered clients
	Clients map[*Client]bool

	// Inbound messages from clients
	Broadcast chan []byte

	// Register requests from clients
	Register chan *Client

	// Unregister requests from clients
	Unregister chan *Client

	// Channel subscriptions
	Subscriptions map[string]map[*Client]bool

	// Mutex for thread-safe access to subscriptions
	SubscriptionsMutex sync.RWMutex

	// Mutex for thread-safe access to clients
	ClientsMutex sync.RWMutex
}

// NewHub creates a new Hub instance
func NewHub() *Hub {
	return &Hub{
		Clients:       make(map[*Client]bool),
		Broadcast:     make(chan []byte),
		Register:      make(chan *Client),
		Unregister:    make(chan *Client),
		Subscriptions: make(map[string]map[*Client]bool),
	}
}

// NewWebSocketMessage creates a new WebSocket message with timestamp
func NewWebSocketMessage(msgType, channel string, data interface{}) *WebSocketMessage {
	return &WebSocketMessage{
		Type:      msgType,
		Channel:   channel,
		Data:      data,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
}

// ToJSON converts the WebSocket message to JSON bytes
func (msg *WebSocketMessage) ToJSON() ([]byte, error) {
	return json.Marshal(msg)
}