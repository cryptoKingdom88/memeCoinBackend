package websocket

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"backendService/models"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

const (
	// Time allowed to write a message to the peer
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer
	maxMessageSize = 512
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		// Allow connections from any origin for now
		// In production, this should be more restrictive
		return true
	},
}

// Handler manages WebSocket connections and message routing
type Handler struct {
	hub *models.Hub
}

// NewHandler creates a new WebSocket handler
func NewHandler(hub *models.Hub) *Handler {
	return &Handler{
		hub: hub,
	}
}

// HandleWebSocket handles WebSocket connection upgrades
func (h *Handler) HandleWebSocket(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("WebSocket upgrade failed: %v", err)
		return
	}

	// Create new client
	client := &models.Client{
		ID:       generateClientID(),
		Conn:     conn,
		Send:     make(chan []byte, 256),
		Channels: make(map[string]bool),
		Hub:      h.hub,
	}

	// Register client with hub
	h.hub.Register <- client

	// Start goroutines for reading and writing
	go h.writePump(client)
	go h.readPump(client)

	log.Printf("Client %s connected", client.ID)
}

// readPump pumps messages from the WebSocket connection to the hub
func (h *Handler) readPump(client *models.Client) {
	defer func() {
		// Recover from any panics in the read pump
		if r := recover(); r != nil {
			log.Printf("Panic in readPump for client %s: %v", client.ID, r)
		}
		
		// Ensure client is properly unregistered and connection closed
		h.cleanupClient(client)
		log.Printf("Client %s disconnected", client.ID)
	}()

	client.Conn.SetReadLimit(maxMessageSize)
	client.Conn.SetReadDeadline(time.Now().Add(pongWait))
	client.Conn.SetPongHandler(func(string) error {
		client.Conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, message, err := client.Conn.ReadMessage()
		if err != nil {
			// Handle different types of WebSocket errors
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure, websocket.CloseNormalClosure) {
				log.Printf("WebSocket unexpected close error for client %s: %v", client.ID, err)
			} else if websocket.IsCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure, websocket.CloseNormalClosure) {
				log.Printf("WebSocket close for client %s: %v", client.ID, err)
			} else {
				log.Printf("WebSocket read error for client %s: %v", client.ID, err)
			}
			break
		}

		// Handle subscription messages with error recovery
		h.handleSubscriptionMessage(client, message)
	}
}

// writePump pumps messages from the hub to the WebSocket connection
func (h *Handler) writePump(client *models.Client) {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		// Recover from any panics in the write pump
		if r := recover(); r != nil {
			log.Printf("Panic in writePump for client %s: %v", client.ID, r)
		}
		
		ticker.Stop()
		h.cleanupClient(client)
	}()

	for {
		select {
		case message, ok := <-client.Send:
			if err := client.Conn.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
				log.Printf("Failed to set write deadline for client %s: %v", client.ID, err)
				return
			}
			
			if !ok {
				// The hub closed the channel
				if err := client.Conn.WriteMessage(websocket.CloseMessage, []byte{}); err != nil {
					log.Printf("Failed to send close message to client %s: %v", client.ID, err)
				}
				return
			}

			w, err := client.Conn.NextWriter(websocket.TextMessage)
			if err != nil {
				log.Printf("Failed to get next writer for client %s: %v", client.ID, err)
				return
			}
			
			if _, err := w.Write(message); err != nil {
				log.Printf("Failed to write message to client %s: %v", client.ID, err)
				w.Close()
				return
			}

			// Add queued messages to the current WebSocket message
			n := len(client.Send)
			for i := 0; i < n; i++ {
				if _, err := w.Write([]byte{'\n'}); err != nil {
					log.Printf("Failed to write newline to client %s: %v", client.ID, err)
					w.Close()
					return
				}
				if _, err := w.Write(<-client.Send); err != nil {
					log.Printf("Failed to write queued message to client %s: %v", client.ID, err)
					w.Close()
					return
				}
			}

			if err := w.Close(); err != nil {
				log.Printf("Failed to close writer for client %s: %v", client.ID, err)
				return
			}

		case <-ticker.C:
			if err := client.Conn.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
				log.Printf("Failed to set ping write deadline for client %s: %v", client.ID, err)
				return
			}
			if err := client.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				log.Printf("Failed to send ping to client %s: %v", client.ID, err)
				return
			}
		}
	}
}

// handleSubscriptionMessage processes subscription/unsubscription requests
func (h *Handler) handleSubscriptionMessage(client *models.Client, message []byte) {
	defer func() {
		// Recover from any panics in message handling
		if r := recover(); r != nil {
			log.Printf("Panic in handleSubscriptionMessage for client %s: %v", client.ID, r)
			h.sendErrorMessage(client, "Internal server error")
		}
	}()

	var subMsg models.SubscriptionMessage
	if err := json.Unmarshal(message, &subMsg); err != nil {
		log.Printf("Invalid subscription message from client %s: %v", client.ID, err)
		h.sendErrorMessage(client, "Invalid message format")
		return
	}

	// Validate message fields
	if subMsg.Type == "" {
		log.Printf("Empty message type from client %s", client.ID)
		h.sendErrorMessage(client, "Message type is required")
		return
	}

	if subMsg.Channel == "" {
		log.Printf("Empty channel from client %s", client.ID)
		h.sendErrorMessage(client, "Channel is required")
		return
	}

	switch subMsg.Type {
	case "subscribe":
		h.subscribeClient(client, subMsg.Channel)
	case "unsubscribe":
		h.unsubscribeClient(client, subMsg.Channel)
	default:
		log.Printf("Unknown message type from client %s: %s", client.ID, subMsg.Type)
		h.sendErrorMessage(client, "Unknown message type")
	}
}

// subscribeClient adds a client to a channel subscription
func (h *Handler) subscribeClient(client *models.Client, channel string) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic in subscribeClient for client %s: %v", client.ID, r)
			h.sendErrorMessage(client, "Failed to subscribe to channel")
		}
	}()

	// Validate channel name
	if channel == "" {
		log.Printf("Empty channel name for client %s", client.ID)
		h.sendErrorMessage(client, "Channel name cannot be empty")
		return
	}

	// Add to client's channel list
	if client.Channels == nil {
		client.Channels = make(map[string]bool)
	}
	client.Channels[channel] = true

	// Add to hub's subscription map with thread safety
	h.hub.SubscriptionsMutex.Lock()
	if h.hub.Subscriptions[channel] == nil {
		h.hub.Subscriptions[channel] = make(map[*models.Client]bool)
	}
	h.hub.Subscriptions[channel][client] = true
	h.hub.SubscriptionsMutex.Unlock()

	log.Printf("Client %s subscribed to channel %s", client.ID, channel)

	// Send confirmation message
	confirmMsg := models.NewWebSocketMessage("subscription_confirmed", channel, map[string]string{
		"status":  "subscribed",
		"channel": channel,
	})
	
	data, err := confirmMsg.ToJSON()
	if err != nil {
		log.Printf("Failed to marshal subscription confirmation for client %s: %v", client.ID, err)
		h.sendErrorMessage(client, "Failed to confirm subscription")
		return
	}

	select {
	case client.Send <- data:
		// Confirmation sent successfully
	default:
		// Client's send channel is full or closed
		log.Printf("Failed to send subscription confirmation to client %s: channel full or closed", client.ID)
		h.cleanupClient(client)
	}
}

// unsubscribeClient removes a client from a channel subscription
func (h *Handler) unsubscribeClient(client *models.Client, channel string) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic in unsubscribeClient for client %s: %v", client.ID, r)
			h.sendErrorMessage(client, "Failed to unsubscribe from channel")
		}
	}()

	// Validate channel name
	if channel == "" {
		log.Printf("Empty channel name for client %s", client.ID)
		h.sendErrorMessage(client, "Channel name cannot be empty")
		return
	}

	// Remove from client's channel list
	if client.Channels != nil {
		delete(client.Channels, channel)
	}

	// Remove from hub's subscription map with thread safety
	h.hub.SubscriptionsMutex.Lock()
	if h.hub.Subscriptions[channel] != nil {
		delete(h.hub.Subscriptions[channel], client)
		if len(h.hub.Subscriptions[channel]) == 0 {
			delete(h.hub.Subscriptions, channel)
		}
	}
	h.hub.SubscriptionsMutex.Unlock()

	log.Printf("Client %s unsubscribed from channel %s", client.ID, channel)

	// Send confirmation message
	confirmMsg := models.NewWebSocketMessage("subscription_confirmed", channel, map[string]string{
		"status":  "unsubscribed",
		"channel": channel,
	})
	
	data, err := confirmMsg.ToJSON()
	if err != nil {
		log.Printf("Failed to marshal unsubscription confirmation for client %s: %v", client.ID, err)
		h.sendErrorMessage(client, "Failed to confirm unsubscription")
		return
	}

	select {
	case client.Send <- data:
		// Confirmation sent successfully
	default:
		// Client's send channel is full or closed
		log.Printf("Failed to send unsubscription confirmation to client %s: channel full or closed", client.ID)
		h.cleanupClient(client)
	}
}

// sendErrorMessage sends an error message to a client
func (h *Handler) sendErrorMessage(client *models.Client, errorMsg string) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic in sendErrorMessage for client %s: %v", client.ID, r)
		}
	}()

	errMsg := models.NewWebSocketMessage("error", "", map[string]string{
		"error": errorMsg,
	})
	
	data, err := errMsg.ToJSON()
	if err != nil {
		log.Printf("Failed to marshal error message for client %s: %v", client.ID, err)
		return
	}

	select {
	case client.Send <- data:
		// Message sent successfully
	default:
		// Client's send channel is full or closed, cleanup the client
		log.Printf("Failed to send error message to client %s: channel full or closed", client.ID)
		h.cleanupClient(client)
	}
}

// cleanupClient ensures proper cleanup of a client connection
func (h *Handler) cleanupClient(client *models.Client) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic in cleanupClient for client %s: %v", client.ID, r)
		}
	}()

	// Unregister client from hub
	select {
	case h.hub.Unregister <- client:
		// Successfully queued for unregistration
	default:
		// Hub's unregister channel is full, force cleanup
		log.Printf("Hub unregister channel full for client %s, forcing cleanup", client.ID)
	}

	// Close the WebSocket connection
	if client.Conn != nil {
		if err := client.Conn.Close(); err != nil {
			log.Printf("Error closing connection for client %s: %v", client.ID, err)
		}
	}
}

// generateClientID generates a unique client ID
func generateClientID() string {
	return time.Now().Format("20060102150405") + "-" + randomString(6)
}

// randomString generates a random string of specified length
func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[time.Now().UnixNano()%int64(len(charset))]
	}
	return string(b)
}