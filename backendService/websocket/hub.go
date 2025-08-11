package websocket

import (
	"log"
	"time"

	"backendService/models"
	"github.com/gorilla/websocket"
)

// HubManager manages the WebSocket hub operations
type HubManager struct {
	hub       *models.Hub
	isRunning bool
	stopChan  chan bool
}

// NewHubManager creates a new hub manager
func NewHubManager(hub *models.Hub) *HubManager {
	return &HubManager{
		hub:       hub,
		isRunning: false,
		stopChan:  make(chan bool, 1),
	}
}

// Run starts the hub's main loop for handling client connections and broadcasts
func (hm *HubManager) Run() {
	log.Println("Starting WebSocket hub manager")
	hm.isRunning = true
	
	defer func() {
		hm.isRunning = false
		if r := recover(); r != nil {
			log.Printf("Panic in hub manager: %v", r)
			// Only restart if not explicitly stopped
			select {
			case <-hm.stopChan:
				log.Println("Hub manager stopped, not restarting")
				return
			default:
				log.Println("Hub manager crashed, restarting after delay")
				time.Sleep(1 * time.Second)
				go hm.Run()
			}
		}
	}()
	
	for {
		select {
		case <-hm.stopChan:
			log.Println("Hub manager received stop signal")
			return
			
		case client := <-hm.hub.Register:
			if client != nil {
				hm.registerClient(client)
			} else {
				log.Printf("Attempted to register nil client")
			}

		case client := <-hm.hub.Unregister:
			if client != nil {
				hm.unregisterClient(client)
			} else {
				log.Printf("Attempted to unregister nil client")
			}

		case message := <-hm.hub.Broadcast:
			if message != nil {
				hm.broadcastMessage(message)
			} else {
				log.Printf("Attempted to broadcast nil message")
			}
		}
	}
}

// registerClient adds a new client to the hub
func (hm *HubManager) registerClient(client *models.Client) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic in registerClient for client %s: %v", client.ID, r)
		}
	}()

	if client == nil {
		log.Printf("Attempted to register nil client")
		return
	}

	hm.hub.ClientsMutex.Lock()
	hm.hub.Clients[client] = true
	clientCount := len(hm.hub.Clients)
	hm.hub.ClientsMutex.Unlock()
	
	log.Printf("Client %s registered. Total clients: %d", client.ID, clientCount)
}

// unregisterClient removes a client from the hub and all subscriptions
func (hm *HubManager) unregisterClient(client *models.Client) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic in unregisterClient for client %s: %v", client.ID, r)
		}
	}()

	if client == nil {
		log.Printf("Attempted to unregister nil client")
		return
	}

	hm.hub.ClientsMutex.Lock()
	_, exists := hm.hub.Clients[client]
	if exists {
		delete(hm.hub.Clients, client)
		clientCount := len(hm.hub.Clients)
		hm.hub.ClientsMutex.Unlock()
		
		// Remove from all channel subscriptions with thread safety
		hm.hub.SubscriptionsMutex.Lock()
		if client.Channels != nil {
			for channel := range client.Channels {
				if hm.hub.Subscriptions[channel] != nil {
					delete(hm.hub.Subscriptions[channel], client)
					if len(hm.hub.Subscriptions[channel]) == 0 {
						delete(hm.hub.Subscriptions, channel)
					}
				}
			}
		}
		hm.hub.SubscriptionsMutex.Unlock()

		// Close send channel safely
		hm.closeSendChannel(client)
		
		log.Printf("Client %s unregistered. Total clients: %d", client.ID, clientCount)
	} else {
		hm.hub.ClientsMutex.Unlock()
		log.Printf("Client %s was not registered", client.ID)
	}
}

// closeSendChannel safely closes a client's send channel
func (hm *HubManager) closeSendChannel(client *models.Client) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic closing send channel for client %s: %v", client.ID, r)
		}
	}()

	if client.Send != nil {
		select {
		case <-client.Send:
			// Channel already closed
		default:
			close(client.Send)
		}
	}
}

// broadcastMessage sends a message to all connected clients
func (hm *HubManager) broadcastMessage(message []byte) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic in broadcastMessage: %v", r)
		}
	}()

	if message == nil {
		log.Printf("Attempted to broadcast nil message")
		return
	}

	hm.hub.ClientsMutex.RLock()
	// Create a copy of clients to avoid holding the lock during broadcast
	clientsCopy := make([]*models.Client, 0, len(hm.hub.Clients))
	for client := range hm.hub.Clients {
		if client != nil {
			clientsCopy = append(clientsCopy, client)
		}
	}
	hm.hub.ClientsMutex.RUnlock()
	
	for _, client := range clientsCopy {
		if client == nil || client.Send == nil {
			continue
		}
		
		select {
		case client.Send <- message:
			// Message sent successfully
		default:
			// Client's send channel is full, remove the client
			log.Printf("Client %s send channel full, removing client", client.ID)
			hm.unregisterClient(client)
		}
	}
}

// BroadcastToChannel sends a message to all clients subscribed to a specific channel
func (hm *HubManager) BroadcastToChannel(channel string, message []byte) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic in BroadcastToChannel for channel %s: %v", channel, r)
		}
	}()

	if channel == "" {
		log.Printf("Attempted to broadcast to empty channel")
		return
	}

	if message == nil {
		log.Printf("Attempted to broadcast nil message to channel %s", channel)
		return
	}

	hm.hub.SubscriptionsMutex.RLock()
	subscribers, exists := hm.hub.Subscriptions[channel]
	if exists {
		// Create a copy of subscribers to avoid holding the lock during broadcast
		subscribersCopy := make([]*models.Client, 0, len(subscribers))
		for client := range subscribers {
			if client != nil {
				subscribersCopy = append(subscribersCopy, client)
			}
		}
		hm.hub.SubscriptionsMutex.RUnlock()
		
		log.Printf("Broadcasting message to %d subscribers of channel %s", len(subscribersCopy), channel)
		
		for _, client := range subscribersCopy {
			if client == nil || client.Send == nil {
				continue
			}
			
			select {
			case client.Send <- message:
				// Message sent successfully
			default:
				// Client's send channel is full, remove the client
				log.Printf("Client %s send channel full for channel %s, removing client", client.ID, channel)
				hm.unregisterClient(client)
			}
		}
	} else {
		hm.hub.SubscriptionsMutex.RUnlock()
		log.Printf("No subscribers for channel %s", channel)
	}
}

// GetClientCount returns the number of connected clients
func (hm *HubManager) GetClientCount() int {
	hm.hub.ClientsMutex.RLock()
	defer hm.hub.ClientsMutex.RUnlock()
	return len(hm.hub.Clients)
}

// GetChannelSubscriberCount returns the number of subscribers for a specific channel
func (hm *HubManager) GetChannelSubscriberCount(channel string) int {
	hm.hub.SubscriptionsMutex.RLock()
	defer hm.hub.SubscriptionsMutex.RUnlock()
	
	if subscribers, exists := hm.hub.Subscriptions[channel]; exists {
		return len(subscribers)
	}
	return 0
}

// GetActiveChannels returns a list of channels that have active subscribers
func (hm *HubManager) GetActiveChannels() []string {
	hm.hub.SubscriptionsMutex.RLock()
	defer hm.hub.SubscriptionsMutex.RUnlock()
	
	channels := make([]string, 0, len(hm.hub.Subscriptions))
	for channel := range hm.hub.Subscriptions {
		channels = append(channels, channel)
	}
	return channels
}

// Shutdown gracefully shuts down the hub manager and disconnects all clients
func (hm *HubManager) Shutdown() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic in hub shutdown: %v", r)
		}
	}()

	log.Println("Shutting down WebSocket hub manager...")

	// Signal the hub manager to stop
	if hm.isRunning {
		select {
		case hm.stopChan <- true:
			log.Println("Stop signal sent to hub manager")
		default:
			log.Println("Stop channel full, hub manager may already be stopping")
		}
	}

	// Wait for hub manager to stop with timeout
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for hm.isRunning {
		select {
		case <-timeout:
			log.Println("Timeout waiting for hub manager to stop, forcing shutdown")
			goto forceShutdown
		case <-ticker.C:
			// Continue waiting
		}
	}

forceShutdown:
	// Get all connected clients
	hm.hub.ClientsMutex.RLock()
	clients := make([]*models.Client, 0, len(hm.hub.Clients))
	for client := range hm.hub.Clients {
		if client != nil {
			clients = append(clients, client)
		}
	}
	hm.hub.ClientsMutex.RUnlock()

	log.Printf("Disconnecting %d clients", len(clients))

	// Send close message to all clients and disconnect them
	for _, client := range clients {
		if client != nil && client.Conn != nil {
			// Set write deadline for close message
			client.Conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
			
			// Send close message
			if err := client.Conn.WriteMessage(websocket.CloseMessage, 
				websocket.FormatCloseMessage(websocket.CloseGoingAway, "Server shutting down")); err != nil {
				log.Printf("Error sending close message to client %s: %v", client.ID, err)
			}
			
			// Close connection
			if err := client.Conn.Close(); err != nil {
				log.Printf("Error closing connection for client %s: %v", client.ID, err)
			}
		}
	}

	// Clear all clients and subscriptions
	hm.hub.ClientsMutex.Lock()
	hm.hub.Clients = make(map[*models.Client]bool)
	hm.hub.ClientsMutex.Unlock()

	hm.hub.SubscriptionsMutex.Lock()
	hm.hub.Subscriptions = make(map[string]map[*models.Client]bool)
	hm.hub.SubscriptionsMutex.Unlock()

	log.Println("WebSocket hub manager shutdown complete")
}

// IsRunning returns whether the hub manager is currently running
func (hm *HubManager) IsRunning() bool {
	return hm.isRunning
}

// GetHealthStatus returns the health status of the hub manager
func (hm *HubManager) GetHealthStatus() map[string]interface{} {
	hm.hub.ClientsMutex.RLock()
	clientCount := len(hm.hub.Clients)
	hm.hub.ClientsMutex.RUnlock()
	
	hm.hub.SubscriptionsMutex.RLock()
	channelCount := len(hm.hub.Subscriptions)
	totalSubscriptions := 0
	for _, subscribers := range hm.hub.Subscriptions {
		totalSubscriptions += len(subscribers)
	}
	hm.hub.SubscriptionsMutex.RUnlock()
	
	return map[string]interface{}{
		"running":              hm.isRunning,
		"client_count":         clientCount,
		"channel_count":        channelCount,
		"total_subscriptions":  totalSubscriptions,
		"timestamp":            time.Now().UTC().Format(time.RFC3339),
	}
}