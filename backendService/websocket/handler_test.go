package websocket

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"backendService/models"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

func TestWebSocketConnection(t *testing.T) {
	// Create hub and handler
	hub := models.NewHub()
	handler := NewHandler(hub)
	hubManager := NewHubManager(hub)
	
	// Start hub manager
	go hubManager.Run()
	
	// Setup Gin router
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/ws", handler.HandleWebSocket)
	
	// Create test server
	server := httptest.NewServer(router)
	defer server.Close()
	
	// Convert HTTP URL to WebSocket URL
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws"
	
	// Connect to WebSocket
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("Failed to connect to WebSocket: %v", err)
	}
	defer conn.Close()
	
	// Test subscription
	subMsg := models.SubscriptionMessage{
		Type:    "subscribe",
		Channel: "dashboard",
	}
	
	if err := conn.WriteJSON(subMsg); err != nil {
		t.Fatalf("Failed to send subscription message: %v", err)
	}
	
	// Read confirmation message
	var response models.WebSocketMessage
	if err := conn.ReadJSON(&response); err != nil {
		t.Fatalf("Failed to read confirmation message: %v", err)
	}
	
	// Verify confirmation
	if response.Type != "subscription_confirmed" {
		t.Errorf("Expected subscription_confirmed, got %s", response.Type)
	}
	
	if response.Channel != "dashboard" {
		t.Errorf("Expected dashboard channel, got %s", response.Channel)
	}
	
	// Give some time for the subscription to be processed
	time.Sleep(100 * time.Millisecond)
	
	// Verify client count
	if hubManager.GetClientCount() != 1 {
		t.Errorf("Expected 1 client, got %d", hubManager.GetClientCount())
	}
	
	// Verify channel subscription count
	if hubManager.GetChannelSubscriberCount("dashboard") != 1 {
		t.Errorf("Expected 1 subscriber for dashboard, got %d", hubManager.GetChannelSubscriberCount("dashboard"))
	}
}

func TestBroadcastToChannel(t *testing.T) {
	// Create hub and manager
	hub := models.NewHub()
	hubManager := NewHubManager(hub)
	
	// Start hub manager
	go hubManager.Run()
	
	// Create a mock client
	client := &models.Client{
		ID:       "test-client",
		Send:     make(chan []byte, 256),
		Channels: make(map[string]bool),
		Hub:      hub,
	}
	
	// Register client
	hub.Register <- client
	time.Sleep(50 * time.Millisecond) // Allow registration to process
	
	// Subscribe to dashboard
	client.Channels["dashboard"] = true
	if hub.Subscriptions["dashboard"] == nil {
		hub.Subscriptions["dashboard"] = make(map[*models.Client]bool)
	}
	hub.Subscriptions["dashboard"][client] = true
	
	// Create test message
	testMsg := models.NewWebSocketMessage("test_data", "dashboard", map[string]string{
		"message": "Hello, World!",
	})
	
	msgBytes, err := testMsg.ToJSON()
	if err != nil {
		t.Fatalf("Failed to marshal test message: %v", err)
	}
	
	// Broadcast to channel
	hubManager.BroadcastToChannel("dashboard", msgBytes)
	
	// Check if message was received
	select {
	case receivedMsg := <-client.Send:
		var parsedMsg models.WebSocketMessage
		if err := json.Unmarshal(receivedMsg, &parsedMsg); err != nil {
			t.Fatalf("Failed to unmarshal received message: %v", err)
		}
		
		if parsedMsg.Type != "test_data" {
			t.Errorf("Expected test_data, got %s", parsedMsg.Type)
		}
		
		if parsedMsg.Channel != "dashboard" {
			t.Errorf("Expected dashboard channel, got %s", parsedMsg.Channel)
		}
		
	case <-time.After(1 * time.Second):
		t.Error("Timeout waiting for broadcast message")
	}
}

func TestSubscriptionMessageParsing(t *testing.T) {
	// Test valid subscription message
	validJSON := `{"type": "subscribe", "channel": "dashboard"}`
	var subMsg models.SubscriptionMessage
	
	err := json.Unmarshal([]byte(validJSON), &subMsg)
	if err != nil {
		t.Fatalf("Failed to parse valid subscription message: %v", err)
	}
	
	if subMsg.Type != "subscribe" {
		t.Errorf("Expected type 'subscribe', got '%s'", subMsg.Type)
	}
	
	if subMsg.Channel != "dashboard" {
		t.Errorf("Expected channel 'dashboard', got '%s'", subMsg.Channel)
	}
	
	// Test unsubscribe message
	unsubJSON := `{"type": "unsubscribe", "channel": "dashboard"}`
	err = json.Unmarshal([]byte(unsubJSON), &subMsg)
	if err != nil {
		t.Fatalf("Failed to parse valid unsubscription message: %v", err)
	}
	
	if subMsg.Type != "unsubscribe" {
		t.Errorf("Expected type 'unsubscribe', got '%s'", subMsg.Type)
	}
}

func TestClientRegistryThreadSafety(t *testing.T) {
	hub := models.NewHub()
	handler := NewHandler(hub)
	hubManager := NewHubManager(hub)
	
	// Start hub manager
	go hubManager.Run()
	
	// Create multiple clients concurrently
	numClients := 10
	clients := make([]*models.Client, numClients)
	
	// Register clients concurrently
	for i := 0; i < numClients; i++ {
		clients[i] = &models.Client{
			ID:       fmt.Sprintf("client-%d", i),
			Send:     make(chan []byte, 256),
			Channels: make(map[string]bool),
			Hub:      hub,
		}
		
		go func(client *models.Client) {
			hub.Register <- client
		}(clients[i])
	}
	
	// Wait for all registrations to complete
	time.Sleep(100 * time.Millisecond)
	
	// Verify all clients are registered
	if hubManager.GetClientCount() != numClients {
		t.Errorf("Expected %d clients, got %d", numClients, hubManager.GetClientCount())
	}
	
	// Subscribe all clients to dashboard using the proper handler method
	for i := 0; i < numClients; i++ {
		handler.subscribeClient(clients[i], "dashboard")
	}
	
	// Wait for subscriptions to complete
	time.Sleep(50 * time.Millisecond)
	
	// Verify subscription count
	if hubManager.GetChannelSubscriberCount("dashboard") != numClients {
		t.Errorf("Expected %d subscribers, got %d", numClients, hubManager.GetChannelSubscriberCount("dashboard"))
	}
	
	// Unregister clients concurrently
	for i := 0; i < numClients; i++ {
		go func(client *models.Client) {
			hub.Unregister <- client
		}(clients[i])
	}
	
	// Wait for unregistrations to complete
	time.Sleep(100 * time.Millisecond)
	
	// Verify all clients are unregistered
	if hubManager.GetClientCount() != 0 {
		t.Errorf("Expected 0 clients, got %d", hubManager.GetClientCount())
	}
	
	// Verify no subscribers remain
	if hubManager.GetChannelSubscriberCount("dashboard") != 0 {
		t.Errorf("Expected 0 subscribers, got %d", hubManager.GetChannelSubscriberCount("dashboard"))
	}
}

func TestUnsubscriptionLogic(t *testing.T) {
	hub := models.NewHub()
	handler := NewHandler(hub)
	hubManager := NewHubManager(hub)
	
	// Start hub manager
	go hubManager.Run()
	
	// Create a mock client
	client := &models.Client{
		ID:       "test-client",
		Send:     make(chan []byte, 256),
		Channels: make(map[string]bool),
		Hub:      hub,
	}
	
	// Register client
	hub.Register <- client
	time.Sleep(50 * time.Millisecond)
	
	// Subscribe to dashboard
	handler.subscribeClient(client, "dashboard")
	
	// Verify subscription
	if !client.Channels["dashboard"] {
		t.Error("Client should be subscribed to dashboard")
	}
	
	if hubManager.GetChannelSubscriberCount("dashboard") != 1 {
		t.Errorf("Expected 1 subscriber, got %d", hubManager.GetChannelSubscriberCount("dashboard"))
	}
	
	// Unsubscribe from dashboard
	handler.unsubscribeClient(client, "dashboard")
	
	// Verify unsubscription
	if client.Channels["dashboard"] {
		t.Error("Client should not be subscribed to dashboard")
	}
	
	if hubManager.GetChannelSubscriberCount("dashboard") != 0 {
		t.Errorf("Expected 0 subscribers, got %d", hubManager.GetChannelSubscriberCount("dashboard"))
	}
}