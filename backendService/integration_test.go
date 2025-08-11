package main

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"backendService/models"
	"backendService/websocket"

	"github.com/gin-gonic/gin"
	gorilla_websocket "github.com/gorilla/websocket"
)

func TestWebSocketServerIntegration(t *testing.T) {
	// Initialize components
	hub := models.NewHub()
	hubManager := websocket.NewHubManager(hub)
	wsHandler := websocket.NewHandler(hub)
	
	// Start hub manager
	go hubManager.Run()
	
	// Setup router
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/ws", wsHandler.HandleWebSocket)
	router.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"status":   "healthy",
			"clients":  hubManager.GetClientCount(),
			"channels": hubManager.GetActiveChannels(),
		})
	})
	
	// Create test server
	server := httptest.NewServer(router)
	defer server.Close()
	
	// Test health endpoint first
	resp, err := server.Client().Get(server.URL + "/health")
	if err != nil {
		t.Fatalf("Failed to call health endpoint: %v", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != 200 {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}
	
	// Test WebSocket connection
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws"
	
	// Connect first client
	conn1, _, err := gorilla_websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("Failed to connect first client: %v", err)
	}
	defer conn1.Close()
	
	// Connect second client
	conn2, _, err := gorilla_websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("Failed to connect second client: %v", err)
	}
	defer conn2.Close()
	
	// Allow connections to be processed
	time.Sleep(100 * time.Millisecond)
	
	// Verify client count
	if hubManager.GetClientCount() != 2 {
		t.Errorf("Expected 2 clients, got %d", hubManager.GetClientCount())
	}
	
	// Subscribe both clients to dashboard
	subMsg := models.SubscriptionMessage{
		Type:    "subscribe",
		Channel: "dashboard",
	}
	
	if err := conn1.WriteJSON(subMsg); err != nil {
		t.Fatalf("Failed to subscribe client 1: %v", err)
	}
	
	if err := conn2.WriteJSON(subMsg); err != nil {
		t.Fatalf("Failed to subscribe client 2: %v", err)
	}
	
	// Read confirmation messages
	var conf1, conf2 models.WebSocketMessage
	if err := conn1.ReadJSON(&conf1); err != nil {
		t.Fatalf("Failed to read confirmation from client 1: %v", err)
	}
	
	if err := conn2.ReadJSON(&conf2); err != nil {
		t.Fatalf("Failed to read confirmation from client 2: %v", err)
	}
	
	// Allow subscriptions to be processed
	time.Sleep(100 * time.Millisecond)
	
	// Verify subscription count
	if hubManager.GetChannelSubscriberCount("dashboard") != 2 {
		t.Errorf("Expected 2 subscribers, got %d", hubManager.GetChannelSubscriberCount("dashboard"))
	}
	
	// Test broadcasting
	testMsg := models.NewWebSocketMessage("token_info", "dashboard", map[string]interface{}{
		"symbol": "BTC",
		"price":  50000.0,
	})
	
	msgBytes, err := testMsg.ToJSON()
	if err != nil {
		t.Fatalf("Failed to marshal test message: %v", err)
	}
	
	// Broadcast to dashboard channel
	hubManager.BroadcastToChannel("dashboard", msgBytes)
	
	// Read messages from both clients
	var msg1, msg2 models.WebSocketMessage
	
	if err := conn1.ReadJSON(&msg1); err != nil {
		t.Fatalf("Failed to read broadcast message from client 1: %v", err)
	}
	
	if err := conn2.ReadJSON(&msg2); err != nil {
		t.Fatalf("Failed to read broadcast message from client 2: %v", err)
	}
	
	// Verify both clients received the same message
	if msg1.Type != "token_info" || msg2.Type != "token_info" {
		t.Error("Both clients should receive token_info message")
	}
	
	if msg1.Channel != "dashboard" || msg2.Channel != "dashboard" {
		t.Error("Both clients should receive message on dashboard channel")
	}
	
	// Test unsubscription
	unsubMsg := models.SubscriptionMessage{
		Type:    "unsubscribe",
		Channel: "dashboard",
	}
	
	if err := conn1.WriteJSON(unsubMsg); err != nil {
		t.Fatalf("Failed to unsubscribe client 1: %v", err)
	}
	
	// Read unsubscription confirmation
	var unsubConf models.WebSocketMessage
	if err := conn1.ReadJSON(&unsubConf); err != nil {
		t.Fatalf("Failed to read unsubscription confirmation: %v", err)
	}
	
	// Allow unsubscription to be processed
	time.Sleep(100 * time.Millisecond)
	
	// Verify subscription count decreased
	if hubManager.GetChannelSubscriberCount("dashboard") != 1 {
		t.Errorf("Expected 1 subscriber after unsubscription, got %d", hubManager.GetChannelSubscriberCount("dashboard"))
	}
}