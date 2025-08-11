package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"backendService/config"
	"backendService/handlers"
	"backendService/kafka"
	"backendService/models"
	"backendService/websocket"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWebSocketErrorHandling(t *testing.T) {
	// Setup
	gin.SetMode(gin.TestMode)
	hub := models.NewHub()
	hubManager := websocket.NewHubManager(hub)
	wsHandler := handlers.NewWebSocketHandler(hub)
	
	// Start hub manager
	go hubManager.Run()
	defer hubManager.Shutdown()
	
	router := gin.New()
	router.GET("/ws", wsHandler.WebSocketUpgrade)
	
	t.Run("Invalid WebSocket upgrade headers", func(t *testing.T) {
		// Test missing Upgrade header
		req := httptest.NewRequest("GET", "/ws", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		
		assert.Equal(t, http.StatusBadRequest, w.Code)
		
		var response map[string]string
		err := json.Unmarshal(w.Body.Bytes(), &response)
		require.NoError(t, err)
		assert.Contains(t, response["error"], "Invalid Upgrade header")
	})
	
	t.Run("Missing WebSocket key", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/ws", nil)
		req.Header.Set("Upgrade", "websocket")
		req.Header.Set("Connection", "Upgrade")
		// Missing Sec-WebSocket-Key
		
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		
		assert.Equal(t, http.StatusBadRequest, w.Code)
		
		var response map[string]string
		err := json.Unmarshal(w.Body.Bytes(), &response)
		require.NoError(t, err)
		assert.Contains(t, response["error"], "Missing Sec-WebSocket-Key")
	})
}

func TestHubManagerErrorHandling(t *testing.T) {
	t.Run("Nil client registration", func(t *testing.T) {
		hub := models.NewHub()
		hubManager := websocket.NewHubManager(hub)
		
		go hubManager.Run()
		defer hubManager.Shutdown()
		
		// Wait for hub to start
		time.Sleep(100 * time.Millisecond)
		
		// Try to register nil client
		hub.Register <- nil
		
		// Wait for processing
		time.Sleep(100 * time.Millisecond)
		
		// Should not crash and client count should be 0
		assert.Equal(t, 0, hubManager.GetClientCount())
	})
	
	t.Run("Nil message broadcast", func(t *testing.T) {
		hub := models.NewHub()
		hubManager := websocket.NewHubManager(hub)
		
		go hubManager.Run()
		defer hubManager.Shutdown()
		
		// Wait for hub to start
		time.Sleep(100 * time.Millisecond)
		
		// Try to broadcast nil message
		hub.Broadcast <- nil
		
		// Wait for processing
		time.Sleep(100 * time.Millisecond)
		
		// Should not crash
		assert.True(t, hubManager.IsRunning())
	})
}

func TestKafkaConsumerErrorHandling(t *testing.T) {
	t.Run("Consumer with invalid brokers", func(t *testing.T) {
		cfg := &config.Config{
			KafkaBrokers:       []string{"invalid:9092"},
			KafkaConsumerGroup: "test-group",
		}
		
		hub := models.NewHub()
		hubManager := websocket.NewHubManager(hub)
		consumerManager := kafka.NewConsumerManager(hubManager, cfg)
		
		// This should not panic even with invalid brokers
		err := consumerManager.Start()
		
		// Should handle the error gracefully
		if err != nil {
			assert.Contains(t, err.Error(), "failed to start any Kafka consumers")
		}
		
		// Cleanup
		consumerManager.Stop()
	})
}

func TestGracefulShutdown(t *testing.T) {
	t.Run("Hub manager shutdown", func(t *testing.T) {
		hub := models.NewHub()
		hubManager := websocket.NewHubManager(hub)
		
		// Start hub manager
		go hubManager.Run()
		
		// Wait for it to start
		time.Sleep(100 * time.Millisecond)
		assert.True(t, hubManager.IsRunning())
		
		// Shutdown
		hubManager.Shutdown()
		
		// Wait for shutdown to complete
		time.Sleep(200 * time.Millisecond)
		assert.False(t, hubManager.IsRunning())
	})
	
	t.Run("Hub manager health status", func(t *testing.T) {
		hub := models.NewHub()
		hubManager := websocket.NewHubManager(hub)
		
		go hubManager.Run()
		defer hubManager.Shutdown()
		
		// Wait for it to start
		time.Sleep(100 * time.Millisecond)
		
		status := hubManager.GetHealthStatus()
		
		assert.True(t, status["running"].(bool))
		assert.Equal(t, 0, status["client_count"].(int))
		assert.Equal(t, 0, status["channel_count"].(int))
		assert.Equal(t, 0, status["total_subscriptions"].(int))
		assert.NotEmpty(t, status["timestamp"])
	})
}

func TestHealthCheckEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	
	hub := models.NewHub()
	hubManager := websocket.NewHubManager(hub)
	
	go hubManager.Run()
	defer hubManager.Shutdown()
	
	router := gin.New()
	router.GET("/health", healthCheck(hubManager))
	
	t.Run("Healthy status", func(t *testing.T) {
		// Wait for hub manager to start
		time.Sleep(100 * time.Millisecond)
		
		req := httptest.NewRequest("GET", "/health", nil)
		w := httptest.NewRecorder()
		
		router.ServeHTTP(w, req)
		
		// Should be OK or degraded (degraded because no clients)
		assert.Contains(t, []int{http.StatusOK}, w.Code)
		
		var response HealthResponse
		err := json.Unmarshal(w.Body.Bytes(), &response)
		require.NoError(t, err)
		
		assert.Contains(t, []string{"healthy", "degraded"}, response.Status)
		assert.Equal(t, 0, response.Clients)
		assert.NotNil(t, response.HubStatus)
		assert.NotEmpty(t, response.Timestamp)
	})
	
	t.Run("Unhealthy status after shutdown", func(t *testing.T) {
		// Shutdown the hub manager
		hubManager.Shutdown()
		
		// Wait for shutdown
		time.Sleep(200 * time.Millisecond)
		
		req := httptest.NewRequest("GET", "/health", nil)
		w := httptest.NewRecorder()
		
		router.ServeHTTP(w, req)
		
		assert.Equal(t, http.StatusServiceUnavailable, w.Code)
		
		var response HealthResponse
		err := json.Unmarshal(w.Body.Bytes(), &response)
		require.NoError(t, err)
		
		assert.Equal(t, "unhealthy", response.Status)
	})
}

func TestWebSocketConnectionErrorRecovery(t *testing.T) {
	t.Run("Hub manager handles client cleanup", func(t *testing.T) {
		hub := models.NewHub()
		hubManager := websocket.NewHubManager(hub)
		
		go hubManager.Run()
		defer hubManager.Shutdown()
		
		// Wait for hub to start
		time.Sleep(100 * time.Millisecond)
		
		// Create a mock client
		client := &models.Client{
			ID:       "test-client",
			Conn:     nil, // nil connection to simulate error
			Send:     make(chan []byte, 1),
			Channels: make(map[string]bool),
			Hub:      hub,
		}
		
		// Register client
		hub.Register <- client
		time.Sleep(100 * time.Millisecond)
		
		initialCount := hubManager.GetClientCount()
		assert.Equal(t, 1, initialCount)
		
		// Unregister client (simulating disconnect)
		hub.Unregister <- client
		time.Sleep(100 * time.Millisecond)
		
		finalCount := hubManager.GetClientCount()
		assert.Equal(t, 0, finalCount)
	})
}