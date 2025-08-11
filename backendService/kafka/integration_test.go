package kafka

import (
	"encoding/json"
	"testing"
	"time"

	"backendService/config"
	"backendService/models"
	"backendService/websocket"

	"github.com/segmentio/kafka-go"
)

// TestKafkaToWebSocketIntegration tests the complete flow from Kafka message to WebSocket broadcast
func TestKafkaToWebSocketIntegration(t *testing.T) {
	// Create test configuration
	cfg := &config.Config{
		KafkaBrokers:      []string{"localhost:9092"},
		KafkaConsumerGroup: "test-integration-group",
		MessageBufferSize: 100,
	}
	
	// Create hub and hub manager
	hub := models.NewHub()
	hubManager := websocket.NewHubManager(hub)
	
	// Start hub manager
	go hubManager.Run()
	
	// Create a mock client to simulate WebSocket subscription
	mockClient := &models.Client{
		ID:       "test-client",
		Send:     make(chan []byte, 10),
		Channels: make(map[string]bool),
		Hub:      hub,
	}
	
	// Register the mock client
	hub.Register <- mockClient
	
	// Subscribe to dashboard channel
	mockClient.Channels["dashboard"] = true
	if hub.Subscriptions["dashboard"] == nil {
		hub.Subscriptions["dashboard"] = make(map[*models.Client]bool)
	}
	hub.Subscriptions["dashboard"][mockClient] = true
	
	// Give some time for registration
	time.Sleep(100 * time.Millisecond)
	
	// Create consumer manager
	cm := NewConsumerManager(hubManager, cfg)
	
	// Create consumer for token-info topic
	consumer, err := cm.createConsumer("token-info")
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	
	// Test data that would come from Kafka
	testTokenData := map[string]interface{}{
		"symbol":      "BTC",
		"price":       50000.0,
		"volume":      1000.0,
		"timestamp":   time.Now().Unix(),
		"change_24h":  2.5,
	}
	
	jsonData, err := json.Marshal(testTokenData)
	if err != nil {
		t.Fatalf("Failed to marshal test data: %v", err)
	}
	
	// Create test Kafka message
	kafkaMessage := kafka.Message{
		Topic:     "token-info",
		Partition: 0,
		Offset:    1,
		Key:       []byte("BTC"),
		Value:     jsonData,
		Time:      time.Now(),
	}
	
	// Process the message through the consumer
	consumer.processMessage(kafkaMessage)
	
	// Wait for message to be processed and sent to WebSocket client
	select {
	case receivedMessage := <-mockClient.Send:
		// Parse the received WebSocket message
		var wsMessage models.WebSocketMessage
		if err := json.Unmarshal(receivedMessage, &wsMessage); err != nil {
			t.Fatalf("Failed to unmarshal WebSocket message: %v", err)
		}
		
		// Verify message structure
		if wsMessage.Type != "token_info" {
			t.Errorf("Expected message type 'token_info', got '%s'", wsMessage.Type)
		}
		
		if wsMessage.Channel != "dashboard" {
			t.Errorf("Expected channel 'dashboard', got '%s'", wsMessage.Channel)
		}
		
		// Verify the data payload
		dataMap, ok := wsMessage.Data.(map[string]interface{})
		if !ok {
			t.Fatal("Expected data to be a map")
		}
		
		if dataMap["symbol"] != "BTC" {
			t.Errorf("Expected symbol 'BTC', got '%v'", dataMap["symbol"])
		}
		
		if dataMap["price"] != 50000.0 {
			t.Errorf("Expected price 50000.0, got '%v'", dataMap["price"])
		}
		
		// Verify timestamp is set
		if wsMessage.Timestamp == "" {
			t.Error("Expected timestamp to be set")
		}
		
		t.Logf("Successfully received WebSocket message: %s", string(receivedMessage))
		
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for WebSocket message")
	}
	
	// Clean up
	consumer.Stop()
	hub.Unregister <- mockClient
}

// TestMultipleTopicsIntegration tests processing messages from all three Kafka topics
func TestMultipleTopicsIntegration(t *testing.T) {
	// Create test configuration
	cfg := &config.Config{
		KafkaBrokers:      []string{"localhost:9092"},
		KafkaConsumerGroup: "test-multi-topic-group",
		MessageBufferSize: 100,
	}
	
	// Create hub and hub manager
	hub := models.NewHub()
	hubManager := websocket.NewHubManager(hub)
	
	// Start hub manager
	go hubManager.Run()
	
	// Create a mock client
	mockClient := &models.Client{
		ID:       "test-client",
		Send:     make(chan []byte, 30), // Larger buffer for multiple messages
		Channels: make(map[string]bool),
		Hub:      hub,
	}
	
	// Register and subscribe
	hub.Register <- mockClient
	mockClient.Channels["dashboard"] = true
	if hub.Subscriptions["dashboard"] == nil {
		hub.Subscriptions["dashboard"] = make(map[*models.Client]bool)
	}
	hub.Subscriptions["dashboard"][mockClient] = true
	
	time.Sleep(100 * time.Millisecond)
	
	// Create consumer manager
	cm := NewConsumerManager(hubManager, cfg)
	
	// Test data for each topic
	testCases := []struct {
		topic       string
		messageType string
		data        map[string]interface{}
	}{
		{
			topic:       "token-info",
			messageType: "token_info",
			data: map[string]interface{}{
				"symbol": "ETH",
				"price":  3000.0,
			},
		},
		{
			topic:       "trade-info",
			messageType: "trade_data",
			data: map[string]interface{}{
				"symbol":    "BTC",
				"price":     50000.0,
				"quantity":  0.1,
				"side":      "buy",
			},
		},
		{
			topic:       "aggregate-info",
			messageType: "aggregate_data",
			data: map[string]interface{}{
				"symbol":     "BTC",
				"volume_24h": 1000000.0,
				"high_24h":   51000.0,
				"low_24h":    49000.0,
			},
		},
	}
	
	// Process messages from each topic
	for _, tc := range testCases {
		consumer, err := cm.createConsumer(tc.topic)
		if err != nil {
			t.Fatalf("Failed to create consumer for topic %s: %v", tc.topic, err)
		}
		
		jsonData, err := json.Marshal(tc.data)
		if err != nil {
			t.Fatalf("Failed to marshal test data for topic %s: %v", tc.topic, err)
		}
		
		kafkaMessage := kafka.Message{
			Topic:     tc.topic,
			Partition: 0,
			Offset:    1,
			Key:       []byte("test-key"),
			Value:     jsonData,
			Time:      time.Now(),
		}
		
		consumer.processMessage(kafkaMessage)
		consumer.Stop()
	}
	
	// Verify we received messages for all topics
	receivedTypes := make(map[string]bool)
	
	for i := 0; i < len(testCases); i++ {
		select {
		case receivedMessage := <-mockClient.Send:
			var wsMessage models.WebSocketMessage
			if err := json.Unmarshal(receivedMessage, &wsMessage); err != nil {
				t.Fatalf("Failed to unmarshal WebSocket message: %v", err)
			}
			
			receivedTypes[wsMessage.Type] = true
			
			if wsMessage.Channel != "dashboard" {
				t.Errorf("Expected channel 'dashboard', got '%s'", wsMessage.Channel)
			}
			
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for WebSocket message")
		}
	}
	
	// Verify all message types were received
	expectedTypes := []string{"token_info", "trade_data", "aggregate_data"}
	for _, expectedType := range expectedTypes {
		if !receivedTypes[expectedType] {
			t.Errorf("Did not receive message of type '%s'", expectedType)
		}
	}
	
	// Clean up
	hub.Unregister <- mockClient
}