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

func TestConsumerManager_CreateConsumer(t *testing.T) {
	// Create test configuration
	cfg := &config.Config{
		KafkaBrokers:      []string{"localhost:9092"},
		KafkaConsumerGroup: "test-group",
	}
	
	// Create hub and hub manager
	hub := models.NewHub()
	hubManager := websocket.NewHubManager(hub)
	
	// Create consumer manager
	cm := NewConsumerManager(hubManager, cfg)
	
	// Test creating a consumer
	consumer, err := cm.createConsumer("test-topic")
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	
	if consumer == nil {
		t.Fatal("Consumer is nil")
	}
	
	if consumer.topic != "test-topic" {
		t.Errorf("Expected topic 'test-topic', got '%s'", consumer.topic)
	}
	
	if consumer.hubManager != hubManager {
		t.Error("Hub manager not set correctly")
	}
	
	// Clean up
	consumer.Stop()
}

func TestConsumer_ProcessMessage(t *testing.T) {
	// Create test configuration
	cfg := &config.Config{
		KafkaBrokers:      []string{"localhost:9092"},
		KafkaConsumerGroup: "test-group",
	}
	
	// Create hub and hub manager
	hub := models.NewHub()
	hubManager := websocket.NewHubManager(hub)
	
	// Create consumer manager
	cm := NewConsumerManager(hubManager, cfg)
	
	// Create consumer for token-info topic
	consumer, err := cm.createConsumer("token-info")
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	
	// Test data
	testData := map[string]interface{}{
		"symbol": "BTC",
		"price":  50000.0,
		"volume": 1000.0,
	}
	
	jsonData, err := json.Marshal(testData)
	if err != nil {
		t.Fatalf("Failed to marshal test data: %v", err)
	}
	
	// Create test Kafka message
	message := kafka.Message{
		Topic:     "token-info",
		Partition: 0,
		Offset:    1,
		Key:       []byte("test-key"),
		Value:     jsonData,
		Time:      time.Now(),
	}
	
	// Process the message (this should not panic)
	consumer.processMessage(message)
	
	// Clean up
	consumer.Stop()
}

func TestConsumer_ProcessMessage_InvalidJSON(t *testing.T) {
	// Create test configuration
	cfg := &config.Config{
		KafkaBrokers:      []string{"localhost:9092"},
		KafkaConsumerGroup: "test-group",
	}
	
	// Create hub and hub manager
	hub := models.NewHub()
	hubManager := websocket.NewHubManager(hub)
	
	// Create consumer manager
	cm := NewConsumerManager(hubManager, cfg)
	
	// Create consumer for token-info topic
	consumer, err := cm.createConsumer("token-info")
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	
	// Create test Kafka message with invalid JSON
	message := kafka.Message{
		Topic:     "token-info",
		Partition: 0,
		Offset:    1,
		Key:       []byte("test-key"),
		Value:     []byte("invalid json"),
		Time:      time.Now(),
	}
	
	// Process the message (this should not panic and should handle the error gracefully)
	consumer.processMessage(message)
	
	// Clean up
	consumer.Stop()
}

func TestConsumer_ProcessMessage_AllTopics(t *testing.T) {
	// Create test configuration
	cfg := &config.Config{
		KafkaBrokers:      []string{"localhost:9092"},
		KafkaConsumerGroup: "test-group",
	}
	
	// Create hub and hub manager
	hub := models.NewHub()
	hubManager := websocket.NewHubManager(hub)
	
	// Create consumer manager
	cm := NewConsumerManager(hubManager, cfg)
	
	topics := []string{"token-info", "trade-info", "aggregate-info"}
	expectedTypes := []string{"token_info", "trade_data", "aggregate_data"}
	
	for i, topic := range topics {
		// Create consumer for each topic
		consumer, err := cm.createConsumer(topic)
		if err != nil {
			t.Fatalf("Failed to create consumer for topic %s: %v", topic, err)
		}
		
		// Test data
		testData := map[string]interface{}{
			"test": "data",
			"type": expectedTypes[i],
		}
		
		jsonData, err := json.Marshal(testData)
		if err != nil {
			t.Fatalf("Failed to marshal test data: %v", err)
		}
		
		// Create test Kafka message
		message := kafka.Message{
			Topic:     topic,
			Partition: 0,
			Offset:    1,
			Key:       []byte("test-key"),
			Value:     jsonData,
			Time:      time.Now(),
		}
		
		// Process the message (this should not panic)
		consumer.processMessage(message)
		
		// Clean up
		consumer.Stop()
	}
}

func TestConsumerManager_StartStop(t *testing.T) {
	// Create test configuration
	cfg := &config.Config{
		KafkaBrokers:      []string{"localhost:9092"},
		KafkaConsumerGroup: "test-group",
	}
	
	// Create hub and hub manager
	hub := models.NewHub()
	hubManager := websocket.NewHubManager(hub)
	
	// Create consumer manager
	cm := NewConsumerManager(hubManager, cfg)
	
	// Note: This test will fail if Kafka is not running locally
	// In a real environment, you would use a test Kafka instance or mock
	
	// Test that Stop() doesn't panic even when no consumers are started
	cm.Stop()
	
	// Verify that consumers slice is empty initially
	if len(cm.consumers) != 0 {
		t.Errorf("Expected 0 consumers initially, got %d", len(cm.consumers))
	}
}