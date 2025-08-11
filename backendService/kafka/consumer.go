package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"backendService/config"
	"backendService/models"
	"backendService/websocket"

	"github.com/segmentio/kafka-go"
)

// Consumer represents a Kafka consumer for a specific topic
type Consumer struct {
	reader     *kafka.Reader
	topic      string
	hubManager *websocket.HubManager
	ctx        context.Context
	cancel     context.CancelFunc
	
	// Error tracking for circuit breaker pattern
	consecutiveErrors int
	lastErrorTime     time.Time
	circuitOpen       bool
	circuitOpenTime   time.Time
}

// ConsumerManager manages multiple Kafka consumers
type ConsumerManager struct {
	consumers  []*Consumer
	hubManager *websocket.HubManager
	config     *config.Config
}

// NewConsumerManager creates a new consumer manager
func NewConsumerManager(hubManager *websocket.HubManager, cfg *config.Config) *ConsumerManager {
	return &ConsumerManager{
		consumers:  make([]*Consumer, 0),
		hubManager: hubManager,
		config:     cfg,
	}
}

// Start initializes and starts all Kafka consumers
func (cm *ConsumerManager) Start() error {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic in ConsumerManager.Start: %v", r)
		}
	}()

	topics := []string{"token-info", "trade-info", "aggregate-info"}
	
	for _, topic := range topics {
		consumer, err := cm.createConsumer(topic)
		if err != nil {
			log.Printf("Failed to create consumer for topic %s: %v", topic, err)
			// Don't fail completely, continue with other topics
			continue
		}
		
		cm.consumers = append(cm.consumers, consumer)
		
		// Start consumer in a goroutine
		go consumer.Start()
		
		log.Printf("Started Kafka consumer for topic: %s", topic)
	}
	
	if len(cm.consumers) == 0 {
		return fmt.Errorf("failed to start any Kafka consumers")
	}
	
	log.Printf("Started %d out of %d Kafka consumers", len(cm.consumers), len(topics))
	return nil
}

// Stop gracefully stops all Kafka consumers
func (cm *ConsumerManager) Stop() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic in ConsumerManager.Stop: %v", r)
		}
	}()

	log.Println("Stopping Kafka consumers...")
	
	// Use a channel to wait for all consumers to stop
	done := make(chan bool, len(cm.consumers))
	
	for _, consumer := range cm.consumers {
		if consumer != nil {
			go func(c *Consumer) {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("Panic stopping consumer for topic %s: %v", c.topic, r)
					}
					done <- true
				}()
				c.Stop()
			}(consumer)
		} else {
			done <- true
		}
	}
	
	// Wait for all consumers to stop with timeout
	timeout := time.After(10 * time.Second)
	stopped := 0
	
	for stopped < len(cm.consumers) {
		select {
		case <-done:
			stopped++
		case <-timeout:
			log.Printf("Timeout waiting for consumers to stop, %d/%d stopped", stopped, len(cm.consumers))
			return
		}
	}
	
	log.Println("All Kafka consumers stopped")
}

// createConsumer creates a new Kafka consumer for a specific topic
func (cm *ConsumerManager) createConsumer(topic string) (*Consumer, error) {
	ctx, cancel := context.WithCancel(context.Background())
	
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cm.config.KafkaBrokers,
		Topic:       topic,
		GroupID:     cm.config.KafkaConsumerGroup,
		MinBytes:    10e3, // 10KB
		MaxBytes:    10e6, // 10MB
		MaxWait:     1 * time.Second,
		StartOffset: kafka.LastOffset,
	})
	
	return &Consumer{
		reader:     reader,
		topic:      topic,
		hubManager: cm.hubManager,
		ctx:        ctx,
		cancel:     cancel,
	}, nil
}

// Start begins consuming messages from Kafka
func (c *Consumer) Start() {
	log.Printf("Starting consumer for topic: %s", c.topic)
	
	retryCount := 0
	maxRetries := 10
	baseDelay := 1 * time.Second
	maxDelay := 30 * time.Second
	circuitBreakerThreshold := 5
	circuitBreakerTimeout := 60 * time.Second
	
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic in consumer for topic %s: %v", c.topic, r)
			c.handleConsumerError(fmt.Errorf("panic: %v", r))
			// Restart consumer after a delay
			time.Sleep(5 * time.Second)
			go c.Start()
		}
	}()
	
	for {
		select {
		case <-c.ctx.Done():
			log.Printf("Consumer for topic %s stopped", c.topic)
			return
		default:
			// Check circuit breaker
			if c.circuitOpen {
				if time.Since(c.circuitOpenTime) > circuitBreakerTimeout {
					log.Printf("Circuit breaker timeout reached for topic %s, attempting to close", c.topic)
					c.circuitOpen = false
					c.consecutiveErrors = 0
				} else {
					time.Sleep(1 * time.Second)
					continue
				}
			}
			
			message, err := c.reader.ReadMessage(c.ctx)
			if err != nil {
				if err == context.Canceled {
					return
				}
				
				c.handleConsumerError(err)
				
				// Circuit breaker logic
				c.consecutiveErrors++
				c.lastErrorTime = time.Now()
				
				if c.consecutiveErrors >= circuitBreakerThreshold {
					log.Printf("Circuit breaker opened for topic %s after %d consecutive errors", 
						c.topic, c.consecutiveErrors)
					c.circuitOpen = true
					c.circuitOpenTime = time.Now()
					continue
				}
				
				retryCount++
				delay := c.calculateBackoffDelay(retryCount, baseDelay, maxDelay)
				
				log.Printf("Error reading message from topic %s (attempt %d/%d, consecutive errors: %d): %v", 
					c.topic, retryCount, maxRetries, c.consecutiveErrors, err)
				
				if retryCount >= maxRetries {
					log.Printf("Max retries reached for topic %s, recreating consumer", c.topic)
					if err := c.recreateReader(); err != nil {
						log.Printf("Failed to recreate reader for topic %s: %v", c.topic, err)
						time.Sleep(maxDelay)
					}
					retryCount = 0
				}
				
				time.Sleep(delay)
				continue
			}
			
			// Reset error counters on successful read
			retryCount = 0
			c.consecutiveErrors = 0
			c.circuitOpen = false
			
			c.processMessage(message)
		}
	}
}

// Stop gracefully stops the consumer
func (c *Consumer) Stop() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic stopping consumer for topic %s: %v", c.topic, r)
		}
	}()

	log.Printf("Stopping consumer for topic: %s", c.topic)
	
	// Cancel the context to stop the consumer loop
	if c.cancel != nil {
		c.cancel()
	}
	
	// Close the reader
	if c.reader != nil {
		if err := c.reader.Close(); err != nil {
			log.Printf("Error closing reader for topic %s: %v", c.topic, err)
		}
	}
	
	log.Printf("Consumer for topic %s stopped", c.topic)
}

// processMessage processes a Kafka message and routes it to WebSocket clients
func (c *Consumer) processMessage(message kafka.Message) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic processing message from topic %s: %v", c.topic, r)
		}
	}()

	log.Printf("Received message from topic %s: key=%s, partition=%d, offset=%d",
		c.topic, string(message.Key), message.Partition, message.Offset)
	
	// Validate message
	if len(message.Value) == 0 {
		log.Printf("Empty message received from topic %s", c.topic)
		return
	}
	
	// Parse the message data
	var messageData interface{}
	if err := json.Unmarshal(message.Value, &messageData); err != nil {
		log.Printf("Error unmarshaling message from topic %s: %v", c.topic, err)
		return
	}
	
	// Create WebSocket message based on topic
	var wsMessage *models.WebSocketMessage
	switch c.topic {
	case "token-info":
		wsMessage = models.NewWebSocketMessage("token_info", "dashboard", messageData)
	case "trade-info":
		wsMessage = models.NewWebSocketMessage("trade_data", "dashboard", messageData)
	case "aggregate-info":
		wsMessage = models.NewWebSocketMessage("aggregate_data", "dashboard", messageData)
	default:
		log.Printf("Unknown topic: %s", c.topic)
		return
	}
	
	// Convert to JSON
	jsonData, err := wsMessage.ToJSON()
	if err != nil {
		log.Printf("Error converting message to JSON for topic %s: %v", c.topic, err)
		return
	}
	
	// Broadcast to subscribed clients
	c.hubManager.BroadcastToChannel("dashboard", jsonData)
	
	log.Printf("Broadcasted message from topic %s to dashboard channel", c.topic)
}

// recreateReader recreates the Kafka reader in case of persistent errors
func (c *Consumer) recreateReader() error {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic recreating reader for topic %s: %v", c.topic, r)
		}
	}()

	log.Printf("Recreating Kafka reader for topic %s", c.topic)
	
	// Store current configuration before closing
	var brokers []string
	var groupID string
	
	if c.reader != nil {
		config := c.reader.Config()
		brokers = config.Brokers
		groupID = config.GroupID
		
		// Close existing reader
		if err := c.reader.Close(); err != nil {
			log.Printf("Error closing existing reader for topic %s: %v", c.topic, err)
		}
	}
	
	// Validate configuration
	if len(brokers) == 0 {
		return fmt.Errorf("no brokers available for topic %s", c.topic)
	}
	
	// Create new reader with same configuration
	c.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       c.topic,
		GroupID:     groupID,
		MinBytes:    10e3, // 10KB
		MaxBytes:    10e6, // 10MB
		MaxWait:     1 * time.Second,
		StartOffset: kafka.LastOffset,
	})
	
	log.Printf("Kafka reader recreated for topic %s", c.topic)
	return nil
}

// handleConsumerError categorizes and handles different types of consumer errors
func (c *Consumer) handleConsumerError(err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic in handleConsumerError for topic %s: %v", c.topic, r)
		}
	}()

	if err == nil {
		return
	}

	// Categorize error types
	errorType := "unknown"
	severity := "error"
	
	switch {
	case err == context.Canceled:
		errorType = "context_canceled"
		severity = "info"
	case err == context.DeadlineExceeded:
		errorType = "timeout"
		severity = "warning"
	case strings.Contains(fmt.Sprintf("%v", err), "no available brokers"):
		errorType = "no_brokers"
		severity = "critical"
	case strings.Contains(fmt.Sprintf("%v", err), "invalid topic"):
		errorType = "invalid_topic"
		severity = "critical"
	case strings.Contains(fmt.Sprintf("%v", err), "network"):
		errorType = "network"
		severity = "warning"
	default:
		if fmt.Sprintf("%v", err) == "EOF" {
			errorType = "connection_closed"
			severity = "warning"
		}
	}
	
	log.Printf("[%s] Consumer error for topic %s (type: %s): %v", 
		severity, c.topic, errorType, err)
	
	// Handle critical errors differently
	if severity == "critical" {
		log.Printf("Critical error detected for topic %s, forcing circuit breaker", c.topic)
		c.circuitOpen = true
		c.circuitOpenTime = time.Now()
		c.consecutiveErrors = 10 // Force circuit breaker
	}
}

// calculateBackoffDelay calculates exponential backoff delay with jitter
func (c *Consumer) calculateBackoffDelay(retryCount int, baseDelay, maxDelay time.Duration) time.Duration {
	// Exponential backoff: baseDelay * 2^retryCount
	delay := time.Duration(retryCount) * baseDelay
	if delay > maxDelay {
		delay = maxDelay
	}
	
	// Add jitter (±25% of delay)
	jitterFactor := float64(2*time.Now().UnixNano()%2 - 1) / 1e9
	jitter := time.Duration(float64(delay) * 0.25 * jitterFactor)
	delay += jitter
	
	if delay < baseDelay {
		delay = baseDelay
	}
	if delay > maxDelay {
		delay = maxDelay
	}
	
	return delay
}