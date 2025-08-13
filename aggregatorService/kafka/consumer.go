package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"aggregatorService/interfaces"

	"github.com/cryptoKingdom88/memeCoinBackend/shared/packet"
	"github.com/segmentio/kafka-go"
)

// Consumer implements the KafkaConsumer interface
type Consumer struct {
	brokers       []string
	topic         string
	consumerGroup string
	reader        *kafka.Reader
	isRunning     bool
	stopChan      chan struct{}
	stopOnce      sync.Once
	wg            sync.WaitGroup
	mutex         sync.RWMutex
}

// NewConsumer creates a new Kafka consumer
func NewConsumer() interfaces.KafkaConsumer {
	return &Consumer{
		stopChan: make(chan struct{}),
	}
}

// Initialize initializes the consumer
func (c *Consumer) Initialize(brokers []string, topic, consumerGroup string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if len(brokers) == 0 {
		return fmt.Errorf("brokers cannot be empty")
	}
	if topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}
	if consumerGroup == "" {
		return fmt.Errorf("consumer group cannot be empty")
	}

	c.brokers = brokers
	c.topic = topic
	c.consumerGroup = consumerGroup

	// Create Kafka reader with immediate processing configuration
	c.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        consumerGroup,
		MinBytes:       1,                // Process immediately
		MaxBytes:       10e6,             // Maximum bytes to read (10MB)
		CommitInterval: 1 * time.Second,  // Frequent commits for immediate processing
		StartOffset:    kafka.LastOffset, // Start from latest offset
	})

	log.Printf("Kafka consumer initialized - brokers: %v, topic: %s, group: %s", brokers, topic, consumerGroup)
	return nil
}

// Start starts consuming messages
func (c *Consumer) Start(ctx context.Context, processor interfaces.BlockAggregator) error {
	c.mutex.Lock()
	if c.isRunning {
		c.mutex.Unlock()
		return fmt.Errorf("consumer is already running")
	}
	if c.reader == nil {
		c.mutex.Unlock()
		return fmt.Errorf("consumer not initialized")
	}
	if processor == nil {
		c.mutex.Unlock()
		return fmt.Errorf("processor cannot be nil")
	}

	c.isRunning = true
	c.mutex.Unlock()

	log.Printf("Starting Kafka consumer for topic: %s", c.topic)

	// Start consumer goroutine
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.consumeMessages(ctx, processor)
	}()

	return nil
}

// Stop stops consuming with graceful shutdown
func (c *Consumer) Stop() error {
	return c.StopWithTimeout(5 * time.Second) // Reduced timeout to match main shutdown
}

// StopWithTimeout stops consuming with a specified timeout for graceful shutdown
func (c *Consumer) StopWithTimeout(timeout time.Duration) error {
	c.mutex.Lock()
	if !c.isRunning {
		c.mutex.Unlock()
		return nil // Already stopped
	}
	c.isRunning = false
	c.mutex.Unlock()

	log.Printf("Stopping Kafka consumer with timeout: %v", timeout)

	// Signal stop using sync.Once to prevent panic from closing already closed channel
	c.stopOnce.Do(func() {
		close(c.stopChan)
	})

	// Wait for consumer goroutine to finish with timeout
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Printf("Consumer goroutine stopped gracefully")
	case <-time.After(timeout):
		log.Printf("Warning: Consumer shutdown timed out after %v", timeout)
		// Continue with cleanup even if timeout occurred
	}

	// Close reader
	if c.reader != nil {
		if err := c.reader.Close(); err != nil {
			log.Printf("Error closing Kafka reader: %v", err)
			return err
		}
		c.reader = nil
	}

	log.Printf("Kafka consumer stopped")
	return nil
}

// consumeMessages is the main message consumption loop - immediate processing
func (c *Consumer) consumeMessages(ctx context.Context, processor interfaces.BlockAggregator) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopChan:
			return
		default:
			// Read message immediately without batching
			message, err := c.reader.ReadMessage(ctx)
			if err != nil {
				if err == context.Canceled {
					return
				}
				// Add backoff to prevent tight error loop
				time.Sleep(time.Second)
				continue
			}

			// Process message immediately
			c.processMessage(ctx, message, processor)
		}
	}
}

// processMessage processes a single Kafka message immediately
func (c *Consumer) processMessage(ctx context.Context, message kafka.Message, processor interfaces.BlockAggregator) {
	// Parse JSON message into trade data
	trades, err := c.parseTradeMessage(message.Value)
	if err != nil {
		return // Skip invalid messages silently
	}

	if len(trades) == 0 {
		return
	}

	// 🎯 CORE LOG: Show received trade count immediately
	log.Printf("📥 Received %d trades from Kafka", len(trades))

	// Process trades through block aggregator
	if err := processor.ProcessTrades(ctx, trades); err != nil {
		log.Printf("❌ Error processing %d trades: %v", len(trades), err)
		return
	}

	// Commit message after successful processing
	if err := c.reader.CommitMessages(ctx, message); err != nil {
		log.Printf("❌ Error committing message: %v", err)
	}
}

// parseTradeMessage parses JSON message into trade data
func (c *Consumer) parseTradeMessage(messageData []byte) ([]packet.TokenTradeHistory, error) {
	if len(messageData) == 0 {
		return nil, fmt.Errorf("empty message data")
	}

	// Try to parse as single trade first
	var singleTrade packet.TokenTradeHistory
	if err := json.Unmarshal(messageData, &singleTrade); err == nil {
		// Validate single trade
		if err := c.validateTrade(singleTrade); err != nil {
			return nil, fmt.Errorf("invalid single trade: %w", err)
		}
		return []packet.TokenTradeHistory{singleTrade}, nil
	}

	// Try to parse as array of trades
	var trades []packet.TokenTradeHistory
	if err := json.Unmarshal(messageData, &trades); err != nil {
		return nil, fmt.Errorf("failed to parse as single trade or trade array: %w", err)
	}

	// Validate all trades
	validTrades := make([]packet.TokenTradeHistory, 0, len(trades))
	for i, trade := range trades {
		if err := c.validateTrade(trade); err != nil {
			log.Printf("Warning: skipping invalid trade at index %d: %v", i, err)
			continue
		}
		validTrades = append(validTrades, trade)
	}

	return validTrades, nil
}

// validateTrade validates a trade data structure
func (c *Consumer) validateTrade(trade packet.TokenTradeHistory) error {
	if trade.Token == "" {
		return fmt.Errorf("token address cannot be empty")
	}
	if trade.Wallet == "" {
		return fmt.Errorf("wallet address cannot be empty")
	}
	if trade.SellBuy != "buy" && trade.SellBuy != "sell" {
		return fmt.Errorf("sell_buy must be 'buy' or 'sell', got: %s", trade.SellBuy)
	}
	if trade.NativeAmount == "" {
		return fmt.Errorf("native amount cannot be empty")
	}
	if trade.TokenAmount == "" {
		return fmt.Errorf("token amount cannot be empty")
	}
	if trade.PriceUsd == "" {
		return fmt.Errorf("price USD cannot be empty")
	}
	if trade.TransTime == "" {
		return fmt.Errorf("transaction time cannot be empty")
	}
	if trade.TxHash == "" {
		return fmt.Errorf("transaction hash cannot be empty")
	}

	return nil
}

// IsRunning returns whether the consumer is currently running
func (c *Consumer) IsRunning() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.isRunning
}

// GetStats returns consumer statistics
func (c *Consumer) GetStats() map[string]interface{} {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	stats := map[string]interface{}{
		"is_running":     c.isRunning,
		"brokers":        c.brokers,
		"topic":          c.topic,
		"consumer_group": c.consumerGroup,
	}

	if c.reader != nil {
		readerStats := c.reader.Stats()
		stats["reader_stats"] = map[string]interface{}{
			"messages":       readerStats.Messages,
			"bytes":          readerStats.Bytes,
			"rebalances":     readerStats.Rebalances,
			"timeouts":       readerStats.Timeouts,
			"errors":         readerStats.Errors,
			"dial_time":      readerStats.DialTime,
			"read_time":      readerStats.ReadTime,
			"wait_time":      readerStats.WaitTime,
			"fetch_size":     readerStats.FetchSize,
			"fetch_bytes":    readerStats.FetchBytes,
			"offset":         readerStats.Offset,
			"lag":            readerStats.Lag,
			"min_bytes":      readerStats.MinBytes,
			"max_bytes":      readerStats.MaxBytes,
			"max_wait":       readerStats.MaxWait,
			"queue_length":   readerStats.QueueLength,
			"queue_capacity": readerStats.QueueCapacity,
		}
	}

	return stats
}

// GetConfig returns the current consumer configuration
func (c *Consumer) GetConfig() map[string]interface{} {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return map[string]interface{}{
		"brokers":        c.brokers,
		"topic":          c.topic,
		"consumer_group": c.consumerGroup,
		"is_running":     c.isRunning,
	}
}

// HealthCheck performs a health check on the consumer
func (c *Consumer) HealthCheck(ctx context.Context) error {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if !c.isRunning {
		return fmt.Errorf("consumer is not running")
	}

	if c.reader == nil {
		return fmt.Errorf("reader is not initialized")
	}

	// Check reader stats for errors
	stats := c.reader.Stats()
	if stats.Errors > 0 {
		return fmt.Errorf("consumer has %d errors", stats.Errors)
	}

	return nil
}

// GetLag returns the current consumer lag
func (c *Consumer) GetLag() int64 {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if c.reader == nil {
		return -1
	}

	stats := c.reader.Stats()
	return stats.Lag
}

// GetOffset returns the current consumer offset
func (c *Consumer) GetOffset() int64 {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if c.reader == nil {
		return -1
	}

	stats := c.reader.Stats()
	return stats.Offset
}
