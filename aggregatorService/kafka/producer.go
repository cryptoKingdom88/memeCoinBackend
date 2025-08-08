package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/cryptoKingdom88/memeCoinBackend/shared/packet"
	"aggregatorService/interfaces"
	"aggregatorService/logging"
)

const (
	TopicAggregateInfo = "aggregate-info"
)

// Producer implements the KafkaProducer interface
type Producer struct {
	brokers    []string
	writer     *kafka.Writer
	logger     *logging.Logger
	isRunning  bool
	mutex      sync.RWMutex
	stats      ProducerStats
}

// ProducerStats holds producer statistics
type ProducerStats struct {
	MessagesSent     int64     `json:"messages_sent"`
	MessagesError    int64     `json:"messages_error"`
	BytesSent        int64     `json:"bytes_sent"`
	LastSentTime     time.Time `json:"last_sent_time"`
	LastErrorTime    time.Time `json:"last_error_time"`
	LastError        string    `json:"last_error"`
	AverageLatency   float64   `json:"average_latency_ms"`
	TotalLatency     int64     `json:"total_latency_ms"`
}

// NewProducer creates a new Kafka producer
func NewProducer() interfaces.KafkaProducer {
	return &Producer{
		logger: logging.NewLogger("aggregator-service", "kafka-producer"),
	}
}

// Initialize initializes the Kafka producer
func (p *Producer) Initialize(brokers []string) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	if len(brokers) == 0 {
		return fmt.Errorf("brokers cannot be empty")
	}
	
	p.brokers = brokers
	
	// Create Kafka writer with optimized configuration
	p.writer = kafka.NewWriter(kafka.WriterConfig{
		Brokers:      brokers,
		Topic:        TopicAggregateInfo,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    50,                     // Batch size for better throughput
		BatchTimeout: 100 * time.Millisecond, // Batch timeout for low latency
		RequiredAcks: 1,                      // Require at least one ack
		Async:        false,                  // Synchronous for reliability
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			p.logger.Error("Kafka producer error", map[string]interface{}{
				"message": fmt.Sprintf(msg, args...),
			})
		}),
		Logger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			p.logger.Debug("Kafka producer info", map[string]interface{}{
				"message": fmt.Sprintf(msg, args...),
			})
		}),
	})
	
	p.isRunning = true
	
	p.logger.Info("Kafka producer initialized", map[string]interface{}{
		"brokers": brokers,
		"topic":   TopicAggregateInfo,
	})
	
	return nil
}

// SendAggregateData sends a single aggregate data to Kafka
func (p *Producer) SendAggregateData(ctx context.Context, aggregateData *packet.TokenAggregateData) error {
	if aggregateData == nil {
		return fmt.Errorf("aggregate data cannot be nil")
	}
	
	p.mutex.RLock()
	if !p.isRunning || p.writer == nil {
		p.mutex.RUnlock()
		return fmt.Errorf("producer is not initialized or not running")
	}
	writer := p.writer
	p.mutex.RUnlock()
	
	startTime := time.Now()
	
	// Marshal aggregate data to JSON
	messageBytes, err := json.Marshal(aggregateData)
	if err != nil {
		p.updateErrorStats(err)
		return fmt.Errorf("failed to marshal aggregate data: %w", err)
	}
	
	// Create Kafka message
	message := kafka.Message{
		Key:   []byte(aggregateData.Token), // Use token address as key for partitioning
		Value: messageBytes,
		Time:  time.Now(),
		Headers: []kafka.Header{
			{Key: "content-type", Value: []byte("application/json")},
			{Key: "version", Value: []byte(aggregateData.Version)},
			{Key: "token", Value: []byte(aggregateData.Token)},
		},
	}
	
	// Send message
	if err := writer.WriteMessages(ctx, message); err != nil {
		p.updateErrorStats(err)
		p.logger.Error("Failed to send aggregate data", map[string]interface{}{
			"token": aggregateData.Token,
			"error": err.Error(),
		})
		return fmt.Errorf("failed to send message to Kafka: %w", err)
	}
	
	// Update success stats
	latency := time.Since(startTime)
	p.updateSuccessStats(len(messageBytes), latency)
	
	p.logger.Debug("Aggregate data sent successfully", map[string]interface{}{
		"token":          aggregateData.Token,
		"data_points":    len(aggregateData.AggregateData),
		"message_size":   len(messageBytes),
		"latency_ms":     latency.Milliseconds(),
	})
	
	return nil
}

// SendBatchAggregateData sends multiple aggregate data in batch
func (p *Producer) SendBatchAggregateData(ctx context.Context, aggregateDataList []*packet.TokenAggregateData) error {
	if len(aggregateDataList) == 0 {
		return nil
	}
	
	p.mutex.RLock()
	if !p.isRunning || p.writer == nil {
		p.mutex.RUnlock()
		return fmt.Errorf("producer is not initialized or not running")
	}
	writer := p.writer
	p.mutex.RUnlock()
	
	startTime := time.Now()
	
	// Prepare batch messages
	messages := make([]kafka.Message, 0, len(aggregateDataList))
	totalBytes := 0
	
	for _, aggregateData := range aggregateDataList {
		if aggregateData == nil {
			continue
		}
		
		// Marshal aggregate data to JSON
		messageBytes, err := json.Marshal(aggregateData)
		if err != nil {
			p.logger.Warn("Failed to marshal aggregate data in batch", map[string]interface{}{
				"token": aggregateData.Token,
				"error": err.Error(),
			})
			continue
		}
		
		// Create Kafka message
		message := kafka.Message{
			Key:   []byte(aggregateData.Token),
			Value: messageBytes,
			Time:  time.Now(),
			Headers: []kafka.Header{
				{Key: "content-type", Value: []byte("application/json")},
				{Key: "version", Value: []byte(aggregateData.Version)},
				{Key: "token", Value: []byte(aggregateData.Token)},
			},
		}
		
		messages = append(messages, message)
		totalBytes += len(messageBytes)
	}
	
	if len(messages) == 0 {
		return fmt.Errorf("no valid messages to send")
	}
	
	// Send batch messages
	if err := writer.WriteMessages(ctx, messages...); err != nil {
		p.updateErrorStats(err)
		p.logger.Error("Failed to send batch aggregate data", map[string]interface{}{
			"batch_size": len(messages),
			"error":      err.Error(),
		})
		return fmt.Errorf("failed to send batch messages to Kafka: %w", err)
	}
	
	// Update success stats
	latency := time.Since(startTime)
	p.updateBatchSuccessStats(len(messages), totalBytes, latency)
	
	p.logger.Info("Batch aggregate data sent successfully", map[string]interface{}{
		"batch_size":   len(messages),
		"total_bytes":  totalBytes,
		"latency_ms":   latency.Milliseconds(),
	})
	
	return nil
}

// HealthCheck performs a health check on the producer
func (p *Producer) HealthCheck(ctx context.Context) error {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	
	if !p.isRunning {
		return fmt.Errorf("producer is not running")
	}
	
	if p.writer == nil {
		return fmt.Errorf("writer is not initialized")
	}
	
	// Check writer stats for errors
	stats := p.writer.Stats()
	if stats.Errors > 0 {
		return fmt.Errorf("producer has %d errors", stats.Errors)
	}
	
	return nil
}

// GetStats returns producer statistics
func (p *Producer) GetStats() map[string]interface{} {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	
	stats := map[string]interface{}{
		"is_running":        p.isRunning,
		"brokers":           p.brokers,
		"topic":             TopicAggregateInfo,
		"messages_sent":     p.stats.MessagesSent,
		"messages_error":    p.stats.MessagesError,
		"bytes_sent":        p.stats.BytesSent,
		"last_sent_time":    p.stats.LastSentTime,
		"last_error_time":   p.stats.LastErrorTime,
		"last_error":        p.stats.LastError,
		"average_latency":   p.stats.AverageLatency,
	}
	
	if p.writer != nil {
		writerStats := p.writer.Stats()
		stats["writer_stats"] = map[string]interface{}{
			"writes":       writerStats.Writes,
			"messages":     writerStats.Messages,
			"bytes":        writerStats.Bytes,
			"errors":       writerStats.Errors,
			"batch_time":   writerStats.BatchTime,
			"batch_queue":  writerStats.BatchQueueTime,
			"batch_size":   writerStats.BatchSize,
			"batch_bytes":  writerStats.BatchBytes,
			"write_time":   writerStats.WriteTime,
			"wait_time":    writerStats.WaitTime,
			"retries":      writerStats.Retries,
		}
	}
	
	return stats
}

// Close closes the producer
func (p *Producer) Close() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	if !p.isRunning {
		return nil // Already closed
	}
	
	p.isRunning = false
	
	if p.writer != nil {
		if err := p.writer.Close(); err != nil {
			p.logger.Error("Error closing Kafka writer", map[string]interface{}{
				"error": err.Error(),
			})
			return err
		}
	}
	
	p.logger.Info("Kafka producer closed", map[string]interface{}{
		"final_stats": p.GetStats(),
	})
	
	return nil
}

// updateSuccessStats updates statistics for successful message send
func (p *Producer) updateSuccessStats(messageSize int, latency time.Duration) {
	p.stats.MessagesSent++
	p.stats.BytesSent += int64(messageSize)
	p.stats.LastSentTime = time.Now()
	p.stats.TotalLatency += latency.Milliseconds()
	p.stats.AverageLatency = float64(p.stats.TotalLatency) / float64(p.stats.MessagesSent)
}

// updateBatchSuccessStats updates statistics for successful batch send
func (p *Producer) updateBatchSuccessStats(messageCount, totalBytes int, latency time.Duration) {
	p.stats.MessagesSent += int64(messageCount)
	p.stats.BytesSent += int64(totalBytes)
	p.stats.LastSentTime = time.Now()
	p.stats.TotalLatency += latency.Milliseconds()
	p.stats.AverageLatency = float64(p.stats.TotalLatency) / float64(p.stats.MessagesSent)
}

// updateErrorStats updates statistics for failed message send
func (p *Producer) updateErrorStats(err error) {
	p.stats.MessagesError++
	p.stats.LastErrorTime = time.Now()
	p.stats.LastError = err.Error()
}