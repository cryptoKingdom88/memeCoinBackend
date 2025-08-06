package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/cryptoKingdom88/memeCoinBackend/shared/packet"
)

// Producer handles sending trade data to Kafka
type Producer struct {
	writer *kafka.Writer
	topic  string
}

// NewProducer creates a new Kafka producer
func NewProducer(brokers []string, topic string) *Producer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		Async:        false, // Synchronous for testing reliability
		BatchTimeout: 10 * time.Millisecond,
		BatchSize:    100,
	}
	
	return &Producer{
		writer: writer,
		topic:  topic,
	}
}

// SendTrades sends a batch of trades to Kafka
func (p *Producer) SendTrades(ctx context.Context, trades []packet.TokenTradeHistory) error {
	if len(trades) == 0 {
		return nil
	}
	
	// Convert trades to Kafka messages
	messages := make([]kafka.Message, len(trades))
	
	for i, trade := range trades {
		// Serialize trade data
		tradeData, err := json.Marshal(trade)
		if err != nil {
			return fmt.Errorf("failed to marshal trade data: %w", err)
		}
		
		// Parse time from string
		tradeTime, err := time.Parse(time.RFC3339, trade.TransTime)
		if err != nil {
			tradeTime = time.Now() // Fallback to current time
		}
		
		messages[i] = kafka.Message{
			Key:   []byte(trade.Token), // Use token address as key for partitioning
			Value: tradeData,
			Time:  tradeTime,
		}
	}
	
	// Send messages
	err := p.writer.WriteMessages(ctx, messages...)
	if err != nil {
		return fmt.Errorf("failed to write messages to Kafka: %w", err)
	}
	
	log.Printf("Successfully sent %d trades to Kafka topic '%s'", len(trades), p.topic)
	return nil
}

// Close closes the producer
func (p *Producer) Close() error {
	return p.writer.Close()
}

// GetStats returns producer statistics
func (p *Producer) GetStats() kafka.WriterStats {
	return p.writer.Stats()
}