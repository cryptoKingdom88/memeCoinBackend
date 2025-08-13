package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/cryptoKingdom88/memeCoinBackend/shared/packet"
	"github.com/segmentio/kafka-go"
)

// Producer handles sending trade data to Kafka
type Producer struct {
	tradeWriter *kafka.Writer
	tokenWriter *kafka.Writer
	tradeTopic  string
	tokenTopic  string
}

// NewProducer creates a new Kafka producer
func NewProducer(brokers []string, tradeTopic, tokenTopic string) *Producer {
	tradeWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        tradeTopic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		Async:        false, // Synchronous for testing reliability
		BatchTimeout: 10 * time.Millisecond,
		BatchSize:    100,
	}

	tokenWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        tokenTopic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		Async:        false, // Synchronous for testing reliability
		BatchTimeout: 10 * time.Millisecond,
		BatchSize:    10,
	}

	return &Producer{
		tradeWriter: tradeWriter,
		tokenWriter: tokenWriter,
		tradeTopic:  tradeTopic,
		tokenTopic:  tokenTopic,
	}
}

// SendTrades sends a batch of trades as a single JSON array message to Kafka
func (p *Producer) SendTrades(ctx context.Context, trades []packet.TokenTradeHistory) error {
	if len(trades) == 0 {
		return nil
	}

	// Serialize entire batch as JSON array
	batchData, err := json.Marshal(trades)
	if err != nil {
		return fmt.Errorf("failed to marshal trade batch: %w", err)
	}

	// Use first trade's time as message time
	messageTime := time.Now()
	if len(trades) > 0 {
		if tradeTime, err := time.Parse(time.RFC3339, trades[0].TransTime); err == nil {
			messageTime = tradeTime
		}
	}

	// Create single message with entire batch
	message := kafka.Message{
		Key:   []byte("batch"), // Use "batch" as key for batch messages
		Value: batchData,
		Time:  messageTime,
	}

	// Send single message
	err = p.tradeWriter.WriteMessages(ctx, message)
	if err != nil {
		return fmt.Errorf("failed to write batch message to Kafka: %w", err)
	}

	log.Printf("✅ Successfully sent batch of %d trades to Kafka topic '%s'", len(trades), p.tradeTopic)
	return nil
}

// SendTokenInfo sends token launch information to Kafka
func (p *Producer) SendTokenInfo(ctx context.Context, tokenInfo *packet.TokenInfo) error {
	if tokenInfo == nil {
		return nil
	}

	// Serialize token info
	tokenData, err := json.Marshal(tokenInfo)
	if err != nil {
		return fmt.Errorf("failed to marshal token info: %w", err)
	}

	// Parse launch time
	launchTime, err := time.Parse(time.RFC3339, tokenInfo.CreateTime)
	if err != nil {
		launchTime = time.Now() // Fallback to current time
	}

	message := kafka.Message{
		Key:   []byte(tokenInfo.Token), // Use token address as key
		Value: tokenData,
		Time:  launchTime,
	}

	// Send message
	err = p.tokenWriter.WriteMessages(ctx, message)
	if err != nil {
		return fmt.Errorf("failed to write token info to Kafka: %w", err)
	}

	log.Printf("Successfully sent token launch info to Kafka topic '%s': %s (%s)",
		p.tokenTopic, tokenInfo.Name, tokenInfo.Symbol)
	return nil
}

// Close closes the producer
func (p *Producer) Close() error {
	err1 := p.tradeWriter.Close()
	err2 := p.tokenWriter.Close()

	if err1 != nil {
		return err1
	}
	return err2
}

// GetTradeStats returns trade producer statistics
func (p *Producer) GetTradeStats() kafka.WriterStats {
	return p.tradeWriter.Stats()
}

// GetTokenStats returns token producer statistics
func (p *Producer) GetTokenStats() kafka.WriterStats {
	return p.tokenWriter.Stats()
}
