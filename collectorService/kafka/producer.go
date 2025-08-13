package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	message "github.com/cryptoKingdom88/memeCoinBackend/shared/packet"

	"github.com/segmentio/kafka-go"
)

var writer *kafka.Writer

const (
	TopicTokenInfo = "token-info"
	TopicTradeInfo = "trade-info"
)

func Init(broker string) error {
	writer = kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{broker},
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    1,                     // No batch send
		BatchTimeout: 10 * time.Millisecond, // Minize send delay
	})

	log.Println("Kafka writer initialized")
	return nil
}

func Close() {
	if writer != nil {
		writer.Close()
		log.Println("Kafka writer closed")
	}
}

func SendKafkaMessage(topic string, value []byte) error {
	if writer == nil {
		log.Println("Need to initialize Kafka writer")
		return fmt.Errorf("kafka writer is not initialized")
	}
	msg := kafka.Message{
		Topic: topic,
		Value: value,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := writer.WriteMessages(ctx, msg)
	if err != nil {
		log.Printf("Kafka send error: %v", err)
	}
	return err
}

func ProduceTokenInfo(tokenInfo message.TokenInfo) {
	// Convert TokenInfo struct to JSON
	tokenInfoBytes, err := json.Marshal(tokenInfo)
	if err != nil {
		log.Printf("Failed to marshal TokenInfo: %v", err)
		return
	}

	// Send to Kafka
	SendKafkaMessage(TopicTokenInfo, tokenInfoBytes)
}

func ProduceTradeInfo(tradeInfo message.TokenTradeHistory) {
	// Convert TokenInfo struct to JSON
	tradeInfoBytes, err := json.Marshal(tradeInfo)
	if err != nil {
		log.Printf("Failed to marshal TradeInfo: %v", err)
		return
	}

	// Send to Kafka
	SendKafkaMessage(TopicTradeInfo, tradeInfoBytes)
}

// ProduceBatchTradeInfo sends multiple trades as a single message
func ProduceBatchTradeInfo(tradeInfos []message.TokenTradeHistory) {
	if len(tradeInfos) == 0 {
		return
	}

	// Convert slice to JSON
	tradeInfosBytes, err := json.Marshal(tradeInfos)
	if err != nil {
		log.Printf("Failed to marshal batch TradeInfo: %v", err)
		return
	}

	// Send to Kafka
	SendKafkaMessage(TopicTradeInfo, tradeInfosBytes)
	log.Printf("✅ Sent batch of %d trades to Kafka", len(tradeInfos))
}
