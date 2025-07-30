package kafka

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

const (
	TopicTokenInfo = "token-info"
	TopicTradeInfo = "trade-info"
)

func StartTokenInfoConsumer(ctx context.Context, broker string) {
	go func() {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:  []string{broker},
			GroupID:  "token-info-group",
			Topic:    TopicTokenInfo,
			MinBytes: 1,
			MaxBytes: 10e6,
		})
		defer reader.Close()

		fmt.Printf("🚀 Consumer started for topic: %s\n", TopicTokenInfo)

		for {
			select {
			case <-ctx.Done():
				log.Printf("🛑 Stopping consumer for topic: %s", TopicTokenInfo)
				return
			default:
				m, err := reader.ReadMessage(context.Background())
				if err != nil {
					log.Printf("❌ [%s] Error reading message: %v", TopicTokenInfo, err)
					continue
				}
				log.Printf("✅ [%s] Message received:\nKey: %s\nValue: %s", TopicTokenInfo, string(m.Key), string(m.Value))
			}
		}
	}()
}

func StartTokenTradeConsumer(ctx context.Context, broker string) {
	go func() {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:  []string{broker},
			GroupID:  "token-trade-group",
			Topic:    TopicTradeInfo,
			MinBytes: 1,
			MaxBytes: 10e6,
		})
		defer reader.Close()

		fmt.Printf("🚀 Consumer started for topic: %s\n", TopicTradeInfo)

		for {
			select {
			case <-ctx.Done():
				log.Printf("🛑 Stopping consumer for topic: %s", TopicTradeInfo)
				return
			default:
				m, err := reader.ReadMessage(context.Background())
				if err != nil {
					log.Printf("❌ [%s] Error reading message: %v", TopicTradeInfo, err)
					continue
				}
				log.Printf("✅ [%s] Message received:\nKey: %s\nValue: %s", TopicTradeInfo, string(m.Key), string(m.Value))
			}
		}
	}()
}
