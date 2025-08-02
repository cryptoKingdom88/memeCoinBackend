package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/cryptoKingdom88/memeCoinBackend/dbSaveService/batch"
	"github.com/cryptoKingdom88/memeCoinBackend/shared/packet"
	"github.com/segmentio/kafka-go"
)

const (
	TopicTokenInfo = "token-info"
	TopicTradeInfo = "trade-info"
)

// StartTokenInfoConsumer starts consuming token info messages and processes them through batch processor
func StartTokenInfoConsumer(ctx context.Context, broker string, processor *batch.Processor) {
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
				
				// Parse JSON message into TokenInfo struct
				var tokenInfo packet.TokenInfo
				if err := json.Unmarshal(m.Value, &tokenInfo); err != nil {
					log.Printf("❌ [%s] Failed to parse JSON message: %v\nMessage: %s", 
						TopicTokenInfo, err, string(m.Value))
					continue
				}
				
				// Add to batch processor
				processor.AddTokenInfo(tokenInfo)
				log.Printf("✅ [%s] Token info parsed and added to batch: %s", 
					TopicTokenInfo, tokenInfo.Token)
			}
		}
	}()
}

// StartTokenTradeConsumer starts consuming token trade messages and processes them through batch processor
func StartTokenTradeConsumer(ctx context.Context, broker string, processor *batch.Processor) {
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
				
				// Parse JSON message into TokenTradeHistory struct
				var tradeHistory packet.TokenTradeHistory
				if err := json.Unmarshal(m.Value, &tradeHistory); err != nil {
					log.Printf("❌ [%s] Failed to parse JSON message: %v\nMessage: %s", 
						TopicTradeInfo, err, string(m.Value))
					continue
				}
				
				// Add to batch processor
				processor.AddTokenTradeHistory(tradeHistory)
				log.Printf("✅ [%s] Trade history parsed and added to batch: %s", 
					TopicTradeInfo, tradeHistory.TxHash)
			}
		}
	}()
}
