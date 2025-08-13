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
		defer func() {
			if r := recover(); r != nil {
				log.Printf("❌ Panic in token info consumer: %v", r)
			}
		}()

		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:  []string{broker},
			GroupID:  "token-info-group",
			Topic:    TopicTokenInfo,
			MinBytes: 1,
			MaxBytes: 10e6,
		})

		defer func() {
			log.Printf("🔄 Closing token info consumer reader")
			if err := reader.Close(); err != nil {
				log.Printf("❌ Error closing token info reader: %v", err)
			} else {
				log.Printf("✅ Token info consumer reader closed")
			}
		}()

		fmt.Printf("🚀 Consumer started for topic: %s\n", TopicTokenInfo)

		for {
			select {
			case <-ctx.Done():
				log.Printf("🛑 Stopping consumer for topic: %s", TopicTokenInfo)
				return
			default:
				// Use context for ReadMessage to allow cancellation
				m, err := reader.ReadMessage(ctx)
				if err != nil {
					if err == context.Canceled {
						log.Printf("🛑 Context cancelled for topic: %s", TopicTokenInfo)
						return
					}
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

				// Parse JSON message into TokenTradeHistory struct (supports both single and array)
				var trades []packet.TokenTradeHistory

				// Try to parse as single trade first
				var singleTrade packet.TokenTradeHistory
				if err := json.Unmarshal(m.Value, &singleTrade); err == nil {
					trades = []packet.TokenTradeHistory{singleTrade}
				} else {
					// Try to parse as array of trades
					if err := json.Unmarshal(m.Value, &trades); err != nil {
						log.Printf("❌ [%s] Failed to parse JSON message: %v\nMessage: %s",
							TopicTradeInfo, err, string(m.Value))
						continue
					}
				}

				// Add all trades to batch processor
				for _, tradeHistory := range trades {
					processor.AddTokenTradeHistory(tradeHistory)
					log.Printf("✅ [%s] Trade history parsed and added to batch: %s",
						TopicTradeInfo, tradeHistory.TxHash)
				}
			}
		}
	}()
}
