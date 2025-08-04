package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/cryptoKingdom88/memeCoinBackend/aggregatorService/aggregator"
	"github.com/cryptoKingdom88/memeCoinBackend/shared/packet"
	"github.com/segmentio/kafka-go"
)

const (
	TopicTradeInfo = "trade-info"
)

// StartTokenTradeConsumer starts consuming token trade messages and processes them through aggregation processor
func StartTokenTradeConsumer(ctx context.Context, broker string, processor *aggregator.Processor) {
	go func() {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:  []string{broker},
			GroupID:  "aggregator-trade-group", // Different group ID from dbSaveService
			Topic:    TopicTradeInfo,
			MinBytes: 1,
			MaxBytes: 10e6,
		})
		defer reader.Close()

		fmt.Printf("🚀 Aggregator consumer started for topic: %s\n", TopicTradeInfo)

		for {
			select {
			case <-ctx.Done():
				log.Printf("🛑 Stopping aggregator consumer for topic: %s", TopicTradeInfo)
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

				// Add to aggregation processor
				processor.AddTradeHistory(tradeHistory)
				log.Printf("✅ [%s] Trade history parsed and added to aggregation: %s",
					TopicTradeInfo, tradeHistory.TxHash)
			}
		}
	}()
}
