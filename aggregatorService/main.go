package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/cryptoKingdom88/memeCoinBackend/aggregatorService/aggregator"
	"github.com/cryptoKingdom88/memeCoinBackend/aggregatorService/config"
	"github.com/cryptoKingdom88/memeCoinBackend/aggregatorService/kafka"
)

func main() {
	// Load configuration from environment variables
	cfg := config.LoadConfig()
	log.Println("✅ Configuration loaded successfully")

	// Initialize aggregation processor
	processor := aggregator.NewProcessor(cfg.AggregationInterval)

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())

	// Start aggregation processor
	go processor.Start(ctx)

	// Start Kafka consumer with aggregation processor
	kafka.StartTokenTradeConsumer(ctx, cfg.KafkaBrokers, processor)

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("🚀 Token Trade Aggregator Service started successfully")
	log.Printf("📊 Aggregation interval: %d seconds", cfg.AggregationInterval)
	log.Println("Press Ctrl+C to exit")

	// Wait for termination signal
	<-sigChan
	log.Println("🛑 Received termination signal")

	// Cancel context to stop all goroutines
	cancel()

	log.Println("🔄 Graceful shutdown completed")
	log.Println("✅ Token Trade Aggregator Service stopped")
}
