package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/cryptoKingdom88/memeCoinBackend/dbSaveService/batch"
	"github.com/cryptoKingdom88/memeCoinBackend/dbSaveService/config"
	"github.com/cryptoKingdom88/memeCoinBackend/dbSaveService/database"
	"github.com/cryptoKingdom88/memeCoinBackend/dbSaveService/kafka"
)

func main() {
	// Load configuration from environment variables
	cfg := config.LoadConfig()
	log.Println("✅ Configuration loaded successfully")

	// Initialize database connection
	db, err := database.NewDB(cfg)
	if err != nil {
		log.Fatalf("❌ Failed to initialize database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("❌ Error closing database: %v", err)
		}
	}()

	// Initialize batch processor
	processor := batch.NewProcessor(db, cfg.BatchInterval)

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())

	// Start batch processor
	go processor.Start(ctx)

	// Start Kafka consumers with batch processor
	kafka.StartTokenInfoConsumer(ctx, cfg.KafkaBrokers, processor)
	kafka.StartTokenTradeConsumer(ctx, cfg.KafkaBrokers, processor)

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("🚀 DB Batch Save Service started successfully")
	log.Printf("📊 Batch interval: %d seconds", cfg.BatchInterval)
	log.Println("Press Ctrl+C to exit")

	// Wait for termination signal
	<-sigChan
	log.Println("🛑 Received termination signal")
	
	// Cancel context to stop all goroutines
	cancel()
	
	log.Println("🔄 Graceful shutdown completed")
	log.Println("✅ DB Batch Save Service stopped")
}
