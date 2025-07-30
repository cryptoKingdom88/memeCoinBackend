package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/cryptoKingdom88/memeCoinBackend/dbSaveService/config"
	"github.com/cryptoKingdom88/memeCoinBackend/dbSaveService/kafka"
)

func main() {
	cfg := config.LoadConfig()
	log.Println("Load configuration is completed.")

	ctx, cancel := context.WithCancel(context.Background())

	kafka.StartTokenInfoConsumer(ctx, cfg.KafkaBrokers)
	kafka.StartTokenTradeConsumer(ctx, cfg.KafkaBrokers)

	// Terminate Signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Println("DB Batch Save Service was started. Press Ctrl+C to Exit")

	<-sigChan // Signal wait for terminate
	cancel()  // Cancel context to stop consumers
	log.Println("Received termination signal")
	log.Println("Terminate, Closing...")
	log.Println("DB Batch Save Service was stopped.")
}
