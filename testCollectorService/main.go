package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"testCollectorService/config"
	"testCollectorService/generator"
	"testCollectorService/kafka"
)

func main() {
	log.Println("Starting Test Collector Service")
	
	// Load configuration
	cfg := config.LoadConfig()
	
	// Print configuration
	log.Printf("Configuration:")
	log.Printf("  Kafka Brokers: %v", cfg.KafkaBrokers)
	log.Printf("  Kafka Topic: %s", cfg.KafkaTopic)
	log.Printf("  Token Count: %d", cfg.TokenCount)
	log.Printf("  Trades Per Token: %d ± %d", cfg.TradesPerToken, cfg.TradeVariation)
	log.Printf("  Batch Interval: %s", cfg.BatchInterval)
	log.Printf("  Test Duration: %s", cfg.TestDuration)
	log.Printf("  Price Range: $%.4f - $%.4f", cfg.MinPrice, cfg.MaxPrice)
	log.Printf("  Amount Range: $%.2f - $%.2f", cfg.MinAmount, cfg.MaxAmount)
	log.Printf("  Buy Probability: %.1f%%", cfg.BuyProbability*100)
	
	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// Create trade generator
	tradeGen := generator.NewTradeGenerator(cfg)
	log.Printf("Initialized trade generator with %d tokens", len(tradeGen.GetTokenAddresses()))
	
	// Print token addresses
	log.Println("Token addresses:")
	for i, addr := range tradeGen.GetTokenAddresses() {
		log.Printf("  %d: %s", i+1, addr)
	}
	
	// Create Kafka producer
	producer := kafka.NewProducer(cfg.KafkaBrokers, cfg.KafkaTopic)
	defer producer.Close()
	
	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	// Start the test
	log.Println("Starting trade generation...")
	
	// Statistics tracking
	var totalTrades int64
	var totalBatches int64
	startTime := time.Now()
	
	// Create ticker for batch generation
	ticker := time.NewTicker(cfg.BatchInterval)
	defer ticker.Stop()
	
	// Create timer for test duration
	testTimer := time.NewTimer(cfg.TestDuration)
	defer testTimer.Stop()
	
	// Statistics ticker
	statsTicker := time.NewTicker(5 * time.Second)
	defer statsTicker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			log.Println("Context cancelled, stopping...")
			return
			
		case <-sigChan:
			log.Println("Received shutdown signal, stopping...")
			cancel()
			return
			
		case <-testTimer.C:
			log.Printf("Test duration completed (%s), stopping...", cfg.TestDuration)
			cancel()
			return
			
		case <-statsTicker.C:
			// Print statistics
			elapsed := time.Since(startTime)
			tradesPerSecond := float64(totalTrades) / elapsed.Seconds()
			batchesPerSecond := float64(totalBatches) / elapsed.Seconds()
			
			log.Printf("Statistics: %d trades in %d batches (%.1f trades/sec, %.1f batches/sec)", 
				totalTrades, totalBatches, tradesPerSecond, batchesPerSecond)
			
			// Print current token prices
			prices := tradeGen.GetCurrentPrices()
			log.Printf("Sample token prices:")
			count := 0
			for token, price := range prices {
				if count < 5 { // Show first 5 tokens
					log.Printf("  %s: $%.6f", token, price)
					count++
				}
			}
			
		case <-ticker.C:
			// Generate and send batch
			trades := tradeGen.GenerateBatch()
			
			if len(trades) > 0 {
				err := producer.SendTrades(ctx, trades)
				if err != nil {
					log.Printf("Error sending trades: %v", err)
					continue
				}
				
				totalTrades += int64(len(trades))
				totalBatches++
				
				// Log batch details
				tokenCounts := make(map[string]int)
				for _, trade := range trades {
					tokenCounts[trade.Token]++
				}
				
				log.Printf("Batch %d: %d trades across %d tokens", 
					totalBatches, len(trades), len(tokenCounts))
			}
		}
	}
}

// printFinalStats prints final statistics
func printFinalStats(totalTrades, totalBatches int64, startTime time.Time) {
	elapsed := time.Since(startTime)
	tradesPerSecond := float64(totalTrades) / elapsed.Seconds()
	batchesPerSecond := float64(totalBatches) / elapsed.Seconds()
	
	log.Println("=== Final Statistics ===")
	log.Printf("Total Runtime: %s", elapsed)
	log.Printf("Total Trades: %d", totalTrades)
	log.Printf("Total Batches: %d", totalBatches)
	log.Printf("Average Trades per Second: %.2f", tradesPerSecond)
	log.Printf("Average Batches per Second: %.2f", batchesPerSecond)
	log.Printf("Average Trades per Batch: %.2f", float64(totalTrades)/float64(totalBatches))
}