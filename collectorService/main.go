package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/cryptoKingdom88/memeCoinBackend/collectorService/client"
	"github.com/cryptoKingdom88/memeCoinBackend/collectorService/config"
)

func main() {
	cfg := config.LoadConfig()
	log.Println("Load configuration is completed.")

	connNewToken, err := client.Connect(cfg.BitqueryAPIKey)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	connTokenTrade, err := client.Connect(cfg.BitqueryAPIKey)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}

	// Load from Query file
	newTokenQuery, err := client.LoadQuery("client/query/newToken.graphql")
	if err != nil {
		log.Fatalf("Failed to load transfers query: %v", err)
	}
	tokenTradeQuery, err := client.LoadQuery("client/query/tokenTrade.graphql")
	if err != nil {
		log.Fatalf("Failed to load dexTrades query: %v", err)
	}

	// Terminate Signal
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	// Channel for terminate subscribe
	doneNewToken := make(chan struct{})
	doneTokenTrade := make(chan struct{})

	// Start to subscribe
	_, err = connNewToken.Subscribe(newTokenQuery, func(msg []byte) {
		client.NewTokenHandler(msg)
	}, doneNewToken)
	if err != nil {
		log.Fatalf("Subscribe newToken error: %v", err)
	}
	_, err = connTokenTrade.Subscribe(tokenTradeQuery, func(msg []byte) {
		client.TokenTradeHandler(msg)
	}, doneTokenTrade)
	if err != nil {
		log.Fatalf("Subscribe tokenTrade error: %v", err)
	}

	log.Println("Collector Service was started. Press Ctrl+C to Exit")

	<-stop // Signal wait for terminate

	log.Println("Terminate, Closing...")

	// close subscribe goroutine
	close(doneNewToken)
	close(doneTokenTrade)

	// close websocket
	connNewToken.Close()
	connTokenTrade.Close()

	log.Println("All connection closed.")
}
