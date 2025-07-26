package main

import (
	"log"
	"time"

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

	// Load from Query file
	newTokenQuery, err := client.LoadQuery("client/query/newToken.graphql")
	if err != nil {
		log.Fatalf("Failed to load transfers query: %v", err)
	}

	// Start to subscribe
	connNewToken.Subscribe(newTokenQuery, client.TransfersHandler)

	connTokenTrade, err := client.Connect(cfg.BitqueryAPIKey)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}

	tokenTradeQuery, err := client.LoadQuery("client/query/tokenTrade.graphql")
	if err != nil {
		log.Fatalf("Failed to load dexTrades query: %v", err)
	}
	connTokenTrade.Subscribe(tokenTradeQuery, client.DexTradesHandler)
	// id1, err := conn.Subscribe(newTokenQuery, client.TransfersHandler)
	// if err != nil {
	// 	log.Fatalf("Subscribe transfers error: %v", err)
	// }
	// id2, err := conn.Subscribe(tokenTradeQuery, client.DexTradesHandler)
	// if err != nil {
	// 	log.Fatalf("Subscribe dexTrades error: %v", err)
	// }

	// defer conn.Unsubscribe(id1)
	// defer conn.Unsubscribe(id2)
	defer connNewToken.Close()
	defer connTokenTrade.Close()

	// Infinite loop
	for {
		time.Sleep(10 * time.Second)
	}
}
