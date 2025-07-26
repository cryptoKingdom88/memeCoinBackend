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

	conn, err := client.Connect(cfg.BitqueryAPIKey)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}

	// 쿼리 파일에서 로드
	newTokenQuery, err := client.LoadQuery("client/query/newToken.graphql")
	if err != nil {
		log.Fatalf("Failed to load transfers query: %v", err)
	}
	tokenTradeQuery, err := client.LoadQuery("client/query/tokenTrade.graphql")
	if err != nil {
		log.Fatalf("Failed to load dexTrades query: %v", err)
	}

	// 각각의 구독 시작
	id1, err := conn.Subscribe(newTokenQuery, client.TransfersHandler)
	if err != nil {
		log.Fatalf("Subscribe transfers error: %v", err)
	}
	id2, err := conn.Subscribe(tokenTradeQuery, client.DexTradesHandler)
	if err != nil {
		log.Fatalf("Subscribe dexTrades error: %v", err)
	}

	// 종료 시점에 unsubscribe
	defer conn.Unsubscribe(id1)
	defer conn.Unsubscribe(id2)

	// 무한 대기 루프
	for {
		time.Sleep(10 * time.Second)
	}
}
