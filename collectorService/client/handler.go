package client

import (
	"encoding/json"
	"log"

	resp "github.com/cryptoKingdom88/memeCoinBackend/collectorService/client/response"
)

func NewTokenHandler(msg []byte) {
	var result resp.TokenSupplyUpdateRawResponse
	if err := json.Unmarshal(msg, &result); err != nil {
		log.Printf("NewTokenHandler JSON error: %v", err)
		return
	}
	log.Printf("[NewToken Created] %v", result)
}

func TokenTradeHandler(msg []byte) {
	var result resp.TokenTradeRawResponse
	if err := json.Unmarshal(msg, &result); err != nil {
		log.Printf("TokenTradeHandler JSON error: %v", err)
		return
	}
	log.Printf("[TokenTraded] %v", result)
}
