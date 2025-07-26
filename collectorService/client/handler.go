package client

import (
	"encoding/json"
	"log"
)

func NewTokenHandler(msg []byte) {
	var result map[string]interface{}
	if err := json.Unmarshal(msg, &result); err != nil {
		log.Printf("NewTokenHandler JSON error: %v", err)
		return
	}
	log.Printf("[NewToken Created] %v", result)
}

func TokenTradeHandler(msg []byte) {
	var result map[string]interface{}
	if err := json.Unmarshal(msg, &result); err != nil {
		log.Printf("TokenTradeHandler JSON error: %v", err)
		return
	}
	log.Printf("[TokenTraded] %v", result)
}
