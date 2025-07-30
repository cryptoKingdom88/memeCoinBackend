package client

import (
	"encoding/json"
	"log"
	"strconv"

	resp "github.com/cryptoKingdom88/memeCoinBackend/collectorService/client/response"
	"github.com/cryptoKingdom88/memeCoinBackend/collectorService/kafka"
	"github.com/cryptoKingdom88/memeCoinBackend/shared/packet"
)

func NewTokenHandler(msg []byte) {
	var result resp.TokenSupplyUpdateRawResponse
	if err := json.Unmarshal(msg, &result); err != nil {
		log.Printf("NewTokenHandler JSON error: %v", err)
		return
	}

	// Convert to TokenInfo struct for Kafka transfer
	for _, tokenUpdate := range result.Solana.TokenSupplyUpdates {
		tokenInfo := packet.TokenInfo{
			Token:       tokenUpdate.TokenSupplyUpdate.Currency.MintAddress,
			Symbol:      tokenUpdate.TokenSupplyUpdate.Currency.Symbol,
			Name:        tokenUpdate.TokenSupplyUpdate.Currency.Name,
			MetaInfo:    tokenUpdate.TokenSupplyUpdate.Currency.Uri,
			TotalSupply: tokenUpdate.TokenSupplyUpdate.Amount,
			IsAMM:       false, // Default value, can be updated based on additional logic
			CreateTime:  tokenUpdate.Block.Time,
		}

		// Log the converted struct
		log.Printf("[NewToken Created] %+v", tokenInfo)
		
		// Send tokenInfo to Kafka producer
		kafka.ProduceTokenInfo(tokenInfo)
	}
}

func TokenTradeHandler(msg []byte) {
	var result resp.TokenTradeRawResponse
	if err := json.Unmarshal(msg, &result); err != nil {
		log.Printf("TokenTradeHandler JSON error: %v", err)
		return
	}

	// make data to transfer by kafka
	for _, trade := range result.Solana.DEXTrades {
		if trade.Trade.Buy.Currency.Native || trade.Trade.Buy.Currency.Wrapped {
			tokenTrade := packet.TokenTradeHistory{
				Token:        trade.Trade.Sell.Currency.MintAddress,
				Wallet:       trade.Trade.Buy.Account.Owner,
				SellBuy:      trade.Instruction.Program.Method,
				NativeAmount: trade.Trade.Buy.Amount,
				TokenAmount:  trade.Trade.Sell.Amount,
				PriceUsd:     strconv.FormatFloat(trade.Trade.Sell.PriceInUSD, 'f', -1, 64),
				TransTime:    trade.Block.Time,
				TxHash:       trade.Transaction.Signature,
			}

			log.Printf("[Token Trade Info] %+v", tokenTrade)
			
			// Send to Kafka producer
			kafka.ProduceTradeInfo(tokenTrade)
		} else {
			tokenTrade := packet.TokenTradeHistory{
				Token:        trade.Trade.Buy.Currency.MintAddress,
				Wallet:       trade.Trade.Buy.Account.Owner,
				SellBuy:      trade.Instruction.Program.Method,
				NativeAmount: trade.Trade.Sell.Amount,
				TokenAmount:  trade.Trade.Buy.Amount,
				PriceUsd:     strconv.FormatFloat(trade.Trade.Buy.PriceInUSD, 'f', -1, 64),
				TransTime:    trade.Block.Time,
				TxHash:       trade.Transaction.Signature,
			}

			log.Printf("[Token Trade Info] %+v", tokenTrade)
			
			// Send to Kafka producer
			kafka.ProduceTradeInfo(tokenTrade)
		}
	}
}
