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

	// Collect all token infos from this block
	var tokenInfos []packet.TokenInfo

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

		if tokenInfo.Symbol == "" || tokenInfo.Name == "" {
			log.Printf("TokenInfo Symbol or Name is empty, skipping...")
			continue
		}

		tokenInfos = append(tokenInfos, tokenInfo)
	}

	if len(tokenInfos) > 0 {
		log.Printf("📦 Block contains %d new tokens", len(tokenInfos))
		// Send token infos individually (not as batch)
		for _, tokenInfo := range tokenInfos {
			log.Printf("[NewToken Created] %+v", tokenInfo)
			kafka.ProduceTokenInfo(tokenInfo)
		}
	}
}

func TokenTradeHandler(msg []byte) {
	var result resp.TokenTradeRawResponse
	if err := json.Unmarshal(msg, &result); err != nil {
		log.Printf("TokenTradeHandler JSON error: %v", err)
		return
	}

	// Collect all trades from this block
	var blockTrades []packet.TokenTradeHistory

	// make data to transfer by kafka
	for _, trade := range result.Solana.DEXTrades {
		var tokenTrade packet.TokenTradeHistory

		if trade.Trade.Buy.Currency.Native || trade.Trade.Buy.Currency.Wrapped {
			tokenTrade = packet.TokenTradeHistory{
				Token:        trade.Trade.Sell.Currency.MintAddress,
				Wallet:       trade.Trade.Buy.Account.Owner,
				SellBuy:      trade.Instruction.Program.Method,
				NativeAmount: trade.Trade.Buy.Amount,
				TokenAmount:  trade.Trade.Sell.Amount,
				PriceUsd:     strconv.FormatFloat(trade.Trade.Sell.PriceInUSD, 'f', -1, 64),
				TransTime:    trade.Block.Time,
				TxHash:       trade.Transaction.Signature,
			}
		} else {
			tokenTrade = packet.TokenTradeHistory{
				Token:        trade.Trade.Buy.Currency.MintAddress,
				Wallet:       trade.Trade.Buy.Account.Owner,
				SellBuy:      trade.Instruction.Program.Method,
				NativeAmount: trade.Trade.Sell.Amount,
				TokenAmount:  trade.Trade.Buy.Amount,
				PriceUsd:     strconv.FormatFloat(trade.Trade.Buy.PriceInUSD, 'f', -1, 64),
				TransTime:    trade.Block.Time,
				TxHash:       trade.Transaction.Signature,
			}
		}

		blockTrades = append(blockTrades, tokenTrade)
	}

	if len(blockTrades) > 0 {
		log.Printf("📦 Block contains %d trades", len(blockTrades))
		// Send all trades from this block at once
		kafka.ProduceBatchTradeInfo(blockTrades)
	}
}
