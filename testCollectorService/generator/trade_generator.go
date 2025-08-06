package generator

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/cryptoKingdom88/memeCoinBackend/shared/packet"
	"testCollectorService/config"
)

// TradeGenerator generates random trade data for testing
type TradeGenerator struct {
	config      *config.Config
	tokenAddresses []string
	walletAddresses []string
	rand        *rand.Rand
	
	// Price tracking for realistic price movements
	tokenPrices map[string]float64
}

// NewTradeGenerator creates a new trade generator
func NewTradeGenerator(cfg *config.Config) *TradeGenerator {
	// Create deterministic but varied token addresses
	tokenAddresses := make([]string, cfg.TokenCount)
	for i := 0; i < cfg.TokenCount; i++ {
		tokenAddresses[i] = fmt.Sprintf("0x%040x", i+1) // Generate token addresses like 0x0000...0001, 0x0000...0002, etc.
	}
	
	// Create a pool of wallet addresses for variety
	walletAddresses := make([]string, 100)
	for i := 0; i < 100; i++ {
		walletAddresses[i] = fmt.Sprintf("0x%040x", 1000+i) // Generate wallet addresses
	}
	
	// Initialize token prices
	tokenPrices := make(map[string]float64)
	source := rand.NewSource(time.Now().UnixNano())
	r := rand.New(source)
	
	for _, token := range tokenAddresses {
		// Initialize each token with a random price within range
		price := cfg.MinPrice + r.Float64()*(cfg.MaxPrice-cfg.MinPrice)
		tokenPrices[token] = price
	}
	
	return &TradeGenerator{
		config:          cfg,
		tokenAddresses:  tokenAddresses,
		walletAddresses: walletAddresses,
		rand:            r,
		tokenPrices:     tokenPrices,
	}
}

// GenerateBatch generates a batch of random trades
func (tg *TradeGenerator) GenerateBatch() []packet.TokenTradeHistory {
	var trades []packet.TokenTradeHistory
	
	for _, tokenAddress := range tg.tokenAddresses {
		// Generate random number of trades for this token (base ± variation)
		numTrades := tg.config.TradesPerToken + tg.rand.Intn(2*tg.config.TradeVariation+1) - tg.config.TradeVariation
		if numTrades < 1 {
			numTrades = 1
		}
		
		tokenTrades := tg.generateTokenTrades(tokenAddress, numTrades)
		trades = append(trades, tokenTrades...)
	}
	
	return trades
}

// generateTokenTrades generates trades for a specific token
func (tg *TradeGenerator) generateTokenTrades(tokenAddress string, count int) []packet.TokenTradeHistory {
	trades := make([]packet.TokenTradeHistory, count)
	currentPrice := tg.tokenPrices[tokenAddress]
	
	for i := 0; i < count; i++ {
		// Simulate realistic price movement (small random walk)
		priceChange := (tg.rand.Float64() - 0.5) * 0.1 // ±5% price change
		newPrice := currentPrice * (1 + priceChange)
		
		// Keep price within bounds
		if newPrice < tg.config.MinPrice {
			newPrice = tg.config.MinPrice
		}
		if newPrice > tg.config.MaxPrice {
			newPrice = tg.config.MaxPrice
		}
		
		currentPrice = newPrice
		
		// Determine buy or sell
		sellBuy := "sell"
		if tg.rand.Float64() < tg.config.BuyProbability {
			sellBuy = "buy"
		}
		
		// Generate random amounts
		nativeAmount := tg.config.MinAmount + tg.rand.Float64()*(tg.config.MaxAmount-tg.config.MinAmount)
		tokenAmount := nativeAmount / currentPrice
		
		// Random wallet
		wallet := tg.walletAddresses[tg.rand.Intn(len(tg.walletAddresses))]
		
		// Generate transaction hash
		txHash := fmt.Sprintf("0x%064x", tg.rand.Uint64())
		
		// Create trade with slight time variation (within last few seconds)
		tradeTime := time.Now().Add(-time.Duration(tg.rand.Intn(5)) * time.Second)
		
		trades[i] = packet.TokenTradeHistory{
			Token:        tokenAddress,
			Wallet:       wallet,
			SellBuy:      sellBuy,
			NativeAmount: fmt.Sprintf("%.6f", nativeAmount),
			TokenAmount:  fmt.Sprintf("%.6f", tokenAmount),
			PriceUsd:     fmt.Sprintf("%.6f", currentPrice),
			TransTime:    tradeTime.Format(time.RFC3339),
			TxHash:       txHash,
		}
	}
	
	// Update stored price for this token
	tg.tokenPrices[tokenAddress] = currentPrice
	
	return trades
}

// GetTokenAddresses returns the list of token addresses being used
func (tg *TradeGenerator) GetTokenAddresses() []string {
	return tg.tokenAddresses
}

// GetCurrentPrices returns current prices for all tokens
func (tg *TradeGenerator) GetCurrentPrices() map[string]float64 {
	prices := make(map[string]float64)
	for token, price := range tg.tokenPrices {
		prices[token] = price
	}
	return prices
}