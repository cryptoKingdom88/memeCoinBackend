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
	allTokenAddresses []string      // All possible token addresses
	activeTokens      []string      // Currently active (launched) tokens
	walletAddresses   []string
	rand              *rand.Rand
	
	// Price tracking for realistic price movements
	tokenPrices map[string]float64
	
	// Token launch tracking
	nextTokenIndex int
	tokenNames     []string
	tokenSymbols   []string
}

// NewTradeGenerator creates a new trade generator
func NewTradeGenerator(cfg *config.Config) *TradeGenerator {
	// Create deterministic but varied token addresses
	allTokenAddresses := make([]string, cfg.TokenCount)
	for i := 0; i < cfg.TokenCount; i++ {
		allTokenAddresses[i] = fmt.Sprintf("0x%040x", i+1) // Generate token addresses like 0x0000...0001, 0x0000...0002, etc.
	}
	
	// Create a pool of wallet addresses for variety
	walletAddresses := make([]string, 100)
	for i := 0; i < 100; i++ {
		walletAddresses[i] = fmt.Sprintf("0x%040x", 1000+i) // Generate wallet addresses
	}
	
	// Generate token names and symbols
	tokenNames := make([]string, cfg.TokenCount)
	tokenSymbols := make([]string, cfg.TokenCount)
	memeNames := []string{"Doge", "Pepe", "Shiba", "Moon", "Rocket", "Diamond", "Ape", "Cat", "Frog", "Lion", "Tiger", "Bear", "Bull", "Wolf", "Eagle", "Shark", "Whale", "Panda", "Koala", "Penguin"}
	memeSuffixes := []string{"Coin", "Token", "Inu", "Floki", "Elon", "Mars", "Safe", "Baby", "Mini", "Max", "Ultra", "Super", "Mega", "Giga", "Tera", "Alpha", "Beta", "Gamma", "Delta", "Omega"}
	
	source := rand.NewSource(time.Now().UnixNano())
	r := rand.New(source)
	
	for i := 0; i < cfg.TokenCount; i++ {
		name := memeNames[r.Intn(len(memeNames))]
		suffix := memeSuffixes[r.Intn(len(memeSuffixes))]
		tokenNames[i] = fmt.Sprintf("%s%s", name, suffix)
		tokenSymbols[i] = fmt.Sprintf("%s%s", name[:min(3, len(name))], suffix[:min(3, len(suffix))])
	}
	
	// Initialize with initial tokens only
	initialTokens := make([]string, cfg.InitialTokens)
	copy(initialTokens, allTokenAddresses[:cfg.InitialTokens])
	
	// Initialize token prices for initial tokens only
	tokenPrices := make(map[string]float64)
	for _, token := range initialTokens {
		// Initialize each token with a random price within range
		price := cfg.MinPrice + r.Float64()*(cfg.MaxPrice-cfg.MinPrice)
		tokenPrices[token] = price
	}
	
	return &TradeGenerator{
		config:            cfg,
		allTokenAddresses: allTokenAddresses,
		activeTokens:      initialTokens,
		walletAddresses:   walletAddresses,
		rand:              r,
		tokenPrices:       tokenPrices,
		nextTokenIndex:    cfg.InitialTokens,
		tokenNames:        tokenNames,
		tokenSymbols:      tokenSymbols,
	}
}

// min helper function
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// LaunchNewToken launches a new token if available
func (tg *TradeGenerator) LaunchNewToken() *packet.TokenInfo {
	if tg.nextTokenIndex >= len(tg.allTokenAddresses) {
		return nil // No more tokens to launch
	}
	
	tokenAddress := tg.allTokenAddresses[tg.nextTokenIndex]
	
	// Initialize price for the new token
	initialPrice := tg.config.MinPrice + tg.rand.Float64()*(tg.config.MaxPrice-tg.config.MinPrice)
	tg.tokenPrices[tokenAddress] = initialPrice
	
	// Add to active tokens
	tg.activeTokens = append(tg.activeTokens, tokenAddress)
	
	// Create token info
	tokenInfo := &packet.TokenInfo{
		Token:       tokenAddress,
		Name:        tg.tokenNames[tg.nextTokenIndex],
		Symbol:      tg.tokenSymbols[tg.nextTokenIndex],
		MetaInfo:    fmt.Sprintf("Initial price: $%.6f", initialPrice),
		TotalSupply: fmt.Sprintf("%.0f", 1000000000+tg.rand.Float64()*9000000000), // 1B-10B supply
		IsAMM:       true,
		CreateTime:  time.Now().Format(time.RFC3339),
	}
	
	tg.nextTokenIndex++
	return tokenInfo
}

// GenerateBatch generates a batch of random trades for active tokens only
func (tg *TradeGenerator) GenerateBatch() []packet.TokenTradeHistory {
	var trades []packet.TokenTradeHistory
	
	// Only generate trades for active (launched) tokens
	for _, tokenAddress := range tg.activeTokens {
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

// HasMoreTokensToLaunch returns true if there are more tokens to launch
func (tg *TradeGenerator) HasMoreTokensToLaunch() bool {
	return tg.nextTokenIndex < len(tg.allTokenAddresses)
}

// GetActiveTokenCount returns the number of currently active tokens
func (tg *TradeGenerator) GetActiveTokenCount() int {
	return len(tg.activeTokens)
}

// GetTotalTokenCount returns the total number of tokens configured
func (tg *TradeGenerator) GetTotalTokenCount() int {
	return len(tg.allTokenAddresses)
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

// GetTokenAddresses returns the list of active token addresses
func (tg *TradeGenerator) GetTokenAddresses() []string {
	return tg.activeTokens
}

// GetAllTokenAddresses returns all possible token addresses
func (tg *TradeGenerator) GetAllTokenAddresses() []string {
	return tg.allTokenAddresses
}

// GetCurrentPrices returns current prices for all tokens
func (tg *TradeGenerator) GetCurrentPrices() map[string]float64 {
	prices := make(map[string]float64)
	for token, price := range tg.tokenPrices {
		prices[token] = price
	}
	return prices
}