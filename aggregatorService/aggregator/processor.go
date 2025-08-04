package aggregator

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/cryptoKingdom88/memeCoinBackend/shared/packet"
)

// TradeAggregation represents aggregated trade data for a token
type TradeAggregation struct {
	Token        string  // Token address
	TotalVolume  float64 // Total trading volume in USD
	BuyVolume    float64 // Total buy volume in USD
	SellVolume   float64 // Total sell volume in USD
	TradeCount   int     // Number of trades
	BuyCount     int     // Number of buy trades
	SellCount    int     // Number of sell trades
	LastPrice    float64 // Last trade price in USD
	HighPrice    float64 // Highest price in the period
	LowPrice     float64 // Lowest price in the period
	StartTime    time.Time // Aggregation period start time
	EndTime      time.Time // Aggregation period end time
}

// Processor handles aggregation of token trade data
type Processor struct {
	interval time.Duration
	
	// Trade data buffer for aggregation
	tradeBuffer []packet.TokenTradeHistory
	
	// Mutex for thread-safe operations
	tradeMutex sync.Mutex
}

// NewProcessor creates a new aggregation processor
func NewProcessor(intervalSeconds int) *Processor {
	return &Processor{
		interval:    time.Duration(intervalSeconds) * time.Second,
		tradeBuffer: make([]packet.TokenTradeHistory, 0),
	}
}

// Start begins the aggregation processing routine
func (p *Processor) Start(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	
	log.Printf("🚀 Aggregation processor started with interval: %v", p.interval)
	
	for {
		select {
		case <-ctx.Done():
			log.Println("🛑 Stopping aggregation processor")
			// Process remaining data before stopping
			p.processAggregation()
			return
		case <-ticker.C:
			p.processAggregation()
		}
	}
}

// AddTradeHistory adds trade history to the buffer for aggregation
func (p *Processor) AddTradeHistory(tradeHistory packet.TokenTradeHistory) {
	p.tradeMutex.Lock()
	defer p.tradeMutex.Unlock()
	
	p.tradeBuffer = append(p.tradeBuffer, tradeHistory)
	log.Printf("📝 Added trade history to aggregation buffer. Buffer size: %d", len(p.tradeBuffer))
}

// processAggregation processes all buffered trade data and generates aggregations
func (p *Processor) processAggregation() {
	// Get and clear buffer
	p.tradeMutex.Lock()
	tradeBatch := make([]packet.TokenTradeHistory, len(p.tradeBuffer))
	copy(tradeBatch, p.tradeBuffer)
	p.tradeBuffer = p.tradeBuffer[:0] // Clear buffer
	p.tradeMutex.Unlock()
	
	if len(tradeBatch) == 0 {
		return
	}
	
	// Group trades by token
	tokenTrades := make(map[string][]packet.TokenTradeHistory)
	for _, trade := range tradeBatch {
		tokenTrades[trade.Token] = append(tokenTrades[trade.Token], trade)
	}
	
	// Generate aggregations for each token
	aggregations := make([]TradeAggregation, 0, len(tokenTrades))
	now := time.Now()
	startTime := now.Add(-p.interval)
	
	for token, trades := range tokenTrades {
		aggregation := p.calculateAggregation(token, trades, startTime, now)
		aggregations = append(aggregations, aggregation)
	}
	
	// Process aggregations (for now just log them)
	p.outputAggregations(aggregations)
	
	log.Printf("🔄 Aggregation processing completed - Processed %d trades for %d tokens", 
		len(tradeBatch), len(aggregations))
}

// calculateAggregation calculates aggregation metrics for a token's trades
func (p *Processor) calculateAggregation(token string, trades []packet.TokenTradeHistory, startTime, endTime time.Time) TradeAggregation {
	aggregation := TradeAggregation{
		Token:     token,
		StartTime: startTime,
		EndTime:   endTime,
		LowPrice:  999999999, // Initialize with high value
	}
	
	for _, trade := range trades {
		// Parse price (assuming it's a string representation of float)
		price := parseFloat(trade.PriceUsd)
		volume := parseFloat(trade.NativeAmount) // Using native amount as volume proxy
		
		// Update aggregation metrics
		aggregation.TotalVolume += volume
		aggregation.TradeCount++
		aggregation.LastPrice = price
		
		// Update high/low prices
		if price > aggregation.HighPrice {
			aggregation.HighPrice = price
		}
		if price < aggregation.LowPrice && price > 0 {
			aggregation.LowPrice = price
		}
		
		// Count buy/sell trades
		if trade.SellBuy == "buy" {
			aggregation.BuyVolume += volume
			aggregation.BuyCount++
		} else if trade.SellBuy == "sell" {
			aggregation.SellVolume += volume
			aggregation.SellCount++
		}
	}
	
	return aggregation
}

// outputAggregations outputs the calculated aggregations (placeholder for future implementation)
func (p *Processor) outputAggregations(aggregations []TradeAggregation) {
	for _, agg := range aggregations {
		log.Printf("📊 Token Aggregation - Token: %s, Volume: %.2f, Trades: %d, Price: %.6f", 
			agg.Token, agg.TotalVolume, agg.TradeCount, agg.LastPrice)
	}
}

// parseFloat safely parses string to float64
func parseFloat(s string) float64 {
	// Simple implementation - in production, use strconv.ParseFloat with error handling
	if s == "" {
		return 0
	}
	// For now, return 0 - implement proper parsing later
	return 0
}