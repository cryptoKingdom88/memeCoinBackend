package models

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/cryptoKingdom88/memeCoinBackend/shared/packet"
)

// TradeData represents a single trade transaction
type TradeData struct {
	Token        string    `json:"token"`
	Wallet       string    `json:"wallet"`
	SellBuy      string    `json:"sell_buy"`      // "buy" or "sell"
	NativeAmount float64   `json:"native_amount"` // Volume in native currency
	TokenAmount  float64   `json:"token_amount"`  // Token quantity
	PriceUsd     float64   `json:"price_usd"`     // USD price
	TransTime    time.Time `json:"trans_time"`    // Transaction timestamp
	TxHash       string    `json:"tx_hash"`       // Transaction hash
}

// AggregateData represents aggregated metrics for a time window
type AggregateData struct {
	SellCount    int64     `json:"sell_count"`
	BuyCount     int64     `json:"buy_count"`
	SellVolume   float64   `json:"sell_volume"`   // USD volume
	BuyVolume    float64   `json:"buy_volume"`    // USD volume
	TotalVolume  float64   `json:"total_volume"`  // USD volume
	PriceChange  float64   `json:"price_change"`  // Price change %
	StartPrice   float64   `json:"start_price"`   // Window start price
	EndPrice     float64   `json:"end_price"`     // Current/end price
	LastUpdate   time.Time `json:"last_update"`   // Last calculation time
}

// TokenData represents all data for a specific token
type TokenData struct {
	TokenAddress string                    `json:"token_address"`
	Trades       []TradeData              `json:"trades"`
	Indices      map[string]int64         `json:"indices"`    // timeframe -> index
	Aggregates   map[string]*AggregateData `json:"aggregates"` // timeframe -> aggregate
	LastUpdate   time.Time                `json:"last_update"`
}

// ToJSON serializes the struct to JSON
func (td *TradeData) ToJSON() ([]byte, error) {
	return json.Marshal(td)
}

// FromJSON deserializes JSON to TradeData
func (td *TradeData) FromJSON(data []byte) error {
	return json.Unmarshal(data, td)
}

// ToJSON serializes the struct to JSON
func (ad *AggregateData) ToJSON() ([]byte, error) {
	return json.Marshal(ad)
}

// FromJSON deserializes JSON to AggregateData
func (ad *AggregateData) FromJSON(data []byte) error {
	return json.Unmarshal(data, ad)
}

// NewAggregateData creates a new AggregateData with zero values
func NewAggregateData() *AggregateData {
	return &AggregateData{
		SellCount:   0,
		BuyCount:    0,
		SellVolume:  0.0,
		BuyVolume:   0.0,
		TotalVolume: 0.0,
		PriceChange: 0.0,
		StartPrice:  0.0,
		EndPrice:    0.0,
		LastUpdate:  time.Now(),
	}
}

// Clone creates a deep copy of AggregateData
func (ad *AggregateData) Clone() *AggregateData {
	return &AggregateData{
		SellCount:   ad.SellCount,
		BuyCount:    ad.BuyCount,
		SellVolume:  ad.SellVolume,
		BuyVolume:   ad.BuyVolume,
		TotalVolume: ad.TotalVolume,
		PriceChange: ad.PriceChange,
		StartPrice:  ad.StartPrice,
		EndPrice:    ad.EndPrice,
		LastUpdate:  ad.LastUpdate,
	}
}

// AddTrade adds a trade to the aggregate data
func (ad *AggregateData) AddTrade(trade TradeData) {
	volume := trade.NativeAmount

	if trade.SellBuy == "sell" {
		ad.SellCount++
		ad.SellVolume += volume
	} else if trade.SellBuy == "buy" {
		ad.BuyCount++
		ad.BuyVolume += volume
	}

	ad.TotalVolume += volume
	ad.EndPrice = trade.PriceUsd

	// Set start price if not set
	if ad.StartPrice == 0 {
		ad.StartPrice = trade.PriceUsd
	}

	// Calculate price change percentage
	if ad.StartPrice > 0 {
		ad.PriceChange = ((ad.EndPrice - ad.StartPrice) / ad.StartPrice) * 100
	}

	ad.LastUpdate = time.Now()
}

// SubtractTrade removes a trade from the aggregate data
func (ad *AggregateData) SubtractTrade(trade TradeData) {
	volume := trade.NativeAmount

	if trade.SellBuy == "sell" {
		ad.SellCount--
		ad.SellVolume -= volume
	} else if trade.SellBuy == "buy" {
		ad.BuyCount--
		ad.BuyVolume -= volume
	}

	ad.TotalVolume -= volume

	// Note: Price range and start/end prices need special handling
	// This is a simplified version - full implementation would need
	// to recalculate these values from remaining trades
	ad.LastUpdate = time.Now()
}

// FromTokenTradeHistory converts from shared packet structure to TradeData
func (td *TradeData) FromTokenTradeHistory(trade packet.TokenTradeHistory) error {
	td.Token = trade.Token
	td.Wallet = trade.Wallet
	td.SellBuy = trade.SellBuy
	td.TxHash = trade.TxHash

	// Convert string amounts to float64
	nativeAmount, err := strconv.ParseFloat(trade.NativeAmount, 64)
	if err != nil {
		return err
	}
	td.NativeAmount = nativeAmount

	tokenAmount, err := strconv.ParseFloat(trade.TokenAmount, 64)
	if err != nil {
		return err
	}
	td.TokenAmount = tokenAmount

	priceUsd, err := strconv.ParseFloat(trade.PriceUsd, 64)
	if err != nil {
		return err
	}
	td.PriceUsd = priceUsd

	// Parse timestamp
	transTime, err := time.Parse(time.RFC3339, trade.TransTime)
	if err != nil {
		return err
	}
	td.TransTime = transTime

	return nil
}

// ToTokenTradeHistory converts TradeData to shared packet structure
func (td *TradeData) ToTokenTradeHistory() packet.TokenTradeHistory {
	return packet.TokenTradeHistory{
		Token:        td.Token,
		Wallet:       td.Wallet,
		SellBuy:      td.SellBuy,
		NativeAmount: strconv.FormatFloat(td.NativeAmount, 'f', -1, 64),
		TokenAmount:  strconv.FormatFloat(td.TokenAmount, 'f', -1, 64),
		PriceUsd:     strconv.FormatFloat(td.PriceUsd, 'f', -1, 64),
		TransTime:    td.TransTime.Format(time.RFC3339),
		TxHash:       td.TxHash,
	}
}

// SerializeIndices serializes indices map to JSON
func SerializeIndices(indices map[string]int64) ([]byte, error) {
	return json.Marshal(indices)
}

// DeserializeIndices deserializes JSON to indices map
func DeserializeIndices(data []byte) (map[string]int64, error) {
	var indices map[string]int64
	err := json.Unmarshal(data, &indices)
	return indices, err
}

// SerializeAggregates serializes aggregates map to JSON
func SerializeAggregates(aggregates map[string]*AggregateData) ([]byte, error) {
	return json.Marshal(aggregates)
}

// DeserializeAggregates deserializes JSON to aggregates map
func DeserializeAggregates(data []byte) (map[string]*AggregateData, error) {
	var aggregates map[string]*AggregateData
	err := json.Unmarshal(data, &aggregates)
	return aggregates, err
}

// ValidateTradeData validates trade data fields
func (td *TradeData) ValidateTradeData() error {
	if td.Token == "" {
		return fmt.Errorf("token address cannot be empty")
	}
	if td.Wallet == "" {
		return fmt.Errorf("wallet address cannot be empty")
	}
	if td.SellBuy != "buy" && td.SellBuy != "sell" {
		return fmt.Errorf("sell_buy must be 'buy' or 'sell', got: %s", td.SellBuy)
	}
	if td.NativeAmount < 0 {
		return fmt.Errorf("native amount cannot be negative: %f", td.NativeAmount)
	}
	if td.TokenAmount < 0 {
		return fmt.Errorf("token amount cannot be negative: %f", td.TokenAmount)
	}
	if td.PriceUsd < 0 {
		return fmt.Errorf("price USD cannot be negative: %f", td.PriceUsd)
	}
	if td.TransTime.IsZero() {
		return fmt.Errorf("transaction time cannot be zero")
	}
	if td.TxHash == "" {
		return fmt.Errorf("transaction hash cannot be empty")
	}
	return nil
}

// ValidateAggregateData validates aggregate data fields
func (ad *AggregateData) ValidateAggregateData() error {
	if ad.SellCount < 0 {
		return fmt.Errorf("sell count cannot be negative: %d", ad.SellCount)
	}
	if ad.BuyCount < 0 {
		return fmt.Errorf("buy count cannot be negative: %d", ad.BuyCount)
	}
	if ad.SellVolume < 0 {
		return fmt.Errorf("sell volume cannot be negative: %f", ad.SellVolume)
	}
	if ad.BuyVolume < 0 {
		return fmt.Errorf("buy volume cannot be negative: %f", ad.BuyVolume)
	}
	if ad.TotalVolume < 0 {
		return fmt.Errorf("total volume cannot be negative: %f", ad.TotalVolume)
	}
	return nil
}

// ParseAndValidateTradeFromKafka parses and validates trade data from Kafka message
func ParseAndValidateTradeFromKafka(kafkaMessage []byte) (*TradeData, error) {
	var tradeHistory packet.TokenTradeHistory
	
	// Parse JSON from Kafka message
	if err := json.Unmarshal(kafkaMessage, &tradeHistory); err != nil {
		return nil, fmt.Errorf("failed to parse Kafka message: %w", err)
	}
	
	// Convert to TradeData
	var trade TradeData
	if err := trade.FromTokenTradeHistory(tradeHistory); err != nil {
		return nil, fmt.Errorf("failed to convert trade data: %w", err)
	}
	
	// Validate trade data
	if err := trade.ValidateTradeData(); err != nil {
		return nil, fmt.Errorf("trade data validation failed: %w", err)
	}
	
	return &trade, nil
}

// BatchParseTradesFromKafka parses multiple trade messages from Kafka efficiently
func BatchParseTradesFromKafka(kafkaMessages [][]byte) ([]*TradeData, []error) {
	trades := make([]*TradeData, 0, len(kafkaMessages))
	errors := make([]error, 0)
	
	for i, message := range kafkaMessages {
		trade, err := ParseAndValidateTradeFromKafka(message)
		if err != nil {
			errors = append(errors, fmt.Errorf("message %d: %w", i, err))
			continue
		}
		trades = append(trades, trade)
	}
	
	return trades, errors
}

// IsExpiredTrade checks if a trade is expired based on the given time window
func (td *TradeData) IsExpiredTrade(currentTime time.Time, windowDuration time.Duration) bool {
	return currentTime.Sub(td.TransTime) > windowDuration
}

// GetTradeVolume returns the USD volume for this trade
func (td *TradeData) GetTradeVolume() float64 {
	return td.NativeAmount
}

// IsBuyTrade returns true if this is a buy trade
func (td *TradeData) IsBuyTrade() bool {
	return td.SellBuy == "buy"
}

// IsSellTrade returns true if this is a sell trade
func (td *TradeData) IsSellTrade() bool {
	return td.SellBuy == "sell"
}

// Reset resets aggregate data to zero values while preserving structure
func (ad *AggregateData) Reset() {
	ad.SellCount = 0
	ad.BuyCount = 0
	ad.SellVolume = 0.0
	ad.BuyVolume = 0.0
	ad.TotalVolume = 0.0
	ad.PriceChange = 0.0
	ad.StartPrice = 0.0
	ad.EndPrice = 0.0
	ad.LastUpdate = time.Now()
}

// IsEmpty returns true if the aggregate data has no trades
func (ad *AggregateData) IsEmpty() bool {
	return ad.SellCount == 0 && ad.BuyCount == 0
}

// GetTotalTradeCount returns the total number of trades (buy + sell)
func (ad *AggregateData) GetTotalTradeCount() int64 {
	return ad.SellCount + ad.BuyCount
}

// TimeWindowUtils provides utility functions for time window calculations
type TimeWindowUtils struct{}

// NewTimeWindowUtils creates a new TimeWindowUtils instance
func NewTimeWindowUtils() *TimeWindowUtils {
	return &TimeWindowUtils{}
}

// GetWindowBoundary calculates the start time boundary for a time window
func (twu *TimeWindowUtils) GetWindowBoundary(currentTime time.Time, windowDuration time.Duration) time.Time {
	return currentTime.Add(-windowDuration)
}

// IsWithinWindow checks if a timestamp is within the specified time window
func (twu *TimeWindowUtils) IsWithinWindow(timestamp, currentTime time.Time, windowDuration time.Duration) bool {
	boundary := twu.GetWindowBoundary(currentTime, windowDuration)
	return timestamp.After(boundary) || timestamp.Equal(boundary)
}

// IsExpired checks if a timestamp is expired (outside) the specified time window
func (twu *TimeWindowUtils) IsExpired(timestamp, currentTime time.Time, windowDuration time.Duration) bool {
	return !twu.IsWithinWindow(timestamp, currentTime, windowDuration)
}

// GetWindowStart returns the start time of the current window aligned to window boundaries
func (twu *TimeWindowUtils) GetWindowStart(currentTime time.Time, windowDuration time.Duration) time.Time {
	// Align to window boundaries (e.g., for 5min window, align to 00:00, 00:05, 00:10, etc.)
	switch windowDuration {
	case time.Minute:
		return currentTime.Truncate(time.Minute)
	case 5 * time.Minute:
		return currentTime.Truncate(5 * time.Minute)
	case 15 * time.Minute:
		return currentTime.Truncate(15 * time.Minute)
	case 30 * time.Minute:
		return currentTime.Truncate(30 * time.Minute)
	case time.Hour:
		return currentTime.Truncate(time.Hour)
	default:
		// For custom durations, truncate to the duration
		return currentTime.Truncate(windowDuration)
	}
}

// GetWindowEnd returns the end time of the current window
func (twu *TimeWindowUtils) GetWindowEnd(currentTime time.Time, windowDuration time.Duration) time.Time {
	windowStart := twu.GetWindowStart(currentTime, windowDuration)
	return windowStart.Add(windowDuration)
}

// CompareTimestamps compares two timestamps and returns:
// -1 if t1 is before t2
//  0 if t1 equals t2
//  1 if t1 is after t2
func (twu *TimeWindowUtils) CompareTimestamps(t1, t2 time.Time) int {
	if t1.Before(t2) {
		return -1
	}
	if t1.After(t2) {
		return 1
	}
	return 0
}

// GetTimeDifference returns the duration between two timestamps
func (twu *TimeWindowUtils) GetTimeDifference(t1, t2 time.Time) time.Duration {
	if t1.After(t2) {
		return t1.Sub(t2)
	}
	return t2.Sub(t1)
}

// IsTimestampRecent checks if a timestamp is within the recent threshold
func (twu *TimeWindowUtils) IsTimestampRecent(timestamp, currentTime time.Time, threshold time.Duration) bool {
	return twu.GetTimeDifference(timestamp, currentTime) <= threshold
}

// GetNextWindowBoundary calculates the next window boundary after the current time
func (twu *TimeWindowUtils) GetNextWindowBoundary(currentTime time.Time, windowDuration time.Duration) time.Time {
	windowStart := twu.GetWindowStart(currentTime, windowDuration)
	return windowStart.Add(windowDuration)
}

// GetPreviousWindowBoundary calculates the previous window boundary before the current time
func (twu *TimeWindowUtils) GetPreviousWindowBoundary(currentTime time.Time, windowDuration time.Duration) time.Time {
	windowStart := twu.GetWindowStart(currentTime, windowDuration)
	return windowStart.Add(-windowDuration)
}

// CalculateWindowsFromTrade determines which time windows a trade belongs to
func (twu *TimeWindowUtils) CalculateWindowsFromTrade(trade TradeData, currentTime time.Time, windowDurations []time.Duration) []time.Duration {
	validWindows := make([]time.Duration, 0, len(windowDurations))
	
	for _, duration := range windowDurations {
		if twu.IsWithinWindow(trade.TransTime, currentTime, duration) {
			validWindows = append(validWindows, duration)
		}
	}
	
	return validWindows
}

// GetWindowAge returns how long ago the window started relative to current time
func (twu *TimeWindowUtils) GetWindowAge(currentTime time.Time, windowDuration time.Duration) time.Duration {
	windowStart := twu.GetWindowStart(currentTime, windowDuration)
	return currentTime.Sub(windowStart)
}

// IsWindowComplete checks if a time window is complete (current time has passed the window end)
func (twu *TimeWindowUtils) IsWindowComplete(currentTime time.Time, windowDuration time.Duration) bool {
	windowEnd := twu.GetWindowEnd(currentTime, windowDuration)
	return currentTime.After(windowEnd) || currentTime.Equal(windowEnd)
}

// GetWindowProgress returns the progress of the current window as a percentage (0.0 to 1.0)
func (twu *TimeWindowUtils) GetWindowProgress(currentTime time.Time, windowDuration time.Duration) float64 {
	windowStart := twu.GetWindowStart(currentTime, windowDuration)
	windowEnd := windowStart.Add(windowDuration)
	
	if currentTime.Before(windowStart) {
		return 0.0
	}
	if currentTime.After(windowEnd) {
		return 1.0
	}
	
	elapsed := currentTime.Sub(windowStart)
	return float64(elapsed) / float64(windowDuration)
}