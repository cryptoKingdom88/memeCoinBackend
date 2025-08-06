package processor

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"aggregatorService/config"
	"aggregatorService/models"
)

// MockRedisManager for testing
type MockRedisManager struct {
	tokenData    map[string]*models.TokenData
	lastUpdates  map[string]time.Time
	mutex        sync.RWMutex
	
	// Track method calls
	updateTokenDataCalled bool
	getTokenDataCalled    bool
	updateLastUpdateCalled bool
}

func NewMockRedisManager() *MockRedisManager {
	return &MockRedisManager{
		tokenData:   make(map[string]*models.TokenData),
		lastUpdates: make(map[string]time.Time),
	}
}

func (m *MockRedisManager) Connect(ctx context.Context) error { return nil }
func (m *MockRedisManager) Close() error { return nil }
func (m *MockRedisManager) Ping(ctx context.Context) error { return nil }

func (m *MockRedisManager) UpdateTokenData(ctx context.Context, tokenAddress string, data *models.TokenData) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.updateTokenDataCalled = true
	m.tokenData[tokenAddress] = data
	return nil
}

func (m *MockRedisManager) GetTokenData(ctx context.Context, tokenAddress string) (*models.TokenData, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	m.getTokenDataCalled = true
	
	if data, exists := m.tokenData[tokenAddress]; exists {
		return data, nil
	}
	
	// Return empty token data if not found
	return &models.TokenData{
		TokenAddress: tokenAddress,
		Trades:       []models.TradeData{},
		Indices:      make(map[string]int64),
		Aggregates:   make(map[string]*models.AggregateData),
		LastUpdate:   time.Time{},
	}, nil
}

func (m *MockRedisManager) UpdateLastUpdate(ctx context.Context, tokenAddress string, timestamp time.Time) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.updateLastUpdateCalled = true
	m.lastUpdates[tokenAddress] = timestamp
	return nil
}

func (m *MockRedisManager) GetLastUpdate(ctx context.Context, tokenAddress string) (time.Time, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	if lastUpdate, exists := m.lastUpdates[tokenAddress]; exists {
		return lastUpdate, nil
	}
	return time.Time{}, nil
}

// Implement remaining interface methods (not used in these tests)
func (m *MockRedisManager) AddTrade(ctx context.Context, tokenAddress string, trade models.TradeData) error { return nil }
func (m *MockRedisManager) GetTrades(ctx context.Context, tokenAddress string, start, end int64) ([]models.TradeData, error) { return nil, nil }
func (m *MockRedisManager) SetIndex(ctx context.Context, tokenAddress, timeframe string, index int64) error { return nil }
func (m *MockRedisManager) GetIndex(ctx context.Context, tokenAddress, timeframe string) (int64, error) { return 0, nil }
func (m *MockRedisManager) SetAggregate(ctx context.Context, tokenAddress, timeframe string, aggregate *models.AggregateData) error { return nil }
func (m *MockRedisManager) GetAggregate(ctx context.Context, tokenAddress, timeframe string) (*models.AggregateData, error) { return nil, nil }
func (m *MockRedisManager) SetTTL(ctx context.Context, tokenAddress string, duration time.Duration) error { return nil }
func (m *MockRedisManager) GetActiveTokens(ctx context.Context) ([]string, error) { return nil, nil }
func (m *MockRedisManager) ExecuteAtomicUpdate(ctx context.Context, script string, keys []string, args []interface{}) error { return nil }

// MockSlidingWindowCalculator for testing
type MockSlidingWindowCalculator struct {
	timeWindows []config.TimeWindow
	
	// Track method calls
	updateWindowsCalled bool
	calculateWindowCalled bool
}

func NewMockSlidingWindowCalculator() *MockSlidingWindowCalculator {
	return &MockSlidingWindowCalculator{
		timeWindows: []config.TimeWindow{
			{Duration: 1 * time.Minute, Name: "1min"},
			{Duration: 5 * time.Minute, Name: "5min"},
			{Duration: 15 * time.Minute, Name: "15min"},
		},
	}
}

func (m *MockSlidingWindowCalculator) Initialize(timeWindows []config.TimeWindow) error {
	m.timeWindows = timeWindows
	return nil
}

func (m *MockSlidingWindowCalculator) UpdateWindows(ctx context.Context, trades []models.TradeData, indices map[string]int64, aggregates map[string]*models.AggregateData, newTrade models.TradeData) (map[string]int64, map[string]*models.AggregateData, error) {
	m.updateWindowsCalled = true
	
	// Simple mock implementation
	newIndices := make(map[string]int64)
	newAggregates := make(map[string]*models.AggregateData)
	
	for _, window := range m.timeWindows {
		// Copy existing or create new
		if idx, exists := indices[window.Name]; exists {
			newIndices[window.Name] = idx
		} else {
			newIndices[window.Name] = 0
		}
		
		if agg, exists := aggregates[window.Name]; exists {
			newAggregates[window.Name] = agg.Clone()
		} else {
			newAggregates[window.Name] = models.NewAggregateData()
		}
		
		// Add the new trade to aggregates
		newAggregates[window.Name].AddTrade(newTrade)
	}
	
	return newIndices, newAggregates, nil
}

func (m *MockSlidingWindowCalculator) CalculateWindow(ctx context.Context, trades []models.TradeData, timeWindow config.TimeWindow, currentTime time.Time) (*models.AggregateData, int64, error) {
	m.calculateWindowCalled = true
	
	aggregate := models.NewAggregateData()
	var index int64 = 0
	
	// Simple calculation: count trades within the time window
	cutoffTime := currentTime.Add(-timeWindow.Duration)
	
	for i, trade := range trades {
		if trade.TransTime.After(cutoffTime) {
			aggregate.AddTrade(trade)
			if index == 0 {
				index = int64(i)
			}
		}
	}
	
	return aggregate, index, nil
}

func (m *MockSlidingWindowCalculator) GetAllTimeWindowNames() []string {
	names := make([]string, len(m.timeWindows))
	for i, window := range m.timeWindows {
		names[i] = window.Name
	}
	return names
}

func (m *MockSlidingWindowCalculator) GetTimeWindowByName(name string) (config.TimeWindow, bool) {
	for _, window := range m.timeWindows {
		if window.Name == name {
			return window, true
		}
	}
	return config.TimeWindow{}, false
}

func TestTokenProcessor_Initialize(t *testing.T) {
	processor := NewTokenProcessor()
	mockRedis := NewMockRedisManager()
	mockCalculator := NewMockSlidingWindowCalculator()
	
	tokenAddress := "0x123456789"
	
	// Test successful initialization
	err := processor.Initialize(tokenAddress, mockRedis, mockCalculator)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	
	// Verify processor is running
	if !processor.IsRunning() {
		t.Error("Expected processor to be running after initialization")
	}
	
	// Verify token address is set
	if processor.GetTokenAddress() != tokenAddress {
		t.Errorf("Expected token address %s, got %s", tokenAddress, processor.GetTokenAddress())
	}
	
	// Test double initialization
	err = processor.Initialize(tokenAddress, mockRedis, mockCalculator)
	if err == nil {
		t.Error("Expected error for double initialization")
	}
	
	// Cleanup
	processor.Shutdown(context.Background())
}

func TestTokenProcessor_ProcessTrade(t *testing.T) {
	processor := NewTokenProcessor()
	mockRedis := NewMockRedisManager()
	mockCalculator := NewMockSlidingWindowCalculator()
	
	tokenAddress := "0x123456789"
	
	// Initialize processor
	err := processor.Initialize(tokenAddress, mockRedis, mockCalculator)
	if err != nil {
		t.Fatalf("Failed to initialize processor: %v", err)
	}
	defer processor.Shutdown(context.Background())
	
	// Create test trade
	trade := models.TradeData{
		Token:        tokenAddress,
		Wallet:       "0xabc",
		SellBuy:      "buy",
		NativeAmount: 100.0,
		TokenAmount:  1000.0,
		PriceUsd:     0.1,
		TransTime:    time.Now(),
		TxHash:       "0x1",
	}
	
	// Test processing trade
	ctx := context.Background()
	err = processor.ProcessTrade(ctx, trade)
	if err != nil {
		t.Errorf("Expected no error processing trade, got: %v", err)
	}
	
	// Wait a bit for async processing
	time.Sleep(100 * time.Millisecond)
	
	// Verify calculator was called
	if !mockCalculator.updateWindowsCalled {
		t.Error("Expected UpdateWindows to be called on calculator")
	}
	
	// Verify trade count
	if processor.GetTradeCount() != 1 {
		t.Errorf("Expected trade count 1, got %d", processor.GetTradeCount())
	}
	
	// Test processing trade for wrong token
	wrongTrade := trade
	wrongTrade.Token = "0xwrong"
	
	err = processor.ProcessTrade(ctx, wrongTrade)
	if err == nil {
		t.Error("Expected error for trade with wrong token address")
	}
	
	// Test processing invalid trade
	invalidTrade := trade
	invalidTrade.SellBuy = "invalid"
	
	err = processor.ProcessTrade(ctx, invalidTrade)
	if err == nil {
		t.Error("Expected error for invalid trade")
	}
}

func TestTokenProcessor_GetAggregates(t *testing.T) {
	processor := NewTokenProcessor()
	mockRedis := NewMockRedisManager()
	mockCalculator := NewMockSlidingWindowCalculator()
	
	tokenAddress := "0x123456789"
	
	// Initialize processor
	err := processor.Initialize(tokenAddress, mockRedis, mockCalculator)
	if err != nil {
		t.Fatalf("Failed to initialize processor: %v", err)
	}
	defer processor.Shutdown(context.Background())
	
	// Process a trade to generate aggregates
	trade := models.TradeData{
		Token:        tokenAddress,
		Wallet:       "0xabc",
		SellBuy:      "buy",
		NativeAmount: 100.0,
		TokenAmount:  1000.0,
		PriceUsd:     0.1,
		TransTime:    time.Now(),
		TxHash:       "0x1",
	}
	
	ctx := context.Background()
	err = processor.ProcessTrade(ctx, trade)
	if err != nil {
		t.Errorf("Expected no error processing trade, got: %v", err)
	}
	
	// Wait for processing
	time.Sleep(100 * time.Millisecond)
	
	// Get aggregates
	aggregates, err := processor.GetAggregates(ctx)
	if err != nil {
		t.Errorf("Expected no error getting aggregates, got: %v", err)
	}
	
	// Verify aggregates exist for expected timeframes
	expectedTimeframes := []string{"1min", "5min", "15min"}
	for _, timeframe := range expectedTimeframes {
		if _, exists := aggregates[timeframe]; !exists {
			t.Errorf("Expected aggregate for timeframe %s", timeframe)
		}
	}
	
	// Verify aggregate data
	oneMinAgg := aggregates["1min"]
	if oneMinAgg.BuyCount != 1 {
		t.Errorf("Expected buy count 1, got %d", oneMinAgg.BuyCount)
	}
	if oneMinAgg.BuyVolume != 100.0 {
		t.Errorf("Expected buy volume 100.0, got %f", oneMinAgg.BuyVolume)
	}
}

func TestTokenProcessor_PerformManualAggregation(t *testing.T) {
	processor := NewTokenProcessor()
	mockRedis := NewMockRedisManager()
	mockCalculator := NewMockSlidingWindowCalculator()
	
	tokenAddress := "0x123456789"
	
	// Set up mock data in Redis
	now := time.Now()
	mockTokenData := &models.TokenData{
		TokenAddress: tokenAddress,
		Trades: []models.TradeData{
			{
				Token:        tokenAddress,
				Wallet:       "0xabc",
				SellBuy:      "buy",
				NativeAmount: 100.0,
				TokenAmount:  1000.0,
				PriceUsd:     0.1,
				TransTime:    now.Add(-30 * time.Second),
				TxHash:       "0x1",
			},
			{
				Token:        tokenAddress,
				Wallet:       "0xdef",
				SellBuy:      "sell",
				NativeAmount: 50.0,
				TokenAmount:  500.0,
				PriceUsd:     0.1,
				TransTime:    now.Add(-2 * time.Minute),
				TxHash:       "0x2",
			},
		},
		Indices:    make(map[string]int64),
		Aggregates: make(map[string]*models.AggregateData),
		LastUpdate: now.Add(-5 * time.Minute),
	}
	mockRedis.tokenData[tokenAddress] = mockTokenData
	
	// Initialize processor
	err := processor.Initialize(tokenAddress, mockRedis, mockCalculator)
	if err != nil {
		t.Fatalf("Failed to initialize processor: %v", err)
	}
	defer processor.Shutdown(context.Background())
	
	// Perform manual aggregation
	ctx := context.Background()
	err = processor.PerformManualAggregation(ctx)
	if err != nil {
		t.Errorf("Expected no error in manual aggregation, got: %v", err)
	}
	
	// Verify calculator was called
	if !mockCalculator.calculateWindowCalled {
		t.Error("Expected CalculateWindow to be called on calculator")
	}
	
	// Verify trade buffer was updated
	if processor.GetTradeCount() != 2 {
		t.Errorf("Expected trade count 2, got %d", processor.GetTradeCount())
	}
	
	// Verify last update was updated
	lastUpdate := processor.GetLastUpdate()
	if time.Since(lastUpdate) > 5*time.Second {
		t.Errorf("Expected recent last update, got %v", lastUpdate)
	}
}

func TestTokenProcessor_Shutdown(t *testing.T) {
	processor := NewTokenProcessor()
	mockRedis := NewMockRedisManager()
	mockCalculator := NewMockSlidingWindowCalculator()
	
	tokenAddress := "0x123456789"
	
	// Initialize processor
	err := processor.Initialize(tokenAddress, mockRedis, mockCalculator)
	if err != nil {
		t.Fatalf("Failed to initialize processor: %v", err)
	}
	
	// Verify processor is running
	if !processor.IsRunning() {
		t.Error("Expected processor to be running")
	}
	
	// Shutdown processor
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	err = processor.Shutdown(ctx)
	if err != nil {
		t.Errorf("Expected no error shutting down, got: %v", err)
	}
	
	// Verify processor is not running
	if processor.IsRunning() {
		t.Error("Expected processor to not be running after shutdown")
	}
	
	// Test double shutdown
	err = processor.Shutdown(ctx)
	if err != nil {
		t.Errorf("Expected no error on double shutdown, got: %v", err)
	}
}

func TestTokenProcessor_ConcurrentProcessing(t *testing.T) {
	processor := NewTokenProcessor()
	mockRedis := NewMockRedisManager()
	mockCalculator := NewMockSlidingWindowCalculator()
	
	tokenAddress := "0x123456789"
	
	// Initialize processor
	err := processor.Initialize(tokenAddress, mockRedis, mockCalculator)
	if err != nil {
		t.Fatalf("Failed to initialize processor: %v", err)
	}
	defer processor.Shutdown(context.Background())
	
	// Process multiple trades concurrently
	ctx := context.Background()
	var wg sync.WaitGroup
	numTrades := 10
	
	for i := 0; i < numTrades; i++ {
		wg.Add(1)
		go func(tradeNum int) {
			defer wg.Done()
			
			trade := models.TradeData{
				Token:        tokenAddress,
				Wallet:       "0xabc",
				SellBuy:      "buy",
				NativeAmount: float64(100 + tradeNum),
				TokenAmount:  1000.0,
				PriceUsd:     0.1,
				TransTime:    time.Now(),
				TxHash:       fmt.Sprintf("0x%d", tradeNum),
			}
			
			err := processor.ProcessTrade(ctx, trade)
			if err != nil {
				t.Errorf("Expected no error processing trade %d, got: %v", tradeNum, err)
			}
		}(i)
	}
	
	wg.Wait()
	
	// Wait for all processing to complete
	time.Sleep(500 * time.Millisecond)
	
	// Verify all trades were processed
	if processor.GetTradeCount() != numTrades {
		t.Errorf("Expected trade count %d, got %d", numTrades, processor.GetTradeCount())
	}
}

func TestTokenProcessor_FlushTradeBuffer(t *testing.T) {
	processor := NewTokenProcessor()
	mockRedis := NewMockRedisManager()
	mockCalculator := NewMockSlidingWindowCalculator()
	
	tokenAddress := "0x123456789"
	
	// Initialize processor
	err := processor.Initialize(tokenAddress, mockRedis, mockCalculator)
	if err != nil {
		t.Fatalf("Failed to initialize processor: %v", err)
	}
	defer processor.Shutdown(context.Background())
	
	// Add some trades to the processing channel
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		trade := models.TradeData{
			Token:        tokenAddress,
			Wallet:       "0xabc",
			SellBuy:      "buy",
			NativeAmount: float64(100 + i),
			TokenAmount:  1000.0,
			PriceUsd:     0.1,
			TransTime:    time.Now(),
			TxHash:       fmt.Sprintf("0x%d", i),
		}
		
		err := processor.ProcessTrade(ctx, trade)
		if err != nil {
			t.Errorf("Expected no error processing trade %d, got: %v", i, err)
		}
	}
	
	// Flush trade buffer
	err = processor.FlushTradeBuffer(ctx)
	if err != nil {
		t.Errorf("Expected no error flushing buffer, got: %v", err)
	}
	
	// Verify processing channel is empty
	channelLength := processor.GetProcessingChannelLength()
	if channelLength != 0 {
		t.Errorf("Expected empty processing channel, got length %d", channelLength)
	}
}

func TestTokenProcessor_ValidateState(t *testing.T) {
	processor := NewTokenProcessor()
	mockRedis := NewMockRedisManager()
	mockCalculator := NewMockSlidingWindowCalculator()
	
	tokenAddress := "0x123456789"
	
	// Initialize processor
	err := processor.Initialize(tokenAddress, mockRedis, mockCalculator)
	if err != nil {
		t.Fatalf("Failed to initialize processor: %v", err)
	}
	defer processor.Shutdown(context.Background())
	
	// Process a trade to create some state
	trade := models.TradeData{
		Token:        tokenAddress,
		Wallet:       "0xabc",
		SellBuy:      "buy",
		NativeAmount: 100.0,
		TokenAmount:  1000.0,
		PriceUsd:     0.1,
		TransTime:    time.Now(),
		TxHash:       "0x1",
	}
	
	ctx := context.Background()
	err = processor.ProcessTrade(ctx, trade)
	if err != nil {
		t.Errorf("Expected no error processing trade, got: %v", err)
	}
	
	// Wait for processing
	time.Sleep(100 * time.Millisecond)
	
	// Validate state
	err = processor.ValidateState()
	if err != nil {
		t.Errorf("Expected no error validating state, got: %v", err)
	}
}

func TestTokenProcessor_GetMemoryUsage(t *testing.T) {
	processor := NewTokenProcessor()
	mockRedis := NewMockRedisManager()
	mockCalculator := NewMockSlidingWindowCalculator()
	
	tokenAddress := "0x123456789"
	
	// Initialize processor
	err := processor.Initialize(tokenAddress, mockRedis, mockCalculator)
	if err != nil {
		t.Fatalf("Failed to initialize processor: %v", err)
	}
	defer processor.Shutdown(context.Background())
	
	// Get initial memory usage
	usage := processor.GetMemoryUsage()
	
	// Verify expected fields exist
	expectedFields := []string{"trade_count", "indices_count", "aggregates_count", "channel_length"}
	for _, field := range expectedFields {
		if _, exists := usage[field]; !exists {
			t.Errorf("Expected memory usage field %s to exist", field)
		}
	}
	
	// Verify initial values
	if usage["trade_count"] != 0 {
		t.Errorf("Expected initial trade count 0, got %d", usage["trade_count"])
	}
	
	// Process a trade
	trade := models.TradeData{
		Token:        tokenAddress,
		Wallet:       "0xabc",
		SellBuy:      "buy",
		NativeAmount: 100.0,
		TokenAmount:  1000.0,
		PriceUsd:     0.1,
		TransTime:    time.Now(),
		TxHash:       "0x1",
	}
	
	ctx := context.Background()
	err = processor.ProcessTrade(ctx, trade)
	if err != nil {
		t.Errorf("Expected no error processing trade, got: %v", err)
	}
	
	// Wait for processing
	time.Sleep(100 * time.Millisecond)
	
	// Get updated memory usage
	usage = processor.GetMemoryUsage()
	
	// Verify trade count increased
	if usage["trade_count"] != 1 {
		t.Errorf("Expected trade count 1, got %d", usage["trade_count"])
	}
}

func TestTokenProcessor_UpdateLastUpdateTimestamp(t *testing.T) {
	processor := NewTokenProcessor()
	mockRedis := NewMockRedisManager()
	mockCalculator := NewMockSlidingWindowCalculator()
	
	tokenAddress := "0x123456789"
	
	// Initialize processor
	err := processor.Initialize(tokenAddress, mockRedis, mockCalculator)
	if err != nil {
		t.Fatalf("Failed to initialize processor: %v", err)
	}
	defer processor.Shutdown(context.Background())
	
	// Get initial last update
	initialUpdate := processor.GetLastUpdate()
	
	// Update timestamp
	ctx := context.Background()
	err = processor.UpdateLastUpdateTimestamp(ctx)
	if err != nil {
		t.Errorf("Expected no error updating timestamp, got: %v", err)
	}
	
	// Verify timestamp was updated
	newUpdate := processor.GetLastUpdate()
	if !newUpdate.After(initialUpdate) {
		t.Errorf("Expected timestamp to be updated, initial: %v, new: %v", initialUpdate, newUpdate)
	}
	
	// Verify Redis was called
	if !mockRedis.updateLastUpdateCalled {
		t.Error("Expected UpdateLastUpdate to be called on Redis manager")
	}
}

func TestTokenProcessor_GetCurrentIndices(t *testing.T) {
	processor := NewTokenProcessor()
	mockRedis := NewMockRedisManager()
	mockCalculator := NewMockSlidingWindowCalculator()
	
	tokenAddress := "0x123456789"
	
	// Initialize processor
	err := processor.Initialize(tokenAddress, mockRedis, mockCalculator)
	if err != nil {
		t.Fatalf("Failed to initialize processor: %v", err)
	}
	defer processor.Shutdown(context.Background())
	
	// Process a trade to generate indices
	trade := models.TradeData{
		Token:        tokenAddress,
		Wallet:       "0xabc",
		SellBuy:      "buy",
		NativeAmount: 100.0,
		TokenAmount:  1000.0,
		PriceUsd:     0.1,
		TransTime:    time.Now(),
		TxHash:       "0x1",
	}
	
	ctx := context.Background()
	err = processor.ProcessTrade(ctx, trade)
	if err != nil {
		t.Errorf("Expected no error processing trade, got: %v", err)
	}
	
	// Wait for processing
	time.Sleep(100 * time.Millisecond)
	
	// Get current indices
	indices := processor.GetCurrentIndices()
	
	// Verify indices exist for expected timeframes
	expectedTimeframes := []string{"1min", "5min", "15min"}
	for _, timeframe := range expectedTimeframes {
		if _, exists := indices[timeframe]; !exists {
			t.Errorf("Expected index for timeframe %s", timeframe)
		}
	}
	
	// Verify indices are non-negative
	for timeframe, index := range indices {
		if index < 0 {
			t.Errorf("Expected non-negative index for timeframe %s, got %d", timeframe, index)
		}
	}
}

func TestTokenProcessor_ProcessTradeNotRunning(t *testing.T) {
	processor := NewTokenProcessor()
	
	// Try to process trade without initialization
	trade := models.TradeData{
		Token:        "0x123456789",
		Wallet:       "0xabc",
		SellBuy:      "buy",
		NativeAmount: 100.0,
		TokenAmount:  1000.0,
		PriceUsd:     0.1,
		TransTime:    time.Now(),
		TxHash:       "0x1",
	}
	
	ctx := context.Background()
	err := processor.ProcessTrade(ctx, trade)
	if err == nil {
		t.Error("Expected error processing trade when processor not running")
	}
}

func TestTokenProcessor_FlushTradeBufferNotRunning(t *testing.T) {
	processor := NewTokenProcessor()
	
	// Try to flush buffer without initialization
	ctx := context.Background()
	err := processor.FlushTradeBuffer(ctx)
	if err == nil {
		t.Error("Expected error flushing buffer when processor not running")
	}
}