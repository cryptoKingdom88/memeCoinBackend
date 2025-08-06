package maintenance

import (
	"context"
	"fmt"
	"testing"
	"time"

	"aggregatorService/models"
)

// MockRedisManager implements interfaces.RedisManager for testing
type MockRedisManager struct {
	activeTokens map[string]bool
	tokenData    map[string]*models.TokenData
	lastUpdates  map[string]time.Time
	
	// Track method calls for verification
	getActiveTokensCalled      bool
	getLastUpdateCalled        map[string]bool
	performManualAggCalled     map[string]bool
	updateLastUpdateCalled     map[string]bool
	setAggregateCalled         map[string]map[string]bool
	setIndexCalled             map[string]map[string]bool
}

func NewMockRedisManager() *MockRedisManager {
	return &MockRedisManager{
		activeTokens:               make(map[string]bool),
		tokenData:                  make(map[string]*models.TokenData),
		lastUpdates:                make(map[string]time.Time),
		getLastUpdateCalled:        make(map[string]bool),
		performManualAggCalled:     make(map[string]bool),
		updateLastUpdateCalled:     make(map[string]bool),
		setAggregateCalled:         make(map[string]map[string]bool),
		setIndexCalled:             make(map[string]map[string]bool),
	}
}

func (m *MockRedisManager) Connect(ctx context.Context) error { return nil }
func (m *MockRedisManager) Close() error { return nil }
func (m *MockRedisManager) Ping(ctx context.Context) error { return nil }

func (m *MockRedisManager) GetActiveTokens(ctx context.Context) ([]string, error) {
	m.getActiveTokensCalled = true
	var tokens []string
	for token := range m.activeTokens {
		tokens = append(tokens, token)
	}
	return tokens, nil
}

func (m *MockRedisManager) GetLastUpdate(ctx context.Context, tokenAddress string) (time.Time, error) {
	if m.getLastUpdateCalled == nil {
		m.getLastUpdateCalled = make(map[string]bool)
	}
	m.getLastUpdateCalled[tokenAddress] = true
	
	if lastUpdate, exists := m.lastUpdates[tokenAddress]; exists {
		return lastUpdate, nil
	}
	return time.Time{}, nil // Return zero time if not found
}

func (m *MockRedisManager) UpdateLastUpdate(ctx context.Context, tokenAddress string, timestamp time.Time) error {
	if m.updateLastUpdateCalled == nil {
		m.updateLastUpdateCalled = make(map[string]bool)
	}
	m.updateLastUpdateCalled[tokenAddress] = true
	m.lastUpdates[tokenAddress] = timestamp
	return nil
}

func (m *MockRedisManager) GetTokenData(ctx context.Context, tokenAddress string) (*models.TokenData, error) {
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

func (m *MockRedisManager) SetAggregate(ctx context.Context, tokenAddress, timeframe string, aggregate *models.AggregateData) error {
	if m.setAggregateCalled[tokenAddress] == nil {
		m.setAggregateCalled[tokenAddress] = make(map[string]bool)
	}
	m.setAggregateCalled[tokenAddress][timeframe] = true
	return nil
}

func (m *MockRedisManager) SetIndex(ctx context.Context, tokenAddress, timeframe string, index int64) error {
	if m.setIndexCalled[tokenAddress] == nil {
		m.setIndexCalled[tokenAddress] = make(map[string]bool)
	}
	m.setIndexCalled[tokenAddress][timeframe] = true
	return nil
}

// Implement remaining interface methods (not used in these tests)
func (m *MockRedisManager) UpdateTokenData(ctx context.Context, tokenAddress string, data *models.TokenData) error { return nil }
func (m *MockRedisManager) AddTrade(ctx context.Context, tokenAddress string, trade models.TradeData) error { return nil }
func (m *MockRedisManager) GetTrades(ctx context.Context, tokenAddress string, start, end int64) ([]models.TradeData, error) { return nil, nil }
func (m *MockRedisManager) GetIndex(ctx context.Context, tokenAddress, timeframe string) (int64, error) { return 0, nil }
func (m *MockRedisManager) GetAggregate(ctx context.Context, tokenAddress, timeframe string) (*models.AggregateData, error) { return nil, nil }
func (m *MockRedisManager) SetTTL(ctx context.Context, tokenAddress string, duration time.Duration) error { return nil }
func (m *MockRedisManager) ExecuteAtomicUpdate(ctx context.Context, script string, keys []string, args []interface{}) error { return nil }

func TestMaintenanceService_Initialize(t *testing.T) {
	service := NewService()
	mockRedis := NewMockRedisManager()
	
	// Test successful initialization
	err := service.Initialize(mockRedis, 60*time.Second, 10*time.Second)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	
	// Test nil redis manager
	err = service.Initialize(nil, 60*time.Second, 10*time.Second)
	if err == nil {
		t.Error("Expected error for nil redis manager")
	}
	
	// Test invalid interval
	err = service.Initialize(mockRedis, 0, 10*time.Second)
	if err == nil {
		t.Error("Expected error for zero interval")
	}
	
	// Test invalid stale threshold
	err = service.Initialize(mockRedis, 60*time.Second, 0)
	if err == nil {
		t.Error("Expected error for zero stale threshold")
	}
}

func TestMaintenanceService_ScanStaleTokens(t *testing.T) {
	service := NewService().(*Service)
	mockRedis := NewMockRedisManager()
	
	// Initialize service
	err := service.Initialize(mockRedis, 60*time.Second, 10*time.Second)
	if err != nil {
		t.Fatalf("Failed to initialize service: %v", err)
	}
	
	ctx := context.Background()
	currentTime := time.Now()
	
	// Setup test data
	mockRedis.activeTokens["token1"] = true
	mockRedis.activeTokens["token2"] = true
	mockRedis.activeTokens["token3"] = true
	
	// token1: recent update (not stale)
	mockRedis.lastUpdates["token1"] = currentTime.Add(-5 * time.Second)
	
	// token2: old update (stale)
	mockRedis.lastUpdates["token2"] = currentTime.Add(-15 * time.Second)
	
	// token3: no update (zero time, stale)
	// Don't set lastUpdates["token3"] so it returns zero time
	
	// Test scanning for stale tokens
	staleTokens, err := service.ScanStaleTokens(ctx)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	
	// Should find token2 and token3 as stale
	expectedStale := 2
	if len(staleTokens) != expectedStale {
		t.Errorf("Expected %d stale tokens, got %d: %v", expectedStale, len(staleTokens), staleTokens)
	}
	
	// Verify the correct tokens are identified as stale
	staleMap := make(map[string]bool)
	for _, token := range staleTokens {
		staleMap[token] = true
	}
	
	if !staleMap["token2"] {
		t.Error("Expected token2 to be stale")
	}
	
	if !staleMap["token3"] {
		t.Error("Expected token3 to be stale")
	}
	
	if staleMap["token1"] {
		t.Error("Expected token1 to NOT be stale")
	}
	
	// Verify Redis methods were called
	if !mockRedis.getActiveTokensCalled {
		t.Error("Expected GetActiveTokens to be called")
	}
	
	for token := range mockRedis.activeTokens {
		if !mockRedis.getLastUpdateCalled[token] {
			t.Errorf("Expected GetLastUpdate to be called for token %s", token)
		}
	}
}

func TestMaintenanceService_PerformManualAggregation(t *testing.T) {
	service := NewService().(*Service)
	mockRedis := NewMockRedisManager()
	
	// Initialize service
	err := service.Initialize(mockRedis, 60*time.Second, 10*time.Second)
	if err != nil {
		t.Fatalf("Failed to initialize service: %v", err)
	}
	
	ctx := context.Background()
	tokenAddress := "test-token"
	
	// Create test trade data
	now := time.Now()
	trades := []models.TradeData{
		{
			Token:        tokenAddress,
			Wallet:       "wallet1",
			SellBuy:      "buy",
			NativeAmount: 100.0,
			TokenAmount:  1000.0,
			PriceUsd:     0.1,
			TransTime:    now.Add(-30 * time.Second), // Within 1min window
			TxHash:       "hash1",
		},
		{
			Token:        tokenAddress,
			Wallet:       "wallet2", 
			SellBuy:      "sell",
			NativeAmount: 50.0,
			TokenAmount:  500.0,
			PriceUsd:     0.1,
			TransTime:    now.Add(-2 * time.Minute), // Outside 1min, within 5min
			TxHash:       "hash2",
		},
	}
	
	// Setup mock token data
	mockRedis.tokenData[tokenAddress] = &models.TokenData{
		TokenAddress: tokenAddress,
		Trades:       trades,
		Indices:      make(map[string]int64),
		Aggregates:   make(map[string]*models.AggregateData),
		LastUpdate:   time.Time{},
	}
	
	// Initialize mock tracking maps
	mockRedis.setAggregateCalled[tokenAddress] = make(map[string]bool)
	mockRedis.setIndexCalled[tokenAddress] = make(map[string]bool)
	
	// Test manual aggregation
	err = service.PerformManualAggregation(ctx, tokenAddress)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	
	// Verify UpdateLastUpdate was called
	if !mockRedis.updateLastUpdateCalled[tokenAddress] {
		t.Error("Expected UpdateLastUpdate to be called")
	}
	
	// Verify aggregates were set for all timeframes
	expectedTimeframes := []string{"1min", "5min", "15min", "30min", "1hour"}
	for _, timeframe := range expectedTimeframes {
		if !mockRedis.setAggregateCalled[tokenAddress][timeframe] {
			t.Errorf("Expected SetAggregate to be called for timeframe %s", timeframe)
		}
		if !mockRedis.setIndexCalled[tokenAddress][timeframe] {
			t.Errorf("Expected SetIndex to be called for timeframe %s", timeframe)
		}
	}
	
	// Verify last update timestamp was updated
	lastUpdate, exists := mockRedis.lastUpdates[tokenAddress]
	if !exists {
		t.Error("Expected last update timestamp to be set")
	}
	
	// Should be very recent (within last few seconds)
	if time.Since(lastUpdate) > 5*time.Second {
		t.Errorf("Expected recent last update timestamp, got: %v", lastUpdate)
	}
}

func TestMaintenanceService_PerformManualAggregation_NoTrades(t *testing.T) {
	service := NewService().(*Service)
	mockRedis := NewMockRedisManager()
	
	// Initialize service
	err := service.Initialize(mockRedis, 60*time.Second, 10*time.Second)
	if err != nil {
		t.Fatalf("Failed to initialize service: %v", err)
	}
	
	ctx := context.Background()
	tokenAddress := "empty-token"
	
	// Setup mock token data with no trades
	mockRedis.tokenData[tokenAddress] = &models.TokenData{
		TokenAddress: tokenAddress,
		Trades:       []models.TradeData{}, // Empty trades
		Indices:      make(map[string]int64),
		Aggregates:   make(map[string]*models.AggregateData),
		LastUpdate:   time.Time{},
	}
	
	// Test manual aggregation with no trades
	err = service.PerformManualAggregation(ctx, tokenAddress)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	
	// Should still update the timestamp
	if !mockRedis.updateLastUpdateCalled[tokenAddress] {
		t.Error("Expected UpdateLastUpdate to be called even with no trades")
	}
	
	// Should not call SetAggregate or SetIndex since no trades
	if mockRedis.setAggregateCalled[tokenAddress] != nil {
		t.Error("Expected SetAggregate to NOT be called when no trades")
	}
	if mockRedis.setIndexCalled[tokenAddress] != nil {
		t.Error("Expected SetIndex to NOT be called when no trades")
	}
}

func TestMaintenanceService_calculateWindowAggregation(t *testing.T) {
	service := NewService().(*Service)
	
	now := time.Now()
	trades := []models.TradeData{
		{
			Token:        "test-token",
			SellBuy:      "buy",
			NativeAmount: 100.0,
			PriceUsd:     0.1,
			TransTime:    now.Add(-30 * time.Second), // Within 1min window
		},
		{
			Token:        "test-token",
			SellBuy:      "sell", 
			NativeAmount: 50.0,
			PriceUsd:     0.12,
			TransTime:    now.Add(-90 * time.Second), // Outside 1min window
		},
		{
			Token:        "test-token",
			SellBuy:      "buy",
			NativeAmount: 75.0,
			PriceUsd:     0.11,
			TransTime:    now.Add(-45 * time.Second), // Within 1min window
		},
	}
	
	// Test 1-minute window calculation
	aggregate, index, err := service.calculateWindowAggregation(trades, 1*time.Minute, now)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	
	// Should include 2 trades (index 0 and 2, excluding index 1 which is outside window)
	expectedBuyCount := int64(2)
	expectedSellCount := int64(0)
	expectedTotalVolume := 175.0 // 100 + 75
	
	if aggregate.BuyCount != expectedBuyCount {
		t.Errorf("Expected buy count %d, got %d", expectedBuyCount, aggregate.BuyCount)
	}
	
	if aggregate.SellCount != expectedSellCount {
		t.Errorf("Expected sell count %d, got %d", expectedSellCount, aggregate.SellCount)
	}
	
	if aggregate.TotalVolume != expectedTotalVolume {
		t.Errorf("Expected total volume %.2f, got %.2f", expectedTotalVolume, aggregate.TotalVolume)
	}
	
	// Index should point to the oldest trade in the window
	if index < 0 {
		t.Errorf("Expected valid index, got %d", index)
	}
	
	// Test empty window
	emptyAggregate, emptyIndex, err := service.calculateWindowAggregation([]models.TradeData{}, 1*time.Minute, now)
	if err != nil {
		t.Errorf("Expected no error for empty trades, got: %v", err)
	}
	
	if emptyAggregate.GetTotalTradeCount() != 0 {
		t.Errorf("Expected empty aggregate, got trade count %d", emptyAggregate.GetTotalTradeCount())
	}
	
	if emptyIndex != 0 {
		t.Errorf("Expected index 0 for empty trades, got %d", emptyIndex)
	}
}

func TestMaintenanceService_GetStatistics(t *testing.T) {
	service := NewService().(*Service)
	mockRedis := NewMockRedisManager()
	
	// Initialize service
	err := service.Initialize(mockRedis, 60*time.Second, 10*time.Second)
	if err != nil {
		t.Fatalf("Failed to initialize service: %v", err)
	}
	
	// Get initial statistics
	stats := service.GetStatistics()
	
	// Verify expected fields exist
	expectedFields := []string{
		"is_running", "interval", "stale_threshold", 
		"total_scans", "stale_tokens_found", "errors_count", "last_scan",
	}
	
	for _, field := range expectedFields {
		if _, exists := stats[field]; !exists {
			t.Errorf("Expected statistics field %s to exist", field)
		}
	}
	
	// Verify initial values
	if stats["is_running"].(bool) != false {
		t.Error("Expected is_running to be false initially")
	}
	
	if stats["total_scans"].(int64) != 0 {
		t.Error("Expected total_scans to be 0 initially")
	}
	
	if stats["interval"].(string) != "1m0s" {
		t.Errorf("Expected interval to be '1m0s', got %s", stats["interval"].(string))
	}
	
	if stats["stale_threshold"].(string) != "10s" {
		t.Errorf("Expected stale_threshold to be '10s', got %s", stats["stale_threshold"].(string))
	}
}

func TestMaintenanceService_PerformManualAggregationWithRetry(t *testing.T) {
	service := NewService().(*Service)
	mockRedis := NewMockRedisManager()
	
	// Initialize service
	err := service.Initialize(mockRedis, 60*time.Second, 10*time.Second)
	if err != nil {
		t.Fatalf("Failed to initialize service: %v", err)
	}
	
	ctx := context.Background()
	tokenAddress := "retry-test-token"
	
	// Create test trade data
	now := time.Now()
	trades := []models.TradeData{
		{
			Token:        tokenAddress,
			Wallet:       "wallet1",
			SellBuy:      "buy",
			NativeAmount: 100.0,
			TokenAmount:  1000.0,
			PriceUsd:     0.1,
			TransTime:    now.Add(-30 * time.Second),
			TxHash:       "hash1",
		},
	}
	
	// Setup mock token data
	mockRedis.tokenData[tokenAddress] = &models.TokenData{
		TokenAddress: tokenAddress,
		Trades:       trades,
		Indices:      make(map[string]int64),
		Aggregates:   make(map[string]*models.AggregateData),
		LastUpdate:   time.Time{},
	}
	
	// Initialize mock tracking maps
	mockRedis.setAggregateCalled[tokenAddress] = make(map[string]bool)
	mockRedis.setIndexCalled[tokenAddress] = make(map[string]bool)
	
	// Test successful manual aggregation with retry logic
	err = service.PerformManualAggregation(ctx, tokenAddress)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	
	// Verify UpdateLastUpdate was called
	if !mockRedis.updateLastUpdateCalled[tokenAddress] {
		t.Error("Expected UpdateLastUpdate to be called")
	}
	
	// Verify aggregates were set for all timeframes
	expectedTimeframes := []string{"1min", "5min", "15min", "30min", "1hour"}
	for _, timeframe := range expectedTimeframes {
		if !mockRedis.setAggregateCalled[tokenAddress][timeframe] {
			t.Errorf("Expected SetAggregate to be called for timeframe %s", timeframe)
		}
		if !mockRedis.setIndexCalled[tokenAddress][timeframe] {
			t.Errorf("Expected SetIndex to be called for timeframe %s", timeframe)
		}
	}
}

func TestMaintenanceService_ScanStaleTokensWithRetry(t *testing.T) {
	service := NewService().(*Service)
	mockRedis := NewMockRedisManager()
	
	// Initialize service
	err := service.Initialize(mockRedis, 60*time.Second, 10*time.Second)
	if err != nil {
		t.Fatalf("Failed to initialize service: %v", err)
	}
	
	ctx := context.Background()
	currentTime := time.Now()
	
	// Setup test data
	mockRedis.activeTokens["token1"] = true
	mockRedis.activeTokens["token2"] = true
	
	// token1: recent update (not stale)
	mockRedis.lastUpdates["token1"] = currentTime.Add(-5 * time.Second)
	
	// token2: old update (stale)
	mockRedis.lastUpdates["token2"] = currentTime.Add(-15 * time.Second)
	
	// Test scanning for stale tokens with retry logic
	staleTokens, err := service.ScanStaleTokens(ctx)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	
	// Should find token2 as stale
	expectedStale := 1
	if len(staleTokens) != expectedStale {
		t.Errorf("Expected %d stale tokens, got %d: %v", expectedStale, len(staleTokens), staleTokens)
	}
	
	// Verify the correct token is identified as stale
	if len(staleTokens) > 0 && staleTokens[0] != "token2" {
		t.Errorf("Expected token2 to be stale, got %s", staleTokens[0])
	}
	
	// Verify Redis methods were called
	if !mockRedis.getActiveTokensCalled {
		t.Error("Expected GetActiveTokens to be called")
	}
}

func TestMaintenanceService_ErrorHandling(t *testing.T) {
	service := NewService().(*Service)
	
	// Test isDataConsistencyError function
	testCases := []struct {
		error    error
		expected bool
	}{
		{fmt.Errorf("invalid aggregate data: negative count"), true},
		{fmt.Errorf("data validation failed"), true},
		{fmt.Errorf("inconsistent state detected"), true},
		{fmt.Errorf("corrupted data found"), true},
		{fmt.Errorf("index out of range"), true},
		{fmt.Errorf("nil pointer dereference"), true},
		{fmt.Errorf("network connection failed"), false},
		{fmt.Errorf("timeout occurred"), false},
	}
	
	for _, tc := range testCases {
		result := service.isDataConsistencyError(tc.error)
		if result != tc.expected {
			t.Errorf("For error '%v', expected %v, got %v", tc.error, tc.expected, result)
		}
	}
}

func TestMaintenanceService_BasicRecovery(t *testing.T) {
	service := NewService().(*Service)
	mockRedis := NewMockRedisManager()
	
	// Initialize service
	err := service.Initialize(mockRedis, 60*time.Second, 10*time.Second)
	if err != nil {
		t.Fatalf("Failed to initialize service: %v", err)
	}
	
	ctx := context.Background()
	tokenAddress := "recovery-test-token"
	
	// Test basic recovery
	err = service.attemptBasicRecovery(ctx, tokenAddress)
	if err != nil {
		t.Errorf("Expected no error during basic recovery, got: %v", err)
	}
	
	// Verify UpdateLastUpdate was called
	if !mockRedis.updateLastUpdateCalled[tokenAddress] {
		t.Error("Expected UpdateLastUpdate to be called during basic recovery")
	}
	
	// Verify timestamp was updated
	lastUpdate, exists := mockRedis.lastUpdates[tokenAddress]
	if !exists {
		t.Error("Expected last update timestamp to be set during recovery")
	}
	
	// Should be very recent (within last few seconds)
	if time.Since(lastUpdate) > 5*time.Second {
		t.Errorf("Expected recent last update timestamp during recovery, got: %v", lastUpdate)
	}
}