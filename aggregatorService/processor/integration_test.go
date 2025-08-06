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
	data map[string]*models.TokenData
	mu   sync.RWMutex
}

func NewMockRedisManager() *MockRedisManager {
	return &MockRedisManager{
		data: make(map[string]*models.TokenData),
	}
}

func (m *MockRedisManager) UpdateTokenData(ctx context.Context, tokenAddress string, data *models.TokenData) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[tokenAddress] = data
	return nil
}

func (m *MockRedisManager) GetTokenData(ctx context.Context, tokenAddress string) (*models.TokenData, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if data, exists := m.data[tokenAddress]; exists {
		return data, nil
	}
	return &models.TokenData{
		TokenAddress: tokenAddress,
		Trades:       []models.TradeData{},
		Indices:      make(map[string]int64),
		Aggregates:   make(map[string]*models.AggregateData),
		LastUpdate:   time.Now(),
	}, nil
}

func (m *MockRedisManager) UpdateLastUpdate(ctx context.Context, tokenAddress string, timestamp time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if data, exists := m.data[tokenAddress]; exists {
		data.LastUpdate = timestamp
	}
	return nil
}

// Implement missing interface methods
func (m *MockRedisManager) Connect(ctx context.Context) error { return nil }
func (m *MockRedisManager) Close() error { return nil }
func (m *MockRedisManager) Ping(ctx context.Context) error { return nil }
func (m *MockRedisManager) AddTrade(ctx context.Context, tokenAddress string, trade models.TradeData) error { return nil }
func (m *MockRedisManager) GetTrades(ctx context.Context, tokenAddress string, start, end int64) ([]models.TradeData, error) { return []models.TradeData{}, nil }
func (m *MockRedisManager) SetIndex(ctx context.Context, tokenAddress, timeframe string, index int64) error { return nil }
func (m *MockRedisManager) GetIndex(ctx context.Context, tokenAddress, timeframe string) (int64, error) { return 0, nil }
func (m *MockRedisManager) SetAggregate(ctx context.Context, tokenAddress, timeframe string, aggregate *models.AggregateData) error { return nil }
func (m *MockRedisManager) GetAggregate(ctx context.Context, tokenAddress, timeframe string) (*models.AggregateData, error) { return &models.AggregateData{}, nil }
func (m *MockRedisManager) SetTTL(ctx context.Context, tokenAddress string, duration time.Duration) error { return nil }
func (m *MockRedisManager) GetActiveTokens(ctx context.Context) ([]string, error) { return []string{}, nil }
func (m *MockRedisManager) GetLastUpdate(ctx context.Context, tokenAddress string) (time.Time, error) { return time.Now(), nil }
func (m *MockRedisManager) ExecuteAtomicUpdate(ctx context.Context, script string, keys []string, args []interface{}) error { return nil }

// MockSlidingWindowCalculator for testing
type MockSlidingWindowCalculator struct {
	shouldPanic bool
	mu          sync.RWMutex
}

func NewMockSlidingWindowCalculator() *MockSlidingWindowCalculator {
	return &MockSlidingWindowCalculator{}
}

func (m *MockSlidingWindowCalculator) SetShouldPanic(shouldPanic bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shouldPanic = shouldPanic
}

func (m *MockSlidingWindowCalculator) UpdateWindows(
	ctx context.Context,
	trades []models.TradeData,
	indices map[string]int64,
	aggregates map[string]*models.AggregateData,
	newTrade models.TradeData,
) (map[string]int64, map[string]*models.AggregateData, error) {
	m.mu.RLock()
	shouldPanic := m.shouldPanic
	m.mu.RUnlock()
	
	if shouldPanic {
		panic("mock calculator panic")
	}
	
	// Return mock data
	newIndices := make(map[string]int64)
	newAggregates := make(map[string]*models.AggregateData)
	
	for k, v := range indices {
		newIndices[k] = v
	}
	for k, v := range aggregates {
		newAggregates[k] = v
	}
	
	return newIndices, newAggregates, nil
}

func (m *MockSlidingWindowCalculator) GetAllTimeWindowNames() []string {
	return []string{"1min", "5min", "15min", "30min", "1hour"}
}

func (m *MockSlidingWindowCalculator) GetTimeWindowByName(name string) (config.TimeWindow, bool) {
	// Return a mock time window
	return config.TimeWindow{
		Name:     name,
		Duration: time.Minute,
	}, true
}

func (m *MockSlidingWindowCalculator) CalculateWindow(
	ctx context.Context,
	trades []models.TradeData,
	timeWindow config.TimeWindow,
	currentTime time.Time,
) (*models.AggregateData, int64, error) {
	return &models.AggregateData{
		SellCount:   0,
		BuyCount:    0,
		SellVolume:  0,
		BuyVolume:   0,
		TotalVolume: 0,
		LastUpdate:  currentTime,
	}, 0, nil
}

// Implement missing interface methods
func (m *MockSlidingWindowCalculator) Initialize(timeWindows []config.TimeWindow) error { return nil }
func (m *MockSlidingWindowCalculator) FindExpiredTrades(
	trades []models.TradeData,
	currentIndex int64,
	timeWindow config.TimeWindow,
	currentTime time.Time,
) ([]models.TradeData, int64, error) {
	return []models.TradeData{}, 0, nil
}

func TestTokenProcessor_PanicRecoveryIntegration(t *testing.T) {
	// Create mock dependencies
	redisManager := NewMockRedisManager()
	calculator := NewMockSlidingWindowCalculator()
	
	// Create token processor
	processor := NewTokenProcessor()
	
	// Initialize processor
	tokenAddress := "test-token-address"
	err := processor.Initialize(tokenAddress, redisManager, calculator)
	if err != nil {
		t.Fatalf("Failed to initialize processor: %v", err)
	}
	
	// Ensure processor is running
	if tp, ok := processor.(*TokenProcessor); ok {
		if !tp.IsRunning() {
			t.Fatal("Expected processor to be running after initialization")
		}
	}
	
	// Create a trade that will cause panic in calculator
	trade := models.TradeData{
		Token:        tokenAddress,
		Wallet:       "test-wallet",
		SellBuy:      "buy",
		NativeAmount: 100.0,
		TokenAmount:  1000.0,
		PriceUsd:     0.1,
		TransTime:    time.Now(),
		TxHash:       "test-tx-hash",
	}
	
	// Set calculator to panic
	calculator.SetShouldPanic(true)
	
	// Process trade that will cause panic
	ctx := context.Background()
	err = processor.ProcessTrade(ctx, trade)
	if err != nil {
		t.Logf("Expected error due to panic: %v", err)
	}
	
	// Wait for panic recovery to occur
	time.Sleep(200 * time.Millisecond)
	
	// Processor should still be running after panic recovery
	if tp, ok := processor.(*TokenProcessor); ok {
		if !tp.IsRunning() {
			t.Error("Expected processor to still be running after panic recovery")
		}
	}
	
	// Get panic recovery stats
	if tp, ok := processor.(*TokenProcessor); ok {
		stats := tp.GetPanicRecoveryStats()
		t.Logf("Panic recovery stats: %+v", stats)
		
		// Check that restart was recorded (this is more reliable than panic count due to goroutine restart)
		if restartCounts, exists := stats["restart_counts"].(map[string]int); exists {
			totalRestarts := 0
			for _, count := range restartCounts {
				totalRestarts += count
			}
			if totalRestarts == 0 {
				t.Error("Expected at least one restart to be recorded")
			}
		}
	}
	
	// Disable panic and try processing again
	calculator.SetShouldPanic(false)
	
	// Process another trade (should succeed)
	trade.TxHash = "test-tx-hash-2"
	err = processor.ProcessTrade(ctx, trade)
	if err != nil {
		t.Errorf("Expected successful trade processing after recovery: %v", err)
	}
	
	// Wait for processing
	time.Sleep(100 * time.Millisecond)
	
	// Shutdown processor
	err = processor.Shutdown(ctx)
	if err != nil {
		t.Errorf("Failed to shutdown processor: %v", err)
	}
}

func TestTokenProcessor_MultipleProcessorsPanicRecovery(t *testing.T) {
	// Create multiple processors to test concurrent panic recovery
	numProcessors := 3
	processors := make([]interface{}, numProcessors)
	
	for i := 0; i < numProcessors; i++ {
		redisManager := NewMockRedisManager()
		calculator := NewMockSlidingWindowCalculator()
		
		processor := NewTokenProcessor()
		tokenAddress := fmt.Sprintf("test-token-%d", i)
		
		err := processor.Initialize(tokenAddress, redisManager, calculator)
		if err != nil {
			t.Fatalf("Failed to initialize processor %d: %v", i, err)
		}
		
		processors[i] = processor
	}
	
	// Create trades for each processor
	var wg sync.WaitGroup
	ctx := context.Background()
	
	for i := 0; i < numProcessors; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			
			processor := processors[index]
			
			// Create trade
			trade := models.TradeData{
				Token:        fmt.Sprintf("test-token-%d", index),
				Wallet:       fmt.Sprintf("test-wallet-%d", index),
				SellBuy:      "buy",
				NativeAmount: 100.0,
				TokenAmount:  1000.0,
				PriceUsd:     0.1,
				TransTime:    time.Now(),
				TxHash:       fmt.Sprintf("test-tx-hash-%d", index),
			}
			
			// Process trade multiple times to test stability
			for j := 0; j < 5; j++ {
				trade.TxHash = fmt.Sprintf("test-tx-hash-%d-%d", index, j)
				
				if tp, ok := processor.(interface {
					ProcessTrade(context.Context, models.TradeData) error
				}); ok {
					err := tp.ProcessTrade(ctx, trade)
					if err != nil {
						t.Logf("Processor %d trade %d error: %v", index, j, err)
					}
				}
				
				// Small delay between trades
				time.Sleep(10 * time.Millisecond)
			}
		}(i)
	}
	
	// Wait for all processors to complete
	wg.Wait()
	
	// Shutdown all processors
	for i, processor := range processors {
		if tp, ok := processor.(interface {
			Shutdown(context.Context) error
		}); ok {
			err := tp.Shutdown(ctx)
			if err != nil {
				t.Errorf("Failed to shutdown processor %d: %v", i, err)
			}
		}
	}
}

func TestTokenProcessor_PanicRecoveryWithRealWorkload(t *testing.T) {
	redisManager := NewMockRedisManager()
	calculator := NewMockSlidingWindowCalculator()
	
	processor := NewTokenProcessor()
	tokenAddress := "workload-test-token"
	
	err := processor.Initialize(tokenAddress, redisManager, calculator)
	if err != nil {
		t.Fatalf("Failed to initialize processor: %v", err)
	}
	
	ctx := context.Background()
	
	// Simulate a realistic workload with occasional panics
	numTrades := 100
	panicInterval := 20 // Panic every 20th trade
	
	var successCount, errorCount int
	var mu sync.Mutex
	
	for i := 0; i < numTrades; i++ {
		// Set calculator to panic occasionally
		if i%panicInterval == 0 && i > 0 {
			calculator.SetShouldPanic(true)
		} else {
			calculator.SetShouldPanic(false)
		}
		
		trade := models.TradeData{
			Token:        tokenAddress,
			Wallet:       fmt.Sprintf("wallet-%d", i%10), // 10 different wallets
			SellBuy:      []string{"buy", "sell"}[i%2],   // Alternate buy/sell
			NativeAmount: float64(100 + i),
			TokenAmount:  float64(1000 + i*10),
			PriceUsd:     0.1 + float64(i)*0.001,
			TransTime:    time.Now().Add(time.Duration(i) * time.Second),
			TxHash:       fmt.Sprintf("workload-tx-%d", i),
		}
		
		err := processor.ProcessTrade(ctx, trade)
		
		mu.Lock()
		if err != nil {
			errorCount++
		} else {
			successCount++
		}
		mu.Unlock()
		
		// Small delay to simulate realistic timing
		time.Sleep(5 * time.Millisecond)
	}
	
	// Wait for all processing to complete
	time.Sleep(500 * time.Millisecond)
	
	t.Logf("Workload test completed: %d successes, %d errors", successCount, errorCount)
	
	// Processor should still be running
	if tp, ok := processor.(*TokenProcessor); ok {
		if !tp.IsRunning() {
			t.Error("Expected processor to still be running after workload test")
		}
	}
	
	// Get final stats
	if tp, ok := processor.(*TokenProcessor); ok {
		stats := tp.GetPanicRecoveryStats()
		t.Logf("Final panic recovery stats: %+v", stats)
		
		memoryUsage := tp.GetMemoryUsage()
		t.Logf("Memory usage: %+v", memoryUsage)
	}
	
	// Shutdown
	err = processor.Shutdown(ctx)
	if err != nil {
		t.Errorf("Failed to shutdown processor: %v", err)
	}
}

// Benchmark panic recovery overhead
func BenchmarkTokenProcessor_PanicRecovery(b *testing.B) {
	redisManager := NewMockRedisManager()
	calculator := NewMockSlidingWindowCalculator()
	
	processor := NewTokenProcessor()
	tokenAddress := "benchmark-token"
	
	err := processor.Initialize(tokenAddress, redisManager, calculator)
	if err != nil {
		b.Fatalf("Failed to initialize processor: %v", err)
	}
	
	ctx := context.Background()
	
	trade := models.TradeData{
		Token:        tokenAddress,
		Wallet:       "benchmark-wallet",
		SellBuy:      "buy",
		NativeAmount: 100.0,
		TokenAmount:  1000.0,
		PriceUsd:     0.1,
		TransTime:    time.Now(),
		TxHash:       "benchmark-tx",
	}
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		trade.TxHash = fmt.Sprintf("benchmark-tx-%d", i)
		
		err := processor.ProcessTrade(ctx, trade)
		if err != nil {
			b.Logf("Trade processing error: %v", err)
		}
	}
	
	b.StopTimer()
	
	// Cleanup
	processor.Shutdown(ctx)
}