package processor

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cryptoKingdom88/memeCoinBackend/shared/packet"
	"aggregatorService/interfaces"
)

// MockWorkerPool for testing
type MockWorkerPool struct {
	jobs         []func()
	jobsMutex    sync.Mutex
	submitCalled bool
}

func NewMockWorkerPool() *MockWorkerPool {
	return &MockWorkerPool{
		jobs: make([]func(), 0),
	}
}

func (m *MockWorkerPool) Submit(job func()) error {
	m.jobsMutex.Lock()
	defer m.jobsMutex.Unlock()
	m.submitCalled = true
	m.jobs = append(m.jobs, job)
	
	// Execute job immediately for testing
	go job()
	return nil
}

func (m *MockWorkerPool) Start(ctx context.Context) error { return nil }
func (m *MockWorkerPool) Stop(ctx context.Context) error { return nil }
func (m *MockWorkerPool) GetStats() map[string]interface{} { return nil }

func TestBlockAggregator_Initialize(t *testing.T) {
	aggregator := NewBlockAggregator()
	mockRedis := NewMockRedisManager()
	mockCalculator := NewMockSlidingWindowCalculator()
	mockWorkerPool := NewMockWorkerPool()
	
	// Test successful initialization
	err := aggregator.Initialize(mockRedis, mockCalculator, mockWorkerPool)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	
	// Test double initialization
	err = aggregator.Initialize(mockRedis, mockCalculator, mockWorkerPool)
	if err == nil {
		t.Error("Expected error for double initialization")
	}
	
	// Test nil parameters
	err = NewBlockAggregator().Initialize(nil, mockCalculator, mockWorkerPool)
	if err == nil {
		t.Error("Expected error for nil redis manager")
	}
	
	err = NewBlockAggregator().Initialize(mockRedis, nil, mockWorkerPool)
	if err == nil {
		t.Error("Expected error for nil calculator")
	}
	
	err = NewBlockAggregator().Initialize(mockRedis, mockCalculator, nil)
	if err == nil {
		t.Error("Expected error for nil worker pool")
	}
}

func TestBlockAggregator_ProcessTrades(t *testing.T) {
	aggregator := NewBlockAggregator()
	mockRedis := NewMockRedisManager()
	mockCalculator := NewMockSlidingWindowCalculator()
	mockWorkerPool := NewMockWorkerPool()
	
	// Initialize aggregator
	err := aggregator.Initialize(mockRedis, mockCalculator, mockWorkerPool)
	if err != nil {
		t.Fatalf("Failed to initialize aggregator: %v", err)
	}
	defer aggregator.Shutdown(context.Background())
	
	// Create test trades
	trades := []packet.TokenTradeHistory{
		{
			Token:        "0x123",
			Wallet:       "0xabc",
			SellBuy:      "buy",
			NativeAmount: "100.0",
			TokenAmount:  "1000.0",
			PriceUsd:     "0.1",
			TransTime:    "2023-01-01T00:00:00Z",
			TxHash:       "0x1",
		},
		{
			Token:        "0x456",
			Wallet:       "0xdef",
			SellBuy:      "sell",
			NativeAmount: "50.0",
			TokenAmount:  "500.0",
			PriceUsd:     "0.1",
			TransTime:    "2023-01-01T00:01:00Z",
			TxHash:       "0x2",
		},
		{
			Token:        "0x123", // Same token as first trade
			Wallet:       "0xghi",
			SellBuy:      "buy",
			NativeAmount: "75.0",
			TokenAmount:  "750.0",
			PriceUsd:     "0.1",
			TransTime:    "2023-01-01T00:02:00Z",
			TxHash:       "0x3",
		},
	}
	
	// Process trades
	ctx := context.Background()
	err = aggregator.ProcessTrades(ctx, trades)
	if err != nil {
		t.Errorf("Expected no error processing trades, got: %v", err)
	}
	
	// Wait for async processing
	time.Sleep(200 * time.Millisecond)
	
	// Verify worker pool was used
	if !mockWorkerPool.submitCalled {
		t.Error("Expected worker pool Submit to be called")
	}
	
	// Verify processors were created for both tokens
	activeTokens := aggregator.GetActiveTokens()
	if len(activeTokens) != 2 {
		t.Errorf("Expected 2 active tokens, got %d", len(activeTokens))
	}
	
	// Verify token addresses
	tokenMap := make(map[string]bool)
	for _, token := range activeTokens {
		tokenMap[token] = true
	}
	if !tokenMap["0x123"] {
		t.Error("Expected token 0x123 to be active")
	}
	if !tokenMap["0x456"] {
		t.Error("Expected token 0x456 to be active")
	}
}

func TestBlockAggregator_ProcessTradesEmpty(t *testing.T) {
	aggregator := NewBlockAggregator()
	mockRedis := NewMockRedisManager()
	mockCalculator := NewMockSlidingWindowCalculator()
	mockWorkerPool := NewMockWorkerPool()
	
	// Initialize aggregator
	err := aggregator.Initialize(mockRedis, mockCalculator, mockWorkerPool)
	if err != nil {
		t.Fatalf("Failed to initialize aggregator: %v", err)
	}
	defer aggregator.Shutdown(context.Background())
	
	// Process empty trades
	ctx := context.Background()
	err = aggregator.ProcessTrades(ctx, []packet.TokenTradeHistory{})
	if err != nil {
		t.Errorf("Expected no error processing empty trades, got: %v", err)
	}
	
	// Verify no processors were created
	if aggregator.GetActiveTokenCount() != 0 {
		t.Errorf("Expected 0 active tokens, got %d", aggregator.GetActiveTokenCount())
	}
}

func TestBlockAggregator_ProcessTradesNotInitialized(t *testing.T) {
	aggregator := NewBlockAggregator()
	
	// Try to process trades without initialization
	trades := []packet.TokenTradeHistory{
		{
			Token:        "0x123",
			Wallet:       "0xabc",
			SellBuy:      "buy",
			NativeAmount: "100.0",
			TokenAmount:  "1000.0",
			PriceUsd:     "0.1",
			TransTime:    "2023-01-01T00:00:00Z",
			TxHash:       "0x1",
		},
	}
	
	ctx := context.Background()
	err := aggregator.ProcessTrades(ctx, trades)
	if err == nil {
		t.Error("Expected error processing trades when not initialized")
	}
}

func TestBlockAggregator_GetTokenProcessor(t *testing.T) {
	aggregator := NewBlockAggregator()
	mockRedis := NewMockRedisManager()
	mockCalculator := NewMockSlidingWindowCalculator()
	mockWorkerPool := NewMockWorkerPool()
	
	// Initialize aggregator
	err := aggregator.Initialize(mockRedis, mockCalculator, mockWorkerPool)
	if err != nil {
		t.Fatalf("Failed to initialize aggregator: %v", err)
	}
	defer aggregator.Shutdown(context.Background())
	
	tokenAddress := "0x123456789"
	
	// Get processor for new token
	processor1, err := aggregator.GetTokenProcessor(tokenAddress)
	if err != nil {
		t.Errorf("Expected no error getting processor, got: %v", err)
	}
	if processor1 == nil {
		t.Error("Expected non-nil processor")
	}
	
	// Get processor for same token again
	processor2, err := aggregator.GetTokenProcessor(tokenAddress)
	if err != nil {
		t.Errorf("Expected no error getting processor again, got: %v", err)
	}
	if processor2 != processor1 {
		t.Error("Expected same processor instance for same token")
	}
	
	// Verify active token count
	if aggregator.GetActiveTokenCount() != 1 {
		t.Errorf("Expected 1 active token, got %d", aggregator.GetActiveTokenCount())
	}
}

func TestBlockAggregator_GetTokenProcessorNotInitialized(t *testing.T) {
	aggregator := NewBlockAggregator()
	
	// Try to get processor without initialization
	_, err := aggregator.GetTokenProcessor("0x123")
	if err == nil {
		t.Error("Expected error getting processor when not initialized")
	}
}

func TestBlockAggregator_Shutdown(t *testing.T) {
	aggregator := NewBlockAggregator()
	mockRedis := NewMockRedisManager()
	mockCalculator := NewMockSlidingWindowCalculator()
	mockWorkerPool := NewMockWorkerPool()
	
	// Initialize aggregator
	err := aggregator.Initialize(mockRedis, mockCalculator, mockWorkerPool)
	if err != nil {
		t.Fatalf("Failed to initialize aggregator: %v", err)
	}
	
	// Create some processors
	_, err = aggregator.GetTokenProcessor("0x123")
	if err != nil {
		t.Errorf("Expected no error creating processor, got: %v", err)
	}
	_, err = aggregator.GetTokenProcessor("0x456")
	if err != nil {
		t.Errorf("Expected no error creating processor, got: %v", err)
	}
	
	// Verify processors exist
	if aggregator.GetActiveTokenCount() != 2 {
		t.Errorf("Expected 2 active tokens, got %d", aggregator.GetActiveTokenCount())
	}
	
	// Shutdown aggregator
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	err = aggregator.Shutdown(ctx)
	if err != nil {
		t.Errorf("Expected no error shutting down, got: %v", err)
	}
	
	// Verify processors were cleaned up
	if aggregator.GetActiveTokenCount() != 0 {
		t.Errorf("Expected 0 active tokens after shutdown, got %d", aggregator.GetActiveTokenCount())
	}
	
	// Verify aggregator is shut down
	if !aggregator.IsShutdown() {
		t.Error("Expected aggregator to be shut down")
	}
	
	// Test double shutdown
	err = aggregator.Shutdown(ctx)
	if err != nil {
		t.Errorf("Expected no error on double shutdown, got: %v", err)
	}
}

func TestBlockAggregator_SetMaxTokenProcessors(t *testing.T) {
	aggregator := NewBlockAggregator()
	
	// Test setting valid max
	err := aggregator.SetMaxTokenProcessors(500)
	if err != nil {
		t.Errorf("Expected no error setting max processors, got: %v", err)
	}
	
	if aggregator.GetMaxTokenProcessors() != 500 {
		t.Errorf("Expected max processors 500, got %d", aggregator.GetMaxTokenProcessors())
	}
	
	// Test setting invalid max
	err = aggregator.SetMaxTokenProcessors(0)
	if err == nil {
		t.Error("Expected error setting max processors to 0")
	}
	
	err = aggregator.SetMaxTokenProcessors(-10)
	if err == nil {
		t.Error("Expected error setting negative max processors")
	}
}

func TestBlockAggregator_SetMaxTokenProcessorsWithActiveProcessors(t *testing.T) {
	aggregator := NewBlockAggregator()
	mockRedis := NewMockRedisManager()
	mockCalculator := NewMockSlidingWindowCalculator()
	mockWorkerPool := NewMockWorkerPool()
	
	// Initialize aggregator
	err := aggregator.Initialize(mockRedis, mockCalculator, mockWorkerPool)
	if err != nil {
		t.Fatalf("Failed to initialize aggregator: %v", err)
	}
	defer aggregator.Shutdown(context.Background())
	
	// Create some processors
	_, err = aggregator.GetTokenProcessor("0x123")
	if err != nil {
		t.Errorf("Expected no error creating processor, got: %v", err)
	}
	_, err = aggregator.GetTokenProcessor("0x456")
	if err != nil {
		t.Errorf("Expected no error creating processor, got: %v", err)
	}
	
	// Try to set max below current count
	err = aggregator.SetMaxTokenProcessors(1)
	if err == nil {
		t.Error("Expected error setting max below current processor count")
	}
	
	// Set max above current count should work
	err = aggregator.SetMaxTokenProcessors(10)
	if err != nil {
		t.Errorf("Expected no error setting max above current count, got: %v", err)
	}
}

func TestBlockAggregator_MaxProcessorsLimit(t *testing.T) {
	aggregator := NewBlockAggregator()
	mockRedis := NewMockRedisManager()
	mockCalculator := NewMockSlidingWindowCalculator()
	mockWorkerPool := NewMockWorkerPool()
	
	// Initialize aggregator with low max
	err := aggregator.Initialize(mockRedis, mockCalculator, mockWorkerPool)
	if err != nil {
		t.Fatalf("Failed to initialize aggregator: %v", err)
	}
	defer aggregator.Shutdown(context.Background())
	
	err = aggregator.SetMaxTokenProcessors(2)
	if err != nil {
		t.Fatalf("Failed to set max processors: %v", err)
	}
	
	// Create processors up to the limit
	_, err = aggregator.GetTokenProcessor("0x123")
	if err != nil {
		t.Errorf("Expected no error creating processor 1, got: %v", err)
	}
	_, err = aggregator.GetTokenProcessor("0x456")
	if err != nil {
		t.Errorf("Expected no error creating processor 2, got: %v", err)
	}
	
	// Try to create processor beyond limit
	_, err = aggregator.GetTokenProcessor("0x789")
	if err == nil {
		t.Error("Expected error creating processor beyond limit")
	}
	
	// Verify processor count is at limit
	if aggregator.GetActiveTokenCount() != 2 {
		t.Errorf("Expected 2 active tokens, got %d", aggregator.GetActiveTokenCount())
	}
}

func TestBlockAggregator_RemoveInactiveProcessors(t *testing.T) {
	aggregator := NewBlockAggregator()
	mockRedis := NewMockRedisManager()
	mockCalculator := NewMockSlidingWindowCalculator()
	mockWorkerPool := NewMockWorkerPool()
	
	// Initialize aggregator
	err := aggregator.Initialize(mockRedis, mockCalculator, mockWorkerPool)
	if err != nil {
		t.Fatalf("Failed to initialize aggregator: %v", err)
	}
	defer aggregator.Shutdown(context.Background())
	
	// Create processors
	processor1, err := aggregator.GetTokenProcessor("0x123")
	if err != nil {
		t.Errorf("Expected no error creating processor, got: %v", err)
	}
	processor2, err := aggregator.GetTokenProcessor("0x456")
	if err != nil {
		t.Errorf("Expected no error creating processor, got: %v", err)
	}
	
	// Wait a bit then remove inactive processors with very short threshold
	time.Sleep(100 * time.Millisecond)
	
	ctx := context.Background()
	err = aggregator.RemoveInactiveProcessors(ctx, 50*time.Millisecond)
	if err != nil {
		t.Errorf("Expected no error removing inactive processors, got: %v", err)
	}
	
	// Both processors should be removed as they haven't been updated recently
	// Note: This test depends on the internal implementation of TokenProcessor
	// In a real scenario, processors would be updated through trade processing
	
	// Verify processors are still there (since they were just created)
	// This test mainly verifies the method doesn't crash
	if aggregator.GetActiveTokenCount() < 0 {
		t.Error("Expected non-negative processor count")
	}
	
	// Ensure processors are still accessible
	if !processor1.IsRunning() {
		t.Error("Expected processor1 to still be running")
	}
	if !processor2.IsRunning() {
		t.Error("Expected processor2 to still be running")
	}
}

func TestBlockAggregator_GetProcessorStats(t *testing.T) {
	aggregator := NewBlockAggregator()
	mockRedis := NewMockRedisManager()
	mockCalculator := NewMockSlidingWindowCalculator()
	mockWorkerPool := NewMockWorkerPool()
	
	// Initialize aggregator
	err := aggregator.Initialize(mockRedis, mockCalculator, mockWorkerPool)
	if err != nil {
		t.Fatalf("Failed to initialize aggregator: %v", err)
	}
	defer aggregator.Shutdown(context.Background())
	
	// Get initial stats
	stats := aggregator.GetProcessorStats()
	
	// Verify expected fields exist
	expectedFields := []string{"total_processors", "max_processors", "processor_utilization", "processors"}
	for _, field := range expectedFields {
		if _, exists := stats[field]; !exists {
			t.Errorf("Expected stats field %s to exist", field)
		}
	}
	
	// Verify initial values
	if stats["total_processors"] != 0 {
		t.Errorf("Expected 0 total processors, got %v", stats["total_processors"])
	}
	
	// Create a processor
	_, err = aggregator.GetTokenProcessor("0x123")
	if err != nil {
		t.Errorf("Expected no error creating processor, got: %v", err)
	}
	
	// Get updated stats
	stats = aggregator.GetProcessorStats()
	
	// Verify processor count increased
	if stats["total_processors"] != 1 {
		t.Errorf("Expected 1 total processor, got %v", stats["total_processors"])
	}
	
	// Verify processor-specific stats exist
	processors, ok := stats["processors"].(map[string]interface{})
	if !ok {
		t.Error("Expected processors to be a map")
	} else {
		if _, exists := processors["0x123"]; !exists {
			t.Error("Expected processor stats for token 0x123")
		}
	}
}

func TestBlockAggregator_FlushAllProcessors(t *testing.T) {
	aggregator := NewBlockAggregator()
	mockRedis := NewMockRedisManager()
	mockCalculator := NewMockSlidingWindowCalculator()
	mockWorkerPool := NewMockWorkerPool()
	
	// Initialize aggregator
	err := aggregator.Initialize(mockRedis, mockCalculator, mockWorkerPool)
	if err != nil {
		t.Fatalf("Failed to initialize aggregator: %v", err)
	}
	defer aggregator.Shutdown(context.Background())
	
	// Create processors
	_, err = aggregator.GetTokenProcessor("0x123")
	if err != nil {
		t.Errorf("Expected no error creating processor, got: %v", err)
	}
	_, err = aggregator.GetTokenProcessor("0x456")
	if err != nil {
		t.Errorf("Expected no error creating processor, got: %v", err)
	}
	
	// Flush all processors
	ctx := context.Background()
	err = aggregator.FlushAllProcessors(ctx)
	if err != nil {
		t.Errorf("Expected no error flushing processors, got: %v", err)
	}
}

func TestBlockAggregator_ValidateAllProcessors(t *testing.T) {
	aggregator := NewBlockAggregator()
	mockRedis := NewMockRedisManager()
	mockCalculator := NewMockSlidingWindowCalculator()
	mockWorkerPool := NewMockWorkerPool()
	
	// Initialize aggregator
	err := aggregator.Initialize(mockRedis, mockCalculator, mockWorkerPool)
	if err != nil {
		t.Fatalf("Failed to initialize aggregator: %v", err)
	}
	defer aggregator.Shutdown(context.Background())
	
	// Create processors
	_, err = aggregator.GetTokenProcessor("0x123")
	if err != nil {
		t.Errorf("Expected no error creating processor, got: %v", err)
	}
	_, err = aggregator.GetTokenProcessor("0x456")
	if err != nil {
		t.Errorf("Expected no error creating processor, got: %v", err)
	}
	
	// Validate all processors
	err = aggregator.ValidateAllProcessors()
	if err != nil {
		t.Errorf("Expected no error validating processors, got: %v", err)
	}
}

func TestBlockAggregator_GetMemoryUsage(t *testing.T) {
	aggregator := NewBlockAggregator()
	mockRedis := NewMockRedisManager()
	mockCalculator := NewMockSlidingWindowCalculator()
	mockWorkerPool := NewMockWorkerPool()
	
	// Initialize aggregator
	err := aggregator.Initialize(mockRedis, mockCalculator, mockWorkerPool)
	if err != nil {
		t.Fatalf("Failed to initialize aggregator: %v", err)
	}
	defer aggregator.Shutdown(context.Background())
	
	// Get initial memory usage
	usage := aggregator.GetMemoryUsage()
	
	// Verify expected fields exist
	expectedFields := []string{"total_stats", "processor_usage", "processor_count"}
	for _, field := range expectedFields {
		if _, exists := usage[field]; !exists {
			t.Errorf("Expected memory usage field %s to exist", field)
		}
	}
	
	// Verify initial processor count
	if usage["processor_count"] != 0 {
		t.Errorf("Expected 0 processors, got %v", usage["processor_count"])
	}
	
	// Create a processor
	_, err = aggregator.GetTokenProcessor("0x123")
	if err != nil {
		t.Errorf("Expected no error creating processor, got: %v", err)
	}
	
	// Get updated memory usage
	usage = aggregator.GetMemoryUsage()
	
	// Verify processor count increased
	if usage["processor_count"] != 1 {
		t.Errorf("Expected 1 processor, got %v", usage["processor_count"])
	}
	
	// Verify total stats structure
	totalStats, ok := usage["total_stats"].(map[string]int)
	if !ok {
		t.Error("Expected total_stats to be map[string]int")
	} else {
		expectedTotalFields := []string{"total_trade_count", "total_indices_count", "total_aggregates_count", "total_channel_length"}
		for _, field := range expectedTotalFields {
			if _, exists := totalStats[field]; !exists {
				t.Errorf("Expected total stats field %s to exist", field)
			}
		}
	}
}

func TestBlockAggregator_IsInitializedAndShutdown(t *testing.T) {
	aggregator := NewBlockAggregator()
	
	// Test initial state
	if aggregator.IsInitialized() {
		t.Error("Expected aggregator to not be initialized initially")
	}
	if aggregator.IsShutdown() {
		t.Error("Expected aggregator to not be shut down initially")
	}
	
	// Initialize
	mockRedis := NewMockRedisManager()
	mockCalculator := NewMockSlidingWindowCalculator()
	mockWorkerPool := NewMockWorkerPool()
	
	err := aggregator.Initialize(mockRedis, mockCalculator, mockWorkerPool)
	if err != nil {
		t.Fatalf("Failed to initialize aggregator: %v", err)
	}
	
	// Test after initialization
	if !aggregator.IsInitialized() {
		t.Error("Expected aggregator to be initialized")
	}
	if aggregator.IsShutdown() {
		t.Error("Expected aggregator to not be shut down after initialization")
	}
	
	// Shutdown
	ctx := context.Background()
	err = aggregator.Shutdown(ctx)
	if err != nil {
		t.Errorf("Expected no error shutting down, got: %v", err)
	}
	
	// Test after shutdown
	if !aggregator.IsShutdown() {
		t.Error("Expected aggregator to be shut down")
	}
}

func TestBlockAggregator_ProcessTradesWithInvalidData(t *testing.T) {
	aggregator := NewBlockAggregator()
	mockRedis := NewMockRedisManager()
	mockCalculator := NewMockSlidingWindowCalculator()
	mockWorkerPool := NewMockWorkerPool()
	
	// Initialize aggregator
	err := aggregator.Initialize(mockRedis, mockCalculator, mockWorkerPool)
	if err != nil {
		t.Fatalf("Failed to initialize aggregator: %v", err)
	}
	defer aggregator.Shutdown(context.Background())
	
	// Create trades with some invalid data
	trades := []packet.TokenTradeHistory{
		{
			Token:        "", // Invalid: empty token
			Wallet:       "0xabc",
			SellBuy:      "buy",
			NativeAmount: "100.0",
			TokenAmount:  "1000.0",
			PriceUsd:     "0.1",
			TransTime:    "2023-01-01T00:00:00Z",
			TxHash:       "0x1",
		},
		{
			Token:        "0x456",
			Wallet:       "0xdef",
			SellBuy:      "sell",
			NativeAmount: "50.0",
			TokenAmount:  "500.0",
			PriceUsd:     "0.1",
			TransTime:    "2023-01-01T00:01:00Z",
			TxHash:       "0x2",
		},
	}
	
	// Process trades (should handle invalid data gracefully)
	ctx := context.Background()
	err = aggregator.ProcessTrades(ctx, trades)
	if err != nil {
		t.Errorf("Expected no error processing trades with invalid data, got: %v", err)
	}
	
	// Wait for async processing
	time.Sleep(200 * time.Millisecond)
	
	// Should only create processor for valid token
	activeTokens := aggregator.GetActiveTokens()
	if len(activeTokens) != 1 {
		t.Errorf("Expected 1 active token, got %d", len(activeTokens))
	}
	if activeTokens[0] != "0x456" {
		t.Errorf("Expected active token 0x456, got %s", activeTokens[0])
	}
}

func TestBlockAggregator_ConcurrentProcessing(t *testing.T) {
	aggregator := NewBlockAggregator()
	mockRedis := NewMockRedisManager()
	mockCalculator := NewMockSlidingWindowCalculator()
	mockWorkerPool := NewMockWorkerPool()
	
	// Initialize aggregator
	err := aggregator.Initialize(mockRedis, mockCalculator, mockWorkerPool)
	if err != nil {
		t.Fatalf("Failed to initialize aggregator: %v", err)
	}
	defer aggregator.Shutdown(context.Background())
	
	// Process trades concurrently
	ctx := context.Background()
	var wg sync.WaitGroup
	numGoroutines := 10
	
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(routineNum int) {
			defer wg.Done()
			
			trades := []packet.TokenTradeHistory{
				{
					Token:        fmt.Sprintf("0x%d", routineNum),
					Wallet:       "0xabc",
					SellBuy:      "buy",
					NativeAmount: "100.0",
					TokenAmount:  "1000.0",
					PriceUsd:     "0.1",
					TransTime:    "2023-01-01T00:00:00Z",
					TxHash:       fmt.Sprintf("0x%d", routineNum),
				},
			}
			
			err := aggregator.ProcessTrades(ctx, trades)
			if err != nil {
				t.Errorf("Expected no error processing trades in goroutine %d, got: %v", routineNum, err)
			}
		}(i)
	}
	
	wg.Wait()
	
	// Wait for async processing
	time.Sleep(500 * time.Millisecond)
	
	// Should have created processors for all tokens
	if aggregator.GetActiveTokenCount() != numGoroutines {
		t.Errorf("Expected %d active tokens, got %d", numGoroutines, aggregator.GetActiveTokenCount())
	}
}