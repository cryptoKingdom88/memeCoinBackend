package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/cryptoKingdom88/memeCoinBackend/shared/packet"
	"aggregatorService/aggregation"
	"aggregatorService/config"
	"aggregatorService/processor"
	"aggregatorService/redis"
	"aggregatorService/worker"
)

// PerformanceTestSuite provides performance testing utilities
type PerformanceTestSuite struct {
	redisManager    *redis.Manager
	calculator      *aggregation.Calculator
	workerPool      *worker.Pool
	blockAggregator *processor.BlockAggregator
	ctx             context.Context
	cancel          context.CancelFunc
}

// NewPerformanceTestSuite creates a new performance test suite
func NewPerformanceTestSuite(b *testing.B) *PerformanceTestSuite {
	ctx, cancel := context.WithCancel(context.Background())
	
	// Create Redis manager (using in-memory mock for performance tests)
	redisManager := redis.NewManager("localhost:6379", "", 0, 20).(*redis.Manager)
	
	// Create calculator
	calculator := aggregation.NewCalculator().(*aggregation.Calculator)
	timeWindows := []config.TimeWindow{
		{Duration: 1 * time.Minute, Name: "1min"},
		{Duration: 5 * time.Minute, Name: "5min"},
		{Duration: 15 * time.Minute, Name: "15min"},
		{Duration: 30 * time.Minute, Name: "30min"},
		{Duration: 1 * time.Hour, Name: "1hour"},
	}
	err := calculator.Initialize(timeWindows)
	if err != nil {
		b.Fatalf("Failed to initialize calculator: %v", err)
	}
	
	// Create worker pool with more workers for performance
	workerPool := worker.NewPool().(*worker.Pool)
	err = workerPool.Initialize(50)
	if err != nil {
		b.Fatalf("Failed to initialize worker pool: %v", err)
	}
	err = workerPool.Start(ctx)
	if err != nil {
		b.Fatalf("Failed to start worker pool: %v", err)
	}
	
	// Create block aggregator
	blockAggregator := processor.NewBlockAggregator().(*processor.BlockAggregator)
	err = blockAggregator.Initialize(redisManager, calculator, workerPool)
	if err != nil {
		b.Fatalf("Failed to initialize block aggregator: %v", err)
	}
	
	return &PerformanceTestSuite{
		redisManager:    redisManager,
		calculator:      calculator,
		workerPool:      workerPool,
		blockAggregator: blockAggregator,
		ctx:             ctx,
		cancel:          cancel,
	}
}

// Cleanup cleans up the performance test suite
func (suite *PerformanceTestSuite) Cleanup() {
	suite.cancel()
	if suite.blockAggregator != nil {
		suite.blockAggregator.Shutdown(context.Background())
	}
	if suite.workerPool != nil {
		suite.workerPool.Stop(context.Background())
	}
	if suite.redisManager != nil {
		suite.redisManager.Close()
	}
}

// BenchmarkSingleTokenProcessing benchmarks processing trades for a single token
func BenchmarkSingleTokenProcessing(b *testing.B) {
	suite := NewPerformanceTestSuite(b)
	defer suite.Cleanup()
	
	tokenAddress := "0x123456789"
	
	// Create a template trade
	templateTrade := packet.TokenTradeHistory{
		Token:        tokenAddress,
		Wallet:       "0xabc",
		SellBuy:      "buy",
		NativeAmount: "100.0",
		TokenAmount:  "1000.0",
		PriceUsd:     "0.1",
		TransTime:    time.Now().Format(time.RFC3339),
		TxHash:       "0x1",
	}
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		// Create a unique trade for each iteration
		trade := templateTrade
		trade.TxHash = fmt.Sprintf("0x%d", i)
		trade.Wallet = fmt.Sprintf("0x%d", i%1000) // Vary wallet addresses
		
		trades := []packet.TokenTradeHistory{trade}
		
		err := suite.blockAggregator.ProcessTrades(suite.ctx, trades)
		if err != nil {
			b.Errorf("Failed to process trade %d: %v", i, err)
		}
	}
	
	// Wait for all processing to complete
	time.Sleep(100 * time.Millisecond)
}

// BenchmarkMultiTokenProcessing benchmarks processing trades for multiple tokens
func BenchmarkMultiTokenProcessing(b *testing.B) {
	suite := NewPerformanceTestSuite(b)
	defer suite.Cleanup()
	
	numTokens := 10
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		// Create trades for multiple tokens
		trades := make([]packet.TokenTradeHistory, numTokens)
		for j := 0; j < numTokens; j++ {
			trades[j] = packet.TokenTradeHistory{
				Token:        fmt.Sprintf("0x%d", j),
				Wallet:       fmt.Sprintf("0x%d", i),
				SellBuy:      "buy",
				NativeAmount: "100.0",
				TokenAmount:  "1000.0",
				PriceUsd:     "0.1",
				TransTime:    time.Now().Format(time.RFC3339),
				TxHash:       fmt.Sprintf("0x%d_%d", i, j),
			}
		}
		
		err := suite.blockAggregator.ProcessTrades(suite.ctx, trades)
		if err != nil {
			b.Errorf("Failed to process trades %d: %v", i, err)
		}
	}
	
	// Wait for all processing to complete
	time.Sleep(200 * time.Millisecond)
}

// BenchmarkSlidingWindowCalculation benchmarks the sliding window calculation
func BenchmarkSlidingWindowCalculation(b *testing.B) {
	calculator := aggregation.NewCalculator()
	
	timeWindows := []config.TimeWindow{
		{Duration: 1 * time.Minute, Name: "1min"},
		{Duration: 5 * time.Minute, Name: "5min"},
		{Duration: 15 * time.Minute, Name: "15min"},
		{Duration: 30 * time.Minute, Name: "30min"},
		{Duration: 1 * time.Hour, Name: "1hour"},
	}
	
	err := calculator.Initialize(timeWindows)
	if err != nil {
		b.Fatalf("Failed to initialize calculator: %v", err)
	}
	
	// Create a large set of historical trades
	numTrades := 1000
	trades := make([]models.TradeData, numTrades)
	currentTime := time.Now()
	
	for i := 0; i < numTrades; i++ {
		trades[i] = models.TradeData{
			Token:        "0x123",
			Wallet:       fmt.Sprintf("0x%d", i),
			SellBuy:      "buy",
			NativeAmount: 100.0,
			TokenAmount:  1000.0,
			PriceUsd:     0.1,
			TransTime:    currentTime.Add(-time.Duration(i) * time.Second),
			TxHash:       fmt.Sprintf("0x%d", i),
		}
	}
	
	// Initial state
	indices := make(map[string]int64)
	aggregates := make(map[string]*models.AggregateData)
	for _, window := range timeWindows {
		indices[window.Name] = 0
		aggregates[window.Name] = models.NewAggregateData()
	}
	
	b.ResetTimer()
	b.ReportAllocs()
	
	ctx := context.Background()
	
	for i := 0; i < b.N; i++ {
		// Create a new trade to add
		newTrade := models.TradeData{
			Token:        "0x123",
			Wallet:       fmt.Sprintf("0xnew%d", i),
			SellBuy:      "sell",
			NativeAmount: 50.0,
			TokenAmount:  500.0,
			PriceUsd:     0.1,
			TransTime:    currentTime.Add(time.Duration(i) * time.Millisecond),
			TxHash:       fmt.Sprintf("0xnew%d", i),
		}
		
		// Update windows
		newIndices, newAggregates, err := calculator.UpdateWindows(
			ctx,
			trades,
			indices,
			aggregates,
			newTrade,
		)
		if err != nil {
			b.Errorf("Failed to update windows %d: %v", i, err)
		}
		
		// Update state for next iteration
		indices = newIndices
		aggregates = newAggregates
	}
}

// BenchmarkConcurrentProcessing benchmarks concurrent trade processing
func BenchmarkConcurrentProcessing(b *testing.B) {
	suite := NewPerformanceTestSuite(b)
	defer suite.Cleanup()
	
	numGoroutines := runtime.NumCPU()
	tradesPerGoroutine := 100
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		
		for g := 0; g < numGoroutines; g++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				
				for t := 0; t < tradesPerGoroutine; t++ {
					trade := packet.TokenTradeHistory{
						Token:        fmt.Sprintf("0x%d", goroutineID%10), // 10 different tokens
						Wallet:       fmt.Sprintf("0x%d_%d", goroutineID, t),
						SellBuy:      "buy",
						NativeAmount: "100.0",
						TokenAmount:  "1000.0",
						PriceUsd:     "0.1",
						TransTime:    time.Now().Format(time.RFC3339),
						TxHash:       fmt.Sprintf("0x%d_%d_%d", i, goroutineID, t),
					}
					
					trades := []packet.TokenTradeHistory{trade}
					err := suite.blockAggregator.ProcessTrades(suite.ctx, trades)
					if err != nil {
						b.Errorf("Failed to process trade: %v", err)
					}
				}
			}(g)
		}
		
		wg.Wait()
	}
	
	// Wait for all processing to complete
	time.Sleep(500 * time.Millisecond)
}

// BenchmarkMemoryAllocation benchmarks memory allocation patterns
func BenchmarkMemoryAllocation(b *testing.B) {
	suite := NewPerformanceTestSuite(b)
	defer suite.Cleanup()
	
	// Get initial memory stats
	var m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		// Create a batch of trades
		batchSize := 50
		trades := make([]packet.TokenTradeHistory, batchSize)
		
		for j := 0; j < batchSize; j++ {
			trades[j] = packet.TokenTradeHistory{
				Token:        fmt.Sprintf("0x%d", j%5), // 5 different tokens
				Wallet:       fmt.Sprintf("0x%d", i*batchSize+j),
				SellBuy:      "buy",
				NativeAmount: "100.0",
				TokenAmount:  "1000.0",
				PriceUsd:     "0.1",
				TransTime:    time.Now().Format(time.RFC3339),
				TxHash:       fmt.Sprintf("0x%d_%d", i, j),
			}
		}
		
		err := suite.blockAggregator.ProcessTrades(suite.ctx, trades)
		if err != nil {
			b.Errorf("Failed to process trades %d: %v", i, err)
		}
	}
	
	// Wait for processing
	time.Sleep(200 * time.Millisecond)
	
	// Get final memory stats
	var m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m2)
	
	b.StopTimer()
	
	// Report memory usage
	allocatedMB := float64(m2.TotalAlloc-m1.TotalAlloc) / 1024 / 1024
	b.ReportMetric(allocatedMB, "MB_allocated")
	
	heapMB := float64(m2.HeapAlloc) / 1024 / 1024
	b.ReportMetric(heapMB, "MB_heap")
}

// BenchmarkHighFrequencyTrading simulates high-frequency trading scenario
func BenchmarkHighFrequencyTrading(b *testing.B) {
	suite := NewPerformanceTestSuite(b)
	defer suite.Cleanup()
	
	// Simulate high-frequency trading with many small batches
	batchSize := 10
	numTokens := 20
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		// Create a small batch of trades for random tokens
		trades := make([]packet.TokenTradeHistory, batchSize)
		
		for j := 0; j < batchSize; j++ {
			tokenID := (i*batchSize + j) % numTokens
			trades[j] = packet.TokenTradeHistory{
				Token:        fmt.Sprintf("0x%d", tokenID),
				Wallet:       fmt.Sprintf("0x%d", i*batchSize+j),
				SellBuy:      []string{"buy", "sell"}[j%2],
				NativeAmount: fmt.Sprintf("%.1f", float64(100+j)),
				TokenAmount:  "1000.0",
				PriceUsd:     fmt.Sprintf("%.4f", 0.1+float64(j)*0.001),
				TransTime:    time.Now().Add(-time.Duration(j) * time.Millisecond).Format(time.RFC3339),
				TxHash:       fmt.Sprintf("0x%d_%d", i, j),
			}
		}
		
		err := suite.blockAggregator.ProcessTrades(suite.ctx, trades)
		if err != nil {
			b.Errorf("Failed to process HFT batch %d: %v", i, err)
		}
	}
	
	// Brief wait for processing
	time.Sleep(50 * time.Millisecond)
}

// BenchmarkAggregateRetrieval benchmarks retrieving aggregates
func BenchmarkAggregateRetrieval(b *testing.B) {
	suite := NewPerformanceTestSuite(b)
	defer suite.Cleanup()
	
	// Set up some processors with data
	numTokens := 10
	tradesPerToken := 100
	
	for tokenNum := 0; tokenNum < numTokens; tokenNum++ {
		tokenAddress := fmt.Sprintf("0x%d", tokenNum)
		
		trades := make([]packet.TokenTradeHistory, tradesPerToken)
		for i := 0; i < tradesPerToken; i++ {
			trades[i] = packet.TokenTradeHistory{
				Token:        tokenAddress,
				Wallet:       fmt.Sprintf("0x%d", i),
				SellBuy:      "buy",
				NativeAmount: "100.0",
				TokenAmount:  "1000.0",
				PriceUsd:     "0.1",
				TransTime:    time.Now().Add(-time.Duration(i) * time.Second).Format(time.RFC3339),
				TxHash:       fmt.Sprintf("0x%d_%d", tokenNum, i),
			}
		}
		
		err := suite.blockAggregator.ProcessTrades(suite.ctx, trades)
		if err != nil {
			b.Fatalf("Failed to set up test data: %v", err)
		}
	}
	
	// Wait for setup to complete
	time.Sleep(1 * time.Second)
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		tokenID := i % numTokens
		tokenAddress := fmt.Sprintf("0x%d", tokenID)
		
		processor, err := suite.blockAggregator.GetTokenProcessor(tokenAddress)
		if err != nil {
			b.Errorf("Failed to get processor %d: %v", i, err)
			continue
		}
		
		_, err = processor.GetAggregates(suite.ctx)
		if err != nil {
			b.Errorf("Failed to get aggregates %d: %v", i, err)
		}
	}
}

// BenchmarkWorkerPoolUtilization benchmarks worker pool utilization
func BenchmarkWorkerPoolUtilization(b *testing.B) {
	suite := NewPerformanceTestSuite(b)
	defer suite.Cleanup()
	
	// Create a large number of small jobs to test worker pool efficiency
	jobsPerIteration := 100
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		
		for j := 0; j < jobsPerIteration; j++ {
			wg.Add(1)
			
			trade := packet.TokenTradeHistory{
				Token:        fmt.Sprintf("0x%d", j%10),
				Wallet:       fmt.Sprintf("0x%d", i*jobsPerIteration+j),
				SellBuy:      "buy",
				NativeAmount: "100.0",
				TokenAmount:  "1000.0",
				PriceUsd:     "0.1",
				TransTime:    time.Now().Format(time.RFC3339),
				TxHash:       fmt.Sprintf("0x%d_%d", i, j),
			}
			
			go func(t packet.TokenTradeHistory) {
				defer wg.Done()
				
				trades := []packet.TokenTradeHistory{t}
				err := suite.blockAggregator.ProcessTrades(suite.ctx, trades)
				if err != nil {
					b.Errorf("Failed to process trade: %v", err)
				}
			}(trade)
		}
		
		wg.Wait()
	}
}

// BenchmarkDataStructureOperations benchmarks core data structure operations
func BenchmarkDataStructureOperations(b *testing.B) {
	// Test AggregateData operations
	b.Run("AggregateData_AddTrade", func(b *testing.B) {
		aggregate := models.NewAggregateData()
		
		trade := models.TradeData{
			Token:        "0x123",
			Wallet:       "0xabc",
			SellBuy:      "buy",
			NativeAmount: 100.0,
			TokenAmount:  1000.0,
			PriceUsd:     0.1,
			TransTime:    time.Now(),
			TxHash:       "0x1",
		}
		
		b.ResetTimer()
		b.ReportAllocs()
		
		for i := 0; i < b.N; i++ {
			aggregate.AddTrade(trade)
		}
	})
	
	b.Run("AggregateData_Clone", func(b *testing.B) {
		aggregate := models.NewAggregateData()
		aggregate.BuyCount = 100
		aggregate.SellCount = 50
		aggregate.BuyVolume = 10000.0
		aggregate.SellVolume = 5000.0
		aggregate.TotalVolume = 15000.0
		
		b.ResetTimer()
		b.ReportAllocs()
		
		for i := 0; i < b.N; i++ {
			_ = aggregate.Clone()
		}
	})
	
	b.Run("TradeData_Validation", func(b *testing.B) {
		trade := models.TradeData{
			Token:        "0x123",
			Wallet:       "0xabc",
			SellBuy:      "buy",
			NativeAmount: 100.0,
			TokenAmount:  1000.0,
			PriceUsd:     0.1,
			TransTime:    time.Now(),
			TxHash:       "0x1",
		}
		
		b.ResetTimer()
		b.ReportAllocs()
		
		for i := 0; i < b.N; i++ {
			_ = trade.ValidateTradeData()
		}
	})
}

// BenchmarkTimeWindowCalculations benchmarks time window boundary calculations
func BenchmarkTimeWindowCalculations(b *testing.B) {
	utils := models.NewTimeWindowUtils()
	currentTime := time.Now()
	windowDuration := 5 * time.Minute
	
	b.Run("GetWindowBoundary", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		
		for i := 0; i < b.N; i++ {
			_ = utils.GetWindowBoundary(currentTime, windowDuration)
		}
	})
	
	b.Run("IsWithinWindow", func(b *testing.B) {
		testTime := currentTime.Add(-2 * time.Minute)
		
		b.ResetTimer()
		b.ReportAllocs()
		
		for i := 0; i < b.N; i++ {
			_ = utils.IsWithinWindow(testTime, currentTime, windowDuration)
		}
	})
	
	b.Run("CompareTimestamps", func(b *testing.B) {
		t1 := currentTime.Add(-1 * time.Minute)
		t2 := currentTime
		
		b.ResetTimer()
		b.ReportAllocs()
		
		for i := 0; i < b.N; i++ {
			_ = utils.CompareTimestamps(t1, t2)
		}
	})
}