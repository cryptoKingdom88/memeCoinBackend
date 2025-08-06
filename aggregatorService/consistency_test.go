package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cryptoKingdom88/memeCoinBackend/shared/packet"
	"aggregatorService/aggregation"
	"aggregatorService/config"
	"aggregatorService/models"
	"aggregatorService/processor"
	"aggregatorService/redis"
	"aggregatorService/worker"
)

// ConsistencyTestSuite provides data consistency testing utilities
type ConsistencyTestSuite struct {
	redisManager    *redis.Manager
	calculator      *aggregation.Calculator
	workerPool      *worker.Pool
	blockAggregator *processor.BlockAggregator
	ctx             context.Context
	cancel          context.CancelFunc
}

// NewConsistencyTestSuite creates a new consistency test suite
func NewConsistencyTestSuite(t *testing.T) *ConsistencyTestSuite {
	ctx, cancel := context.WithCancel(context.Background())
	
	// Create Redis manager
	redisManager := redis.NewManager("localhost:6379", "", 0, 10).(*redis.Manager)
	
	// Create calculator
	calculator := aggregation.NewCalculator().(*aggregation.Calculator)
	timeWindows := []config.TimeWindow{
		{Duration: 1 * time.Minute, Name: "1min"},
		{Duration: 5 * time.Minute, Name: "5min"},
		{Duration: 15 * time.Minute, Name: "15min"},
	}
	err := calculator.Initialize(timeWindows)
	if err != nil {
		t.Fatalf("Failed to initialize calculator: %v", err)
	}
	
	// Create worker pool
	workerPool := worker.NewPool().(*worker.Pool)
	err = workerPool.Initialize(20)
	if err != nil {
		t.Fatalf("Failed to initialize worker pool: %v", err)
	}
	err = workerPool.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start worker pool: %v", err)
	}
	
	// Create block aggregator
	blockAggregator := processor.NewBlockAggregator().(*processor.BlockAggregator)
	err = blockAggregator.Initialize(redisManager, calculator, workerPool)
	if err != nil {
		t.Fatalf("Failed to initialize block aggregator: %v", err)
	}
	
	return &ConsistencyTestSuite{
		redisManager:    redisManager,
		calculator:      calculator,
		workerPool:      workerPool,
		blockAggregator: blockAggregator,
		ctx:             ctx,
		cancel:          cancel,
	}
}

// Cleanup cleans up the consistency test suite
func (suite *ConsistencyTestSuite) Cleanup() {
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

func TestDataConsistencyUnderConcurrentLoad(t *testing.T) {
	suite := NewConsistencyTestSuite(t)
	defer suite.Cleanup()
	
	tokenAddress := "0x123456789"
	numGoroutines := 10
	tradesPerGoroutine := 50
	
	// Track expected totals
	var expectedBuyCount, expectedSellCount int64
	var expectedTotalVolume float64
	var mutex sync.Mutex
	
	// Process trades concurrently
	var wg sync.WaitGroup
	
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			
			localBuyCount := 0
			localSellCount := 0
			localTotalVolume := 0.0
			
			for i := 0; i < tradesPerGoroutine; i++ {
				sellBuy := "buy"
				if i%3 == 0 {
					sellBuy = "sell"
					localSellCount++
				} else {
					localBuyCount++
				}
				
				amount := float64(100 + i)
				localTotalVolume += amount
				
				trade := packet.TokenTradeHistory{
					Token:        tokenAddress,
					Wallet:       fmt.Sprintf("0x%d_%d", goroutineID, i),
					SellBuy:      sellBuy,
					NativeAmount: fmt.Sprintf("%.1f", amount),
					TokenAmount:  "1000.0",
					PriceUsd:     "0.1",
					TransTime:    time.Now().Add(-time.Duration(i) * time.Second).Format(time.RFC3339),
					TxHash:       fmt.Sprintf("0x%d_%d", goroutineID, i),
				}
				
				trades := []packet.TokenTradeHistory{trade}
				err := suite.blockAggregator.ProcessTrades(suite.ctx, trades)
				if err != nil {
					t.Errorf("Failed to process trade %d-%d: %v", goroutineID, i, err)
				}
			}
			
			// Update expected totals
			mutex.Lock()
			expectedBuyCount += int64(localBuyCount)
			expectedSellCount += int64(localSellCount)
			expectedTotalVolume += localTotalVolume
			mutex.Unlock()
		}(g)
	}
	
	wg.Wait()
	
	// Wait for all processing to complete
	time.Sleep(2 * time.Second)
	
	// Verify data consistency
	processor, err := suite.blockAggregator.GetTokenProcessor(tokenAddress)
	if err != nil {
		t.Fatalf("Failed to get processor: %v", err)
	}
	
	// Check trade count consistency
	tradeCount := processor.GetTradeCount()
	expectedTradeCount := numGoroutines * tradesPerGoroutine
	if tradeCount != expectedTradeCount {
		t.Errorf("Trade count mismatch: expected %d, got %d", expectedTradeCount, tradeCount)
	}
	
	// Check aggregate consistency
	aggregates, err := processor.GetAggregates(suite.ctx)
	if err != nil {
		t.Fatalf("Failed to get aggregates: %v", err)
	}
	
	// Verify 15min window (should include all trades)
	fifteenMinAgg := aggregates["15min"]
	if fifteenMinAgg == nil {
		t.Fatal("Expected 15min aggregate")
	}
	
	totalTrades := fifteenMinAgg.BuyCount + fifteenMinAgg.SellCount
	if totalTrades != int64(expectedTradeCount) {
		t.Errorf("Aggregate trade count mismatch: expected %d, got %d", expectedTradeCount, totalTrades)
	}
	
	// Verify buy/sell counts are reasonable
	if fifteenMinAgg.BuyCount == 0 {
		t.Error("Expected some buy trades in aggregate")
	}
	if fifteenMinAgg.SellCount == 0 {
		t.Error("Expected some sell trades in aggregate")
	}
	
	// Verify total volume is reasonable
	if fifteenMinAgg.TotalVolume == 0 {
		t.Error("Expected non-zero total volume in aggregate")
	}
	
	// Verify no data corruption in aggregates
	if fifteenMinAgg.BuyCount < 0 || fifteenMinAgg.SellCount < 0 {
		t.Error("Negative trade counts detected - data corruption")
	}
	if fifteenMinAgg.BuyVolume < 0 || fifteenMinAgg.SellVolume < 0 || fifteenMinAgg.TotalVolume < 0 {
		t.Error("Negative volumes detected - data corruption")
	}
}

func TestAggregateCalculationConsistency(t *testing.T) {
	suite := NewConsistencyTestSuite(t)
	defer suite.Cleanup()
	
	tokenAddress := "0x123456789"
	
	// Create a known set of trades
	trades := []packet.TokenTradeHistory{
		{
			Token:        tokenAddress,
			Wallet:       "0x1",
			SellBuy:      "buy",
			NativeAmount: "100.0",
			TokenAmount:  "1000.0",
			PriceUsd:     "0.10",
			TransTime:    time.Now().Add(-30 * time.Second).Format(time.RFC3339),
			TxHash:       "0x1",
		},
		{
			Token:        tokenAddress,
			Wallet:       "0x2",
			SellBuy:      "sell",
			NativeAmount: "50.0",
			TokenAmount:  "500.0",
			PriceUsd:     "0.10",
			TransTime:    time.Now().Add(-20 * time.Second).Format(time.RFC3339),
			TxHash:       "0x2",
		},
		{
			Token:        tokenAddress,
			Wallet:       "0x3",
			SellBuy:      "buy",
			NativeAmount: "75.0",
			TokenAmount:  "750.0",
			PriceUsd:     "0.10",
			TransTime:    time.Now().Add(-10 * time.Second).Format(time.RFC3339),
			TxHash:       "0x3",
		},
	}
	
	// Process trades
	err := suite.blockAggregator.ProcessTrades(suite.ctx, trades)
	if err != nil {
		t.Fatalf("Failed to process trades: %v", err)
	}
	
	// Wait for processing
	time.Sleep(500 * time.Millisecond)
	
	// Get aggregates
	processor, err := suite.blockAggregator.GetTokenProcessor(tokenAddress)
	if err != nil {
		t.Fatalf("Failed to get processor: %v", err)
	}
	
	aggregates, err := processor.GetAggregates(suite.ctx)
	if err != nil {
		t.Fatalf("Failed to get aggregates: %v", err)
	}
	
	// Verify 1min aggregate consistency
	oneMinAgg := aggregates["1min"]
	if oneMinAgg == nil {
		t.Fatal("Expected 1min aggregate")
	}
	
	// Expected values for 1min window (all trades should be included)
	expectedBuyCount := int64(2)  // trades 1 and 3
	expectedSellCount := int64(1) // trade 2
	expectedBuyVolume := 175.0    // 100 + 75
	expectedSellVolume := 50.0    // 50
	expectedTotalVolume := 225.0  // 175 + 50
	
	if oneMinAgg.BuyCount != expectedBuyCount {
		t.Errorf("1min buy count: expected %d, got %d", expectedBuyCount, oneMinAgg.BuyCount)
	}
	if oneMinAgg.SellCount != expectedSellCount {
		t.Errorf("1min sell count: expected %d, got %d", expectedSellCount, oneMinAgg.SellCount)
	}
	if oneMinAgg.BuyVolume != expectedBuyVolume {
		t.Errorf("1min buy volume: expected %.2f, got %.2f", expectedBuyVolume, oneMinAgg.BuyVolume)
	}
	if oneMinAgg.SellVolume != expectedSellVolume {
		t.Errorf("1min sell volume: expected %.2f, got %.2f", expectedSellVolume, oneMinAgg.SellVolume)
	}
	if oneMinAgg.TotalVolume != expectedTotalVolume {
		t.Errorf("1min total volume: expected %.2f, got %.2f", expectedTotalVolume, oneMinAgg.TotalVolume)
	}
	
	// Verify volume consistency
	calculatedTotal := oneMinAgg.BuyVolume + oneMinAgg.SellVolume
	if calculatedTotal != oneMinAgg.TotalVolume {
		t.Errorf("Volume consistency check failed: buy(%.2f) + sell(%.2f) = %.2f, but total = %.2f",
			oneMinAgg.BuyVolume, oneMinAgg.SellVolume, calculatedTotal, oneMinAgg.TotalVolume)
	}
	
	// Verify trade count consistency
	calculatedTradeCount := oneMinAgg.BuyCount + oneMinAgg.SellCount
	expectedTradeCount := int64(len(trades))
	if calculatedTradeCount != expectedTradeCount {
		t.Errorf("Trade count consistency check failed: buy(%d) + sell(%d) = %d, expected %d",
			oneMinAgg.BuyCount, oneMinAgg.SellCount, calculatedTradeCount, expectedTradeCount)
	}
}

func TestSlidingWindowConsistency(t *testing.T) {
	suite := NewConsistencyTestSuite(t)
	defer suite.Cleanup()
	
	tokenAddress := "0x123456789"
	currentTime := time.Now()
	
	// Create trades at specific time intervals to test window boundaries
	trades := []packet.TokenTradeHistory{
		{
			Token:        tokenAddress,
			Wallet:       "0x1",
			SellBuy:      "buy",
			NativeAmount: "100.0",
			TokenAmount:  "1000.0",
			PriceUsd:     "0.10",
			TransTime:    currentTime.Add(-10 * time.Minute).Format(time.RFC3339), // Outside 5min, within 15min
			TxHash:       "0x1",
		},
		{
			Token:        tokenAddress,
			Wallet:       "0x2",
			SellBuy:      "sell",
			NativeAmount: "50.0",
			TokenAmount:  "500.0",
			PriceUsd:     "0.10",
			TransTime:    currentTime.Add(-3 * time.Minute).Format(time.RFC3339), // Within 5min and 15min
			TxHash:       "0x2",
		},
		{
			Token:        tokenAddress,
			Wallet:       "0x3",
			SellBuy:      "buy",
			NativeAmount: "75.0",
			TokenAmount:  "750.0",
			PriceUsd:     "0.10",
			TransTime:    currentTime.Add(-30 * time.Second).Format(time.RFC3339), // Within 1min, 5min, and 15min
			TxHash:       "0x3",
		},
	}
	
	// Process trades
	err := suite.blockAggregator.ProcessTrades(suite.ctx, trades)
	if err != nil {
		t.Fatalf("Failed to process trades: %v", err)
	}
	
	// Wait for processing
	time.Sleep(500 * time.Millisecond)
	
	// Get aggregates
	processor, err := suite.blockAggregator.GetTokenProcessor(tokenAddress)
	if err != nil {
		t.Fatalf("Failed to get processor: %v", err)
	}
	
	aggregates, err := processor.GetAggregates(suite.ctx)
	if err != nil {
		t.Fatalf("Failed to get aggregates: %v", err)
	}
	
	// Verify 1min window (should have only trade 3)
	oneMinAgg := aggregates["1min"]
	if oneMinAgg == nil {
		t.Fatal("Expected 1min aggregate")
	}
	if oneMinAgg.GetTotalTradeCount() != 1 {
		t.Errorf("1min window: expected 1 trade, got %d", oneMinAgg.GetTotalTradeCount())
	}
	if oneMinAgg.BuyCount != 1 || oneMinAgg.SellCount != 0 {
		t.Errorf("1min window: expected 1 buy, 0 sell, got %d buy, %d sell", oneMinAgg.BuyCount, oneMinAgg.SellCount)
	}
	
	// Verify 5min window (should have trades 2 and 3)
	fiveMinAgg := aggregates["5min"]
	if fiveMinAgg == nil {
		t.Fatal("Expected 5min aggregate")
	}
	if fiveMinAgg.GetTotalTradeCount() != 2 {
		t.Errorf("5min window: expected 2 trades, got %d", fiveMinAgg.GetTotalTradeCount())
	}
	if fiveMinAgg.BuyCount != 1 || fiveMinAgg.SellCount != 1 {
		t.Errorf("5min window: expected 1 buy, 1 sell, got %d buy, %d sell", fiveMinAgg.BuyCount, fiveMinAgg.SellCount)
	}
	
	// Verify 15min window (should have all trades)
	fifteenMinAgg := aggregates["15min"]
	if fifteenMinAgg == nil {
		t.Fatal("Expected 15min aggregate")
	}
	if fifteenMinAgg.GetTotalTradeCount() != 3 {
		t.Errorf("15min window: expected 3 trades, got %d", fifteenMinAgg.GetTotalTradeCount())
	}
	if fifteenMinAgg.BuyCount != 2 || fifteenMinAgg.SellCount != 1 {
		t.Errorf("15min window: expected 2 buy, 1 sell, got %d buy, %d sell", fifteenMinAgg.BuyCount, fifteenMinAgg.SellCount)
	}
	
	// Verify window hierarchy consistency
	if oneMinAgg.TotalVolume > fiveMinAgg.TotalVolume {
		t.Error("Window hierarchy violation: 1min volume > 5min volume")
	}
	if fiveMinAgg.TotalVolume > fifteenMinAgg.TotalVolume {
		t.Error("Window hierarchy violation: 5min volume > 15min volume")
	}
	
	if oneMinAgg.GetTotalTradeCount() > fiveMinAgg.GetTotalTradeCount() {
		t.Error("Window hierarchy violation: 1min trade count > 5min trade count")
	}
	if fiveMinAgg.GetTotalTradeCount() > fifteenMinAgg.GetTotalTradeCount() {
		t.Error("Window hierarchy violation: 5min trade count > 15min trade count")
	}
}

func TestProcessorStateConsistency(t *testing.T) {
	suite := NewConsistencyTestSuite(t)
	defer suite.Cleanup()
	
	tokenAddress := "0x123456789"
	
	// Process some initial trades
	initialTrades := []packet.TokenTradeHistory{
		{
			Token:        tokenAddress,
			Wallet:       "0x1",
			SellBuy:      "buy",
			NativeAmount: "100.0",
			TokenAmount:  "1000.0",
			PriceUsd:     "0.10",
			TransTime:    time.Now().Add(-2 * time.Minute).Format(time.RFC3339),
			TxHash:       "0x1",
		},
		{
			Token:        tokenAddress,
			Wallet:       "0x2",
			SellBuy:      "sell",
			NativeAmount: "50.0",
			TokenAmount:  "500.0",
			PriceUsd:     "0.10",
			TransTime:    time.Now().Add(-1 * time.Minute).Format(time.RFC3339),
			TxHash:       "0x2",
		},
	}
	
	err := suite.blockAggregator.ProcessTrades(suite.ctx, initialTrades)
	if err != nil {
		t.Fatalf("Failed to process initial trades: %v", err)
	}
	
	// Wait for processing
	time.Sleep(300 * time.Millisecond)
	
	// Get processor
	processor, err := suite.blockAggregator.GetTokenProcessor(tokenAddress)
	if err != nil {
		t.Fatalf("Failed to get processor: %v", err)
	}
	
	// Verify initial state
	initialTradeCount := processor.GetTradeCount()
	if initialTradeCount != 2 {
		t.Errorf("Expected 2 initial trades, got %d", initialTradeCount)
	}
	
	initialAggregates, err := processor.GetAggregates(suite.ctx)
	if err != nil {
		t.Fatalf("Failed to get initial aggregates: %v", err)
	}
	
	// Process additional trades
	additionalTrades := []packet.TokenTradeHistory{
		{
			Token:        tokenAddress,
			Wallet:       "0x3",
			SellBuy:      "buy",
			NativeAmount: "75.0",
			TokenAmount:  "750.0",
			PriceUsd:     "0.10",
			TransTime:    time.Now().Add(-30 * time.Second).Format(time.RFC3339),
			TxHash:       "0x3",
		},
	}
	
	err = suite.blockAggregator.ProcessTrades(suite.ctx, additionalTrades)
	if err != nil {
		t.Fatalf("Failed to process additional trades: %v", err)
	}
	
	// Wait for processing
	time.Sleep(300 * time.Millisecond)
	
	// Verify state consistency after additional processing
	finalTradeCount := processor.GetTradeCount()
	expectedFinalCount := initialTradeCount + len(additionalTrades)
	if finalTradeCount != expectedFinalCount {
		t.Errorf("Expected %d final trades, got %d", expectedFinalCount, finalTradeCount)
	}
	
	finalAggregates, err := processor.GetAggregates(suite.ctx)
	if err != nil {
		t.Fatalf("Failed to get final aggregates: %v", err)
	}
	
	// Verify aggregates were updated correctly
	initialOneMin := initialAggregates["1min"]
	finalOneMin := finalAggregates["1min"]
	
	if finalOneMin.GetTotalTradeCount() <= initialOneMin.GetTotalTradeCount() {
		t.Error("Expected trade count to increase after processing additional trades")
	}
	
	if finalOneMin.TotalVolume <= initialOneMin.TotalVolume {
		t.Error("Expected total volume to increase after processing additional trades")
	}
	
	// Verify no negative values
	for timeframe, agg := range finalAggregates {
		if agg.BuyCount < 0 || agg.SellCount < 0 {
			t.Errorf("Negative trade counts in %s window: buy=%d, sell=%d", timeframe, agg.BuyCount, agg.SellCount)
		}
		if agg.BuyVolume < 0 || agg.SellVolume < 0 || agg.TotalVolume < 0 {
			t.Errorf("Negative volumes in %s window: buy=%.2f, sell=%.2f, total=%.2f", 
				timeframe, agg.BuyVolume, agg.SellVolume, agg.TotalVolume)
		}
	}
}

func TestConcurrentProcessorAccess(t *testing.T) {
	suite := NewConsistencyTestSuite(t)
	defer suite.Cleanup()
	
	tokenAddress := "0x123456789"
	numGoroutines := 5
	operationsPerGoroutine := 20
	
	// Process initial trades to create processor
	initialTrade := packet.TokenTradeHistory{
		Token:        tokenAddress,
		Wallet:       "0x0",
		SellBuy:      "buy",
		NativeAmount: "100.0",
		TokenAmount:  "1000.0",
		PriceUsd:     "0.10",
		TransTime:    time.Now().Format(time.RFC3339),
		TxHash:       "0x0",
	}
	
	err := suite.blockAggregator.ProcessTrades(suite.ctx, []packet.TokenTradeHistory{initialTrade})
	if err != nil {
		t.Fatalf("Failed to process initial trade: %v", err)
	}
	
	// Wait for processor creation
	time.Sleep(200 * time.Millisecond)
	
	// Concurrently access processor
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*operationsPerGoroutine)
	
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			
			for i := 0; i < operationsPerGoroutine; i++ {
				// Alternate between processing trades and getting aggregates
				if i%2 == 0 {
					// Process a trade
					trade := packet.TokenTradeHistory{
						Token:        tokenAddress,
						Wallet:       fmt.Sprintf("0x%d_%d", goroutineID, i),
						SellBuy:      "buy",
						NativeAmount: "100.0",
						TokenAmount:  "1000.0",
						PriceUsd:     "0.10",
						TransTime:    time.Now().Format(time.RFC3339),
						TxHash:       fmt.Sprintf("0x%d_%d", goroutineID, i),
					}
					
					err := suite.blockAggregator.ProcessTrades(suite.ctx, []packet.TokenTradeHistory{trade})
					if err != nil {
						errors <- fmt.Errorf("goroutine %d, operation %d: process trade failed: %v", goroutineID, i, err)
					}
				} else {
					// Get aggregates
					processor, err := suite.blockAggregator.GetTokenProcessor(tokenAddress)
					if err != nil {
						errors <- fmt.Errorf("goroutine %d, operation %d: get processor failed: %v", goroutineID, i, err)
						continue
					}
					
					_, err = processor.GetAggregates(suite.ctx)
					if err != nil {
						errors <- fmt.Errorf("goroutine %d, operation %d: get aggregates failed: %v", goroutineID, i, err)
					}
				}
			}
		}(g)
	}
	
	wg.Wait()
	close(errors)
	
	// Check for errors
	var errorCount int
	for err := range errors {
		t.Errorf("Concurrent access error: %v", err)
		errorCount++
	}
	
	if errorCount > 0 {
		t.Errorf("Total concurrent access errors: %d", errorCount)
	}
	
	// Wait for all processing to complete
	time.Sleep(500 * time.Millisecond)
	
	// Verify final state is consistent
	processor, err := suite.blockAggregator.GetTokenProcessor(tokenAddress)
	if err != nil {
		t.Fatalf("Failed to get processor for final verification: %v", err)
	}
	
	finalAggregates, err := processor.GetAggregates(suite.ctx)
	if err != nil {
		t.Fatalf("Failed to get final aggregates: %v", err)
	}
	
	// Verify no data corruption
	for timeframe, agg := range finalAggregates {
		if agg.BuyCount < 0 || agg.SellCount < 0 {
			t.Errorf("Data corruption detected in %s: negative trade counts", timeframe)
		}
		if agg.BuyVolume < 0 || agg.SellVolume < 0 || agg.TotalVolume < 0 {
			t.Errorf("Data corruption detected in %s: negative volumes", timeframe)
		}
		
		// Verify volume consistency
		calculatedTotal := agg.BuyVolume + agg.SellVolume
		if calculatedTotal != agg.TotalVolume {
			t.Errorf("Volume inconsistency in %s: buy(%.2f) + sell(%.2f) != total(%.2f)", 
				timeframe, agg.BuyVolume, agg.SellVolume, agg.TotalVolume)
		}
	}
}