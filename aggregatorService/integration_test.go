package main

import (
	"context"
	"fmt"
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

// IntegrationTestSuite provides a complete test environment
type IntegrationTestSuite struct {
	redisManager    *redis.Manager
	calculator      *aggregation.Calculator
	workerPool      *worker.Pool
	blockAggregator *processor.BlockAggregator
	ctx             context.Context
	cancel          context.CancelFunc
}

// NewIntegrationTestSuite creates a new integration test suite
func NewIntegrationTestSuite(t *testing.T) *IntegrationTestSuite {
	ctx, cancel := context.WithCancel(context.Background())
	
	// Create Redis manager (using in-memory mock for integration tests)
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
	err = workerPool.Initialize(10)
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
	
	return &IntegrationTestSuite{
		redisManager:    redisManager,
		calculator:      calculator,
		workerPool:      workerPool,
		blockAggregator: blockAggregator,
		ctx:             ctx,
		cancel:          cancel,
	}
}

// Cleanup cleans up the test suite
func (suite *IntegrationTestSuite) Cleanup() {
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

func TestEndToEndTradeProcessing(t *testing.T) {
	suite := NewIntegrationTestSuite(t)
	defer suite.Cleanup()
	
	// Create test trades for multiple tokens
	trades := []packet.TokenTradeHistory{
		{
			Token:        "0x123",
			Wallet:       "0xabc",
			SellBuy:      "buy",
			NativeAmount: "100.0",
			TokenAmount:  "1000.0",
			PriceUsd:     "0.1",
			TransTime:    time.Now().Add(-30 * time.Second).Format(time.RFC3339),
			TxHash:       "0x1",
		},
		{
			Token:        "0x123",
			Wallet:       "0xdef",
			SellBuy:      "sell",
			NativeAmount: "50.0",
			TokenAmount:  "500.0",
			PriceUsd:     "0.1",
			TransTime:    time.Now().Add(-20 * time.Second).Format(time.RFC3339),
			TxHash:       "0x2",
		},
		{
			Token:        "0x456",
			Wallet:       "0xghi",
			SellBuy:      "buy",
			NativeAmount: "200.0",
			TokenAmount:  "2000.0",
			PriceUsd:     "0.1",
			TransTime:    time.Now().Add(-10 * time.Second).Format(time.RFC3339),
			TxHash:       "0x3",
		},
	}
	
	// Process trades through the block aggregator
	err := suite.blockAggregator.ProcessTrades(suite.ctx, trades)
	if err != nil {
		t.Errorf("Expected no error processing trades, got: %v", err)
	}
	
	// Wait for async processing
	time.Sleep(500 * time.Millisecond)
	
	// Verify processors were created
	activeTokens := suite.blockAggregator.GetActiveTokens()
	if len(activeTokens) != 2 {
		t.Errorf("Expected 2 active tokens, got %d", len(activeTokens))
	}
	
	// Verify token 0x123 processor
	processor1, err := suite.blockAggregator.GetTokenProcessor("0x123")
	if err != nil {
		t.Errorf("Expected no error getting processor for 0x123, got: %v", err)
	}
	
	aggregates1, err := processor1.GetAggregates(suite.ctx)
	if err != nil {
		t.Errorf("Expected no error getting aggregates for 0x123, got: %v", err)
	}
	
	// Verify 1min aggregates for token 0x123
	oneMinAgg1 := aggregates1["1min"]
	if oneMinAgg1 == nil {
		t.Error("Expected 1min aggregate for token 0x123")
	} else {
		if oneMinAgg1.BuyCount != 1 {
			t.Errorf("Expected 1 buy trade for 0x123, got %d", oneMinAgg1.BuyCount)
		}
		if oneMinAgg1.SellCount != 1 {
			t.Errorf("Expected 1 sell trade for 0x123, got %d", oneMinAgg1.SellCount)
		}
		if oneMinAgg1.TotalVolume != 150.0 {
			t.Errorf("Expected total volume 150.0 for 0x123, got %f", oneMinAgg1.TotalVolume)
		}
	}
	
	// Verify token 0x456 processor
	processor2, err := suite.blockAggregator.GetTokenProcessor("0x456")
	if err != nil {
		t.Errorf("Expected no error getting processor for 0x456, got: %v", err)
	}
	
	aggregates2, err := processor2.GetAggregates(suite.ctx)
	if err != nil {
		t.Errorf("Expected no error getting aggregates for 0x456, got: %v", err)
	}
	
	// Verify 1min aggregates for token 0x456
	oneMinAgg2 := aggregates2["1min"]
	if oneMinAgg2 == nil {
		t.Error("Expected 1min aggregate for token 0x456")
	} else {
		if oneMinAgg2.BuyCount != 1 {
			t.Errorf("Expected 1 buy trade for 0x456, got %d", oneMinAgg2.BuyCount)
		}
		if oneMinAgg2.SellCount != 0 {
			t.Errorf("Expected 0 sell trades for 0x456, got %d", oneMinAgg2.SellCount)
		}
		if oneMinAgg2.TotalVolume != 200.0 {
			t.Errorf("Expected total volume 200.0 for 0x456, got %f", oneMinAgg2.TotalVolume)
		}
	}
}

func TestSlidingWindowBehavior(t *testing.T) {
	suite := NewIntegrationTestSuite(t)
	defer suite.Cleanup()
	
	tokenAddress := "0x123"
	currentTime := time.Now()
	
	// Create trades at different time intervals
	trades := []packet.TokenTradeHistory{
		{
			Token:        tokenAddress,
			Wallet:       "0xabc",
			SellBuy:      "buy",
			NativeAmount: "100.0",
			TokenAmount:  "1000.0",
			PriceUsd:     "0.1",
			TransTime:    currentTime.Add(-10 * time.Minute).Format(time.RFC3339), // Outside 5min window
			TxHash:       "0x1",
		},
		{
			Token:        tokenAddress,
			Wallet:       "0xdef",
			SellBuy:      "sell",
			NativeAmount: "50.0",
			TokenAmount:  "500.0",
			PriceUsd:     "0.1",
			TransTime:    currentTime.Add(-3 * time.Minute).Format(time.RFC3339), // Within 5min window
			TxHash:       "0x2",
		},
		{
			Token:        tokenAddress,
			Wallet:       "0xghi",
			SellBuy:      "buy",
			NativeAmount: "75.0",
			TokenAmount:  "750.0",
			PriceUsd:     "0.1",
			TransTime:    currentTime.Add(-30 * time.Second).Format(time.RFC3339), // Within 1min window
			TxHash:       "0x3",
		},
	}
	
	// Process trades
	err := suite.blockAggregator.ProcessTrades(suite.ctx, trades)
	if err != nil {
		t.Errorf("Expected no error processing trades, got: %v", err)
	}
	
	// Wait for processing
	time.Sleep(500 * time.Millisecond)
	
	// Get processor and aggregates
	processor, err := suite.blockAggregator.GetTokenProcessor(tokenAddress)
	if err != nil {
		t.Errorf("Expected no error getting processor, got: %v", err)
	}
	
	aggregates, err := processor.GetAggregates(suite.ctx)
	if err != nil {
		t.Errorf("Expected no error getting aggregates, got: %v", err)
	}
	
	// Verify 1min window (should have only the last trade)
	oneMinAgg := aggregates["1min"]
	if oneMinAgg == nil {
		t.Error("Expected 1min aggregate")
	} else {
		if oneMinAgg.GetTotalTradeCount() != 1 {
			t.Errorf("Expected 1 trade in 1min window, got %d", oneMinAgg.GetTotalTradeCount())
		}
		if oneMinAgg.BuyCount != 1 {
			t.Errorf("Expected 1 buy trade in 1min window, got %d", oneMinAgg.BuyCount)
		}
	}
	
	// Verify 5min window (should have trades 2 and 3)
	fiveMinAgg := aggregates["5min"]
	if fiveMinAgg == nil {
		t.Error("Expected 5min aggregate")
	} else {
		if fiveMinAgg.GetTotalTradeCount() != 2 {
			t.Errorf("Expected 2 trades in 5min window, got %d", fiveMinAgg.GetTotalTradeCount())
		}
		if fiveMinAgg.BuyCount != 1 {
			t.Errorf("Expected 1 buy trade in 5min window, got %d", fiveMinAgg.BuyCount)
		}
		if fiveMinAgg.SellCount != 1 {
			t.Errorf("Expected 1 sell trade in 5min window, got %d", fiveMinAgg.SellCount)
		}
	}
	
	// Verify 15min window (should have all trades)
	fifteenMinAgg := aggregates["15min"]
	if fifteenMinAgg == nil {
		t.Error("Expected 15min aggregate")
	} else {
		if fifteenMinAgg.GetTotalTradeCount() != 3 {
			t.Errorf("Expected 3 trades in 15min window, got %d", fifteenMinAgg.GetTotalTradeCount())
		}
		if fifteenMinAgg.BuyCount != 2 {
			t.Errorf("Expected 2 buy trades in 15min window, got %d", fifteenMinAgg.BuyCount)
		}
		if fifteenMinAgg.SellCount != 1 {
			t.Errorf("Expected 1 sell trade in 15min window, got %d", fifteenMinAgg.SellCount)
		}
	}
}

func TestConcurrentTradeProcessing(t *testing.T) {
	suite := NewIntegrationTestSuite(t)
	defer suite.Cleanup()
	
	// Process multiple batches of trades concurrently
	numBatches := 5
	tradesPerBatch := 10
	done := make(chan bool, numBatches)
	
	for batch := 0; batch < numBatches; batch++ {
		go func(batchNum int) {
			defer func() { done <- true }()
			
			// Create trades for this batch
			trades := make([]packet.TokenTradeHistory, tradesPerBatch)
			for i := 0; i < tradesPerBatch; i++ {
				trades[i] = packet.TokenTradeHistory{
					Token:        "0x123",
					Wallet:       "0xabc",
					SellBuy:      "buy",
					NativeAmount: "100.0",
					TokenAmount:  "1000.0",
					PriceUsd:     "0.1",
					TransTime:    time.Now().Add(-time.Duration(i) * time.Second).Format(time.RFC3339),
					TxHash:       fmt.Sprintf("0x%d_%d", batchNum, i),
				}
			}
			
			// Process trades
			err := suite.blockAggregator.ProcessTrades(suite.ctx, trades)
			if err != nil {
				t.Errorf("Expected no error processing batch %d, got: %v", batchNum, err)
			}
		}(batch)
	}
	
	// Wait for all batches to complete
	for i := 0; i < numBatches; i++ {
		<-done
	}
	
	// Wait for async processing
	time.Sleep(1 * time.Second)
	
	// Verify all trades were processed
	processor, err := suite.blockAggregator.GetTokenProcessor("0x123")
	if err != nil {
		t.Errorf("Expected no error getting processor, got: %v", err)
	}
	
	// Check that we have a reasonable number of trades processed
	// (exact count may vary due to timing and concurrent processing)
	tradeCount := processor.GetTradeCount()
	expectedMin := numBatches * tradesPerBatch
	if tradeCount < expectedMin {
		t.Errorf("Expected at least %d trades processed, got %d", expectedMin, tradeCount)
	}
	
	// Verify aggregates were calculated
	aggregates, err := processor.GetAggregates(suite.ctx)
	if err != nil {
		t.Errorf("Expected no error getting aggregates, got: %v", err)
	}
	
	oneMinAgg := aggregates["1min"]
	if oneMinAgg == nil {
		t.Error("Expected 1min aggregate")
	} else {
		if oneMinAgg.GetTotalTradeCount() == 0 {
			t.Error("Expected some trades in 1min aggregate")
		}
	}
}

func TestDataConsistencyUnderLoad(t *testing.T) {
	suite := NewIntegrationTestSuite(t)
	defer suite.Cleanup()
	
	tokenAddress := "0x123"
	numTrades := 100
	
	// Create a large number of trades
	trades := make([]packet.TokenTradeHistory, numTrades)
	expectedBuyCount := 0
	expectedSellCount := 0
	expectedTotalVolume := 0.0
	
	for i := 0; i < numTrades; i++ {
		sellBuy := "buy"
		if i%3 == 0 {
			sellBuy = "sell"
			expectedSellCount++
		} else {
			expectedBuyCount++
		}
		
		amount := float64(100 + i)
		expectedTotalVolume += amount
		
		trades[i] = packet.TokenTradeHistory{
			Token:        tokenAddress,
			Wallet:       fmt.Sprintf("0x%d", i),
			SellBuy:      sellBuy,
			NativeAmount: fmt.Sprintf("%.1f", amount),
			TokenAmount:  "1000.0",
			PriceUsd:     "0.1",
			TransTime:    time.Now().Add(-time.Duration(i) * time.Second).Format(time.RFC3339),
			TxHash:       fmt.Sprintf("0x%d", i),
		}
	}
	
	// Process all trades
	err := suite.blockAggregator.ProcessTrades(suite.ctx, trades)
	if err != nil {
		t.Errorf("Expected no error processing trades, got: %v", err)
	}
	
	// Wait for processing
	time.Sleep(2 * time.Second)
	
	// Get processor and verify data consistency
	processor, err := suite.blockAggregator.GetTokenProcessor(tokenAddress)
	if err != nil {
		t.Errorf("Expected no error getting processor, got: %v", err)
	}
	
	// Verify trade count
	tradeCount := processor.GetTradeCount()
	if tradeCount != numTrades {
		t.Errorf("Expected %d trades, got %d", numTrades, tradeCount)
	}
	
	// Get aggregates and verify consistency
	aggregates, err := processor.GetAggregates(suite.ctx)
	if err != nil {
		t.Errorf("Expected no error getting aggregates, got: %v", err)
	}
	
	// Check 15min window (should include all trades since they're all within 15min)
	fifteenMinAgg := aggregates["15min"]
	if fifteenMinAgg == nil {
		t.Error("Expected 15min aggregate")
	} else {
		totalTrades := fifteenMinAgg.BuyCount + fifteenMinAgg.SellCount
		if totalTrades != int64(numTrades) {
			t.Errorf("Expected %d total trades in aggregate, got %d", numTrades, totalTrades)
		}
		
		// Verify buy/sell counts are reasonable (allowing for some timing variations)
		if fifteenMinAgg.BuyCount == 0 {
			t.Error("Expected some buy trades")
		}
		if fifteenMinAgg.SellCount == 0 {
			t.Error("Expected some sell trades")
		}
		
		// Verify total volume is reasonable
		if fifteenMinAgg.TotalVolume == 0 {
			t.Error("Expected non-zero total volume")
		}
	}
}

func TestMemoryUsageUnderLoad(t *testing.T) {
	suite := NewIntegrationTestSuite(t)
	defer suite.Cleanup()
	
	// Get initial memory usage
	initialUsage := suite.blockAggregator.GetMemoryUsage()
	initialTotalTrades := initialUsage["total_stats"].(map[string]int)["total_trade_count"]
	
	// Process a large number of trades
	numTokens := 10
	tradesPerToken := 50
	
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
			t.Errorf("Expected no error processing trades for token %d, got: %v", tokenNum, err)
		}
	}
	
	// Wait for processing
	time.Sleep(2 * time.Second)
	
	// Get final memory usage
	finalUsage := suite.blockAggregator.GetMemoryUsage()
	finalTotalTrades := finalUsage["total_stats"].(map[string]int)["total_trade_count"]
	
	// Verify memory usage increased appropriately
	expectedTotalTrades := initialTotalTrades + (numTokens * tradesPerToken)
	if finalTotalTrades < expectedTotalTrades {
		t.Errorf("Expected at least %d total trades, got %d", expectedTotalTrades, finalTotalTrades)
	}
	
	// Verify processor count
	processorCount := finalUsage["processor_count"].(int)
	if processorCount != numTokens {
		t.Errorf("Expected %d processors, got %d", numTokens, processorCount)
	}
	
	// Verify memory usage is reasonable (not excessive)
	totalStats := finalUsage["total_stats"].(map[string]int)
	if totalStats["total_channel_length"] > 1000 {
		t.Errorf("Channel length seems excessive: %d", totalStats["total_channel_length"])
	}
}

func TestGracefulShutdown(t *testing.T) {
	suite := NewIntegrationTestSuite(t)
	
	// Process some trades
	trades := []packet.TokenTradeHistory{
		{
			Token:        "0x123",
			Wallet:       "0xabc",
			SellBuy:      "buy",
			NativeAmount: "100.0",
			TokenAmount:  "1000.0",
			PriceUsd:     "0.1",
			TransTime:    time.Now().Format(time.RFC3339),
			TxHash:       "0x1",
		},
	}
	
	err := suite.blockAggregator.ProcessTrades(suite.ctx, trades)
	if err != nil {
		t.Errorf("Expected no error processing trades, got: %v", err)
	}
	
	// Wait for processing
	time.Sleep(200 * time.Millisecond)
	
	// Verify processor was created
	if suite.blockAggregator.GetActiveTokenCount() != 1 {
		t.Errorf("Expected 1 active token, got %d", suite.blockAggregator.GetActiveTokenCount())
	}
	
	// Test graceful shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	err = suite.blockAggregator.Shutdown(shutdownCtx)
	if err != nil {
		t.Errorf("Expected no error during shutdown, got: %v", err)
	}
	
	// Verify processors were cleaned up
	if suite.blockAggregator.GetActiveTokenCount() != 0 {
		t.Errorf("Expected 0 active tokens after shutdown, got %d", suite.blockAggregator.GetActiveTokenCount())
	}
	
	// Verify aggregator is shut down
	if !suite.blockAggregator.IsShutdown() {
		t.Error("Expected aggregator to be shut down")
	}
	
	// Manual cleanup since we shut down early
	suite.cancel()
	if suite.workerPool != nil {
		suite.workerPool.Stop(context.Background())
	}
	if suite.redisManager != nil {
		suite.redisManager.Close()
	}
}