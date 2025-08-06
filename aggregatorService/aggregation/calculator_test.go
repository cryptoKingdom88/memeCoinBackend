package aggregation

import (
	"context"
	"testing"
	"time"

	"aggregatorService/config"
	"aggregatorService/models"
)

func TestTimeWindowUtilities(t *testing.T) {
	calculator := NewCalculator()
	
	// Test time windows configuration
	timeWindows := []config.TimeWindow{
		{Duration: 1 * time.Minute, Name: "1min"},
		{Duration: 5 * time.Minute, Name: "5min"},
		{Duration: 15 * time.Minute, Name: "15min"},
		{Duration: 30 * time.Minute, Name: "30min"},
		{Duration: 1 * time.Hour, Name: "1hour"},
	}
	
	err := calculator.Initialize(timeWindows)
	if err != nil {
		t.Fatalf("Failed to initialize calculator: %v", err)
	}
	
	// Test GetTimeWindowByName
	window, found := calculator.(*Calculator).GetTimeWindowByName("5min")
	if !found {
		t.Error("Expected to find 5min window")
	}
	if window.Duration != 5*time.Minute {
		t.Errorf("Expected 5min duration, got %v", window.Duration)
	}
	
	// Test GetAllTimeWindowNames
	names := calculator.(*Calculator).GetAllTimeWindowNames()
	expectedNames := []string{"1min", "5min", "15min", "30min", "1hour"}
	if len(names) != len(expectedNames) {
		t.Errorf("Expected %d names, got %d", len(expectedNames), len(names))
	}
	
	// Test GetTimeWindowDurations
	durations := calculator.(*Calculator).GetTimeWindowDurations()
	if len(durations) != 5 {
		t.Errorf("Expected 5 durations, got %d", len(durations))
	}
}

func TestCalculateWindow(t *testing.T) {
	calculator := NewCalculator()
	
	timeWindows := []config.TimeWindow{
		{Duration: 5 * time.Minute, Name: "5min"},
	}
	
	err := calculator.Initialize(timeWindows)
	if err != nil {
		t.Fatalf("Failed to initialize calculator: %v", err)
	}
	
	// Create test trades
	currentTime := time.Now()
	trades := []models.TradeData{
		{
			Token:        "0x123",
			Wallet:       "0xabc",
			SellBuy:      "buy",
			NativeAmount: 100.0,
			TokenAmount:  1000.0,
			PriceUsd:     0.1,
			TransTime:    currentTime.Add(-2 * time.Minute), // Within 5min window
			TxHash:       "0x1",
		},
		{
			Token:        "0x123",
			Wallet:       "0xdef",
			SellBuy:      "sell",
			NativeAmount: 50.0,
			TokenAmount:  500.0,
			PriceUsd:     0.1,
			TransTime:    currentTime.Add(-7 * time.Minute), // Outside 5min window
			TxHash:       "0x2",
		},
		{
			Token:        "0x123",
			Wallet:       "0xghi",
			SellBuy:      "buy",
			NativeAmount: 200.0,
			TokenAmount:  2000.0,
			PriceUsd:     0.1,
			TransTime:    currentTime.Add(-1 * time.Minute), // Within 5min window
			TxHash:       "0x3",
		},
	}
	
	// Test CalculateWindow
	aggregate, index, err := calculator.CalculateWindow(
		context.Background(),
		trades,
		timeWindows[0],
		currentTime,
	)
	
	if err != nil {
		t.Fatalf("CalculateWindow failed: %v", err)
	}
	
	// Should have 2 buy trades (trades 0 and 2), 0 sell trades
	if aggregate.BuyCount != 2 {
		t.Errorf("Expected 2 buy trades, got %d", aggregate.BuyCount)
	}
	if aggregate.SellCount != 0 {
		t.Errorf("Expected 0 sell trades, got %d", aggregate.SellCount)
	}
	if aggregate.BuyVolume != 300.0 {
		t.Errorf("Expected buy volume 300.0, got %f", aggregate.BuyVolume)
	}
	if aggregate.TotalVolume != 300.0 {
		t.Errorf("Expected total volume 300.0, got %f", aggregate.TotalVolume)
	}
	
	// Index should point to after the expired trade
	if index != 1 {
		t.Errorf("Expected index 1, got %d", index)
	}
}

func TestTimeWindowUtils(t *testing.T) {
	utils := models.NewTimeWindowUtils()
	currentTime := time.Now()
	windowDuration := 5 * time.Minute
	
	// Test GetWindowBoundary
	boundary := utils.GetWindowBoundary(currentTime, windowDuration)
	expectedBoundary := currentTime.Add(-windowDuration)
	if !boundary.Equal(expectedBoundary) {
		t.Errorf("Expected boundary %v, got %v", expectedBoundary, boundary)
	}
	
	// Test IsWithinWindow
	withinTime := currentTime.Add(-2 * time.Minute)
	if !utils.IsWithinWindow(withinTime, currentTime, windowDuration) {
		t.Error("Expected time to be within window")
	}
	
	outsideTime := currentTime.Add(-7 * time.Minute)
	if utils.IsWithinWindow(outsideTime, currentTime, windowDuration) {
		t.Error("Expected time to be outside window")
	}
	
	// Test IsExpired
	if !utils.IsExpired(outsideTime, currentTime, windowDuration) {
		t.Error("Expected time to be expired")
	}
	
	if utils.IsExpired(withinTime, currentTime, windowDuration) {
		t.Error("Expected time to not be expired")
	}
	
	// Test CompareTimestamps
	t1 := currentTime.Add(-1 * time.Minute)
	t2 := currentTime
	
	if utils.CompareTimestamps(t1, t2) != -1 {
		t.Error("Expected t1 to be before t2")
	}
	if utils.CompareTimestamps(t2, t1) != 1 {
		t.Error("Expected t2 to be after t1")
	}
	if utils.CompareTimestamps(t1, t1) != 0 {
		t.Error("Expected timestamps to be equal")
	}
}

func TestValidateTimeWindows(t *testing.T) {
	calculator := NewCalculator()
	
	// Test empty time windows
	err := calculator.Initialize([]config.TimeWindow{})
	if err == nil {
		t.Error("Expected error for empty time windows")
	}
	
	// Test invalid duration
	invalidWindows := []config.TimeWindow{
		{Duration: 0, Name: "invalid"},
	}
	err = calculator.Initialize(invalidWindows)
	if err == nil {
		t.Error("Expected error for invalid duration")
	}
	
	// Test empty name
	invalidWindows = []config.TimeWindow{
		{Duration: 1 * time.Minute, Name: ""},
	}
	err = calculator.Initialize(invalidWindows)
	if err == nil {
		t.Error("Expected error for empty name")
	}
	
	// Test valid windows
	validWindows := []config.TimeWindow{
		{Duration: 1 * time.Minute, Name: "1min"},
		{Duration: 5 * time.Minute, Name: "5min"},
	}
	err = calculator.Initialize(validWindows)
	if err != nil {
		t.Errorf("Expected no error for valid windows, got: %v", err)
	}
}

func TestFindExpiredTrades(t *testing.T) {
	calculator := NewCalculator()
	
	timeWindows := []config.TimeWindow{
		{Duration: 5 * time.Minute, Name: "5min"},
	}
	
	err := calculator.Initialize(timeWindows)
	if err != nil {
		t.Fatalf("Failed to initialize calculator: %v", err)
	}
	
	currentTime := time.Now()
	trades := []models.TradeData{
		{
			Token:     "0x123",
			TransTime: currentTime.Add(-10 * time.Minute), // Expired
			SellBuy:   "buy",
			NativeAmount: 100.0,
		},
		{
			Token:     "0x123",
			TransTime: currentTime.Add(-8 * time.Minute), // Expired
			SellBuy:   "sell",
			NativeAmount: 50.0,
		},
		{
			Token:     "0x123",
			TransTime: currentTime.Add(-2 * time.Minute), // Valid
			SellBuy:   "buy",
			NativeAmount: 200.0,
		},
	}
	
	// Test finding expired trades starting from index 0
	expiredTrades, newIndex, err := calculator.(*Calculator).FindExpiredTrades(
		trades,
		0,
		timeWindows[0],
		currentTime,
	)
	
	if err != nil {
		t.Fatalf("FindExpiredTrades failed: %v", err)
	}
	
	// Should find 2 expired trades
	if len(expiredTrades) != 2 {
		t.Errorf("Expected 2 expired trades, got %d", len(expiredTrades))
	}
	
	// New index should be 2 (pointing to the first valid trade)
	if newIndex != 2 {
		t.Errorf("Expected new index 2, got %d", newIndex)
	}
	
	// Test with no expired trades
	validTrades := []models.TradeData{
		{
			Token:     "0x123",
			TransTime: currentTime.Add(-2 * time.Minute), // Valid
			SellBuy:   "buy",
			NativeAmount: 100.0,
		},
	}
	
	expiredTrades, newIndex, err = calculator.(*Calculator).FindExpiredTrades(
		validTrades,
		0,
		timeWindows[0],
		currentTime,
	)
	
	if err != nil {
		t.Fatalf("FindExpiredTrades failed: %v", err)
	}
	
	if len(expiredTrades) != 0 {
		t.Errorf("Expected 0 expired trades, got %d", len(expiredTrades))
	}
	
	if newIndex != 0 {
		t.Errorf("Expected new index 0, got %d", newIndex)
	}
}

func TestUpdateWindows(t *testing.T) {
	calculator := NewCalculator()
	
	timeWindows := []config.TimeWindow{
		{Duration: 5 * time.Minute, Name: "5min"},
		{Duration: 10 * time.Minute, Name: "10min"},
	}
	
	err := calculator.Initialize(timeWindows)
	if err != nil {
		t.Fatalf("Failed to initialize calculator: %v", err)
	}
	
	currentTime := time.Now()
	
	// Initial trades
	trades := []models.TradeData{
		{
			Token:        "0x123",
			TransTime:    currentTime.Add(-8 * time.Minute), // Valid for 10min, expired for 5min
			SellBuy:      "buy",
			NativeAmount: 100.0,
		},
		{
			Token:        "0x123",
			TransTime:    currentTime.Add(-3 * time.Minute), // Valid for both windows
			SellBuy:      "sell",
			NativeAmount: 50.0,
		},
	}
	
	// Initial indices and aggregates
	indices := map[string]int64{
		"5min":  0,
		"10min": 0,
	}
	
	aggregates := map[string]*models.AggregateData{
		"5min":  models.NewAggregateData(),
		"10min": models.NewAggregateData(),
	}
	
	// Don't pre-populate aggregates - let UpdateWindows handle it
	// The UpdateWindows method will calculate from scratch based on current state
	
	// New trade to add
	newTrade := models.TradeData{
		Token:        "0x123",
		TransTime:    currentTime.Add(-1 * time.Minute), // Valid for both windows
		SellBuy:      "buy",
		NativeAmount: 200.0,
	}
	
	// Update windows
	newIndices, newAggregates, err := calculator.UpdateWindows(
		context.Background(),
		trades,
		indices,
		aggregates,
		newTrade,
	)
	
	if err != nil {
		t.Fatalf("UpdateWindows failed: %v", err)
	}
	
	// Check that we got updated maps
	if newIndices == nil {
		t.Error("Expected non-nil indices")
	}
	if newAggregates == nil {
		t.Error("Expected non-nil aggregates")
	}
	
	// Check 5min window - should have the sell trade from original trades + new buy trade
	fiveMinAgg := newAggregates["5min"]
	if fiveMinAgg == nil {
		t.Error("Expected 5min aggregate to exist")
	} else {
		// Should have 1 buy trade (new trade) and 1 sell trade (from original trades[1])
		if fiveMinAgg.BuyCount != 1 {
			t.Errorf("Expected 1 buy trade in 5min window, got %d", fiveMinAgg.BuyCount)
		}
		if fiveMinAgg.SellCount != 1 {
			t.Errorf("Expected 1 sell trade in 5min window, got %d", fiveMinAgg.SellCount)
		}
	}
	
	// Check 10min window - should have all trades (original buy + original sell + new buy)
	tenMinAgg := newAggregates["10min"]
	if tenMinAgg == nil {
		t.Error("Expected 10min aggregate to exist")
	} else {
		// Should have 2 buy trades (original + new) and 1 sell trade (original)
		if tenMinAgg.BuyCount != 2 {
			t.Errorf("Expected 2 buy trades in 10min window, got %d", tenMinAgg.BuyCount)
		}
		if tenMinAgg.SellCount != 1 {
			t.Errorf("Expected 1 sell trade in 10min window, got %d", tenMinAgg.SellCount)
		}
	}
}

func TestCalculateIncrementalUpdate(t *testing.T) {
	calculator := NewCalculator()
	
	// Create initial aggregate
	initialAggregate := models.NewAggregateData()
	initialAggregate.BuyCount = 2
	initialAggregate.SellCount = 1
	initialAggregate.BuyVolume = 300.0
	initialAggregate.SellVolume = 100.0
	initialAggregate.TotalVolume = 400.0
	
	// Expired trades to subtract
	expiredTrades := []models.TradeData{
		{
			SellBuy:      "buy",
			NativeAmount: 100.0,
		},
	}
	
	// New trades to add
	newTrades := []models.TradeData{
		{
			SellBuy:      "sell",
			NativeAmount: 150.0,
		},
	}
	
	// Calculate incremental update
	updatedAggregate := calculator.(*Calculator).CalculateIncrementalUpdate(
		initialAggregate,
		expiredTrades,
		newTrades,
	)
	
	// Check results
	if updatedAggregate.BuyCount != 1 { // 2 - 1 = 1
		t.Errorf("Expected buy count 1, got %d", updatedAggregate.BuyCount)
	}
	if updatedAggregate.SellCount != 2 { // 1 + 1 = 2
		t.Errorf("Expected sell count 2, got %d", updatedAggregate.SellCount)
	}
	if updatedAggregate.BuyVolume != 200.0 { // 300 - 100 = 200
		t.Errorf("Expected buy volume 200.0, got %f", updatedAggregate.BuyVolume)
	}
	if updatedAggregate.SellVolume != 250.0 { // 100 + 150 = 250
		t.Errorf("Expected sell volume 250.0, got %f", updatedAggregate.SellVolume)
	}
	if updatedAggregate.TotalVolume != 450.0 { // 400 - 100 + 150 = 450
		t.Errorf("Expected total volume 450.0, got %f", updatedAggregate.TotalVolume)
	}
}

func TestIndexManager(t *testing.T) {
	indexManager := NewIndexManager()
	
	// Test initial state
	_, exists := indexManager.GetPreviousIndex("5min")
	if exists {
		t.Error("Expected no previous index initially")
	}
	
	// Test tracking index
	indexManager.TrackIndex("5min", 10)
	
	previousIndex, exists := indexManager.GetPreviousIndex("5min")
	if !exists {
		t.Error("Expected previous index to exist after tracking")
	}
	if previousIndex != 10 {
		t.Errorf("Expected previous index 10, got %d", previousIndex)
	}
	
	// Test index change detection
	if !indexManager.HasIndexChanged("5min", 15) {
		t.Error("Expected index to be detected as changed")
	}
	
	if indexManager.HasIndexChanged("5min", 10) {
		t.Error("Expected index to be detected as unchanged")
	}
	
	// Test index history
	indexManager.TrackIndex("5min", 15)
	indexManager.TrackIndex("5min", 20)
	
	history := indexManager.GetIndexHistory("5min")
	if len(history) != 2 {
		t.Errorf("Expected history length 2, got %d", len(history))
	}
	if history[0] != 10 || history[1] != 15 {
		t.Errorf("Expected history [10, 15], got %v", history)
	}
}

func TestOptimizedCalculator(t *testing.T) {
	calculator := NewOptimizedCalculator()
	
	timeWindows := []config.TimeWindow{
		{Duration: 5 * time.Minute, Name: "5min"},
	}
	
	err := calculator.Initialize(timeWindows)
	if err != nil {
		t.Fatalf("Failed to initialize calculator: %v", err)
	}
	
	currentTime := time.Now()
	
	// Initial trades
	trades := []models.TradeData{
		{
			Token:        "0x123",
			TransTime:    currentTime.Add(-3 * time.Minute),
			SellBuy:      "buy",
			NativeAmount: 100.0,
		},
	}
	
	// Initial state
	indices := map[string]int64{"5min": 0}
	aggregates := map[string]*models.AggregateData{
		"5min": models.NewAggregateData(),
	}
	aggregates["5min"].AddTrade(trades[0])
	
	// First update - should trigger full calculation
	newTrade1 := models.TradeData{
		Token:        "0x123",
		TransTime:    currentTime.Add(-1 * time.Minute),
		SellBuy:      "sell",
		NativeAmount: 50.0,
	}
	
	newIndices, newAggregates, err := calculator.(*OptimizedCalculator).UpdateWindowsOptimized(
		context.Background(),
		trades,
		indices,
		aggregates,
		newTrade1,
	)
	
	if err != nil {
		t.Fatalf("UpdateWindowsOptimized failed: %v", err)
	}
	
	// Check results
	if newAggregates["5min"].BuyCount != 1 {
		t.Errorf("Expected 1 buy trade, got %d", newAggregates["5min"].BuyCount)
	}
	if newAggregates["5min"].SellCount != 1 {
		t.Errorf("Expected 1 sell trade, got %d", newAggregates["5min"].SellCount)
	}
	
	// Second update with same index - should use optimization
	newTrade2 := models.TradeData{
		Token:        "0x123",
		TransTime:    currentTime.Add(-30 * time.Second),
		SellBuy:      "buy",
		NativeAmount: 75.0,
	}
	
	updatedTrades := append(trades, newTrade1)
	finalIndices, finalAggregates, err := calculator.(*OptimizedCalculator).UpdateWindowsOptimized(
		context.Background(),
		updatedTrades,
		newIndices,
		newAggregates,
		newTrade2,
	)
	
	if err != nil {
		t.Fatalf("Second UpdateWindowsOptimized failed: %v", err)
	}
	
	// Check final results
	if finalAggregates["5min"].BuyCount != 2 {
		t.Errorf("Expected 2 buy trades, got %d", finalAggregates["5min"].BuyCount)
	}
	if finalAggregates["5min"].SellCount != 1 {
		t.Errorf("Expected 1 sell trade, got %d", finalAggregates["5min"].SellCount)
	}
	
	// Verify indices were updated
	if finalIndices["5min"] < 0 {
		t.Errorf("Expected valid index, got %d", finalIndices["5min"])
	}
}

func TestAtomicUpdateBatch(t *testing.T) {
	batch := NewAtomicUpdateBatch()
	
	// Create test aggregates
	agg1 := models.NewAggregateData()
	agg1.BuyCount = 1
	agg1.BuyVolume = 100.0
	
	agg2 := models.NewAggregateData()
	agg2.SellCount = 1
	agg2.SellVolume = 50.0
	
	// Add updates to batch
	batch.AddUpdate("5min", 10, agg1)
	batch.AddUpdate("10min", 5, agg2)
	
	// Test batch application
	indices := make(map[string]int64)
	aggregates := make(map[string]*models.AggregateData)
	
	err := batch.ApplyBatch(indices, aggregates)
	if err != nil {
		t.Fatalf("ApplyBatch failed: %v", err)
	}
	
	// Verify updates were applied
	if indices["5min"] != 10 {
		t.Errorf("Expected index 10 for 5min, got %d", indices["5min"])
	}
	if indices["10min"] != 5 {
		t.Errorf("Expected index 5 for 10min, got %d", indices["10min"])
	}
	
	if aggregates["5min"].BuyCount != 1 {
		t.Errorf("Expected buy count 1 for 5min, got %d", aggregates["5min"].BuyCount)
	}
	if aggregates["10min"].SellCount != 1 {
		t.Errorf("Expected sell count 1 for 10min, got %d", aggregates["10min"].SellCount)
	}
}

func TestUpdateWindowsAtomic(t *testing.T) {
	calculator := NewOptimizedCalculator()
	
	timeWindows := []config.TimeWindow{
		{Duration: 5 * time.Minute, Name: "5min"},
		{Duration: 10 * time.Minute, Name: "10min"},
	}
	
	err := calculator.Initialize(timeWindows)
	if err != nil {
		t.Fatalf("Failed to initialize calculator: %v", err)
	}
	
	currentTime := time.Now()
	
	// Initial trades
	trades := []models.TradeData{
		{
			Token:        "0x123",
			TransTime:    currentTime.Add(-3 * time.Minute),
			SellBuy:      "buy",
			NativeAmount: 100.0,
		},
	}
	
	// Initial state - start with empty aggregates (will trigger recalculation from scratch)
	indices := make(map[string]int64)
	aggregates := make(map[string]*models.AggregateData)
	
	// New trade to add
	newTrade := models.TradeData{
		Token:        "0x123",
		TransTime:    currentTime.Add(-1 * time.Minute),
		SellBuy:      "sell",
		NativeAmount: 50.0,
	}
	
	// Atomic update
	newIndices, newAggregates, err := calculator.(*OptimizedCalculator).UpdateWindowsAtomic(
		context.Background(),
		trades,
		indices,
		aggregates,
		newTrade,
	)
	
	if err != nil {
		t.Fatalf("UpdateWindowsAtomic failed: %v", err)
	}
	
	// Verify both windows were updated atomically
	if newAggregates["5min"] == nil {
		t.Error("Expected 5min aggregate to exist")
	}
	if newAggregates["10min"] == nil {
		t.Error("Expected 10min aggregate to exist")
	}
	
	// Both windows should have the same trades (both within 10min, both within 5min)
	// Original trade: buy at -3min, New trade: sell at -1min
	if newAggregates["5min"].BuyCount != 1 {
		t.Errorf("Expected 1 buy trade in 5min, got %d", newAggregates["5min"].BuyCount)
	}
	if newAggregates["5min"].SellCount != 1 {
		t.Errorf("Expected 1 sell trade in 5min, got %d", newAggregates["5min"].SellCount)
	}
	
	if newAggregates["10min"].BuyCount != 1 {
		t.Errorf("Expected 1 buy trade in 10min, got %d", newAggregates["10min"].BuyCount)
	}
	if newAggregates["10min"].SellCount != 1 {
		t.Errorf("Expected 1 sell trade in 10min, got %d", newAggregates["10min"].SellCount)
	}
	
	// Verify indices were set
	if _, exists := newIndices["5min"]; !exists {
		t.Error("Expected 5min index to be set")
	}
	if _, exists := newIndices["10min"]; !exists {
		t.Error("Expected 10min index to be set")
	}
}