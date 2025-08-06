package aggregation

import (
	"context"
	"testing"
	"time"

	"aggregatorService/config"
	"aggregatorService/models"
)

func TestSlidingWindowCalculator_Initialize(t *testing.T) {
	calculator := NewCalculator()
	
	// Test successful initialization
	timeWindows := []config.TimeWindow{
		{Duration: 1 * time.Minute, Name: "1min"},
		{Duration: 5 * time.Minute, Name: "5min"},
		{Duration: 15 * time.Minute, Name: "15min"},
	}
	
	err := calculator.Initialize(timeWindows)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	
	// Test empty time windows
	err = calculator.Initialize([]config.TimeWindow{})
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
}

func TestSlidingWindowCalculator_UpdateWindowsBasic(t *testing.T) {
	calculator := NewCalculator()
	
	timeWindows := []config.TimeWindow{
		{Duration: 5 * time.Minute, Name: "5min"},
	}
	
	err := calculator.Initialize(timeWindows)
	if err != nil {
		t.Fatalf("Failed to initialize calculator: %v", err)
	}
	
	currentTime := time.Now()
	
	// Initial empty state
	trades := []models.TradeData{}
	indices := map[string]int64{"5min": 0}
	aggregates := map[string]*models.AggregateData{
		"5min": models.NewAggregateData(),
	}
	
	// New trade to add
	newTrade := models.TradeData{
		Token:        "0x123",
		Wallet:       "0xabc",
		SellBuy:      "buy",
		NativeAmount: 100.0,
		TokenAmount:  1000.0,
		PriceUsd:     0.1,
		TransTime:    currentTime.Add(-1 * time.Minute), // Within 5min window
		TxHash:       "0x1",
	}
	
	// Update windows
	ctx := context.Background()
	newIndices, newAggregates, err := calculator.UpdateWindows(
		ctx,
		trades,
		indices,
		aggregates,
		newTrade,
	)
	
	if err != nil {
		t.Fatalf("UpdateWindows failed: %v", err)
	}
	
	// Verify results
	if newIndices["5min"] != 0 {
		t.Errorf("Expected index 0, got %d", newIndices["5min"])
	}
	
	fiveMinAgg := newAggregates["5min"]
	if fiveMinAgg.BuyCount != 1 {
		t.Errorf("Expected buy count 1, got %d", fiveMinAgg.BuyCount)
	}
	if fiveMinAgg.BuyVolume != 100.0 {
		t.Errorf("Expected buy volume 100.0, got %f", fiveMinAgg.BuyVolume)
	}
	if fiveMinAgg.TotalVolume != 100.0 {
		t.Errorf("Expected total volume 100.0, got %f", fiveMinAgg.TotalVolume)
	}
}

func TestSlidingWindowCalculator_UpdateWindowsWithExpiredData(t *testing.T) {
	calculator := NewCalculator()
	
	timeWindows := []config.TimeWindow{
		{Duration: 5 * time.Minute, Name: "5min"},
	}
	
	err := calculator.Initialize(timeWindows)
	if err != nil {
		t.Fatalf("Failed to initialize calculator: %v", err)
	}
	
	currentTime := time.Now()
	
	// Existing trades with one expired
	trades := []models.TradeData{
		{
			Token:        "0x123",
			Wallet:       "0xabc",
			SellBuy:      "buy",
			NativeAmount: 50.0,
			TokenAmount:  500.0,
			PriceUsd:     0.1,
			TransTime:    currentTime.Add(-7 * time.Minute), // Expired (outside 5min)
			TxHash:       "0x1",
		},
		{
			Token:        "0x123",
			Wallet:       "0xdef",
			SellBuy:      "sell",
			NativeAmount: 75.0,
			TokenAmount:  750.0,
			PriceUsd:     0.1,
			TransTime:    currentTime.Add(-2 * time.Minute), // Valid (within 5min)
			TxHash:       "0x2",
		},
	}
	
	// Initial state includes the expired trade
	indices := map[string]int64{"5min": 0}
	aggregates := map[string]*models.AggregateData{
		"5min": {
			SellCount:   1,
			BuyCount:    1,
			SellVolume:  75.0,
			BuyVolume:   50.0,
			TotalVolume: 125.0,
			LastUpdate:  currentTime.Add(-1 * time.Minute),
		},
	}
	
	// New trade to add
	newTrade := models.TradeData{
		Token:        "0x123",
		Wallet:       "0xghi",
		SellBuy:      "buy",
		NativeAmount: 100.0,
		TokenAmount:  1000.0,
		PriceUsd:     0.1,
		TransTime:    currentTime.Add(-30 * time.Second), // Valid (within 5min)
		TxHash:       "0x3",
	}
	
	// Update windows
	ctx := context.Background()
	newIndices, newAggregates, err := calculator.UpdateWindows(
		ctx,
		trades,
		indices,
		aggregates,
		newTrade,
	)
	
	if err != nil {
		t.Fatalf("UpdateWindows failed: %v", err)
	}
	
	// Verify expired data was removed and new data added
	fiveMinAgg := newAggregates["5min"]
	
	// Should have: original sell trade (75) + new buy trade (100) = 175
	// Should NOT have: expired buy trade (50)
	expectedBuyCount := int64(1)  // Only new buy trade
	expectedSellCount := int64(1) // Original sell trade
	expectedTotalVolume := 175.0  // 75 (sell) + 100 (new buy)
	
	if fiveMinAgg.BuyCount != expectedBuyCount {
		t.Errorf("Expected buy count %d, got %d", expectedBuyCount, fiveMinAgg.BuyCount)
	}
	if fiveMinAgg.SellCount != expectedSellCount {
		t.Errorf("Expected sell count %d, got %d", expectedSellCount, fiveMinAgg.SellCount)
	}
	if fiveMinAgg.TotalVolume != expectedTotalVolume {
		t.Errorf("Expected total volume %.2f, got %.2f", expectedTotalVolume, fiveMinAgg.TotalVolume)
	}
	
	// Index should point to the first valid trade (index 1)
	if newIndices["5min"] != 1 {
		t.Errorf("Expected index 1, got %d", newIndices["5min"])
	}
}

func TestSlidingWindowCalculator_UpdateWindowsMultipleTimeframes(t *testing.T) {
	calculator := NewCalculator()
	
	timeWindows := []config.TimeWindow{
		{Duration: 1 * time.Minute, Name: "1min"},
		{Duration: 5 * time.Minute, Name: "5min"},
		{Duration: 15 * time.Minute, Name: "15min"},
	}
	
	err := calculator.Initialize(timeWindows)
	if err != nil {
		t.Fatalf("Failed to initialize calculator: %v", err)
	}
	
	currentTime := time.Now()
	
	// Existing trades at different ages
	trades := []models.TradeData{
		{
			Token:        "0x123",
			Wallet:       "0xabc",
			SellBuy:      "buy",
			NativeAmount: 50.0,
			TransTime:    currentTime.Add(-10 * time.Minute), // Valid for 15min only
			TxHash:       "0x1",
		},
		{
			Token:        "0x123",
			Wallet:       "0xdef",
			SellBuy:      "sell",
			NativeAmount: 75.0,
			TransTime:    currentTime.Add(-3 * time.Minute), // Valid for 5min and 15min
			TxHash:       "0x2",
		},
		{
			Token:        "0x123",
			Wallet:       "0xghi",
			SellBuy:      "buy",
			NativeAmount: 25.0,
			TransTime:    currentTime.Add(-30 * time.Second), // Valid for all windows
			TxHash:       "0x3",
		},
	}
	
	// Initial state
	indices := map[string]int64{
		"1min":  0,
		"5min":  0,
		"15min": 0,
	}
	aggregates := map[string]*models.AggregateData{
		"1min":  models.NewAggregateData(),
		"5min":  models.NewAggregateData(),
		"15min": models.NewAggregateData(),
	}
	
	// New trade
	newTrade := models.TradeData{
		Token:        "0x123",
		Wallet:       "0xjkl",
		SellBuy:      "sell",
		NativeAmount: 100.0,
		TransTime:    currentTime.Add(-10 * time.Second), // Valid for all windows
		TxHash:       "0x4",
	}
	
	// Update windows
	ctx := context.Background()
	_, newAggregates, err := calculator.UpdateWindows(
		ctx,
		trades,
		indices,
		aggregates,
		newTrade,
	)
	
	if err != nil {
		t.Fatalf("UpdateWindows failed: %v", err)
	}
	
	// Verify 1min window (should have trades 2, 3, and new trade)
	oneMinAgg := newAggregates["1min"]
	if oneMinAgg.BuyCount != 1 { // Trade 3
		t.Errorf("1min: Expected buy count 1, got %d", oneMinAgg.BuyCount)
	}
	if oneMinAgg.SellCount != 1 { // New trade
		t.Errorf("1min: Expected sell count 1, got %d", oneMinAgg.SellCount)
	}
	expectedOneMinVolume := 125.0 // 25 (trade 3) + 100 (new trade)
	if oneMinAgg.TotalVolume != expectedOneMinVolume {
		t.Errorf("1min: Expected total volume %.2f, got %.2f", expectedOneMinVolume, oneMinAgg.TotalVolume)
	}
	
	// Verify 5min window (should have trades 2, 3, and new trade)
	fiveMinAgg := newAggregates["5min"]
	if fiveMinAgg.BuyCount != 1 { // Trade 3
		t.Errorf("5min: Expected buy count 1, got %d", fiveMinAgg.BuyCount)
	}
	if fiveMinAgg.SellCount != 2 { // Trade 2 + new trade
		t.Errorf("5min: Expected sell count 2, got %d", fiveMinAgg.SellCount)
	}
	expectedFiveMinVolume := 200.0 // 75 (trade 2) + 25 (trade 3) + 100 (new trade)
	if fiveMinAgg.TotalVolume != expectedFiveMinVolume {
		t.Errorf("5min: Expected total volume %.2f, got %.2f", expectedFiveMinVolume, fiveMinAgg.TotalVolume)
	}
	
	// Verify 15min window (should have all trades)
	fifteenMinAgg := newAggregates["15min"]
	if fifteenMinAgg.BuyCount != 2 { // Trade 1 + trade 3
		t.Errorf("15min: Expected buy count 2, got %d", fifteenMinAgg.BuyCount)
	}
	if fifteenMinAgg.SellCount != 2 { // Trade 2 + new trade
		t.Errorf("15min: Expected sell count 2, got %d", fifteenMinAgg.SellCount)
	}
	expectedFifteenMinVolume := 250.0 // 50 + 75 + 25 + 100
	if fifteenMinAgg.TotalVolume != expectedFifteenMinVolume {
		t.Errorf("15min: Expected total volume %.2f, got %.2f", expectedFifteenMinVolume, fifteenMinAgg.TotalVolume)
	}
}

func TestSlidingWindowCalculator_CalculateWindow(t *testing.T) {
	calculator := NewCalculator()
	
	timeWindows := []config.TimeWindow{
		{Duration: 5 * time.Minute, Name: "5min"},
	}
	
	err := calculator.Initialize(timeWindows)
	if err != nil {
		t.Fatalf("Failed to initialize calculator: %v", err)
	}
	
	currentTime := time.Now()
	
	// Create test trades
	trades := []models.TradeData{
		{
			Token:        "0x123",
			Wallet:       "0xabc",
			SellBuy:      "buy",
			NativeAmount: 100.0,
			TokenAmount:  1000.0,
			PriceUsd:     0.1,
			TransTime:    currentTime.Add(-7 * time.Minute), // Outside window
			TxHash:       "0x1",
		},
		{
			Token:        "0x123",
			Wallet:       "0xdef",
			SellBuy:      "sell",
			NativeAmount: 50.0,
			TokenAmount:  500.0,
			PriceUsd:     0.1,
			TransTime:    currentTime.Add(-3 * time.Minute), // Within window
			TxHash:       "0x2",
		},
		{
			Token:        "0x123",
			Wallet:       "0xghi",
			SellBuy:      "buy",
			NativeAmount: 75.0,
			TokenAmount:  750.0,
			PriceUsd:     0.1,
			TransTime:    currentTime.Add(-1 * time.Minute), // Within window
			TxHash:       "0x3",
		},
	}
	
	// Calculate window
	ctx := context.Background()
	aggregate, index, err := calculator.CalculateWindow(
		ctx,
		trades,
		timeWindows[0],
		currentTime,
	)
	
	if err != nil {
		t.Fatalf("CalculateWindow failed: %v", err)
	}
	
	// Should include trades 1 and 2 (indices 1 and 2)
	if aggregate.BuyCount != 1 {
		t.Errorf("Expected buy count 1, got %d", aggregate.BuyCount)
	}
	if aggregate.SellCount != 1 {
		t.Errorf("Expected sell count 1, got %d", aggregate.SellCount)
	}
	if aggregate.TotalVolume != 125.0 {
		t.Errorf("Expected total volume 125.0, got %f", aggregate.TotalVolume)
	}
	
	// Index should point to the first valid trade (index 1)
	if index != 1 {
		t.Errorf("Expected index 1, got %d", index)
	}
}

func TestSlidingWindowCalculator_CalculateWindowEmpty(t *testing.T) {
	calculator := NewCalculator()
	
	timeWindows := []config.TimeWindow{
		{Duration: 5 * time.Minute, Name: "5min"},
	}
	
	err := calculator.Initialize(timeWindows)
	if err != nil {
		t.Fatalf("Failed to initialize calculator: %v", err)
	}
	
	currentTime := time.Now()
	
	// Test with empty trades
	ctx := context.Background()
	aggregate, index, err := calculator.CalculateWindow(
		ctx,
		[]models.TradeData{},
		timeWindows[0],
		currentTime,
	)
	
	if err != nil {
		t.Fatalf("CalculateWindow failed: %v", err)
	}
	
	// Should return empty aggregate
	if aggregate.GetTotalTradeCount() != 0 {
		t.Errorf("Expected empty aggregate, got trade count %d", aggregate.GetTotalTradeCount())
	}
	if index != 0 {
		t.Errorf("Expected index 0, got %d", index)
	}
}

func TestSlidingWindowCalculator_CalculateWindowAllExpired(t *testing.T) {
	calculator := NewCalculator()
	
	timeWindows := []config.TimeWindow{
		{Duration: 5 * time.Minute, Name: "5min"},
	}
	
	err := calculator.Initialize(timeWindows)
	if err != nil {
		t.Fatalf("Failed to initialize calculator: %v", err)
	}
	
	currentTime := time.Now()
	
	// Create trades that are all expired
	trades := []models.TradeData{
		{
			Token:        "0x123",
			Wallet:       "0xabc",
			SellBuy:      "buy",
			NativeAmount: 100.0,
			TransTime:    currentTime.Add(-10 * time.Minute), // Expired
			TxHash:       "0x1",
		},
		{
			Token:        "0x123",
			Wallet:       "0xdef",
			SellBuy:      "sell",
			NativeAmount: 50.0,
			TransTime:    currentTime.Add(-7 * time.Minute), // Expired
			TxHash:       "0x2",
		},
	}
	
	// Calculate window
	ctx := context.Background()
	aggregate, index, err := calculator.CalculateWindow(
		ctx,
		trades,
		timeWindows[0],
		currentTime,
	)
	
	if err != nil {
		t.Fatalf("CalculateWindow failed: %v", err)
	}
	
	// Should return empty aggregate
	if aggregate.GetTotalTradeCount() != 0 {
		t.Errorf("Expected empty aggregate, got trade count %d", aggregate.GetTotalTradeCount())
	}
	
	// Index should point beyond all trades
	if index != int64(len(trades)) {
		t.Errorf("Expected index %d, got %d", len(trades), index)
	}
}

func TestSlidingWindowCalculator_GetTimeWindowByName(t *testing.T) {
	calculator := NewCalculator()
	
	timeWindows := []config.TimeWindow{
		{Duration: 1 * time.Minute, Name: "1min"},
		{Duration: 5 * time.Minute, Name: "5min"},
		{Duration: 15 * time.Minute, Name: "15min"},
	}
	
	err := calculator.Initialize(timeWindows)
	if err != nil {
		t.Fatalf("Failed to initialize calculator: %v", err)
	}
	
	// Test existing window
	window, found := calculator.GetTimeWindowByName("5min")
	if !found {
		t.Error("Expected to find 5min window")
	}
	if window.Duration != 5*time.Minute {
		t.Errorf("Expected 5min duration, got %v", window.Duration)
	}
	if window.Name != "5min" {
		t.Errorf("Expected name '5min', got '%s'", window.Name)
	}
	
	// Test non-existent window
	_, found = calculator.GetTimeWindowByName("nonexistent")
	if found {
		t.Error("Expected to not find nonexistent window")
	}
}

func TestSlidingWindowCalculator_GetAllTimeWindowNames(t *testing.T) {
	calculator := NewCalculator()
	
	timeWindows := []config.TimeWindow{
		{Duration: 1 * time.Minute, Name: "1min"},
		{Duration: 5 * time.Minute, Name: "5min"},
		{Duration: 15 * time.Minute, Name: "15min"},
	}
	
	err := calculator.Initialize(timeWindows)
	if err != nil {
		t.Fatalf("Failed to initialize calculator: %v", err)
	}
	
	names := calculator.GetAllTimeWindowNames()
	
	if len(names) != 3 {
		t.Errorf("Expected 3 names, got %d", len(names))
	}
	
	expectedNames := map[string]bool{
		"1min":  true,
		"5min":  true,
		"15min": true,
	}
	
	for _, name := range names {
		if !expectedNames[name] {
			t.Errorf("Unexpected window name: %s", name)
		}
	}
}

func TestSlidingWindowCalculator_PriceCalculations(t *testing.T) {
	calculator := NewCalculator()
	
	timeWindows := []config.TimeWindow{
		{Duration: 5 * time.Minute, Name: "5min"},
	}
	
	err := calculator.Initialize(timeWindows)
	if err != nil {
		t.Fatalf("Failed to initialize calculator: %v", err)
	}
	
	currentTime := time.Now()
	
	// Create trades with different prices
	trades := []models.TradeData{
		{
			Token:        "0x123",
			SellBuy:      "buy",
			NativeAmount: 100.0,
			PriceUsd:     0.08, // Low price
			TransTime:    currentTime.Add(-4 * time.Minute),
			TxHash:       "0x1",
		},
		{
			Token:        "0x123",
			SellBuy:      "sell",
			NativeAmount: 50.0,
			PriceUsd:     0.12, // High price
			TransTime:    currentTime.Add(-2 * time.Minute),
			TxHash:       "0x2",
		},
		{
			Token:        "0x123",
			SellBuy:      "buy",
			NativeAmount: 75.0,
			PriceUsd:     0.10, // End price
			TransTime:    currentTime.Add(-30 * time.Second),
			TxHash:       "0x3",
		},
	}
	
	// Calculate window
	ctx := context.Background()
	aggregate, _, err := calculator.CalculateWindow(
		ctx,
		trades,
		timeWindows[0],
		currentTime,
	)
	
	if err != nil {
		t.Fatalf("CalculateWindow failed: %v", err)
	}
	
	// Verify price calculations
	if aggregate.StartPrice != 0.08 {
		t.Errorf("Expected start price 0.08, got %f", aggregate.StartPrice)
	}
	if aggregate.EndPrice != 0.10 {
		t.Errorf("Expected end price 0.10, got %f", aggregate.EndPrice)
	}
	
	// Price change should be (0.10 - 0.08) / 0.08 * 100 = 25%
	expectedPriceChange := 25.0
	if aggregate.PriceChange != expectedPriceChange {
		t.Errorf("Expected price change %.2f%%, got %.2f%%", expectedPriceChange, aggregate.PriceChange)
	}
}

func TestSlidingWindowCalculator_VolumeCalculations(t *testing.T) {
	calculator := NewCalculator()
	
	timeWindows := []config.TimeWindow{
		{Duration: 5 * time.Minute, Name: "5min"},
	}
	
	err := calculator.Initialize(timeWindows)
	if err != nil {
		t.Fatalf("Failed to initialize calculator: %v", err)
	}
	
	currentTime := time.Now()
	
	// Create trades with different volumes
	trades := []models.TradeData{
		{
			Token:        "0x123",
			SellBuy:      "buy",
			NativeAmount: 100.0,
			TransTime:    currentTime.Add(-3 * time.Minute),
			TxHash:       "0x1",
		},
		{
			Token:        "0x123",
			SellBuy:      "buy",
			NativeAmount: 150.0,
			TransTime:    currentTime.Add(-2 * time.Minute),
			TxHash:       "0x2",
		},
		{
			Token:        "0x123",
			SellBuy:      "sell",
			NativeAmount: 75.0,
			TransTime:    currentTime.Add(-1 * time.Minute),
			TxHash:       "0x3",
		},
		{
			Token:        "0x123",
			SellBuy:      "sell",
			NativeAmount: 25.0,
			TransTime:    currentTime.Add(-30 * time.Second),
			TxHash:       "0x4",
		},
	}
	
	// Calculate window
	ctx := context.Background()
	aggregate, _, err := calculator.CalculateWindow(
		ctx,
		trades,
		timeWindows[0],
		currentTime,
	)
	
	if err != nil {
		t.Fatalf("CalculateWindow failed: %v", err)
	}
	
	// Verify volume calculations
	expectedBuyVolume := 250.0  // 100 + 150
	expectedSellVolume := 100.0 // 75 + 25
	expectedTotalVolume := 350.0 // 250 + 100
	
	if aggregate.BuyVolume != expectedBuyVolume {
		t.Errorf("Expected buy volume %.2f, got %.2f", expectedBuyVolume, aggregate.BuyVolume)
	}
	if aggregate.SellVolume != expectedSellVolume {
		t.Errorf("Expected sell volume %.2f, got %.2f", expectedSellVolume, aggregate.SellVolume)
	}
	if aggregate.TotalVolume != expectedTotalVolume {
		t.Errorf("Expected total volume %.2f, got %.2f", expectedTotalVolume, aggregate.TotalVolume)
	}
	
	// Verify trade counts
	if aggregate.BuyCount != 2 {
		t.Errorf("Expected buy count 2, got %d", aggregate.BuyCount)
	}
	if aggregate.SellCount != 2 {
		t.Errorf("Expected sell count 2, got %d", aggregate.SellCount)
	}
}

func TestSlidingWindowCalculator_EdgeCases(t *testing.T) {
	calculator := NewCalculator()
	
	timeWindows := []config.TimeWindow{
		{Duration: 5 * time.Minute, Name: "5min"},
	}
	
	err := calculator.Initialize(timeWindows)
	if err != nil {
		t.Fatalf("Failed to initialize calculator: %v", err)
	}
	
	currentTime := time.Now()
	
	// Test with trade exactly at window boundary
	trades := []models.TradeData{
		{
			Token:        "0x123",
			SellBuy:      "buy",
			NativeAmount: 100.0,
			TransTime:    currentTime.Add(-5 * time.Minute), // Exactly at boundary
			TxHash:       "0x1",
		},
		{
			Token:        "0x123",
			SellBuy:      "sell",
			NativeAmount: 50.0,
			TransTime:    currentTime.Add(-5*time.Minute + 1*time.Second), // Just inside boundary
			TxHash:       "0x2",
		},
	}
	
	// Calculate window
	ctx := context.Background()
	aggregate, index, err := calculator.CalculateWindow(
		ctx,
		trades,
		timeWindows[0],
		currentTime,
	)
	
	if err != nil {
		t.Fatalf("CalculateWindow failed: %v", err)
	}
	
	// Trade exactly at boundary should be excluded, trade just inside should be included
	if aggregate.GetTotalTradeCount() != 1 {
		t.Errorf("Expected 1 trade in window, got %d", aggregate.GetTotalTradeCount())
	}
	if aggregate.SellCount != 1 {
		t.Errorf("Expected 1 sell trade, got %d", aggregate.SellCount)
	}
	if index != 1 {
		t.Errorf("Expected index 1, got %d", index)
	}
}

func TestSlidingWindowCalculator_ConcurrentAccess(t *testing.T) {
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
			Token:        "0x123",
			SellBuy:      "buy",
			NativeAmount: 100.0,
			TransTime:    currentTime.Add(-1 * time.Minute),
			TxHash:       "0x1",
		},
	}
	
	// Test concurrent access to calculator methods
	ctx := context.Background()
	done := make(chan bool, 10)
	
	for i := 0; i < 10; i++ {
		go func() {
			defer func() { done <- true }()
			
			// Test CalculateWindow
			_, _, err := calculator.CalculateWindow(ctx, trades, timeWindows[0], currentTime)
			if err != nil {
				t.Errorf("Concurrent CalculateWindow failed: %v", err)
			}
			
			// Test GetTimeWindowByName
			_, found := calculator.GetTimeWindowByName("5min")
			if !found {
				t.Error("Concurrent GetTimeWindowByName failed")
			}
			
			// Test GetAllTimeWindowNames
			names := calculator.GetAllTimeWindowNames()
			if len(names) != 1 {
				t.Errorf("Concurrent GetAllTimeWindowNames failed: expected 1, got %d", len(names))
			}
		}()
	}
	
	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}
}