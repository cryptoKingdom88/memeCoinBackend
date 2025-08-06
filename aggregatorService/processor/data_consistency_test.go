package processor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"aggregatorService/models"
)

func TestDataConsistencyValidator_ValidateTokenData(t *testing.T) {
	redisManager := NewMockRedisManager()
	calculator := NewMockSlidingWindowCalculator()
	validator := NewDataConsistencyValidator(redisManager, calculator)
	
	ctx := context.Background()
	tokenAddress := "test-token"
	
	// Create test token data with some issues
	tokenData := &models.TokenData{
		TokenAddress: tokenAddress,
		Trades: []models.TradeData{
			{
				Token:        tokenAddress,
				Wallet:       "wallet1",
				SellBuy:      "buy",
				NativeAmount: 100.0,
				TokenAmount:  1000.0,
				PriceUsd:     0.1,
				TransTime:    time.Now().Add(-10 * time.Minute),
				TxHash:       "hash1",
			},
			{
				Token:        tokenAddress,
				Wallet:       "wallet2",
				SellBuy:      "sell",
				NativeAmount: -50.0, // Negative amount (issue)
				TokenAmount:  500.0,
				PriceUsd:     0.1,
				TransTime:    time.Now().Add(-5 * time.Minute),
				TxHash:       "hash2",
			},
			{
				Token:        tokenAddress,
				Wallet:       "wallet3",
				SellBuy:      "buy",
				NativeAmount: 75.0,
				TokenAmount:  750.0,
				PriceUsd:     0.1,
				TransTime:    time.Now().Add(-3 * time.Minute),
				TxHash:       "hash1", // Duplicate hash (issue)
			},
		},
		Indices: map[string]int64{
			"1min":  0,
			"5min":  1,
			"15min": -1, // Negative index (issue)
		},
		Aggregates: map[string]*models.AggregateData{
			"1min": {
				SellCount:   1,
				BuyCount:    2,
				SellVolume:  50.0,
				BuyVolume:   175.0,
				TotalVolume: 300.0, // Inconsistent total (issue)
				LastUpdate:  time.Now(),
			},
		},
		LastUpdate: time.Now(),
	}
	
	// Set up mock to return this data
	redisManager.data[tokenAddress] = tokenData
	
	// Validate the data
	result, err := validator.ValidateTokenData(ctx, tokenAddress)
	if err != nil {
		t.Fatalf("Validation failed: %v", err)
	}
	
	// Check that issues were found
	if result.IsConsistent {
		t.Error("Expected data to be inconsistent")
	}
	
	if len(result.Issues) == 0 {
		t.Error("Expected validation issues to be found")
	}
	
	// Check for specific issue types
	issueTypes := make(map[string]bool)
	for _, issue := range result.Issues {
		issueTypes[issue.Type] = true
	}
	
	expectedIssues := []string{"negative_amount", "duplicate_trade", "negative_index", "inconsistent_total_volume"}
	for _, expectedType := range expectedIssues {
		if !issueTypes[expectedType] {
			t.Errorf("Expected issue type %s not found", expectedType)
		}
	}
	
	t.Logf("Found %d validation issues", len(result.Issues))
	for _, issue := range result.Issues {
		t.Logf("Issue: %s - %s (severity: %s)", issue.Type, issue.Description, issue.Severity)
	}
}

func TestDataConsistencyValidator_ValidateTradeData(t *testing.T) {
	validator := NewDataConsistencyValidator(nil, nil)
	
	// Test with valid data
	validTokenData := &models.TokenData{
		Trades: []models.TradeData{
			{
				Token:        "test-token",
				Wallet:       "wallet1",
				SellBuy:      "buy",
				NativeAmount: 100.0,
				TokenAmount:  1000.0,
				PriceUsd:     0.1,
				TransTime:    time.Now().Add(-10 * time.Minute),
				TxHash:       "hash1",
			},
			{
				Token:        "test-token",
				Wallet:       "wallet2",
				SellBuy:      "sell",
				NativeAmount: 50.0,
				TokenAmount:  500.0,
				PriceUsd:     0.1,
				TransTime:    time.Now().Add(-5 * time.Minute),
				TxHash:       "hash2",
			},
		},
	}
	
	result := &ValidationResult{Issues: []ValidationIssue{}}
	err := validator.validateTradeData(validTokenData, result)
	if err != nil {
		t.Fatalf("Validation failed: %v", err)
	}
	
	if len(result.Issues) > 0 {
		t.Errorf("Expected no issues for valid data, got %d", len(result.Issues))
	}
	
	// Test with invalid data
	invalidTokenData := &models.TokenData{
		Trades: []models.TradeData{
			{
				Token:        "test-token",
				Wallet:       "wallet1",
				SellBuy:      "buy",
				NativeAmount: -100.0, // Negative amount
				TokenAmount:  1000.0,
				PriceUsd:     0.1,
				TransTime:    time.Now().Add(-10 * time.Minute),
				TxHash:       "hash1",
			},
			{
				Token:        "test-token",
				Wallet:       "wallet2",
				SellBuy:      "sell",
				NativeAmount: 50.0,
				TokenAmount:  -500.0, // Negative amount
				PriceUsd:     -0.1,   // Negative price
				TransTime:    time.Now().Add(10 * time.Minute), // Future timestamp
				TxHash:       "hash1", // Duplicate hash
			},
		},
	}
	
	result = &ValidationResult{Issues: []ValidationIssue{}}
	err = validator.validateTradeData(invalidTokenData, result)
	if err != nil {
		t.Fatalf("Validation failed: %v", err)
	}
	
	if len(result.Issues) == 0 {
		t.Error("Expected issues for invalid data")
	}
	
	// Check for specific issue types
	issueTypes := make(map[string]int)
	for _, issue := range result.Issues {
		issueTypes[issue.Type]++
	}
	
	if issueTypes["negative_amount"] < 2 {
		t.Error("Expected at least 2 negative amount issues")
	}
	
	if issueTypes["negative_price"] == 0 {
		t.Error("Expected negative price issue")
	}
	
	if issueTypes["future_timestamp"] == 0 {
		t.Error("Expected future timestamp issue")
	}
	
	if issueTypes["duplicate_trade"] == 0 {
		t.Error("Expected duplicate trade issue")
	}
}

func TestDataConsistencyValidator_ValidateIndices(t *testing.T) {
	calculator := NewMockSlidingWindowCalculator()
	validator := NewDataConsistencyValidator(nil, calculator)
	
	// Test with valid indices
	validTokenData := &models.TokenData{
		Trades: make([]models.TradeData, 10), // 10 trades
		Indices: map[string]int64{
			"1min":  2,
			"5min":  5,
			"15min": 8,
			"30min": 9,
			"1hour": 10,
		},
	}
	
	result := &ValidationResult{Issues: []ValidationIssue{}}
	err := validator.validateIndices(validTokenData, result)
	if err != nil {
		t.Fatalf("Validation failed: %v", err)
	}
	
	if len(result.Issues) > 0 {
		t.Errorf("Expected no issues for valid indices, got %d", len(result.Issues))
		for _, issue := range result.Issues {
			t.Logf("Issue: %s - %s", issue.Type, issue.Description)
		}
	}
	
	// Test with invalid indices
	invalidTokenData := &models.TokenData{
		Trades: make([]models.TradeData, 5), // 5 trades
		Indices: map[string]int64{
			"1min":  -1,  // Negative index
			"5min":  10,  // Out of bounds
			"15min": 3,   // Valid
			// Missing 30min and 1hour
			"invalid": 2, // Unexpected index
		},
	}
	
	result = &ValidationResult{Issues: []ValidationIssue{}}
	err = validator.validateIndices(invalidTokenData, result)
	if err != nil {
		t.Fatalf("Validation failed: %v", err)
	}
	
	if len(result.Issues) == 0 {
		t.Error("Expected issues for invalid indices")
	}
	
	// Check for specific issue types
	issueTypes := make(map[string]int)
	for _, issue := range result.Issues {
		issueTypes[issue.Type]++
	}
	
	if issueTypes["negative_index"] == 0 {
		t.Error("Expected negative index issue")
	}
	
	if issueTypes["index_out_of_bounds"] == 0 {
		t.Error("Expected index out of bounds issue")
	}
	
	if issueTypes["missing_index"] < 2 {
		t.Error("Expected at least 2 missing index issues")
	}
	
	if issueTypes["unexpected_index"] == 0 {
		t.Error("Expected unexpected index issue")
	}
}

func TestDataConsistencyValidator_ValidateAggregates(t *testing.T) {
	calculator := NewMockSlidingWindowCalculator()
	validator := NewDataConsistencyValidator(nil, calculator)
	ctx := context.Background()
	
	// Test with valid aggregates
	validTokenData := &models.TokenData{
		Aggregates: map[string]*models.AggregateData{
			"1min": {
				SellCount:   1,
				BuyCount:    2,
				SellVolume:  50.0,
				BuyVolume:   100.0,
				TotalVolume: 150.0,
				StartPrice:  0.1,
				EndPrice:    0.12,
				HighPrice:   0.15,
				LowPrice:    0.09,
				LastUpdate:  time.Now(),
			},
			"5min": {
				SellCount:   2,
				BuyCount:    3,
				SellVolume:  75.0,
				BuyVolume:   125.0,
				TotalVolume: 200.0,
				StartPrice:  0.1,
				EndPrice:    0.11,
				HighPrice:   0.13,
				LowPrice:    0.08,
				LastUpdate:  time.Now(),
			},
			"15min": {
				SellCount:   3,
				BuyCount:    4,
				SellVolume:  100.0,
				BuyVolume:   150.0,
				TotalVolume: 250.0,
				StartPrice:  0.1,
				EndPrice:    0.10,
				HighPrice:   0.12,
				LowPrice:    0.07,
				LastUpdate:  time.Now(),
			},
			"30min": {
				SellCount:   4,
				BuyCount:    5,
				SellVolume:  125.0,
				BuyVolume:   175.0,
				TotalVolume: 300.0,
				StartPrice:  0.1,
				EndPrice:    0.09,
				HighPrice:   0.11,
				LowPrice:    0.06,
				LastUpdate:  time.Now(),
			},
			"1hour": {
				SellCount:   5,
				BuyCount:    6,
				SellVolume:  150.0,
				BuyVolume:   200.0,
				TotalVolume: 350.0,
				StartPrice:  0.1,
				EndPrice:    0.08,
				HighPrice:   0.10,
				LowPrice:    0.05,
				LastUpdate:  time.Now(),
			},
		},
	}
	
	result := &ValidationResult{Issues: []ValidationIssue{}}
	err := validator.validateAggregates(ctx, validTokenData, result)
	if err != nil {
		t.Fatalf("Validation failed: %v", err)
	}
	
	if len(result.Issues) > 0 {
		t.Errorf("Expected no issues for valid aggregates, got %d", len(result.Issues))
		for _, issue := range result.Issues {
			t.Logf("Issue: %s - %s", issue.Type, issue.Description)
		}
	}
	
	// Test with invalid aggregates
	invalidTokenData := &models.TokenData{
		Aggregates: map[string]*models.AggregateData{
			"1min": {
				SellCount:   -1,   // Negative count
				BuyCount:    2,
				SellVolume:  -50.0, // Negative volume
				BuyVolume:   100.0,
				TotalVolume: 200.0, // Inconsistent total (should be 50.0)
				StartPrice:  0.1,
				EndPrice:    0.12,
				HighPrice:   0.08, // High < Low (invalid)
				LowPrice:    0.15,
				LastUpdate:  time.Now(),
			},
			// Missing other required aggregates
		},
	}
	
	result = &ValidationResult{Issues: []ValidationIssue{}}
	err = validator.validateAggregates(ctx, invalidTokenData, result)
	if err != nil {
		t.Fatalf("Validation failed: %v", err)
	}
	
	if len(result.Issues) == 0 {
		t.Error("Expected issues for invalid aggregates")
	}
	
	// Check for specific issue types
	issueTypes := make(map[string]int)
	for _, issue := range result.Issues {
		issueTypes[issue.Type]++
	}
	
	if issueTypes["negative_count"] == 0 {
		t.Error("Expected negative count issue")
	}
	
	if issueTypes["negative_volume"] == 0 {
		t.Error("Expected negative volume issue")
	}
	
	if issueTypes["inconsistent_total_volume"] == 0 {
		t.Error("Expected inconsistent total volume issue")
	}
	
	if issueTypes["invalid_price_range"] == 0 {
		t.Error("Expected invalid price range issue")
	}
	
	if issueTypes["missing_aggregate"] < 4 {
		t.Error("Expected at least 4 missing aggregate issues")
	}
}

func TestDataConsistencyValidator_ApplyRecovery(t *testing.T) {
	redisManager := NewMockRedisManager()
	calculator := NewMockSlidingWindowCalculator()
	validator := NewDataConsistencyValidator(redisManager, calculator)
	
	ctx := context.Background()
	tokenAddress := "test-token"
	
	// Create token data with issues that can be recovered
	tokenData := &models.TokenData{
		TokenAddress: tokenAddress,
		Trades: []models.TradeData{
			{
				Token:        tokenAddress,
				Wallet:       "wallet1",
				SellBuy:      "buy",
				NativeAmount: 100.0,
				TokenAmount:  1000.0,
				PriceUsd:     0.1,
				TransTime:    time.Now().Add(-10 * time.Minute),
				TxHash:       "hash1",
			},
			{
				Token:        tokenAddress,
				Wallet:       "wallet2",
				SellBuy:      "sell",
				NativeAmount: 50.0,
				TokenAmount:  500.0,
				PriceUsd:     0.1,
				TransTime:    time.Now().Add(-15 * time.Minute), // Out of order
				TxHash:       "hash2",
			},
			{
				Token:        tokenAddress,
				Wallet:       "wallet3",
				SellBuy:      "buy",
				NativeAmount: 75.0,
				TokenAmount:  750.0,
				PriceUsd:     0.1,
				TransTime:    time.Now().Add(-5 * time.Minute),
				TxHash:       "hash1", // Duplicate
			},
		},
		Indices: map[string]int64{
			"1min": -1, // Invalid index
		},
		Aggregates: map[string]*models.AggregateData{
			// Missing aggregates
		},
		LastUpdate: time.Time{}, // Zero timestamp
	}
	
	// Create validation result with recoverable issues
	result := &ValidationResult{
		TokenAddress: tokenAddress,
		Issues: []ValidationIssue{
			{Type: "duplicate_trade", Severity: "high"},
			{Type: "unsorted_trades", Severity: "medium"},
			{Type: "negative_index", Severity: "high"},
			{Type: "missing_aggregate", Severity: "medium"},
			{Type: "zero_timestamp", Severity: "medium"},
		},
	}
	
	// Apply recovery
	err := validator.applyRecovery(ctx, tokenAddress, tokenData, result)
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}
	
	// Check that trades are now sorted
	for i := 1; i < len(tokenData.Trades); i++ {
		if tokenData.Trades[i].TransTime.Before(tokenData.Trades[i-1].TransTime) {
			t.Error("Trades are still not sorted after recovery")
		}
	}
	
	// Check that duplicates are removed
	seenHashes := make(map[string]bool)
	for _, trade := range tokenData.Trades {
		if trade.TxHash != "" {
			if seenHashes[trade.TxHash] {
				t.Error("Duplicate trades still exist after recovery")
			}
			seenHashes[trade.TxHash] = true
		}
	}
	
	// Check that indices are recalculated
	for windowName, index := range tokenData.Indices {
		if index < 0 {
			t.Errorf("Index for %s is still negative after recovery: %d", windowName, index)
		}
	}
	
	// Check that aggregates are recalculated
	expectedWindows := calculator.GetAllTimeWindowNames()
	for _, windowName := range expectedWindows {
		if _, exists := tokenData.Aggregates[windowName]; !exists {
			t.Errorf("Aggregate for %s is still missing after recovery", windowName)
		}
	}
	
	// Check that timestamps are fixed
	if tokenData.LastUpdate.IsZero() {
		t.Error("Last update timestamp is still zero after recovery")
	}
}

func TestDataConsistencyValidator_RecoverFromRedisOnStartup(t *testing.T) {
	redisManager := NewMockRedisManager()
	calculator := NewMockSlidingWindowCalculator()
	validator := NewDataConsistencyValidator(redisManager, calculator)
	
	ctx := context.Background()
	
	// Set up mock data with some tokens having issues
	token1 := "token1"
	token2 := "token2"
	
	// Token 1 has issues
	redisManager.data[token1] = &models.TokenData{
		TokenAddress: token1,
		Trades: []models.TradeData{
			{
				Token:        token1,
				Wallet:       "wallet1",
				SellBuy:      "buy",
				NativeAmount: -100.0, // Negative amount (critical issue)
				TokenAmount:  1000.0,
				PriceUsd:     0.1,
				TransTime:    time.Now().Add(-10 * time.Minute),
				TxHash:       "hash1",
			},
		},
		Indices:    make(map[string]int64),
		Aggregates: make(map[string]*models.AggregateData),
		LastUpdate: time.Time{}, // Zero timestamp
	}
	
	// Token 2 is valid
	redisManager.data[token2] = &models.TokenData{
		TokenAddress: token2,
		Trades: []models.TradeData{
			{
				Token:        token2,
				Wallet:       "wallet1",
				SellBuy:      "buy",
				NativeAmount: 100.0,
				TokenAmount:  1000.0,
				PriceUsd:     0.1,
				TransTime:    time.Now().Add(-10 * time.Minute),
				TxHash:       "hash2",
			},
		},
		Indices: map[string]int64{
			"1min":  0,
			"5min":  0,
			"15min": 0,
			"30min": 0,
			"1hour": 0,
		},
		Aggregates: map[string]*models.AggregateData{
			"1min":  {SellCount: 0, BuyCount: 1, LastUpdate: time.Now()},
			"5min":  {SellCount: 0, BuyCount: 1, LastUpdate: time.Now()},
			"15min": {SellCount: 0, BuyCount: 1, LastUpdate: time.Now()},
			"30min": {SellCount: 0, BuyCount: 1, LastUpdate: time.Now()},
			"1hour": {SellCount: 0, BuyCount: 1, LastUpdate: time.Now()},
		},
		LastUpdate: time.Now(),
	}
	
	// Mock GetActiveTokens to return our test tokens
	// This would need to be implemented in the mock
	
	// For this test, we'll directly validate the tokens
	result1, err := validator.ValidateTokenData(ctx, token1)
	if err != nil {
		t.Fatalf("Validation failed for token1: %v", err)
	}
	
	result2, err := validator.ValidateTokenData(ctx, token2)
	if err != nil {
		t.Fatalf("Validation failed for token2: %v", err)
	}
	
	// Check results
	if result1.IsConsistent {
		t.Error("Expected token1 to be inconsistent")
	}
	
	if !result2.IsConsistent {
		t.Error("Expected token2 to be consistent")
		for _, issue := range result2.Issues {
			t.Logf("Token2 issue: %s - %s", issue.Type, issue.Description)
		}
	}
	
	// Check that recovery was applied for token1
	if !result1.RecoveryApplied {
		t.Error("Expected recovery to be applied for token1")
	}
}

func TestDataConsistencyValidator_GetValidationSummary(t *testing.T) {
	validator := NewDataConsistencyValidator(nil, nil)
	
	// Create test validation results
	results := []*ValidationResult{
		{
			TokenAddress: "token1",
			IsConsistent: true,
			Issues:       []ValidationIssue{},
		},
		{
			TokenAddress:    "token2",
			IsConsistent:    false,
			RecoveryApplied: true,
			Issues: []ValidationIssue{
				{Type: "negative_amount", Severity: "high"},
				{Type: "duplicate_trade", Severity: "high"},
			},
		},
		{
			TokenAddress:    "token3",
			IsConsistent:    false,
			RecoveryApplied: false,
			Issues: []ValidationIssue{
				{Type: "missing_index", Severity: "medium"},
				{Type: "zero_timestamp", Severity: "low"},
			},
		},
	}
	
	summary := validator.GetValidationSummary(results)
	
	// Check summary values
	if summary["total_tokens"] != 3 {
		t.Errorf("Expected total_tokens 3, got %v", summary["total_tokens"])
	}
	
	if summary["consistent_tokens"] != 1 {
		t.Errorf("Expected consistent_tokens 1, got %v", summary["consistent_tokens"])
	}
	
	if summary["tokens_with_issues"] != 2 {
		t.Errorf("Expected tokens_with_issues 2, got %v", summary["tokens_with_issues"])
	}
	
	if summary["recoveries_applied"] != 1 {
		t.Errorf("Expected recoveries_applied 1, got %v", summary["recoveries_applied"])
	}
	
	// Check issue type counts
	issueTypes := summary["issue_types"].(map[string]int)
	if issueTypes["negative_amount"] != 1 {
		t.Errorf("Expected negative_amount count 1, got %d", issueTypes["negative_amount"])
	}
	
	if issueTypes["duplicate_trade"] != 1 {
		t.Errorf("Expected duplicate_trade count 1, got %d", issueTypes["duplicate_trade"])
	}
	
	// Check severity counts
	severityCounts := summary["severity_counts"].(map[string]int)
	if severityCounts["high"] != 2 {
		t.Errorf("Expected high severity count 2, got %d", severityCounts["high"])
	}
	
	if severityCounts["medium"] != 1 {
		t.Errorf("Expected medium severity count 1, got %d", severityCounts["medium"])
	}
	
	if severityCounts["low"] != 1 {
		t.Errorf("Expected low severity count 1, got %d", severityCounts["low"])
	}
}

func TestDataConsistencyValidator_ValidationInterval(t *testing.T) {
	validator := NewDataConsistencyValidator(nil, nil)
	
	// Test default interval
	if validator.validationInterval != 5*time.Minute {
		t.Errorf("Expected default validation interval 5m, got %v", validator.validationInterval)
	}
	
	// Test setting custom interval
	customInterval := 10 * time.Minute
	validator.SetValidationInterval(customInterval)
	
	if validator.validationInterval != customInterval {
		t.Errorf("Expected validation interval %v, got %v", customInterval, validator.validationInterval)
	}
	
	// Test validation needed logic
	tokenAddress := "test-token"
	
	// Should need validation initially
	if !validator.IsValidationNeeded(tokenAddress) {
		t.Error("Expected validation to be needed initially")
	}
	
	// Record a validation
	validator.validationMutex.Lock()
	validator.lastValidation[tokenAddress] = time.Now()
	validator.validationMutex.Unlock()
	
	// Should not need validation immediately after
	if validator.IsValidationNeeded(tokenAddress) {
		t.Error("Expected validation to not be needed immediately after validation")
	}
	
	// Should need validation after interval passes
	validator.validationMutex.Lock()
	validator.lastValidation[tokenAddress] = time.Now().Add(-customInterval - time.Minute)
	validator.validationMutex.Unlock()
	
	if !validator.IsValidationNeeded(tokenAddress) {
		t.Error("Expected validation to be needed after interval passes")
	}
}

// Benchmark tests
func BenchmarkDataConsistencyValidator_ValidateTokenData(b *testing.B) {
	redisManager := NewMockRedisManager()
	calculator := NewMockSlidingWindowCalculator()
	validator := NewDataConsistencyValidator(redisManager, calculator)
	
	ctx := context.Background()
	tokenAddress := "benchmark-token"
	
	// Create test data with many trades
	trades := make([]models.TradeData, 1000)
	for i := 0; i < 1000; i++ {
		trades[i] = models.TradeData{
			Token:        tokenAddress,
			Wallet:       fmt.Sprintf("wallet%d", i%10),
			SellBuy:      []string{"buy", "sell"}[i%2],
			NativeAmount: float64(100 + i),
			TokenAmount:  float64(1000 + i*10),
			PriceUsd:     0.1 + float64(i)*0.001,
			TransTime:    time.Now().Add(-time.Duration(i) * time.Second),
			TxHash:       fmt.Sprintf("hash%d", i),
		}
	}
	
	tokenData := &models.TokenData{
		TokenAddress: tokenAddress,
		Trades:       trades,
		Indices: map[string]int64{
			"1min":  100,
			"5min":  300,
			"15min": 500,
			"30min": 700,
			"1hour": 900,
		},
		Aggregates: map[string]*models.AggregateData{
			"1min":  {SellCount: 50, BuyCount: 50, LastUpdate: time.Now()},
			"5min":  {SellCount: 150, BuyCount: 150, LastUpdate: time.Now()},
			"15min": {SellCount: 250, BuyCount: 250, LastUpdate: time.Now()},
			"30min": {SellCount: 350, BuyCount: 350, LastUpdate: time.Now()},
			"1hour": {SellCount: 450, BuyCount: 450, LastUpdate: time.Now()},
		},
		LastUpdate: time.Now(),
	}
	
	redisManager.data[tokenAddress] = tokenData
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_, err := validator.ValidateTokenData(ctx, tokenAddress)
		if err != nil {
			b.Fatalf("Validation failed: %v", err)
		}
	}
}