package processor

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"aggregatorService/interfaces"
	"aggregatorService/models"
)

// DataConsistencyValidator handles data integrity checks and recovery
type DataConsistencyValidator struct {
	redisManager interfaces.RedisManager
	calculator   interfaces.SlidingWindowCalculator
	
	// Validation configuration
	validationInterval time.Duration
	maxValidationAge   time.Duration
	
	// State tracking
	lastValidation map[string]time.Time
	validationMutex sync.RWMutex
	
	// Error tracking
	errorLogger *ErrorLogger
}

// ValidationResult contains the result of a data consistency check
type ValidationResult struct {
	TokenAddress    string
	IsConsistent    bool
	Issues          []ValidationIssue
	ValidationTime  time.Time
	RecoveryApplied bool
}

// ValidationIssue represents a specific consistency issue
type ValidationIssue struct {
	Type        string
	Description string
	Severity    string // "low", "medium", "high", "critical"
	Field       string
	Expected    interface{}
	Actual      interface{}
}

// ConsistencyMetrics tracks validation statistics
type ConsistencyMetrics struct {
	TotalValidations    int64
	FailedValidations   int64
	RecoveriesApplied   int64
	LastValidationTime  time.Time
	AverageValidationTime time.Duration
	mutex               sync.RWMutex
}

// NewDataConsistencyValidator creates a new data consistency validator
func NewDataConsistencyValidator(redisManager interfaces.RedisManager, calculator interfaces.SlidingWindowCalculator) *DataConsistencyValidator {
	return &DataConsistencyValidator{
		redisManager:       redisManager,
		calculator:         calculator,
		validationInterval: 5 * time.Minute,  // Validate every 5 minutes
		maxValidationAge:   30 * time.Minute, // Consider data stale after 30 minutes
		lastValidation:     make(map[string]time.Time),
		errorLogger:        NewErrorLogger("DataConsistencyValidator"),
	}
}

// ValidateTokenData performs comprehensive validation of token data
func (dcv *DataConsistencyValidator) ValidateTokenData(ctx context.Context, tokenAddress string) (*ValidationResult, error) {
	startTime := time.Now()
	
	result := &ValidationResult{
		TokenAddress:   tokenAddress,
		IsConsistent:   true,
		Issues:         []ValidationIssue{},
		ValidationTime: startTime,
	}
	
	// Load token data from Redis
	tokenData, err := dcv.redisManager.GetTokenData(ctx, tokenAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to load token data: %w", err)
	}
	
	// Validate trade data integrity
	if err := dcv.validateTradeData(tokenData, result); err != nil {
		dcv.errorLogger.LogError("validate_trade_data", err, map[string]interface{}{
			"token_address": tokenAddress,
		})
	}
	
	// Validate indices consistency
	if err := dcv.validateIndices(tokenData, result); err != nil {
		dcv.errorLogger.LogError("validate_indices", err, map[string]interface{}{
			"token_address": tokenAddress,
		})
	}
	
	// Validate aggregates consistency
	if err := dcv.validateAggregates(ctx, tokenData, result); err != nil {
		dcv.errorLogger.LogError("validate_aggregates", err, map[string]interface{}{
			"token_address": tokenAddress,
		})
	}
	
	// Validate timestamps
	if err := dcv.validateTimestamps(tokenData, result); err != nil {
		dcv.errorLogger.LogError("validate_timestamps", err, map[string]interface{}{
			"token_address": tokenAddress,
		})
	}
	
	// Apply recovery if issues found
	if len(result.Issues) > 0 {
		result.IsConsistent = false
		
		// Check if recovery should be applied
		if dcv.shouldApplyRecovery(result) {
			if err := dcv.applyRecovery(ctx, tokenAddress, tokenData, result); err != nil {
				dcv.errorLogger.LogError("apply_recovery", err, map[string]interface{}{
					"token_address": tokenAddress,
					"issues_count":  len(result.Issues),
				})
			} else {
				result.RecoveryApplied = true
			}
		}
	}
	
	// Update validation tracking
	dcv.validationMutex.Lock()
	dcv.lastValidation[tokenAddress] = startTime
	dcv.validationMutex.Unlock()
	
	log.Printf("Data consistency validation completed for token %s: consistent=%v, issues=%d, recovery_applied=%v, duration=%v",
		tokenAddress, result.IsConsistent, len(result.Issues), result.RecoveryApplied, time.Since(startTime))
	
	return result, nil
}

// validateTradeData validates the integrity of trade data
func (dcv *DataConsistencyValidator) validateTradeData(tokenData *models.TokenData, result *ValidationResult) error {
	if tokenData == nil {
		result.Issues = append(result.Issues, ValidationIssue{
			Type:        "null_data",
			Description: "Token data is null",
			Severity:    "critical",
			Field:       "token_data",
		})
		return nil
	}
	
	// Check for duplicate trades
	seenHashes := make(map[string]int)
	for i, trade := range tokenData.Trades {
		if trade.TxHash == "" {
			result.Issues = append(result.Issues, ValidationIssue{
				Type:        "missing_tx_hash",
				Description: fmt.Sprintf("Trade at index %d has empty transaction hash", i),
				Severity:    "high",
				Field:       "tx_hash",
			})
			continue
		}
		
		if prevIndex, exists := seenHashes[trade.TxHash]; exists {
			result.Issues = append(result.Issues, ValidationIssue{
				Type:        "duplicate_trade",
				Description: fmt.Sprintf("Duplicate transaction hash %s found at indices %d and %d", trade.TxHash, prevIndex, i),
				Severity:    "high",
				Field:       "tx_hash",
				Expected:    "unique",
				Actual:      "duplicate",
			})
		}
		seenHashes[trade.TxHash] = i
		
		// Validate trade data fields
		if err := trade.ValidateTradeData(); err != nil {
			result.Issues = append(result.Issues, ValidationIssue{
				Type:        "invalid_trade_data",
				Description: fmt.Sprintf("Trade at index %d failed validation: %v", i, err),
				Severity:    "medium",
				Field:       "trade_data",
			})
		}
		
		// Check for reasonable values
		if trade.NativeAmount < 0 {
			result.Issues = append(result.Issues, ValidationIssue{
				Type:        "negative_amount",
				Description: fmt.Sprintf("Trade at index %d has negative native amount: %f", i, trade.NativeAmount),
				Severity:    "high",
				Field:       "native_amount",
				Expected:    ">= 0",
				Actual:      trade.NativeAmount,
			})
		}
		
		if trade.TokenAmount < 0 {
			result.Issues = append(result.Issues, ValidationIssue{
				Type:        "negative_amount",
				Description: fmt.Sprintf("Trade at index %d has negative token amount: %f", i, trade.TokenAmount),
				Severity:    "high",
				Field:       "token_amount",
				Expected:    ">= 0",
				Actual:      trade.TokenAmount,
			})
		}
		
		if trade.PriceUsd < 0 {
			result.Issues = append(result.Issues, ValidationIssue{
				Type:        "negative_price",
				Description: fmt.Sprintf("Trade at index %d has negative price: %f", i, trade.PriceUsd),
				Severity:    "high",
				Field:       "price_usd",
				Expected:    ">= 0",
				Actual:      trade.PriceUsd,
			})
		}
		
		// Check timestamp is not in the future
		if trade.TransTime.After(time.Now().Add(5 * time.Minute)) {
			result.Issues = append(result.Issues, ValidationIssue{
				Type:        "future_timestamp",
				Description: fmt.Sprintf("Trade at index %d has future timestamp: %v", i, trade.TransTime),
				Severity:    "medium",
				Field:       "trans_time",
				Expected:    "not in future",
				Actual:      trade.TransTime,
			})
		}
	}
	
	// Check trades are sorted by timestamp
	for i := 1; i < len(tokenData.Trades); i++ {
		if tokenData.Trades[i].TransTime.Before(tokenData.Trades[i-1].TransTime) {
			result.Issues = append(result.Issues, ValidationIssue{
				Type:        "unsorted_trades",
				Description: fmt.Sprintf("Trades are not sorted by timestamp at indices %d and %d", i-1, i),
				Severity:    "medium",
				Field:       "trade_order",
				Expected:    "chronological order",
				Actual:      "out of order",
			})
			break // Only report once
		}
	}
	
	return nil
}

// validateIndices validates the consistency of time window indices
func (dcv *DataConsistencyValidator) validateIndices(tokenData *models.TokenData, result *ValidationResult) error {
	if tokenData.Indices == nil {
		result.Issues = append(result.Issues, ValidationIssue{
			Type:        "null_indices",
			Description: "Indices map is null",
			Severity:    "high",
			Field:       "indices",
		})
		return nil
	}
	
	// Get expected time windows
	timeWindowNames := dcv.calculator.GetAllTimeWindowNames()
	
	// Check that all expected indices exist
	for _, windowName := range timeWindowNames {
		index, exists := tokenData.Indices[windowName]
		if !exists {
			result.Issues = append(result.Issues, ValidationIssue{
				Type:        "missing_index",
				Description: fmt.Sprintf("Missing index for time window %s", windowName),
				Severity:    "medium",
				Field:       "indices",
				Expected:    "index exists",
				Actual:      "missing",
			})
			continue
		}
		
		// Validate index bounds
		if index < 0 {
			result.Issues = append(result.Issues, ValidationIssue{
				Type:        "negative_index",
				Description: fmt.Sprintf("Negative index for time window %s: %d", windowName, index),
				Severity:    "high",
				Field:       "indices",
				Expected:    ">= 0",
				Actual:      index,
			})
		}
		
		if index > int64(len(tokenData.Trades)) {
			result.Issues = append(result.Issues, ValidationIssue{
				Type:        "index_out_of_bounds",
				Description: fmt.Sprintf("Index for time window %s (%d) exceeds trade count (%d)", windowName, index, len(tokenData.Trades)),
				Severity:    "high",
				Field:       "indices",
				Expected:    fmt.Sprintf("<= %d", len(tokenData.Trades)),
				Actual:      index,
			})
		}
	}
	
	// Check for unexpected indices
	for windowName := range tokenData.Indices {
		found := false
		for _, expectedName := range timeWindowNames {
			if windowName == expectedName {
				found = true
				break
			}
		}
		if !found {
			result.Issues = append(result.Issues, ValidationIssue{
				Type:        "unexpected_index",
				Description: fmt.Sprintf("Unexpected index for time window %s", windowName),
				Severity:    "low",
				Field:       "indices",
				Expected:    "known time window",
				Actual:      windowName,
			})
		}
	}
	
	return nil
}

// validateAggregates validates the consistency of aggregation data
func (dcv *DataConsistencyValidator) validateAggregates(ctx context.Context, tokenData *models.TokenData, result *ValidationResult) error {
	if tokenData.Aggregates == nil {
		result.Issues = append(result.Issues, ValidationIssue{
			Type:        "null_aggregates",
			Description: "Aggregates map is null",
			Severity:    "high",
			Field:       "aggregates",
		})
		return nil
	}
	
	// Get expected time windows
	timeWindowNames := dcv.calculator.GetAllTimeWindowNames()
	
	// Check that all expected aggregates exist
	for _, windowName := range timeWindowNames {
		aggregate, exists := tokenData.Aggregates[windowName]
		if !exists {
			result.Issues = append(result.Issues, ValidationIssue{
				Type:        "missing_aggregate",
				Description: fmt.Sprintf("Missing aggregate for time window %s", windowName),
				Severity:    "medium",
				Field:       "aggregates",
				Expected:    "aggregate exists",
				Actual:      "missing",
			})
			continue
		}
		
		// Validate aggregate data
		if err := aggregate.ValidateAggregateData(); err != nil {
			result.Issues = append(result.Issues, ValidationIssue{
				Type:        "invalid_aggregate",
				Description: fmt.Sprintf("Invalid aggregate for time window %s: %v", windowName, err),
				Severity:    "medium",
				Field:       "aggregates",
			})
		}
		
		// Check for negative values
		if aggregate.SellCount < 0 {
			result.Issues = append(result.Issues, ValidationIssue{
				Type:        "negative_count",
				Description: fmt.Sprintf("Negative sell count for time window %s: %d", windowName, aggregate.SellCount),
				Severity:    "high",
				Field:       "sell_count",
				Expected:    ">= 0",
				Actual:      aggregate.SellCount,
			})
		}
		
		if aggregate.BuyCount < 0 {
			result.Issues = append(result.Issues, ValidationIssue{
				Type:        "negative_count",
				Description: fmt.Sprintf("Negative buy count for time window %s: %d", windowName, aggregate.BuyCount),
				Severity:    "high",
				Field:       "buy_count",
				Expected:    ">= 0",
				Actual:      aggregate.BuyCount,
			})
		}
		
		if aggregate.SellVolume < 0 {
			result.Issues = append(result.Issues, ValidationIssue{
				Type:        "negative_volume",
				Description: fmt.Sprintf("Negative sell volume for time window %s: %f", windowName, aggregate.SellVolume),
				Severity:    "high",
				Field:       "sell_volume",
				Expected:    ">= 0",
				Actual:      aggregate.SellVolume,
			})
		}
		
		if aggregate.BuyVolume < 0 {
			result.Issues = append(result.Issues, ValidationIssue{
				Type:        "negative_volume",
				Description: fmt.Sprintf("Negative buy volume for time window %s: %f", windowName, aggregate.BuyVolume),
				Severity:    "high",
				Field:       "buy_volume",
				Expected:    ">= 0",
				Actual:      aggregate.BuyVolume,
			})
		}
		
		// Check total volume consistency
		expectedTotal := aggregate.SellVolume + aggregate.BuyVolume
		if abs(aggregate.TotalVolume-expectedTotal) > 0.01 { // Allow small floating point errors
			result.Issues = append(result.Issues, ValidationIssue{
				Type:        "inconsistent_total_volume",
				Description: fmt.Sprintf("Total volume inconsistent for time window %s: expected %f, got %f", windowName, expectedTotal, aggregate.TotalVolume),
				Severity:    "medium",
				Field:       "total_volume",
				Expected:    expectedTotal,
				Actual:      aggregate.TotalVolume,
			})
		}
		
		// Check price consistency
		if aggregate.StartPrice < 0 || aggregate.EndPrice < 0 {
			result.Issues = append(result.Issues, ValidationIssue{
				Type:        "negative_price",
				Description: fmt.Sprintf("Negative price values for time window %s", windowName),
				Severity:    "high",
				Field:       "prices",
				Expected:    ">= 0",
				Actual:      "negative values found",
			})
		}
	}
	
	return nil
}

// validateTimestamps validates timestamp consistency
func (dcv *DataConsistencyValidator) validateTimestamps(tokenData *models.TokenData, result *ValidationResult) error {
	now := time.Now()
	
	// Check last update timestamp
	if tokenData.LastUpdate.IsZero() {
		result.Issues = append(result.Issues, ValidationIssue{
			Type:        "zero_timestamp",
			Description: "Last update timestamp is zero",
			Severity:    "medium",
			Field:       "last_update",
			Expected:    "valid timestamp",
			Actual:      "zero",
		})
	} else if tokenData.LastUpdate.After(now.Add(5 * time.Minute)) {
		result.Issues = append(result.Issues, ValidationIssue{
			Type:        "future_timestamp",
			Description: fmt.Sprintf("Last update timestamp is in the future: %v", tokenData.LastUpdate),
			Severity:    "medium",
			Field:       "last_update",
			Expected:    "not in future",
			Actual:      tokenData.LastUpdate,
		})
	}
	
	// Check aggregate timestamps
	for windowName, aggregate := range tokenData.Aggregates {
		if aggregate.LastUpdate.IsZero() {
			result.Issues = append(result.Issues, ValidationIssue{
				Type:        "zero_timestamp",
				Description: fmt.Sprintf("Aggregate last update timestamp is zero for time window %s", windowName),
				Severity:    "medium",
				Field:       "aggregate_last_update",
				Expected:    "valid timestamp",
				Actual:      "zero",
			})
		} else if aggregate.LastUpdate.After(now.Add(5 * time.Minute)) {
			result.Issues = append(result.Issues, ValidationIssue{
				Type:        "future_timestamp",
				Description: fmt.Sprintf("Aggregate last update timestamp is in the future for time window %s: %v", windowName, aggregate.LastUpdate),
				Severity:    "medium",
				Field:       "aggregate_last_update",
				Expected:    "not in future",
				Actual:      aggregate.LastUpdate,
			})
		}
	}
	
	return nil
}

// shouldApplyRecovery determines if recovery should be applied based on issues
func (dcv *DataConsistencyValidator) shouldApplyRecovery(result *ValidationResult) bool {
	criticalIssues := 0
	highIssues := 0
	
	for _, issue := range result.Issues {
		switch issue.Severity {
		case "critical":
			criticalIssues++
		case "high":
			highIssues++
		}
	}
	
	// Apply recovery if there are critical issues or multiple high severity issues
	return criticalIssues > 0 || highIssues > 2
}

// applyRecovery attempts to fix consistency issues
func (dcv *DataConsistencyValidator) applyRecovery(ctx context.Context, tokenAddress string, tokenData *models.TokenData, result *ValidationResult) error {
	log.Printf("Applying data consistency recovery for token %s with %d issues", tokenAddress, len(result.Issues))
	
	recoveryApplied := false
	
	// Group issues by type for efficient recovery
	issuesByType := make(map[string][]ValidationIssue)
	for _, issue := range result.Issues {
		issuesByType[issue.Type] = append(issuesByType[issue.Type], issue)
	}
	
	// Apply specific recovery strategies
	for issueType, issues := range issuesByType {
		switch issueType {
		case "duplicate_trade":
			if err := dcv.removeDuplicateTrades(tokenData, issues); err != nil {
				dcv.errorLogger.LogError("remove_duplicate_trades", err, map[string]interface{}{
					"token_address": tokenAddress,
					"issues_count":  len(issues),
				})
			} else {
				recoveryApplied = true
			}
			
		case "unsorted_trades":
			if err := dcv.sortTrades(tokenData); err != nil {
				dcv.errorLogger.LogError("sort_trades", err, map[string]interface{}{
					"token_address": tokenAddress,
				})
			} else {
				recoveryApplied = true
			}
			
		case "missing_index", "negative_index", "index_out_of_bounds":
			if err := dcv.recalculateIndices(ctx, tokenData); err != nil {
				dcv.errorLogger.LogError("recalculate_indices", err, map[string]interface{}{
					"token_address": tokenAddress,
				})
			} else {
				recoveryApplied = true
			}
			
		case "missing_aggregate", "invalid_aggregate", "negative_count", "negative_volume", "inconsistent_total_volume":
			if err := dcv.recalculateAggregates(ctx, tokenData); err != nil {
				dcv.errorLogger.LogError("recalculate_aggregates", err, map[string]interface{}{
					"token_address": tokenAddress,
				})
			} else {
				recoveryApplied = true
			}
			
		case "zero_timestamp":
			if err := dcv.fixTimestamps(tokenData); err != nil {
				dcv.errorLogger.LogError("fix_timestamps", err, map[string]interface{}{
					"token_address": tokenAddress,
				})
			} else {
				recoveryApplied = true
			}
		}
	}
	
	// Save recovered data back to Redis if any recovery was applied
	if recoveryApplied {
		if err := dcv.redisManager.UpdateTokenData(ctx, tokenAddress, tokenData); err != nil {
			return fmt.Errorf("failed to save recovered data: %w", err)
		}
		
		log.Printf("Data consistency recovery completed for token %s", tokenAddress)
	}
	
	return nil
}

// removeDuplicateTrades removes duplicate trades based on transaction hash
func (dcv *DataConsistencyValidator) removeDuplicateTrades(tokenData *models.TokenData, issues []ValidationIssue) error {
	if len(tokenData.Trades) == 0 {
		return nil
	}
	
	// Create a map to track unique trades
	uniqueTrades := make([]models.TradeData, 0, len(tokenData.Trades))
	seenHashes := make(map[string]bool)
	
	for _, trade := range tokenData.Trades {
		if trade.TxHash == "" || !seenHashes[trade.TxHash] {
			uniqueTrades = append(uniqueTrades, trade)
			if trade.TxHash != "" {
				seenHashes[trade.TxHash] = true
			}
		}
	}
	
	removedCount := len(tokenData.Trades) - len(uniqueTrades)
	tokenData.Trades = uniqueTrades
	
	log.Printf("Removed %d duplicate trades", removedCount)
	return nil
}

// sortTrades sorts trades by timestamp
func (dcv *DataConsistencyValidator) sortTrades(tokenData *models.TokenData) error {
	if len(tokenData.Trades) <= 1 {
		return nil
	}
	
	// Sort trades by timestamp
	for i := 0; i < len(tokenData.Trades)-1; i++ {
		for j := i + 1; j < len(tokenData.Trades); j++ {
			if tokenData.Trades[i].TransTime.After(tokenData.Trades[j].TransTime) {
				tokenData.Trades[i], tokenData.Trades[j] = tokenData.Trades[j], tokenData.Trades[i]
			}
		}
	}
	
	log.Printf("Sorted %d trades by timestamp", len(tokenData.Trades))
	return nil
}

// recalculateIndices recalculates all time window indices
func (dcv *DataConsistencyValidator) recalculateIndices(ctx context.Context, tokenData *models.TokenData) error {
	if tokenData.Indices == nil {
		tokenData.Indices = make(map[string]int64)
	}
	
	currentTime := time.Now()
	timeWindowNames := dcv.calculator.GetAllTimeWindowNames()
	
	for _, windowName := range timeWindowNames {
		timeWindow, exists := dcv.calculator.GetTimeWindowByName(windowName)
		if !exists {
			continue
		}
		
		// Find the index for this time window
		index := int64(0)
		cutoffTime := currentTime.Add(-timeWindow.Duration)
		
		for i, trade := range tokenData.Trades {
			if trade.TransTime.Before(cutoffTime) {
				index = int64(i + 1)
			} else {
				break
			}
		}
		
		tokenData.Indices[windowName] = index
	}
	
	log.Printf("Recalculated indices for %d time windows", len(timeWindowNames))
	return nil
}

// recalculateAggregates recalculates all aggregation data
func (dcv *DataConsistencyValidator) recalculateAggregates(ctx context.Context, tokenData *models.TokenData) error {
	if tokenData.Aggregates == nil {
		tokenData.Aggregates = make(map[string]*models.AggregateData)
	}
	
	currentTime := time.Now()
	timeWindowNames := dcv.calculator.GetAllTimeWindowNames()
	
	for _, windowName := range timeWindowNames {
		timeWindow, exists := dcv.calculator.GetTimeWindowByName(windowName)
		if !exists {
			continue
		}
		
		// Recalculate aggregate for this time window
		aggregate, _, err := dcv.calculator.CalculateWindow(ctx, tokenData.Trades, timeWindow, currentTime)
		if err != nil {
			dcv.errorLogger.LogError("calculate_window", err, map[string]interface{}{
				"window_name": windowName,
			})
			continue
		}
		
		tokenData.Aggregates[windowName] = aggregate
	}
	
	log.Printf("Recalculated aggregates for %d time windows", len(timeWindowNames))
	return nil
}

// fixTimestamps fixes zero or invalid timestamps
func (dcv *DataConsistencyValidator) fixTimestamps(tokenData *models.TokenData) error {
	currentTime := time.Now()
	
	// Fix last update timestamp
	if tokenData.LastUpdate.IsZero() {
		tokenData.LastUpdate = currentTime
	}
	
	// Fix aggregate timestamps
	for _, aggregate := range tokenData.Aggregates {
		if aggregate.LastUpdate.IsZero() {
			aggregate.LastUpdate = currentTime
		}
	}
	
	log.Printf("Fixed timestamps for token data")
	return nil
}

// ValidateAllActiveTokens validates all active tokens
func (dcv *DataConsistencyValidator) ValidateAllActiveTokens(ctx context.Context) ([]*ValidationResult, error) {
	// Get all active tokens
	activeTokens, err := dcv.redisManager.GetActiveTokens(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get active tokens: %w", err)
	}
	
	results := make([]*ValidationResult, 0, len(activeTokens))
	
	// Validate each token
	for _, tokenAddress := range activeTokens {
		result, err := dcv.ValidateTokenData(ctx, tokenAddress)
		if err != nil {
			dcv.errorLogger.LogError("validate_token", err, map[string]interface{}{
				"token_address": tokenAddress,
			})
			continue
		}
		
		results = append(results, result)
	}
	
	return results, nil
}

// GetValidationSummary returns a summary of validation results
func (dcv *DataConsistencyValidator) GetValidationSummary(results []*ValidationResult) map[string]interface{} {
	summary := map[string]interface{}{
		"total_tokens":      len(results),
		"consistent_tokens": 0,
		"tokens_with_issues": 0,
		"recoveries_applied": 0,
		"issue_types":       make(map[string]int),
		"severity_counts":   make(map[string]int),
	}
	
	for _, result := range results {
		if result.IsConsistent {
			summary["consistent_tokens"] = summary["consistent_tokens"].(int) + 1
		} else {
			summary["tokens_with_issues"] = summary["tokens_with_issues"].(int) + 1
		}
		
		if result.RecoveryApplied {
			summary["recoveries_applied"] = summary["recoveries_applied"].(int) + 1
		}
		
		// Count issue types and severities
		issueTypes := summary["issue_types"].(map[string]int)
		severityCounts := summary["severity_counts"].(map[string]int)
		
		for _, issue := range result.Issues {
			issueTypes[issue.Type]++
			severityCounts[issue.Severity]++
		}
	}
	
	return summary
}

// RecoverFromRedisOnStartup performs state recovery from Redis on service startup
func (dcv *DataConsistencyValidator) RecoverFromRedisOnStartup(ctx context.Context) error {
	log.Printf("Starting data consistency recovery from Redis on startup")
	
	// Get all active tokens
	activeTokens, err := dcv.redisManager.GetActiveTokens(ctx)
	if err != nil {
		return fmt.Errorf("failed to get active tokens for startup recovery: %w", err)
	}
	
	log.Printf("Found %d active tokens for startup recovery", len(activeTokens))
	
	recoveredTokens := 0
	failedTokens := 0
	
	// Validate and recover each token
	for _, tokenAddress := range activeTokens {
		result, err := dcv.ValidateTokenData(ctx, tokenAddress)
		if err != nil {
			dcv.errorLogger.LogError("startup_validation", err, map[string]interface{}{
				"token_address": tokenAddress,
			})
			failedTokens++
			continue
		}
		
		if !result.IsConsistent {
			if result.RecoveryApplied {
				recoveredTokens++
			} else {
				failedTokens++
			}
		}
	}
	
	log.Printf("Startup recovery completed: %d tokens recovered, %d tokens failed", recoveredTokens, failedTokens)
	
	if failedTokens > 0 {
		return fmt.Errorf("startup recovery failed for %d tokens", failedTokens)
	}
	
	return nil
}

// abs returns the absolute value of a float64
func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// SetValidationInterval sets the validation interval
func (dcv *DataConsistencyValidator) SetValidationInterval(interval time.Duration) {
	dcv.validationInterval = interval
}

// SetMaxValidationAge sets the maximum validation age
func (dcv *DataConsistencyValidator) SetMaxValidationAge(age time.Duration) {
	dcv.maxValidationAge = age
}

// GetLastValidationTime returns the last validation time for a token
func (dcv *DataConsistencyValidator) GetLastValidationTime(tokenAddress string) (time.Time, bool) {
	dcv.validationMutex.RLock()
	defer dcv.validationMutex.RUnlock()
	
	lastValidation, exists := dcv.lastValidation[tokenAddress]
	return lastValidation, exists
}

// IsValidationNeeded checks if validation is needed for a token
func (dcv *DataConsistencyValidator) IsValidationNeeded(tokenAddress string) bool {
	lastValidation, exists := dcv.GetLastValidationTime(tokenAddress)
	if !exists {
		return true // Never validated
	}
	
	return time.Since(lastValidation) > dcv.validationInterval
}

// ClearValidationHistory clears validation history for a token
func (dcv *DataConsistencyValidator) ClearValidationHistory(tokenAddress string) {
	dcv.validationMutex.Lock()
	defer dcv.validationMutex.Unlock()
	
	delete(dcv.lastValidation, tokenAddress)
}