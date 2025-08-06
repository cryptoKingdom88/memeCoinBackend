package maintenance

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"aggregatorService/interfaces"
	"aggregatorService/models"
)

// Service implements the MaintenanceService interface
type Service struct {
	redisManager   interfaces.RedisManager
	interval       time.Duration
	staleThreshold time.Duration
	
	// Control channels
	stopChan   chan struct{}
	doneChan   chan struct{}
	
	// State management
	mutex      sync.RWMutex
	isRunning  bool
	lastScan   time.Time
	
	// Statistics
	totalScans       int64
	staleTokensFound int64
	errorsCount      int64
}

// NewService creates a new maintenance service
func NewService() interfaces.MaintenanceService {
	return &Service{
		stopChan: make(chan struct{}),
		doneChan: make(chan struct{}),
	}
}

// Initialize initializes the maintenance service with configuration
func (s *Service) Initialize(redisManager interfaces.RedisManager, interval, staleThreshold time.Duration) error {
	if redisManager == nil {
		return fmt.Errorf("redis manager cannot be nil")
	}
	
	if interval <= 0 {
		return fmt.Errorf("maintenance interval must be positive, got: %v", interval)
	}
	
	if staleThreshold <= 0 {
		return fmt.Errorf("stale threshold must be positive, got: %v", staleThreshold)
	}
	
	s.mutex.Lock()
	defer s.mutex.Unlock()
	
	s.redisManager = redisManager
	s.interval = interval
	s.staleThreshold = staleThreshold
	
	log.Printf("Maintenance service initialized with interval: %v, stale threshold: %v", 
		interval, staleThreshold)
	
	return nil
}

// Start starts the maintenance service with periodic scanning
func (s *Service) Start(ctx context.Context) error {
	s.mutex.Lock()
	if s.isRunning {
		s.mutex.Unlock()
		return fmt.Errorf("maintenance service is already running")
	}
	
	if s.redisManager == nil {
		s.mutex.Unlock()
		return fmt.Errorf("maintenance service not initialized")
	}
	
	s.isRunning = true
	s.mutex.Unlock()
	
	log.Printf("Starting maintenance service with %v interval", s.interval)
	
	// Start the maintenance loop in a goroutine
	go s.maintenanceLoop(ctx)
	
	return nil
}

// maintenanceLoop runs the periodic maintenance tasks
func (s *Service) maintenanceLoop(ctx context.Context) {
	defer close(s.doneChan)
	
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	
	log.Printf("Maintenance loop started, scanning every %v", s.interval)
	
	for {
		select {
		case <-ctx.Done():
			log.Printf("Maintenance loop stopping due to context cancellation")
			s.setRunning(false)
			return
			
		case <-s.stopChan:
			log.Printf("Maintenance loop stopping due to stop signal")
			s.setRunning(false)
			return
			
		case <-ticker.C:
			s.performMaintenanceCycle(ctx)
		}
	}
}

// performMaintenanceCycle performs a single maintenance cycle
func (s *Service) performMaintenanceCycle(ctx context.Context) {
	startTime := time.Now()
	
	s.mutex.Lock()
	s.totalScans++
	s.lastScan = startTime
	s.mutex.Unlock()
	
	log.Printf("Starting maintenance cycle #%d", s.totalScans)
	
	// Scan for stale tokens
	staleTokens, err := s.ScanStaleTokens(ctx)
	if err != nil {
		s.incrementErrorCount()
		log.Printf("Error scanning for stale tokens: %v", err)
		return
	}
	
	if len(staleTokens) == 0 {
		log.Printf("Maintenance cycle completed in %v - no stale tokens found", 
			time.Since(startTime))
		return
	}
	
	log.Printf("Found %d stale tokens: %v", len(staleTokens), staleTokens)
	
	// Process stale tokens with improved error handling and recovery
	processedCount := 0
	errorCount := 0
	recoveredCount := 0
	
	for _, tokenAddress := range staleTokens {
		// Attempt manual aggregation with recovery mechanisms
		if err := s.performManualAggregationWithRecovery(ctx, tokenAddress); err != nil {
			errorCount++
			log.Printf("Error performing manual aggregation for token %s: %v", 
				tokenAddress, err)
			
			// Attempt basic recovery - at least update the timestamp to prevent continuous retries
			if recoveryErr := s.attemptBasicRecovery(ctx, tokenAddress); recoveryErr != nil {
				log.Printf("Failed to recover token %s: %v", tokenAddress, recoveryErr)
			} else {
				recoveredCount++
				log.Printf("Successfully recovered token %s with basic timestamp update", tokenAddress)
			}
		} else {
			processedCount++
		}
	}
	
	s.mutex.Lock()
	s.staleTokensFound += int64(len(staleTokens))
	s.errorsCount += int64(errorCount)
	s.mutex.Unlock()
	
	duration := time.Since(startTime)
	log.Printf("Maintenance cycle completed in %v - processed %d/%d stale tokens (%d errors, %d recovered)", 
		duration, processedCount, len(staleTokens), errorCount, recoveredCount)
}

// ScanStaleTokens scans Redis for tokens that haven't been updated recently
func (s *Service) ScanStaleTokens(ctx context.Context) ([]string, error) {
	// Implement retry logic for Redis connection failures
	maxRetries := 3
	baseDelay := 200 * time.Millisecond
	
	var activeTokens []string
	var err error
	
	// Get all active tokens from Redis with retry logic
	for attempt := 0; attempt < maxRetries; attempt++ {
		activeTokens, err = s.redisManager.GetActiveTokens(ctx)
		if err == nil {
			break
		}
		
		if attempt < maxRetries-1 {
			delay := time.Duration(1<<attempt) * baseDelay // Exponential backoff
			log.Printf("Failed to get active tokens (attempt %d/%d), retrying in %v: %v", 
				attempt+1, maxRetries, delay, err)
			
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("context cancelled during retry: %w", ctx.Err())
			case <-time.After(delay):
				continue
			}
		}
	}
	
	if err != nil {
		s.incrementErrorCount()
		return nil, fmt.Errorf("failed to get active tokens after %d attempts: %w", maxRetries, err)
	}
	
	if len(activeTokens) == 0 {
		return []string{}, nil
	}
	
	log.Printf("Scanning %d active tokens for staleness", len(activeTokens))
	
	var staleTokens []string
	currentTime := time.Now()
	failedTokens := make([]string, 0)
	
	// Check each token's last update timestamp with error tracking
	for _, tokenAddress := range activeTokens {
		lastUpdate, err := s.getLastUpdateWithRetry(ctx, tokenAddress, maxRetries, baseDelay)
		if err != nil {
			failedTokens = append(failedTokens, tokenAddress)
			log.Printf("Warning: failed to get last update for token %s after retries: %v", 
				tokenAddress, err)
			continue
		}
		
		// If last update is zero (never updated) or older than threshold, consider it stale
		if lastUpdate.IsZero() || currentTime.Sub(lastUpdate) > s.staleThreshold {
			staleTokens = append(staleTokens, tokenAddress)
			log.Printf("Token %s is stale - last update: %v (age: %v)", 
				tokenAddress, lastUpdate, currentTime.Sub(lastUpdate))
		}
	}
	
	// Log summary of scan results
	if len(failedTokens) > 0 {
		log.Printf("Warning: Failed to check %d tokens during staleness scan: %v", 
			len(failedTokens), failedTokens)
		s.incrementErrorCount()
	}
	
	log.Printf("Staleness scan completed - found %d stale tokens out of %d active tokens (%d failed checks)", 
		len(staleTokens), len(activeTokens), len(failedTokens))
	
	return staleTokens, nil
}

// getLastUpdateWithRetry gets the last update timestamp with retry logic
func (s *Service) getLastUpdateWithRetry(ctx context.Context, tokenAddress string, maxRetries int, baseDelay time.Duration) (time.Time, error) {
	var lastUpdate time.Time
	var err error
	
	for attempt := 0; attempt < maxRetries; attempt++ {
		lastUpdate, err = s.redisManager.GetLastUpdate(ctx, tokenAddress)
		if err == nil {
			return lastUpdate, nil
		}
		
		if attempt < maxRetries-1 {
			delay := time.Duration(1<<attempt) * baseDelay
			
			select {
			case <-ctx.Done():
				return time.Time{}, fmt.Errorf("context cancelled during retry: %w", ctx.Err())
			case <-time.After(delay):
				continue
			}
		}
	}
	
	return time.Time{}, fmt.Errorf("failed after %d attempts: %w", maxRetries, err)
}

// PerformManualAggregation performs manual aggregation calculation for a stale token
func (s *Service) PerformManualAggregation(ctx context.Context, tokenAddress string) error {
	log.Printf("Performing manual aggregation for token: %s", tokenAddress)
	
	// Implement retry logic with exponential backoff for Redis operations
	maxRetries := 3
	baseDelay := 100 * time.Millisecond
	
	// Get token data from Redis with retry logic
	var tokenData *models.TokenData
	var err error
	
	for attempt := 0; attempt < maxRetries; attempt++ {
		tokenData, err = s.redisManager.GetTokenData(ctx, tokenAddress)
		if err == nil {
			break
		}
		
		if attempt < maxRetries-1 {
			delay := time.Duration(1<<attempt) * baseDelay // Exponential backoff
			log.Printf("Failed to get token data for %s (attempt %d/%d), retrying in %v: %v", 
				tokenAddress, attempt+1, maxRetries, delay, err)
			
			select {
			case <-ctx.Done():
				return fmt.Errorf("context cancelled during retry: %w", ctx.Err())
			case <-time.After(delay):
				continue
			}
		}
	}
	
	if err != nil {
		s.incrementErrorCount()
		return fmt.Errorf("failed to get token data after %d attempts: %w", maxRetries, err)
	}
	
	if len(tokenData.Trades) == 0 {
		log.Printf("No trades found for token %s, updating timestamp only", tokenAddress)
		return s.updateLastUpdateWithRetry(ctx, tokenAddress, time.Now(), maxRetries, baseDelay)
	}
	
	// Recalculate aggregations for all time windows
	timeWindows := []struct {
		name     string
		duration time.Duration
	}{
		{"1min", 1 * time.Minute},
		{"5min", 5 * time.Minute},
		{"15min", 15 * time.Minute},
		{"30min", 30 * time.Minute},
		{"1hour", 1 * time.Hour},
	}
	
	currentTime := time.Now()
	updatedAggregates := make(map[string]*models.AggregateData)
	updatedIndices := make(map[string]int64)
	calculationErrors := make([]string, 0)
	
	// Calculate aggregations for each time window with error tracking
	for _, window := range timeWindows {
		aggregate, index, err := s.calculateWindowAggregation(
			tokenData.Trades, window.duration, currentTime)
		if err != nil {
			errorMsg := fmt.Sprintf("failed to calculate %s aggregation: %v", window.name, err)
			calculationErrors = append(calculationErrors, errorMsg)
			log.Printf("Warning: %s for token %s", errorMsg, tokenAddress)
			continue
		}
		
		// Validate aggregate data before storing
		if err := aggregate.ValidateAggregateData(); err != nil {
			errorMsg := fmt.Sprintf("invalid %s aggregate data: %v", window.name, err)
			calculationErrors = append(calculationErrors, errorMsg)
			log.Printf("Warning: %s for token %s", errorMsg, tokenAddress)
			continue
		}
		
		updatedAggregates[window.name] = aggregate
		updatedIndices[window.name] = index
	}
	
	// If no valid aggregations were calculated, still update timestamp but log the issue
	if len(updatedAggregates) == 0 && len(calculationErrors) > 0 {
		log.Printf("Warning: No valid aggregations calculated for token %s due to errors: %v", 
			tokenAddress, calculationErrors)
		s.incrementErrorCount()
		// Still update timestamp to prevent continuous retries
		return s.updateLastUpdateWithRetry(ctx, tokenAddress, currentTime, maxRetries, baseDelay)
	}
	
	// Update Redis with new aggregations and indices using atomic operations where possible
	if len(updatedAggregates) > 0 {
		updateErrors := make([]string, 0)
		
		// Update aggregates with retry logic
		for timeframe, aggregate := range updatedAggregates {
			if err := s.setAggregateWithRetry(ctx, tokenAddress, timeframe, aggregate, maxRetries, baseDelay); err != nil {
				errorMsg := fmt.Sprintf("failed to set %s aggregate: %v", timeframe, err)
				updateErrors = append(updateErrors, errorMsg)
				log.Printf("Error: %s for token %s", errorMsg, tokenAddress)
			}
		}
		
		// Update indices with retry logic
		for timeframe, index := range updatedIndices {
			if err := s.setIndexWithRetry(ctx, tokenAddress, timeframe, index, maxRetries, baseDelay); err != nil {
				errorMsg := fmt.Sprintf("failed to set %s index: %v", timeframe, err)
				updateErrors = append(updateErrors, errorMsg)
				log.Printf("Error: %s for token %s", errorMsg, tokenAddress)
			}
		}
		
		// If there were update errors, log them but don't fail the entire operation
		if len(updateErrors) > 0 {
			log.Printf("Warning: Partial update failures for token %s: %v", tokenAddress, updateErrors)
			s.incrementErrorCount()
		}
	}
	
	// Update last update timestamp with retry logic
	if err := s.updateLastUpdateWithRetry(ctx, tokenAddress, currentTime, maxRetries, baseDelay); err != nil {
		s.incrementErrorCount()
		return fmt.Errorf("failed to update last update timestamp after retries: %w", err)
	}
	
	log.Printf("Manual aggregation completed for token %s - updated %d time windows", 
		tokenAddress, len(updatedAggregates))
	
	return nil
}

// updateLastUpdateWithRetry updates the last update timestamp with retry logic
func (s *Service) updateLastUpdateWithRetry(ctx context.Context, tokenAddress string, timestamp time.Time, maxRetries int, baseDelay time.Duration) error {
	var err error
	
	for attempt := 0; attempt < maxRetries; attempt++ {
		err = s.redisManager.UpdateLastUpdate(ctx, tokenAddress, timestamp)
		if err == nil {
			return nil
		}
		
		if attempt < maxRetries-1 {
			delay := time.Duration(1<<attempt) * baseDelay
			log.Printf("Failed to update last update for %s (attempt %d/%d), retrying in %v: %v", 
				tokenAddress, attempt+1, maxRetries, delay, err)
			
			select {
			case <-ctx.Done():
				return fmt.Errorf("context cancelled during retry: %w", ctx.Err())
			case <-time.After(delay):
				continue
			}
		}
	}
	
	return fmt.Errorf("failed after %d attempts: %w", maxRetries, err)
}

// setAggregateWithRetry sets aggregate data with retry logic
func (s *Service) setAggregateWithRetry(ctx context.Context, tokenAddress, timeframe string, aggregate *models.AggregateData, maxRetries int, baseDelay time.Duration) error {
	var err error
	
	for attempt := 0; attempt < maxRetries; attempt++ {
		err = s.redisManager.SetAggregate(ctx, tokenAddress, timeframe, aggregate)
		if err == nil {
			return nil
		}
		
		if attempt < maxRetries-1 {
			delay := time.Duration(1<<attempt) * baseDelay
			log.Printf("Failed to set %s aggregate for %s (attempt %d/%d), retrying in %v: %v", 
				timeframe, tokenAddress, attempt+1, maxRetries, delay, err)
			
			select {
			case <-ctx.Done():
				return fmt.Errorf("context cancelled during retry: %w", ctx.Err())
			case <-time.After(delay):
				continue
			}
		}
	}
	
	return fmt.Errorf("failed after %d attempts: %w", maxRetries, err)
}

// setIndexWithRetry sets index data with retry logic
func (s *Service) setIndexWithRetry(ctx context.Context, tokenAddress, timeframe string, index int64, maxRetries int, baseDelay time.Duration) error {
	var err error
	
	for attempt := 0; attempt < maxRetries; attempt++ {
		err = s.redisManager.SetIndex(ctx, tokenAddress, timeframe, index)
		if err == nil {
			return nil
		}
		
		if attempt < maxRetries-1 {
			delay := time.Duration(1<<attempt) * baseDelay
			log.Printf("Failed to set %s index for %s (attempt %d/%d), retrying in %v: %v", 
				timeframe, tokenAddress, attempt+1, maxRetries, delay, err)
			
			select {
			case <-ctx.Done():
				return fmt.Errorf("context cancelled during retry: %w", ctx.Err())
			case <-time.After(delay):
				continue
			}
		}
	}
	
	return fmt.Errorf("failed after %d attempts: %w", maxRetries, err)
}

// performManualAggregationWithRecovery performs manual aggregation with additional recovery mechanisms
func (s *Service) performManualAggregationWithRecovery(ctx context.Context, tokenAddress string) error {
	// First attempt normal manual aggregation
	if err := s.PerformManualAggregation(ctx, tokenAddress); err != nil {
		log.Printf("Manual aggregation failed for token %s, attempting recovery: %v", tokenAddress, err)
		
		// Check if this is a data consistency issue
		if s.isDataConsistencyError(err) {
			log.Printf("Detected data consistency issue for token %s, attempting data recovery", tokenAddress)
			
			// Attempt to recover by reinitializing token data
			if recoveryErr := s.recoverTokenData(ctx, tokenAddress); recoveryErr != nil {
				return fmt.Errorf("manual aggregation failed and recovery failed: %w (original error: %v)", recoveryErr, err)
			}
			
			// Retry manual aggregation after recovery
			if retryErr := s.PerformManualAggregation(ctx, tokenAddress); retryErr != nil {
				return fmt.Errorf("manual aggregation failed after recovery: %w (original error: %v)", retryErr, err)
			}
			
			log.Printf("Successfully recovered and processed token %s", tokenAddress)
			return nil
		}
		
		return err
	}
	
	return nil
}

// attemptBasicRecovery attempts basic recovery by updating timestamp to prevent continuous retries
func (s *Service) attemptBasicRecovery(ctx context.Context, tokenAddress string) error {
	log.Printf("Attempting basic recovery for token %s", tokenAddress)
	
	// Simply update the timestamp to prevent this token from being continuously retried
	maxRetries := 2
	baseDelay := 100 * time.Millisecond
	
	if err := s.updateLastUpdateWithRetry(ctx, tokenAddress, time.Now(), maxRetries, baseDelay); err != nil {
		return fmt.Errorf("basic recovery failed: %w", err)
	}
	
	return nil
}

// isDataConsistencyError checks if an error indicates a data consistency issue
func (s *Service) isDataConsistencyError(err error) bool {
	errorStr := err.Error()
	
	// Check for common data consistency error patterns
	consistencyIndicators := []string{
		"invalid aggregate data",
		"data validation failed",
		"inconsistent state",
		"corrupted data",
		"index out of range",
		"nil pointer",
	}
	
	for _, indicator := range consistencyIndicators {
		if strings.Contains(strings.ToLower(errorStr), indicator) {
			return true
		}
	}
	
	return false
}

// recoverTokenData attempts to recover token data by reinitializing it
func (s *Service) recoverTokenData(ctx context.Context, tokenAddress string) error {
	log.Printf("Attempting to recover token data for %s", tokenAddress)
	
	// Get fresh token data from Redis
	tokenData, err := s.redisManager.GetTokenData(ctx, tokenAddress)
	if err != nil {
		return fmt.Errorf("failed to get token data for recovery: %w", err)
	}
	
	// Validate and clean the trade data
	validTrades := make([]models.TradeData, 0, len(tokenData.Trades))
	for _, trade := range tokenData.Trades {
		if err := trade.ValidateTradeData(); err != nil {
			log.Printf("Warning: Removing invalid trade data during recovery: %v", err)
			continue
		}
		validTrades = append(validTrades, trade)
	}
	
	// If no valid trades remain, clear all data and update timestamp
	if len(validTrades) == 0 {
		log.Printf("No valid trades found during recovery for token %s, clearing data", tokenAddress)
		
		// Clear indices and aggregates
		timeframes := []string{"1min", "5min", "15min", "30min", "1hour"}
		for _, timeframe := range timeframes {
			// Set empty aggregate
			emptyAggregate := models.NewAggregateData()
			if err := s.redisManager.SetAggregate(ctx, tokenAddress, timeframe, emptyAggregate); err != nil {
				log.Printf("Warning: Failed to clear %s aggregate during recovery: %v", timeframe, err)
			}
			
			// Set zero index
			if err := s.redisManager.SetIndex(ctx, tokenAddress, timeframe, 0); err != nil {
				log.Printf("Warning: Failed to clear %s index during recovery: %v", timeframe, err)
			}
		}
		
		// Update timestamp
		return s.redisManager.UpdateLastUpdate(ctx, tokenAddress, time.Now())
	}
	
	// Update token data with cleaned trades
	cleanedTokenData := &models.TokenData{
		TokenAddress: tokenAddress,
		Trades:       validTrades,
		Indices:      make(map[string]int64),
		Aggregates:   make(map[string]*models.AggregateData),
		LastUpdate:   time.Now(),
	}
	
	if err := s.redisManager.UpdateTokenData(ctx, tokenAddress, cleanedTokenData); err != nil {
		return fmt.Errorf("failed to update cleaned token data: %w", err)
	}
	
	log.Printf("Successfully recovered token data for %s with %d valid trades", tokenAddress, len(validTrades))
	return nil
}

// validateTokenDataConsistency validates the consistency of token data
func (s *Service) validateTokenDataConsistency(ctx context.Context, tokenAddress string) error {
	tokenData, err := s.redisManager.GetTokenData(ctx, tokenAddress)
	if err != nil {
		return fmt.Errorf("failed to get token data for validation: %w", err)
	}
	
	// Validate trade data
	for i, trade := range tokenData.Trades {
		if err := trade.ValidateTradeData(); err != nil {
			return fmt.Errorf("invalid trade at index %d: %w", i, err)
		}
	}
	
	// Validate aggregate data
	for timeframe, aggregate := range tokenData.Aggregates {
		if aggregate == nil {
			return fmt.Errorf("nil aggregate for timeframe %s", timeframe)
		}
		
		if err := aggregate.ValidateAggregateData(); err != nil {
			return fmt.Errorf("invalid aggregate for timeframe %s: %w", timeframe, err)
		}
	}
	
	// Validate indices
	for timeframe, index := range tokenData.Indices {
		if index < 0 {
			return fmt.Errorf("negative index for timeframe %s: %d", timeframe, index)
		}
		
		if index > int64(len(tokenData.Trades)) {
			return fmt.Errorf("index out of range for timeframe %s: %d > %d", timeframe, index, len(tokenData.Trades))
		}
	}
	
	return nil
}

// calculateWindowAggregation calculates aggregation for a specific time window
func (s *Service) calculateWindowAggregation(trades []models.TradeData, windowDuration time.Duration, currentTime time.Time) (*models.AggregateData, int64, error) {
	aggregate := models.NewAggregateData()
	windowBoundary := currentTime.Add(-windowDuration)
	
	var validTradeCount int64
	var startIndex int64 = -1
	
	// Process trades in reverse order (newest first, as they're stored in Redis LPUSH order)
	for i := len(trades) - 1; i >= 0; i-- {
		trade := trades[i]
		
		// Check if trade is within the time window
		if trade.TransTime.After(windowBoundary) || trade.TransTime.Equal(windowBoundary) {
			aggregate.AddTrade(trade)
			validTradeCount++
			
			// Track the index of the first valid trade (oldest in window)
			if startIndex == -1 {
				startIndex = int64(i)
			}
		}
	}
	
	// If no trades in window, return empty aggregate
	if validTradeCount == 0 {
		return aggregate, 0, nil
	}
	
	// The index represents the position of the oldest trade in the current window
	// This will be used for efficient sliding window updates
	return aggregate, startIndex, nil
}

// Stop stops the maintenance service
func (s *Service) Stop() error {
	s.mutex.Lock()
	if !s.isRunning {
		s.mutex.Unlock()
		return fmt.Errorf("maintenance service is not running")
	}
	s.mutex.Unlock()
	
	log.Printf("Stopping maintenance service...")
	
	// Signal the maintenance loop to stop
	close(s.stopChan)
	
	// Wait for the maintenance loop to finish
	select {
	case <-s.doneChan:
		log.Printf("Maintenance service stopped successfully")
	case <-time.After(10 * time.Second):
		log.Printf("Warning: maintenance service stop timeout")
	}
	
	s.setRunning(false)
	return nil
}

// GetStatistics returns maintenance service statistics
func (s *Service) GetStatistics() map[string]interface{} {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	
	return map[string]interface{}{
		"is_running":         s.isRunning,
		"interval":           s.interval.String(),
		"stale_threshold":    s.staleThreshold.String(),
		"total_scans":        s.totalScans,
		"stale_tokens_found": s.staleTokensFound,
		"errors_count":       s.errorsCount,
		"last_scan":          s.lastScan,
	}
}

// IsRunning returns whether the maintenance service is currently running
func (s *Service) IsRunning() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.isRunning
}

// GetLastScanTime returns the timestamp of the last maintenance scan
func (s *Service) GetLastScanTime() time.Time {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.lastScan
}

// setRunning safely sets the running state
func (s *Service) setRunning(running bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.isRunning = running
}

// incrementErrorCount safely increments the error count
func (s *Service) incrementErrorCount() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.errorsCount++
}

// GetInterval returns the maintenance interval
func (s *Service) GetInterval() time.Duration {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.interval
}

// GetStaleThreshold returns the stale threshold
func (s *Service) GetStaleThreshold() time.Duration {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.staleThreshold
}

// UpdateConfiguration updates the maintenance service configuration
func (s *Service) UpdateConfiguration(interval, staleThreshold time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("maintenance interval must be positive, got: %v", interval)
	}
	
	if staleThreshold <= 0 {
		return fmt.Errorf("stale threshold must be positive, got: %v", staleThreshold)
	}
	
	s.mutex.Lock()
	defer s.mutex.Unlock()
	
	s.interval = interval
	s.staleThreshold = staleThreshold
	
	log.Printf("Maintenance service configuration updated - interval: %v, stale threshold: %v", 
		interval, staleThreshold)
	
	return nil
}

// ForceMaintenanceCycle forces an immediate maintenance cycle (for testing/debugging)
func (s *Service) ForceMaintenanceCycle(ctx context.Context) error {
	s.mutex.RLock()
	if !s.isRunning {
		s.mutex.RUnlock()
		return fmt.Errorf("maintenance service is not running")
	}
	s.mutex.RUnlock()
	
	log.Printf("Forcing immediate maintenance cycle")
	s.performMaintenanceCycle(ctx)
	return nil
}

// GetActiveTokenCount returns the number of active tokens from the last scan
func (s *Service) GetActiveTokenCount(ctx context.Context) (int, error) {
	activeTokens, err := s.redisManager.GetActiveTokens(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get active tokens: %w", err)
	}
	return len(activeTokens), nil
}

// ValidateTokenHealth checks the health of a specific token's data
func (s *Service) ValidateTokenHealth(ctx context.Context, tokenAddress string) (map[string]interface{}, error) {
	tokenData, err := s.redisManager.GetTokenData(ctx, tokenAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to get token data: %w", err)
	}
	
	lastUpdate, err := s.redisManager.GetLastUpdate(ctx, tokenAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to get last update: %w", err)
	}
	
	currentTime := time.Now()
	isStale := lastUpdate.IsZero() || currentTime.Sub(lastUpdate) > s.staleThreshold
	
	health := map[string]interface{}{
		"token_address":    tokenAddress,
		"trade_count":      len(tokenData.Trades),
		"last_update":      lastUpdate,
		"age":              currentTime.Sub(lastUpdate).String(),
		"is_stale":         isStale,
		"has_indices":      len(tokenData.Indices) > 0,
		"has_aggregates":   len(tokenData.Aggregates) > 0,
		"timeframes":       make(map[string]interface{}),
	}
	
	// Check each timeframe's health
	timeframes := health["timeframes"].(map[string]interface{})
	for timeframe, aggregate := range tokenData.Aggregates {
		timeframes[timeframe] = map[string]interface{}{
			"has_data":     !aggregate.IsEmpty(),
			"trade_count":  aggregate.GetTotalTradeCount(),
			"last_update":  aggregate.LastUpdate,
			"total_volume": aggregate.TotalVolume,
		}
	}
	
	return health, nil
}