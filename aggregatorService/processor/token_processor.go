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

// TokenProcessor implements per-token processing with goroutine-safe operations
type TokenProcessor struct {
	tokenAddress string
	redisManager interfaces.RedisManager
	calculator   interfaces.SlidingWindowCalculator
	
	// Trade buffering
	tradeBuffer []models.TradeData
	bufferMutex sync.RWMutex
	
	// Current state
	indices    map[string]int64
	aggregates map[string]*models.AggregateData
	lastUpdate time.Time
	stateMutex sync.RWMutex
	
	// Processing control
	processingChan chan models.TradeData
	shutdownChan   chan struct{}
	wg             sync.WaitGroup
	isRunning      bool
	runningMutex   sync.RWMutex
	
	// Panic recovery and error handling
	panicRecovery *PanicRecovery
	errorLogger   *ErrorLogger
	healthChecker *HealthChecker
	goroutineID   string
	
	// Data consistency validation
	consistencyValidator *DataConsistencyValidator
	lastValidationTime   time.Time
	validationMutex      sync.RWMutex
}

// NewTokenProcessor creates a new token processor
func NewTokenProcessor() interfaces.TokenProcessor {
	tp := &TokenProcessor{
		tradeBuffer:    make([]models.TradeData, 0),
		indices:        make(map[string]int64),
		aggregates:     make(map[string]*models.AggregateData),
		processingChan: make(chan models.TradeData, 100), // Buffer up to 100 trades
		shutdownChan:   make(chan struct{}),
	}
	
	// Initialize panic recovery and error handling
	tp.panicRecovery = NewPanicRecovery(3, 5*time.Second) // Max 3 restarts, 5 second delay
	tp.errorLogger = NewErrorLogger("TokenProcessor")
	tp.healthChecker = NewHealthChecker(10 * time.Second) // Check health every 10 seconds
	tp.goroutineID = fmt.Sprintf("token_processor_%d", time.Now().UnixNano())
	
	// Initialize data consistency validator (will be set during Initialize)
	tp.consistencyValidator = nil
	
	// Set up panic recovery callbacks
	tp.panicRecovery.SetPanicCallback(tp.handlePanic)
	tp.panicRecovery.SetRestartCallback(tp.handleRestart)
	
	return tp
}

// Initialize initializes the processor for a specific token
func (tp *TokenProcessor) Initialize(tokenAddress string, redisManager interfaces.RedisManager, calculator interfaces.SlidingWindowCalculator) error {
	tp.runningMutex.Lock()
	defer tp.runningMutex.Unlock()
	
	if tp.isRunning {
		return fmt.Errorf("processor already initialized and running")
	}
	
	tp.tokenAddress = tokenAddress
	tp.redisManager = redisManager
	tp.calculator = calculator
	tp.goroutineID = fmt.Sprintf("token_processor_%s_%d", tokenAddress, time.Now().UnixNano())
	
	// Initialize data consistency validator
	tp.consistencyValidator = NewDataConsistencyValidator(redisManager, calculator)
	
	// Load existing data from Redis
	if err := tp.loadStateFromRedis(context.Background()); err != nil {
		tp.errorLogger.LogError("initialize_load_state", err, map[string]interface{}{
			"token_address": tokenAddress,
		})
		// Continue with empty state
	}
	
	// Register with health checker
	tp.healthChecker.RegisterGoroutine(tp.goroutineID, map[string]interface{}{
		"token_address": tokenAddress,
		"component":     "TokenProcessor",
	})
	
	// Start health monitoring
	tp.healthChecker.StartHealthMonitoring(context.Background())
	
	// Start processing goroutine with panic recovery
	tp.wg.Add(1)
	tp.startProcessingWorkerWithRecovery()
	
	tp.isRunning = true
	log.Printf("TokenProcessor initialized for token %s with goroutine ID %s", tokenAddress, tp.goroutineID)
	
	return nil
}

// ProcessTrade processes a single trade (goroutine-safe)
func (tp *TokenProcessor) ProcessTrade(ctx context.Context, trade models.TradeData) error {
	tp.runningMutex.RLock()
	if !tp.isRunning {
		tp.runningMutex.RUnlock()
		return fmt.Errorf("processor not running")
	}
	tp.runningMutex.RUnlock()
	
	// Validate trade data
	if err := trade.ValidateTradeData(); err != nil {
		return fmt.Errorf("invalid trade data: %w", err)
	}
	
	// Ensure trade is for this token
	if trade.Token != tp.tokenAddress {
		return fmt.Errorf("trade token %s does not match processor token %s", trade.Token, tp.tokenAddress)
	}
	
	// Send trade to processing channel (non-blocking)
	select {
	case tp.processingChan <- trade:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		// Channel is full, process synchronously as fallback
		return tp.processTradeDirect(ctx, trade)
	}
}

// startProcessingWorkerWithRecovery starts the processing worker with panic recovery
func (tp *TokenProcessor) startProcessingWorkerWithRecovery() {
	processorInfo := map[string]interface{}{
		"token_address": tp.tokenAddress,
		"goroutine_id":  tp.goroutineID,
		"component":     "TokenProcessor",
	}
	
	tp.panicRecovery.RecoverAndRestart(tp.goroutineID, processorInfo, tp.processTradesWorker)
}

// processTradesWorker is the main processing goroutine
func (tp *TokenProcessor) processTradesWorker() {
	defer tp.wg.Done()
	
	// Update health status
	tp.healthChecker.UpdateGoroutineHealth(tp.goroutineID, true)
	
	for {
		select {
		case trade := <-tp.processingChan:
			// Update health status to show we're active
			tp.healthChecker.UpdateGoroutineHealth(tp.goroutineID, true)
			
			if err := tp.processTradeDirect(context.Background(), trade); err != nil {
				tp.errorLogger.LogError("process_trade", err, map[string]interface{}{
					"token_address": tp.tokenAddress,
					"trade_hash":    trade.TxHash,
				})
			}
			
		case <-tp.shutdownChan:
			log.Printf("Processing worker for token %s received shutdown signal", tp.tokenAddress)
			
			// Process remaining trades in channel
			for {
				select {
				case trade := <-tp.processingChan:
					if err := tp.processTradeDirect(context.Background(), trade); err != nil {
						tp.errorLogger.LogError("process_final_trade", err, map[string]interface{}{
							"token_address": tp.tokenAddress,
							"trade_hash":    trade.TxHash,
						})
					}
				default:
					// Unregister from health checker
					tp.healthChecker.UnregisterGoroutine(tp.goroutineID)
					return
				}
			}
		}
	}
}

// processTradeDirect processes a trade directly (internal method)
func (tp *TokenProcessor) processTradeDirect(ctx context.Context, trade models.TradeData) error {
	tp.stateMutex.Lock()
	defer tp.stateMutex.Unlock()
	
	// Add trade to buffer
	tp.bufferMutex.Lock()
	tp.tradeBuffer = append(tp.tradeBuffer, trade)
	currentTrades := make([]models.TradeData, len(tp.tradeBuffer))
	copy(currentTrades, tp.tradeBuffer)
	tp.bufferMutex.Unlock()
	
	// Update sliding windows
	newIndices, newAggregates, err := tp.calculator.UpdateWindows(
		ctx,
		currentTrades[:len(currentTrades)-1], // All trades except the new one
		tp.indices,
		tp.aggregates,
		trade,
	)
	if err != nil {
		return fmt.Errorf("failed to update sliding windows: %w", err)
	}
	
	// Update internal state
	tp.indices = newIndices
	tp.aggregates = newAggregates
	tp.lastUpdate = time.Now()
	
	// Persist to Redis atomically
	if err := tp.persistStateToRedis(ctx); err != nil {
		log.Printf("Warning: failed to persist state to Redis for token %s: %v", tp.tokenAddress, err)
		// Don't return error as the in-memory state is updated
	}
	
	return nil
}

// GetAggregates returns current aggregation data (goroutine-safe)
func (tp *TokenProcessor) GetAggregates(ctx context.Context) (map[string]*models.AggregateData, error) {
	tp.stateMutex.RLock()
	defer tp.stateMutex.RUnlock()
	
	// Create a deep copy to prevent external modification
	result := make(map[string]*models.AggregateData)
	for timeframe, aggregate := range tp.aggregates {
		result[timeframe] = aggregate.Clone()
	}
	
	return result, nil
}

// PerformManualAggregation performs manual aggregation for maintenance
func (tp *TokenProcessor) PerformManualAggregation(ctx context.Context) error {
	tp.stateMutex.Lock()
	defer tp.stateMutex.Unlock()
	
	// Load current trades from Redis
	tokenData, err := tp.redisManager.GetTokenData(ctx, tp.tokenAddress)
	if err != nil {
		return fmt.Errorf("failed to load token data from Redis: %w", err)
	}
	
	// Update trade buffer with Redis data
	tp.bufferMutex.Lock()
	tp.tradeBuffer = tokenData.Trades
	tp.bufferMutex.Unlock()
	
	// Recalculate all windows from scratch
	currentTime := time.Now()
	
	// Get all time window names from calculator
	windowNames := tp.calculator.GetAllTimeWindowNames()
	
	for _, windowName := range windowNames {
		// Get time window configuration
		timeWindow, exists := tp.calculator.GetTimeWindowByName(windowName)
		if !exists {
			continue
		}
		
		// Recalculate window
		aggregate, index, err := tp.calculator.CalculateWindow(
			ctx,
			tp.tradeBuffer,
			timeWindow,
			currentTime,
		)
		if err != nil {
			log.Printf("Warning: failed to recalculate window %s for token %s: %v", windowName, tp.tokenAddress, err)
			continue
		}
		
		// Update state
		tp.indices[windowName] = index
		tp.aggregates[windowName] = aggregate
	}
	
	tp.lastUpdate = currentTime
	
	// Persist updated state to Redis
	if err := tp.persistStateToRedis(ctx); err != nil {
		return fmt.Errorf("failed to persist manual aggregation results: %w", err)
	}
	
	log.Printf("Manual aggregation completed for token %s", tp.tokenAddress)
	return nil
}

// Shutdown gracefully shuts down the processor
func (tp *TokenProcessor) Shutdown(ctx context.Context) error {
	tp.runningMutex.Lock()
	defer tp.runningMutex.Unlock()
	
	if !tp.isRunning {
		return nil // Already shut down
	}
	
	// Signal shutdown
	close(tp.shutdownChan)
	
	// Wait for processing goroutine to finish with timeout
	done := make(chan struct{})
	go func() {
		tp.wg.Wait()
		close(done)
	}()
	
	select {
	case <-done:
		// Graceful shutdown completed
	case <-ctx.Done():
		// Timeout occurred
		log.Printf("Warning: TokenProcessor shutdown timed out for token %s", tp.tokenAddress)
	case <-time.After(5 * time.Second):
		// Fallback timeout
		log.Printf("Warning: TokenProcessor shutdown timed out (5s) for token %s", tp.tokenAddress)
	}
	
	tp.isRunning = false
	log.Printf("TokenProcessor shut down for token %s", tp.tokenAddress)
	
	return nil
}

// loadStateFromRedis loads processor state from Redis
func (tp *TokenProcessor) loadStateFromRedis(ctx context.Context) error {
	tokenData, err := tp.redisManager.GetTokenData(ctx, tp.tokenAddress)
	if err != nil {
		return fmt.Errorf("failed to get token data: %w", err)
	}
	
	// Update internal state
	tp.bufferMutex.Lock()
	tp.tradeBuffer = tokenData.Trades
	tp.bufferMutex.Unlock()
	
	tp.indices = tokenData.Indices
	tp.aggregates = tokenData.Aggregates
	tp.lastUpdate = tokenData.LastUpdate
	
	return nil
}

// persistStateToRedis persists processor state to Redis
func (tp *TokenProcessor) persistStateToRedis(ctx context.Context) error {
	// Create token data structure
	tp.bufferMutex.RLock()
	tokenData := &models.TokenData{
		TokenAddress: tp.tokenAddress,
		Trades:       make([]models.TradeData, len(tp.tradeBuffer)),
		Indices:      make(map[string]int64),
		Aggregates:   make(map[string]*models.AggregateData),
		LastUpdate:   tp.lastUpdate,
	}
	
	// Copy trade buffer
	copy(tokenData.Trades, tp.tradeBuffer)
	tp.bufferMutex.RUnlock()
	
	// Copy indices and aggregates
	for k, v := range tp.indices {
		tokenData.Indices[k] = v
	}
	for k, v := range tp.aggregates {
		tokenData.Aggregates[k] = v.Clone()
	}
	
	// Update token data in Redis
	return tp.redisManager.UpdateTokenData(ctx, tp.tokenAddress, tokenData)
}

// GetTokenAddress returns the token address this processor handles
func (tp *TokenProcessor) GetTokenAddress() string {
	return tp.tokenAddress
}

// GetLastUpdate returns the last update timestamp
func (tp *TokenProcessor) GetLastUpdate() time.Time {
	tp.stateMutex.RLock()
	defer tp.stateMutex.RUnlock()
	return tp.lastUpdate
}

// GetTradeCount returns the current number of trades in buffer
func (tp *TokenProcessor) GetTradeCount() int {
	tp.bufferMutex.RLock()
	defer tp.bufferMutex.RUnlock()
	return len(tp.tradeBuffer)
}

// IsRunning returns whether the processor is currently running
func (tp *TokenProcessor) IsRunning() bool {
	tp.runningMutex.RLock()
	defer tp.runningMutex.RUnlock()
	return tp.isRunning
}

// GetProcessingChannelLength returns the current length of the processing channel
func (tp *TokenProcessor) GetProcessingChannelLength() int {
	return len(tp.processingChan)
}

// FlushTradeBuffer forces processing of all buffered trades
func (tp *TokenProcessor) FlushTradeBuffer(ctx context.Context) error {
	tp.runningMutex.RLock()
	if !tp.isRunning {
		tp.runningMutex.RUnlock()
		return fmt.Errorf("processor not running")
	}
	tp.runningMutex.RUnlock()
	
	// Wait for processing channel to be empty
	for len(tp.processingChan) > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
			// Continue waiting
		}
	}
	
	return nil
}

// GetCurrentIndices returns a copy of current indices
func (tp *TokenProcessor) GetCurrentIndices() map[string]int64 {
	tp.stateMutex.RLock()
	defer tp.stateMutex.RUnlock()
	
	result := make(map[string]int64)
	for k, v := range tp.indices {
		result[k] = v
	}
	return result
}

// UpdateLastUpdateTimestamp updates the last update timestamp
func (tp *TokenProcessor) UpdateLastUpdateTimestamp(ctx context.Context) error {
	tp.stateMutex.Lock()
	tp.lastUpdate = time.Now()
	tp.stateMutex.Unlock()
	
	// Update in Redis
	return tp.redisManager.UpdateLastUpdate(ctx, tp.tokenAddress, tp.lastUpdate)
}

// ValidateState validates the current processor state
func (tp *TokenProcessor) ValidateState() error {
	tp.stateMutex.RLock()
	defer tp.stateMutex.RUnlock()
	
	// Validate aggregates
	for timeframe, aggregate := range tp.aggregates {
		if aggregate == nil {
			return fmt.Errorf("nil aggregate for timeframe %s", timeframe)
		}
		if err := aggregate.ValidateAggregateData(); err != nil {
			return fmt.Errorf("invalid aggregate for timeframe %s: %w", timeframe, err)
		}
	}
	
	// Validate indices
	for timeframe, index := range tp.indices {
		if index < 0 {
			return fmt.Errorf("negative index for timeframe %s: %d", timeframe, index)
		}
	}
	
	return nil
}

// GetMemoryUsage returns approximate memory usage statistics
func (tp *TokenProcessor) GetMemoryUsage() map[string]int {
	tp.bufferMutex.RLock()
	tradeCount := len(tp.tradeBuffer)
	tp.bufferMutex.RUnlock()
	
	tp.stateMutex.RLock()
	indicesCount := len(tp.indices)
	aggregatesCount := len(tp.aggregates)
	tp.stateMutex.RUnlock()
	
	return map[string]int{
		"trade_count":      tradeCount,
		"indices_count":    indicesCount,
		"aggregates_count": aggregatesCount,
		"channel_length":   len(tp.processingChan),
	}
}

// handlePanic handles panic events from the panic recovery system
func (tp *TokenProcessor) handlePanic(panicInfo *PanicInfo) {
	// Record panic in health checker
	tp.healthChecker.RecordPanic(tp.goroutineID)
	
	// Log detailed panic information
	tp.errorLogger.LogPanic("process_trades_worker", panicInfo.PanicValue, map[string]interface{}{
		"token_address":  tp.tokenAddress,
		"goroutine_id":   panicInfo.GoroutineID,
		"restart_count":  panicInfo.RestartCount,
		"timestamp":      panicInfo.Timestamp,
		"processor_info": panicInfo.ProcessorInfo,
	})
	
	// Try to save current state to Redis before restart
	if err := tp.saveStateOnPanic(); err != nil {
		tp.errorLogger.LogError("save_state_on_panic", err, map[string]interface{}{
			"token_address": tp.tokenAddress,
		})
	}
}

// handleRestart handles goroutine restart events
func (tp *TokenProcessor) handleRestart(goroutineID string, attempt int) error {
	// Record restart in health checker
	tp.healthChecker.RecordRestart(goroutineID)
	
	// Log restart attempt
	tp.errorLogger.LogRestart(goroutineID, attempt, map[string]interface{}{
		"token_address": tp.tokenAddress,
	})
	
	// Validate processor state before restart
	if err := tp.ValidateState(); err != nil {
		tp.errorLogger.LogError("validate_state_on_restart", err, map[string]interface{}{
			"token_address": tp.tokenAddress,
			"attempt":       attempt,
		})
		
		// Try to recover state from Redis
		if err := tp.recoverStateFromRedis(); err != nil {
			tp.errorLogger.LogError("recover_state_from_redis", err, map[string]interface{}{
				"token_address": tp.tokenAddress,
			})
			return fmt.Errorf("failed to recover state from Redis: %w", err)
		}
	}
	
	// Recreate processing channel if needed
	if tp.processingChan == nil {
		tp.processingChan = make(chan models.TradeData, 100)
	}
	
	// Recreate shutdown channel if needed
	if tp.shutdownChan == nil {
		tp.shutdownChan = make(chan struct{})
	}
	
	// Update goroutine ID for the new instance
	tp.goroutineID = fmt.Sprintf("token_processor_%s_%d", tp.tokenAddress, time.Now().UnixNano())
	
	// Re-register with health checker
	tp.healthChecker.RegisterGoroutine(tp.goroutineID, map[string]interface{}{
		"token_address": tp.tokenAddress,
		"component":     "TokenProcessor",
		"restart_count": attempt,
	})
	
	// Increment wait group for the new goroutine
	tp.wg.Add(1)
	
	return nil
}

// saveStateOnPanic attempts to save the current state when a panic occurs
func (tp *TokenProcessor) saveStateOnPanic() error {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic occurred while saving state on panic for token %s: %v", tp.tokenAddress, r)
		}
	}()
	
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	return tp.persistStateToRedis(ctx)
}

// recoverStateFromRedis attempts to recover state from Redis after a restart
func (tp *TokenProcessor) recoverStateFromRedis() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	// Load state from Redis
	if err := tp.loadStateFromRedis(ctx); err != nil {
		// If loading fails, initialize with empty state
		tp.stateMutex.Lock()
		tp.indices = make(map[string]int64)
		tp.aggregates = make(map[string]*models.AggregateData)
		tp.lastUpdate = time.Now()
		tp.stateMutex.Unlock()
		
		tp.bufferMutex.Lock()
		tp.tradeBuffer = make([]models.TradeData, 0)
		tp.bufferMutex.Unlock()
		
		log.Printf("Initialized empty state for token %s after recovery failure", tp.tokenAddress)
	}
	
	return nil
}

// GetPanicRecoveryStats returns panic recovery statistics
func (tp *TokenProcessor) GetPanicRecoveryStats() map[string]interface{} {
	restartCounts := tp.panicRecovery.GetAllRestartCounts()
	health, exists := tp.healthChecker.GetGoroutineHealth(tp.goroutineID)
	
	stats := map[string]interface{}{
		"goroutine_id":    tp.goroutineID,
		"restart_counts":  restartCounts,
		"health_exists":   exists,
	}
	
	if exists {
		stats["health"] = map[string]interface{}{
			"last_seen":     health.LastSeen,
			"panic_count":   health.PanicCount,
			"restart_count": health.RestartCount,
			"is_healthy":    health.IsHealthy,
		}
	}
	
	return stats
}

// ResetPanicRecovery resets panic recovery statistics
func (tp *TokenProcessor) ResetPanicRecovery() {
	tp.panicRecovery.ResetRestartCount(tp.goroutineID)
	tp.healthChecker.UpdateGoroutineHealth(tp.goroutineID, true)
}

// ValidateDataConsistency performs data consistency validation
func (tp *TokenProcessor) ValidateDataConsistency(ctx context.Context) (*ValidationResult, error) {
	if tp.consistencyValidator == nil {
		return nil, fmt.Errorf("consistency validator not initialized")
	}
	
	tp.validationMutex.Lock()
	tp.lastValidationTime = time.Now()
	tp.validationMutex.Unlock()
	
	return tp.consistencyValidator.ValidateTokenData(ctx, tp.tokenAddress)
}

// IsValidationNeeded checks if data consistency validation is needed
func (tp *TokenProcessor) IsValidationNeeded() bool {
	if tp.consistencyValidator == nil {
		return false
	}
	
	tp.validationMutex.RLock()
	lastValidation := tp.lastValidationTime
	tp.validationMutex.RUnlock()
	
	// Validate every 5 minutes or if never validated
	return lastValidation.IsZero() || time.Since(lastValidation) > 5*time.Minute
}

// GetLastValidationTime returns the last validation time
func (tp *TokenProcessor) GetLastValidationTime() time.Time {
	tp.validationMutex.RLock()
	defer tp.validationMutex.RUnlock()
	return tp.lastValidationTime
}

// RecoverFromRedis recovers processor state from Redis
func (tp *TokenProcessor) RecoverFromRedis(ctx context.Context) error {
	if tp.consistencyValidator == nil {
		return fmt.Errorf("consistency validator not initialized")
	}
	
	// Validate and recover data
	result, err := tp.consistencyValidator.ValidateTokenData(ctx, tp.tokenAddress)
	if err != nil {
		return fmt.Errorf("failed to validate data during recovery: %w", err)
	}
	
	// Log recovery results
	if !result.IsConsistent {
		tp.errorLogger.LogError("data_inconsistency_detected", fmt.Errorf("data inconsistency detected"), map[string]interface{}{
			"token_address":    tp.tokenAddress,
			"issues_count":     len(result.Issues),
			"recovery_applied": result.RecoveryApplied,
		})
	}
	
	// Reload state from Redis after potential recovery
	return tp.loadStateFromRedis(ctx)
}