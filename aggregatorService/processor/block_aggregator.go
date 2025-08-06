package processor

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/cryptoKingdom88/memeCoinBackend/shared/packet"
	"aggregatorService/interfaces"
	"aggregatorService/models"
)

// BlockAggregator implements initial trade grouping by token address
type BlockAggregator struct {
	redisManager interfaces.RedisManager
	calculator   interfaces.SlidingWindowCalculator
	workerPool   interfaces.WorkerPool
	
	// Token processor management
	tokenProcessors map[string]interfaces.TokenProcessor
	processorMutex  sync.RWMutex
	
	// Configuration
	maxTokenProcessors int
	
	// Lifecycle management
	isInitialized bool
	isShutdown    bool
	shutdownMutex sync.RWMutex
}

// NewBlockAggregator creates a new block aggregator
func NewBlockAggregator() interfaces.BlockAggregator {
	return &BlockAggregator{
		tokenProcessors:    make(map[string]interfaces.TokenProcessor),
		maxTokenProcessors: 1000, // Default limit
	}
}

// Initialize initializes the block aggregator
func (ba *BlockAggregator) Initialize(redisManager interfaces.RedisManager, calculator interfaces.SlidingWindowCalculator, workerPool interfaces.WorkerPool) error {
	ba.shutdownMutex.Lock()
	defer ba.shutdownMutex.Unlock()
	
	if ba.isInitialized {
		return fmt.Errorf("block aggregator already initialized")
	}
	
	if redisManager == nil {
		return fmt.Errorf("redis manager cannot be nil")
	}
	if calculator == nil {
		return fmt.Errorf("calculator cannot be nil")
	}
	if workerPool == nil {
		return fmt.Errorf("worker pool cannot be nil")
	}
	
	ba.redisManager = redisManager
	ba.calculator = calculator
	ba.workerPool = workerPool
	ba.isInitialized = true
	
	log.Printf("BlockAggregator initialized with max token processors: %d", ba.maxTokenProcessors)
	return nil
}

// ProcessTrades processes multiple trades grouped by token address
func (ba *BlockAggregator) ProcessTrades(ctx context.Context, trades []packet.TokenTradeHistory) error {
	ba.shutdownMutex.RLock()
	if ba.isShutdown {
		ba.shutdownMutex.RUnlock()
		return fmt.Errorf("block aggregator is shut down")
	}
	if !ba.isInitialized {
		ba.shutdownMutex.RUnlock()
		return fmt.Errorf("block aggregator not initialized")
	}
	ba.shutdownMutex.RUnlock()
	
	if len(trades) == 0 {
		return nil // Nothing to process
	}
	
	// Group trades by token address
	tradeGroups := ba.groupTradesByToken(trades)
	
	// Process each token group
	var wg sync.WaitGroup
	errorChan := make(chan error, len(tradeGroups))
	
	for tokenAddress, tokenTrades := range tradeGroups {
		wg.Add(1)
		
		// Submit processing job to worker pool
		err := ba.workerPool.Submit(func() {
			defer wg.Done()
			
			if err := ba.processTokenTrades(ctx, tokenAddress, tokenTrades); err != nil {
				errorChan <- fmt.Errorf("failed to process trades for token %s: %w", tokenAddress, err)
			}
		})
		
		if err != nil {
			wg.Done()
			errorChan <- fmt.Errorf("failed to submit job to worker pool: %w", err)
		}
	}
	
	// Wait for all processing to complete
	wg.Wait()
	close(errorChan)
	
	// Collect any errors
	var errors []error
	for err := range errorChan {
		errors = append(errors, err)
	}
	
	if len(errors) > 0 {
		// Log all errors but return the first one
		for _, err := range errors {
			log.Printf("Trade processing error: %v", err)
		}
		return errors[0]
	}
	
	log.Printf("Successfully processed %d trades for %d tokens", len(trades), len(tradeGroups))
	return nil
}

// GetTokenProcessor gets or creates a token processor for the given token address
func (ba *BlockAggregator) GetTokenProcessor(tokenAddress string) (interfaces.TokenProcessor, error) {
	ba.shutdownMutex.RLock()
	if ba.isShutdown {
		ba.shutdownMutex.RUnlock()
		return nil, fmt.Errorf("block aggregator is shut down")
	}
	if !ba.isInitialized {
		ba.shutdownMutex.RUnlock()
		return nil, fmt.Errorf("block aggregator not initialized")
	}
	ba.shutdownMutex.RUnlock()
	
	return ba.getOrCreateProcessor(tokenAddress)
}

// Shutdown gracefully shuts down all processors
func (ba *BlockAggregator) Shutdown(ctx context.Context) error {
	ba.shutdownMutex.Lock()
	defer ba.shutdownMutex.Unlock()
	
	if ba.isShutdown {
		return nil // Already shut down
	}
	
	log.Printf("Shutting down BlockAggregator with %d token processors", len(ba.tokenProcessors))
	
	// Shutdown all token processors
	var wg sync.WaitGroup
	errorChan := make(chan error, len(ba.tokenProcessors))
	
	ba.processorMutex.RLock()
	for tokenAddress, processor := range ba.tokenProcessors {
		wg.Add(1)
		go func(addr string, proc interfaces.TokenProcessor) {
			defer wg.Done()
			if err := proc.Shutdown(ctx); err != nil {
				errorChan <- fmt.Errorf("failed to shutdown processor for token %s: %w", addr, err)
			}
		}(tokenAddress, processor)
	}
	ba.processorMutex.RUnlock()
	
	// Wait for all shutdowns to complete with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	
	select {
	case <-done:
		// All processors shut down successfully
	case <-ctx.Done():
		log.Printf("Warning: BlockAggregator shutdown timed out due to context cancellation")
	case <-time.After(10 * time.Second):
		log.Printf("Warning: BlockAggregator shutdown timed out after 10 seconds")
	}
	
	close(errorChan)
	
	// Clear processor map
	ba.processorMutex.Lock()
	ba.tokenProcessors = make(map[string]interfaces.TokenProcessor)
	ba.processorMutex.Unlock()
	
	ba.isShutdown = true
	
	// Collect any errors
	var errors []error
	for err := range errorChan {
		errors = append(errors, err)
		log.Printf("Processor shutdown error: %v", err)
	}
	
	log.Printf("BlockAggregator shutdown completed")
	
	if len(errors) > 0 {
		return errors[0] // Return first error
	}
	return nil
}

// groupTradesByToken groups trades by token address for efficient processing
func (ba *BlockAggregator) groupTradesByToken(trades []packet.TokenTradeHistory) map[string][]packet.TokenTradeHistory {
	groups := make(map[string][]packet.TokenTradeHistory)
	
	for _, trade := range trades {
		tokenAddress := trade.Token
		if tokenAddress == "" {
			log.Printf("Warning: skipping trade with empty token address")
			continue
		}
		
		groups[tokenAddress] = append(groups[tokenAddress], trade)
	}
	
	return groups
}

// processTokenTrades processes all trades for a specific token
func (ba *BlockAggregator) processTokenTrades(ctx context.Context, tokenAddress string, trades []packet.TokenTradeHistory) error {
	// Get or create processor for this token
	processor, err := ba.getOrCreateProcessor(tokenAddress)
	if err != nil {
		return fmt.Errorf("failed to get processor for token %s: %w", tokenAddress, err)
	}
	
	// Convert and process each trade
	for _, trade := range trades {
		// Convert from packet format to internal format
		var tradeData models.TradeData
		if err := tradeData.FromTokenTradeHistory(trade); err != nil {
			log.Printf("Warning: failed to convert trade for token %s: %v", tokenAddress, err)
			continue
		}
		
		// Process the trade
		if err := processor.ProcessTrade(ctx, tradeData); err != nil {
			log.Printf("Warning: failed to process trade for token %s: %v", tokenAddress, err)
			continue
		}
	}
	
	return nil
}

// getOrCreateProcessor gets an existing processor or creates a new one
func (ba *BlockAggregator) getOrCreateProcessor(tokenAddress string) (interfaces.TokenProcessor, error) {
	// First, try to get existing processor with read lock
	ba.processorMutex.RLock()
	if processor, exists := ba.tokenProcessors[tokenAddress]; exists {
		ba.processorMutex.RUnlock()
		return processor, nil
	}
	ba.processorMutex.RUnlock()
	
	// Need to create new processor, acquire write lock
	ba.processorMutex.Lock()
	defer ba.processorMutex.Unlock()
	
	// Double-check in case another goroutine created it
	if processor, exists := ba.tokenProcessors[tokenAddress]; exists {
		return processor, nil
	}
	
	// Check if we've reached the maximum number of processors
	if len(ba.tokenProcessors) >= ba.maxTokenProcessors {
		return nil, fmt.Errorf("maximum number of token processors (%d) reached", ba.maxTokenProcessors)
	}
	
	// Create new processor
	processor := NewTokenProcessor()
	if err := processor.Initialize(tokenAddress, ba.redisManager, ba.calculator); err != nil {
		return nil, fmt.Errorf("failed to initialize processor for token %s: %w", tokenAddress, err)
	}
	
	// Store processor
	ba.tokenProcessors[tokenAddress] = processor
	
	log.Printf("Created new TokenProcessor for token %s (total processors: %d)", tokenAddress, len(ba.tokenProcessors))
	return processor, nil
}

// GetActiveTokenCount returns the number of active token processors
func (ba *BlockAggregator) GetActiveTokenCount() int {
	ba.processorMutex.RLock()
	defer ba.processorMutex.RUnlock()
	return len(ba.tokenProcessors)
}

// GetActiveTokens returns a list of active token addresses
func (ba *BlockAggregator) GetActiveTokens() []string {
	ba.processorMutex.RLock()
	defer ba.processorMutex.RUnlock()
	
	tokens := make([]string, 0, len(ba.tokenProcessors))
	for tokenAddress := range ba.tokenProcessors {
		tokens = append(tokens, tokenAddress)
	}
	return tokens
}

// SetMaxTokenProcessors sets the maximum number of token processors
func (ba *BlockAggregator) SetMaxTokenProcessors(max int) error {
	if max <= 0 {
		return fmt.Errorf("max token processors must be positive, got: %d", max)
	}
	
	ba.processorMutex.Lock()
	defer ba.processorMutex.Unlock()
	
	if len(ba.tokenProcessors) > max {
		return fmt.Errorf("cannot set max to %d, currently have %d active processors", max, len(ba.tokenProcessors))
	}
	
	ba.maxTokenProcessors = max
	return nil
}

// GetMaxTokenProcessors returns the maximum number of token processors
func (ba *BlockAggregator) GetMaxTokenProcessors() int {
	ba.processorMutex.RLock()
	defer ba.processorMutex.RUnlock()
	return ba.maxTokenProcessors
}

// RemoveInactiveProcessors removes processors that haven't been updated recently
func (ba *BlockAggregator) RemoveInactiveProcessors(ctx context.Context, inactiveThreshold time.Duration) error {
	ba.processorMutex.Lock()
	defer ba.processorMutex.Unlock()
	
	currentTime := time.Now()
	var toRemove []string
	
	// Find inactive processors
	for tokenAddress, processor := range ba.tokenProcessors {
		// Check if processor has a method to get last update time
		if tp, ok := processor.(*TokenProcessor); ok {
			lastUpdate := tp.GetLastUpdate()
			if currentTime.Sub(lastUpdate) > inactiveThreshold {
				toRemove = append(toRemove, tokenAddress)
			}
		}
	}
	
	// Remove inactive processors
	for _, tokenAddress := range toRemove {
		processor := ba.tokenProcessors[tokenAddress]
		
		// Shutdown the processor
		if err := processor.Shutdown(ctx); err != nil {
			log.Printf("Warning: failed to shutdown inactive processor for token %s: %v", tokenAddress, err)
		}
		
		// Remove from map
		delete(ba.tokenProcessors, tokenAddress)
		log.Printf("Removed inactive TokenProcessor for token %s", tokenAddress)
	}
	
	if len(toRemove) > 0 {
		log.Printf("Removed %d inactive processors (threshold: %v)", len(toRemove), inactiveThreshold)
	}
	
	return nil
}

// GetProcessorStats returns statistics about all processors
func (ba *BlockAggregator) GetProcessorStats() map[string]interface{} {
	ba.processorMutex.RLock()
	defer ba.processorMutex.RUnlock()
	
	stats := map[string]interface{}{
		"total_processors":     len(ba.tokenProcessors),
		"max_processors":       ba.maxTokenProcessors,
		"processor_utilization": float64(len(ba.tokenProcessors)) / float64(ba.maxTokenProcessors),
	}
	
	// Collect per-processor stats
	processorStats := make(map[string]interface{})
	for tokenAddress, processor := range ba.tokenProcessors {
		if tp, ok := processor.(*TokenProcessor); ok {
			processorStats[tokenAddress] = map[string]interface{}{
				"last_update":    tp.GetLastUpdate(),
				"trade_count":    tp.GetTradeCount(),
				"is_running":     tp.IsRunning(),
				"channel_length": tp.GetProcessingChannelLength(),
			}
		}
	}
	stats["processors"] = processorStats
	
	return stats
}

// FlushAllProcessors forces all processors to flush their trade buffers
func (ba *BlockAggregator) FlushAllProcessors(ctx context.Context) error {
	ba.processorMutex.RLock()
	processors := make([]interfaces.TokenProcessor, 0, len(ba.tokenProcessors))
	for _, processor := range ba.tokenProcessors {
		processors = append(processors, processor)
	}
	ba.processorMutex.RUnlock()
	
	var wg sync.WaitGroup
	errorChan := make(chan error, len(processors))
	
	for _, processor := range processors {
		wg.Add(1)
		go func(proc interfaces.TokenProcessor) {
			defer wg.Done()
			if tp, ok := proc.(*TokenProcessor); ok {
				if err := tp.FlushTradeBuffer(ctx); err != nil {
					errorChan <- err
				}
			}
		}(processor)
	}
	
	wg.Wait()
	close(errorChan)
	
	// Collect errors
	var errors []error
	for err := range errorChan {
		errors = append(errors, err)
	}
	
	if len(errors) > 0 {
		return fmt.Errorf("failed to flush %d processors: %v", len(errors), errors[0])
	}
	
	return nil
}

// ValidateAllProcessors validates the state of all processors
func (ba *BlockAggregator) ValidateAllProcessors() error {
	ba.processorMutex.RLock()
	defer ba.processorMutex.RUnlock()
	
	for tokenAddress, processor := range ba.tokenProcessors {
		if tp, ok := processor.(*TokenProcessor); ok {
			if err := tp.ValidateState(); err != nil {
				return fmt.Errorf("processor validation failed for token %s: %w", tokenAddress, err)
			}
		}
	}
	
	return nil
}

// GetMemoryUsage returns memory usage statistics for all processors
func (ba *BlockAggregator) GetMemoryUsage() map[string]interface{} {
	ba.processorMutex.RLock()
	defer ba.processorMutex.RUnlock()
	
	totalStats := map[string]int{
		"total_trade_count":      0,
		"total_indices_count":    0,
		"total_aggregates_count": 0,
		"total_channel_length":   0,
	}
	
	processorUsage := make(map[string]map[string]int)
	
	for tokenAddress, processor := range ba.tokenProcessors {
		if tp, ok := processor.(*TokenProcessor); ok {
			usage := tp.GetMemoryUsage()
			processorUsage[tokenAddress] = usage
			
			totalStats["total_trade_count"] += usage["trade_count"]
			totalStats["total_indices_count"] += usage["indices_count"]
			totalStats["total_aggregates_count"] += usage["aggregates_count"]
			totalStats["total_channel_length"] += usage["channel_length"]
		}
	}
	
	return map[string]interface{}{
		"total_stats":     totalStats,
		"processor_usage": processorUsage,
		"processor_count": len(ba.tokenProcessors),
	}
}

// IsInitialized returns whether the aggregator is initialized
func (ba *BlockAggregator) IsInitialized() bool {
	ba.shutdownMutex.RLock()
	defer ba.shutdownMutex.RUnlock()
	return ba.isInitialized
}

// IsShutdown returns whether the aggregator is shut down
func (ba *BlockAggregator) IsShutdown() bool {
	ba.shutdownMutex.RLock()
	defer ba.shutdownMutex.RUnlock()
	return ba.isShutdown
}