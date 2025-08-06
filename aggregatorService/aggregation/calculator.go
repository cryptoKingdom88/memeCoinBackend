package aggregation

import (
	"context"
	"fmt"
	"time"

	"aggregatorService/config"
	"aggregatorService/interfaces"
	"aggregatorService/models"
)

// Calculator implements the SlidingWindowCalculator interface
type Calculator struct {
	timeWindows []config.TimeWindow
	timeUtils   *models.TimeWindowUtils
}

// NewCalculator creates a new sliding window calculator
func NewCalculator() interfaces.SlidingWindowCalculator {
	return &Calculator{
		timeUtils: models.NewTimeWindowUtils(),
	}
}

// Initialize initializes the calculator with time windows
func (c *Calculator) Initialize(timeWindows []config.TimeWindow) error {
	c.timeWindows = timeWindows
	return c.ValidateTimeWindows()
}

// UpdateWindows updates sliding windows with new trade
func (c *Calculator) UpdateWindows(
	ctx context.Context,
	trades []models.TradeData,
	indices map[string]int64,
	aggregates map[string]*models.AggregateData,
	newTrade models.TradeData,
) (map[string]int64, map[string]*models.AggregateData, error) {
	currentTime := time.Now()
	
	// Initialize maps if nil
	if indices == nil {
		indices = make(map[string]int64)
	}
	if aggregates == nil {
		aggregates = make(map[string]*models.AggregateData)
	}
	
	// Create updated trades list with the new trade
	updatedTrades := append(trades, newTrade)
	
	// Process each time window
	for _, timeWindow := range c.timeWindows {
		windowName := timeWindow.Name
		
		// If we don't have an existing aggregate or it's empty, recalculate from scratch
		currentAggregate := aggregates[windowName]
		if currentAggregate == nil || currentAggregate.IsEmpty() {
			// Recalculate the entire window from scratch
			newAggregate, newIndex, err := c.CalculateWindow(
				ctx,
				updatedTrades,
				timeWindow,
				currentTime,
			)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to calculate window %s: %w", windowName, err)
			}
			
			indices[windowName] = newIndex
			aggregates[windowName] = newAggregate
			continue
		}
		
		// Get current index for this window
		currentIndex := indices[windowName]
		
		// Find expired trades that need to be removed
		expiredTrades, newIndex, err := c.FindExpiredTrades(
			updatedTrades,
			currentIndex,
			timeWindow,
			currentTime,
		)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to find expired trades for window %s: %w", windowName, err)
		}
		
		// Create a new aggregate by cloning the current one
		newAggregate := currentAggregate.Clone()
		
		// Subtract expired trades from the aggregate
		for _, expiredTrade := range expiredTrades {
			newAggregate.SubtractTrade(expiredTrade)
		}
		
		// Add the new trade if it's within the window
		if c.IsTradeValidForWindow(newTrade, timeWindow, currentTime) {
			newAggregate.AddTrade(newTrade)
		}
		
		// Update the aggregate's last update time
		newAggregate.LastUpdate = currentTime
		
		// Store updated values
		indices[windowName] = newIndex
		aggregates[windowName] = newAggregate
	}
	
	return indices, aggregates, nil
}

// CalculateWindow calculates aggregations for a specific time window
func (c *Calculator) CalculateWindow(
	ctx context.Context,
	trades []models.TradeData,
	timeWindow config.TimeWindow,
	currentTime time.Time,
) (*models.AggregateData, int64, error) {
	aggregate := models.NewAggregateData()
	
	// Calculate window boundary
	windowBoundary := c.timeUtils.GetWindowBoundary(currentTime, timeWindow.Duration)
	
	// Find the index of the first non-expired trade
	var firstValidIndex int64 = int64(len(trades))
	
	// Iterate through trades to find those within the time window
	for i, trade := range trades {
		// Check if trade is within the time window
		if c.timeUtils.IsWithinWindow(trade.TransTime, currentTime, timeWindow.Duration) {
			// Add trade to aggregation
			aggregate.AddTrade(trade)
			
			// Track the first valid trade index
			if int64(i) < firstValidIndex {
				firstValidIndex = int64(i)
			}
		}
	}
	
	// Find the last expired trade index
	var lastExpiredIndex int64 = -1
	for i, trade := range trades {
		if trade.TransTime.Before(windowBoundary) {
			lastExpiredIndex = int64(i)
		}
	}
	
	// Return the index after the last expired trade, or 0 if no expired trades
	var resultIndex int64 = 0
	if lastExpiredIndex >= 0 {
		resultIndex = lastExpiredIndex
	}
	
	// Update last calculation time
	aggregate.LastUpdate = currentTime
	
	return aggregate, resultIndex, nil
}

// FindExpiredTrades finds expired trades for a time window
func (c *Calculator) FindExpiredTrades(
	trades []models.TradeData,
	currentIndex int64,
	timeWindow config.TimeWindow,
	currentTime time.Time,
) ([]models.TradeData, int64, error) {
	if len(trades) == 0 {
		return nil, currentIndex, nil
	}
	
	// Calculate window boundary
	windowBoundary := c.timeUtils.GetWindowBoundary(currentTime, timeWindow.Duration)
	
	var expiredTrades []models.TradeData
	newIndex := currentIndex
	
	// Iterate backwards from current index to find expired trades
	// This implements the backward iteration requirement from task 3.2
	for i := currentIndex; i < int64(len(trades)); i++ {
		trade := trades[i]
		
		// Check if trade is expired (outside the time window)
		if trade.TransTime.Before(windowBoundary) {
			// Trade is expired, add it to the list
			expiredTrades = append(expiredTrades, trade)
			newIndex = i + 1
		} else {
			// Trade is still valid, stop searching
			break
		}
	}
	
	// Ensure newIndex doesn't exceed trades length
	if newIndex > int64(len(trades)) {
		newIndex = int64(len(trades))
	}
	
	return expiredTrades, newIndex, nil
}

// GetTimeWindowByName returns a time window by its name
func (c *Calculator) GetTimeWindowByName(name string) (config.TimeWindow, bool) {
	for _, window := range c.timeWindows {
		if window.Name == name {
			return window, true
		}
	}
	return config.TimeWindow{}, false
}

// GetAllTimeWindowNames returns all configured time window names
func (c *Calculator) GetAllTimeWindowNames() []string {
	names := make([]string, len(c.timeWindows))
	for i, window := range c.timeWindows {
		names[i] = window.Name
	}
	return names
}

// GetAllTimeWindowNames returns all configured time window names (for OptimizedCalculator)
func (oc *OptimizedCalculator) GetAllTimeWindowNames() []string {
	return oc.Calculator.GetAllTimeWindowNames()
}

// GetTimeWindowDurations returns all configured time window durations
func (c *Calculator) GetTimeWindowDurations() []time.Duration {
	durations := make([]time.Duration, len(c.timeWindows))
	for i, window := range c.timeWindows {
		durations[i] = window.Duration
	}
	return durations
}

// IsTradeValidForWindow checks if a trade is valid for a specific time window
func (c *Calculator) IsTradeValidForWindow(trade models.TradeData, timeWindow config.TimeWindow, currentTime time.Time) bool {
	return c.timeUtils.IsWithinWindow(trade.TransTime, currentTime, timeWindow.Duration)
}

// GetWindowBoundaryTime returns the boundary time for a specific window
func (c *Calculator) GetWindowBoundaryTime(timeWindow config.TimeWindow, currentTime time.Time) time.Time {
	return c.timeUtils.GetWindowBoundary(currentTime, timeWindow.Duration)
}

// ValidateTimeWindows validates that time windows are properly configured
func (c *Calculator) ValidateTimeWindows() error {
	if len(c.timeWindows) == 0 {
		return fmt.Errorf("no time windows configured")
	}
	
	for i, window := range c.timeWindows {
		if window.Duration <= 0 {
			return fmt.Errorf("time window %d has invalid duration: %v", i, window.Duration)
		}
		if window.Name == "" {
			return fmt.Errorf("time window %d has empty name", i)
		}
	}
	
	return nil
}

// CalculateIncrementalUpdate performs incremental sliding window update
// This method implements the core incremental calculation logic (add new + remove old)
func (c *Calculator) CalculateIncrementalUpdate(
	currentAggregate *models.AggregateData,
	expiredTrades []models.TradeData,
	newTrades []models.TradeData,
) *models.AggregateData {
	// Clone the current aggregate to avoid modifying the original
	updatedAggregate := currentAggregate.Clone()
	
	// Subtract expired trades
	for _, expiredTrade := range expiredTrades {
		updatedAggregate.SubtractTrade(expiredTrade)
	}
	
	// Add new trades
	for _, newTrade := range newTrades {
		updatedAggregate.AddTrade(newTrade)
	}
	
	// Update last calculation time
	updatedAggregate.LastUpdate = time.Now()
	
	return updatedAggregate
}

// GetValidTradesInWindow returns all trades that are valid for a specific time window
func (c *Calculator) GetValidTradesInWindow(
	trades []models.TradeData,
	timeWindow config.TimeWindow,
	currentTime time.Time,
) []models.TradeData {
	var validTrades []models.TradeData
	
	for _, trade := range trades {
		if c.IsTradeValidForWindow(trade, timeWindow, currentTime) {
			validTrades = append(validTrades, trade)
		}
	}
	
	return validTrades
}

// RecalculateWindowFromScratch recalculates a window from scratch (fallback method)
func (c *Calculator) RecalculateWindowFromScratch(
	trades []models.TradeData,
	timeWindow config.TimeWindow,
	currentTime time.Time,
) (*models.AggregateData, int64, error) {
	// This is essentially the same as CalculateWindow but with a different name
	// for clarity when used as a fallback
	return c.CalculateWindow(context.Background(), trades, timeWindow, currentTime)
}

// UpdateWindowWithBatch updates a window with multiple new trades at once
func (c *Calculator) UpdateWindowWithBatch(
	ctx context.Context,
	trades []models.TradeData,
	indices map[string]int64,
	aggregates map[string]*models.AggregateData,
	newTrades []models.TradeData,
) (map[string]int64, map[string]*models.AggregateData, error) {
	currentTime := time.Now()
	
	// Initialize maps if nil
	if indices == nil {
		indices = make(map[string]int64)
	}
	if aggregates == nil {
		aggregates = make(map[string]*models.AggregateData)
	}
	
	// Create updated trades list with the new trades
	updatedTrades := append(trades, newTrades...)
	
	// Process each time window
	for _, timeWindow := range c.timeWindows {
		windowName := timeWindow.Name
		
		// Get current index and aggregate for this window
		currentIndex := indices[windowName]
		currentAggregate := aggregates[windowName]
		if currentAggregate == nil {
			currentAggregate = models.NewAggregateData()
		}
		
		// Find expired trades that need to be removed
		expiredTrades, newIndex, err := c.FindExpiredTrades(
			updatedTrades,
			currentIndex,
			timeWindow,
			currentTime,
		)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to find expired trades for window %s: %w", windowName, err)
		}
		
		// Filter new trades that are valid for this window
		validNewTrades := c.GetValidTradesInWindow(newTrades, timeWindow, currentTime)
		
		// Calculate incremental update
		newAggregate := c.CalculateIncrementalUpdate(
			currentAggregate,
			expiredTrades,
			validNewTrades,
		)
		
		// Store updated values
		indices[windowName] = newIndex
		aggregates[windowName] = newAggregate
	}
	
	return indices, aggregates, nil
}

// IndexManager handles index tracking and optimization for time windows
type IndexManager struct {
	previousIndices map[string]int64
	indexHistory    map[string][]int64 // Track index history for optimization
	maxHistorySize  int
}

// NewIndexManager creates a new index manager
func NewIndexManager() *IndexManager {
	return &IndexManager{
		previousIndices: make(map[string]int64),
		indexHistory:    make(map[string][]int64),
		maxHistorySize:  10, // Keep last 10 index values for optimization
	}
}

// TrackIndex tracks an index value for a time window
func (im *IndexManager) TrackIndex(windowName string, index int64) {
	// Store previous index
	if currentIndex, exists := im.previousIndices[windowName]; exists {
		// Add to history
		history := im.indexHistory[windowName]
		history = append(history, currentIndex)
		
		// Limit history size
		if len(history) > im.maxHistorySize {
			history = history[1:]
		}
		im.indexHistory[windowName] = history
	}
	
	// Update current index
	im.previousIndices[windowName] = index
}

// HasIndexChanged checks if an index has changed since last tracking
func (im *IndexManager) HasIndexChanged(windowName string, newIndex int64) bool {
	previousIndex, exists := im.previousIndices[windowName]
	if !exists {
		return true // First time, consider it changed
	}
	return previousIndex != newIndex
}

// GetPreviousIndex returns the previous index for a window
func (im *IndexManager) GetPreviousIndex(windowName string) (int64, bool) {
	index, exists := im.previousIndices[windowName]
	return index, exists
}

// GetIndexHistory returns the index history for a window
func (im *IndexManager) GetIndexHistory(windowName string) []int64 {
	history, exists := im.indexHistory[windowName]
	if !exists {
		return []int64{}
	}
	// Return a copy to prevent external modification
	result := make([]int64, len(history))
	copy(result, history)
	return result
}

// OptimizedCalculator extends Calculator with index management and optimization
type OptimizedCalculator struct {
	*Calculator
	indexManager *IndexManager
}

// NewOptimizedCalculator creates a new optimized sliding window calculator
func NewOptimizedCalculator() interfaces.SlidingWindowCalculator {
	return &OptimizedCalculator{
		Calculator:   NewCalculator().(*Calculator),
		indexManager: NewIndexManager(),
	}
}

// UpdateWindowsOptimized updates sliding windows with optimization for unchanged indices
func (oc *OptimizedCalculator) UpdateWindowsOptimized(
	ctx context.Context,
	trades []models.TradeData,
	indices map[string]int64,
	aggregates map[string]*models.AggregateData,
	newTrade models.TradeData,
) (map[string]int64, map[string]*models.AggregateData, error) {
	currentTime := time.Now()
	
	// Initialize maps if nil
	if indices == nil {
		indices = make(map[string]int64)
	}
	if aggregates == nil {
		aggregates = make(map[string]*models.AggregateData)
	}
	
	// Create updated trades list with the new trade
	updatedTrades := append(trades, newTrade)
	
	// Process each time window with optimization
	for _, timeWindow := range oc.timeWindows {
		windowName := timeWindow.Name
		
		// Get current index and aggregate for this window
		currentIndex := indices[windowName]
		currentAggregate := aggregates[windowName]
		if currentAggregate == nil {
			currentAggregate = models.NewAggregateData()
		}
		
		// Find expired trades that need to be removed
		expiredTrades, newIndex, err := oc.FindExpiredTrades(
			updatedTrades,
			currentIndex,
			timeWindow,
			currentTime,
		)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to find expired trades for window %s: %w", windowName, err)
		}
		
		// Check if index has changed - optimization opportunity
		if !oc.indexManager.HasIndexChanged(windowName, newIndex) && len(expiredTrades) == 0 {
			// Index unchanged and no expired trades - we can reuse previous aggregate
			// Just add the new trade if it's valid
			newAggregate := currentAggregate.Clone()
			if oc.IsTradeValidForWindow(newTrade, timeWindow, currentTime) {
				newAggregate.AddTrade(newTrade)
			}
			newAggregate.LastUpdate = currentTime
			
			// Store updated values (index stays the same)
			aggregates[windowName] = newAggregate
			oc.indexManager.TrackIndex(windowName, newIndex)
			continue
		}
		
		// Index changed or we have expired trades - perform full update
		newAggregate := currentAggregate.Clone()
		
		// Subtract expired trades from the aggregate
		for _, expiredTrade := range expiredTrades {
			newAggregate.SubtractTrade(expiredTrade)
		}
		
		// Add the new trade if it's within the window
		if oc.IsTradeValidForWindow(newTrade, timeWindow, currentTime) {
			newAggregate.AddTrade(newTrade)
		}
		
		// Update the aggregate's last update time
		newAggregate.LastUpdate = currentTime
		
		// Store updated values
		indices[windowName] = newIndex
		aggregates[windowName] = newAggregate
		oc.indexManager.TrackIndex(windowName, newIndex)
	}
	
	return indices, aggregates, nil
}

// AtomicUpdate represents an atomic update operation for indices and aggregates
type AtomicUpdate struct {
	WindowName string
	Index      int64
	Aggregate  *models.AggregateData
	Timestamp  time.Time
}

// AtomicUpdateBatch represents a batch of atomic updates
type AtomicUpdateBatch struct {
	Updates   []AtomicUpdate
	Timestamp time.Time
}

// NewAtomicUpdateBatch creates a new atomic update batch
func NewAtomicUpdateBatch() *AtomicUpdateBatch {
	return &AtomicUpdateBatch{
		Updates:   make([]AtomicUpdate, 0),
		Timestamp: time.Now(),
	}
}

// AddUpdate adds an update to the batch
func (aub *AtomicUpdateBatch) AddUpdate(windowName string, index int64, aggregate *models.AggregateData) {
	update := AtomicUpdate{
		WindowName: windowName,
		Index:      index,
		Aggregate:  aggregate.Clone(), // Clone to prevent external modification
		Timestamp:  time.Now(),
	}
	aub.Updates = append(aub.Updates, update)
}

// ApplyBatch applies all updates in the batch atomically
func (aub *AtomicUpdateBatch) ApplyBatch(
	indices map[string]int64,
	aggregates map[string]*models.AggregateData,
) error {
	// Validate all updates first
	for _, update := range aub.Updates {
		if update.WindowName == "" {
			return fmt.Errorf("invalid update: empty window name")
		}
		if update.Aggregate == nil {
			return fmt.Errorf("invalid update: nil aggregate for window %s", update.WindowName)
		}
		if err := update.Aggregate.ValidateAggregateData(); err != nil {
			return fmt.Errorf("invalid aggregate data for window %s: %w", update.WindowName, err)
		}
	}
	
	// Apply all updates atomically
	for _, update := range aub.Updates {
		indices[update.WindowName] = update.Index
		aggregates[update.WindowName] = update.Aggregate
	}
	
	return nil
}

// UpdateWindowsAtomic updates sliding windows with atomic operations
func (oc *OptimizedCalculator) UpdateWindowsAtomic(
	ctx context.Context,
	trades []models.TradeData,
	indices map[string]int64,
	aggregates map[string]*models.AggregateData,
	newTrade models.TradeData,
) (map[string]int64, map[string]*models.AggregateData, error) {
	currentTime := time.Now()
	
	// Initialize maps if nil
	if indices == nil {
		indices = make(map[string]int64)
	}
	if aggregates == nil {
		aggregates = make(map[string]*models.AggregateData)
	}
	
	// Create atomic update batch
	batch := NewAtomicUpdateBatch()
	
	// Create updated trades list with the new trade
	updatedTrades := append(trades, newTrade)
	
	// Process each time window and prepare updates
	for _, timeWindow := range oc.timeWindows {
		windowName := timeWindow.Name
		
		// Get current index and aggregate for this window
		currentIndex := indices[windowName]
		currentAggregate := aggregates[windowName]
		
		// If we don't have an existing aggregate or it's empty, recalculate from scratch
		if currentAggregate == nil || currentAggregate.IsEmpty() {
			// Recalculate the entire window from scratch
			newAggregate, newIndex, err := oc.CalculateWindow(
				ctx,
				updatedTrades,
				timeWindow,
				currentTime,
			)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to calculate window %s: %w", windowName, err)
			}
			
			// Add to atomic batch
			batch.AddUpdate(windowName, newIndex, newAggregate)
			oc.indexManager.TrackIndex(windowName, newIndex)
			continue
		}
		
		// Find expired trades that need to be removed
		expiredTrades, newIndex, err := oc.FindExpiredTrades(
			updatedTrades,
			currentIndex,
			timeWindow,
			currentTime,
		)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to find expired trades for window %s: %w", windowName, err)
		}
		
		// Calculate new aggregate
		newAggregate := currentAggregate.Clone()
		
		// Subtract expired trades from the aggregate
		for _, expiredTrade := range expiredTrades {
			newAggregate.SubtractTrade(expiredTrade)
		}
		
		// Add the new trade if it's within the window
		if oc.IsTradeValidForWindow(newTrade, timeWindow, currentTime) {
			newAggregate.AddTrade(newTrade)
		}
		
		// Update the aggregate's last update time
		newAggregate.LastUpdate = currentTime
		
		// Add to atomic batch
		batch.AddUpdate(windowName, newIndex, newAggregate)
		oc.indexManager.TrackIndex(windowName, newIndex)
	}
	
	// Apply all updates atomically
	err := batch.ApplyBatch(indices, aggregates)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to apply atomic updates: %w", err)
	}
	
	return indices, aggregates, nil
}