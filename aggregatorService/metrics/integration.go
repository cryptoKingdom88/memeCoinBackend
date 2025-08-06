package metrics

import (
	"context"
	"time"

	"aggregatorService/interfaces"
)

// MetricsWrapper provides helper functions to integrate metrics collection
type MetricsWrapper struct {
	collector interfaces.MetricsCollector
}

// NewMetricsWrapper creates a new metrics wrapper
func NewMetricsWrapper(collector interfaces.MetricsCollector) *MetricsWrapper {
	return &MetricsWrapper{
		collector: collector,
	}
}

// TimedTradeProcessing wraps trade processing with metrics collection
func (mw *MetricsWrapper) TimedTradeProcessing(tokenAddress string, fn func() error) error {
	start := time.Now()
	err := fn()
	duration := time.Since(start)
	
	mw.collector.RecordTradeProcessed(tokenAddress, duration)
	return err
}

// TimedAggregationCalculation wraps aggregation calculation with metrics collection
func (mw *MetricsWrapper) TimedAggregationCalculation(tokenAddress, timeframe string, fn func() error) error {
	start := time.Now()
	err := fn()
	duration := time.Since(start)
	
	mw.collector.RecordAggregationCalculated(tokenAddress, timeframe, duration)
	return err
}

// TimedRedisOperation wraps Redis operations with metrics collection
func (mw *MetricsWrapper) TimedRedisOperation(operation string, fn func() error) error {
	start := time.Now()
	err := fn()
	duration := time.Since(start)
	success := err == nil
	
	mw.collector.RecordRedisOperation(operation, duration, success)
	return err
}

// TimedRedisOperationWithContext wraps Redis operations with context and metrics collection
func (mw *MetricsWrapper) TimedRedisOperationWithContext(ctx context.Context, operation string, fn func(context.Context) error) error {
	start := time.Now()
	err := fn(ctx)
	duration := time.Since(start)
	success := err == nil
	
	mw.collector.RecordRedisOperation(operation, duration, success)
	return err
}

// UpdateSystemMetrics updates system-level metrics
func (mw *MetricsWrapper) UpdateSystemMetrics(activeTokens, workerCount int, memoryUsage int64) {
	mw.collector.RecordActiveTokens(activeTokens)
	mw.collector.RecordWorkerCount(workerCount)
	mw.collector.RecordMemoryUsage(memoryUsage)
}

// GetCollector returns the underlying metrics collector
func (mw *MetricsWrapper) GetCollector() interfaces.MetricsCollector {
	return mw.collector
}