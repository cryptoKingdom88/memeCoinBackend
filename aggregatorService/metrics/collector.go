package metrics

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"aggregatorService/interfaces"
)

// TradeMetrics holds trade processing metrics
type TradeMetrics struct {
	TotalProcessed    int64         `json:"total_processed"`
	ProcessingRate    float64       `json:"processing_rate_per_sec"`
	AvgProcessingTime time.Duration `json:"avg_processing_time"`
	LastProcessedAt   time.Time     `json:"last_processed_at"`
}

// AggregationMetrics holds aggregation calculation metrics
type AggregationMetrics struct {
	TotalCalculations int64                    `json:"total_calculations"`
	CalculationRate   float64                  `json:"calculation_rate_per_sec"`
	AvgLatency        time.Duration            `json:"avg_latency"`
	LatencyByWindow   map[string]time.Duration `json:"latency_by_window"`
	LastCalculatedAt  time.Time                `json:"last_calculated_at"`
}

// RedisMetrics holds Redis operation metrics
type RedisMetrics struct {
	TotalOperations   int64                    `json:"total_operations"`
	SuccessfulOps     int64                    `json:"successful_operations"`
	FailedOps         int64                    `json:"failed_operations"`
	SuccessRate       float64                  `json:"success_rate"`
	AvgLatency        time.Duration            `json:"avg_latency"`
	LatencyByOp       map[string]time.Duration `json:"latency_by_operation"`
	LastOperationAt   time.Time                `json:"last_operation_at"`
}

// SystemMetrics holds system resource metrics
type SystemMetrics struct {
	ActiveTokens     int   `json:"active_tokens"`
	WorkerCount      int   `json:"worker_count"`
	GoroutineCount   int   `json:"goroutine_count"`
	MemoryUsage      int64 `json:"memory_usage_bytes"`
	MemoryAllocated  int64 `json:"memory_allocated_bytes"`
	MemorySystem     int64 `json:"memory_system_bytes"`
	GCPauseTotal     int64 `json:"gc_pause_total_ns"`
	LastUpdatedAt    time.Time `json:"last_updated_at"`
}

// Collector implements the MetricsCollector interface
type Collector struct {
	mu sync.RWMutex
	
	// Metrics data
	tradeMetrics       *TradeMetrics
	aggregationMetrics *AggregationMetrics
	redisMetrics       *RedisMetrics
	systemMetrics      *SystemMetrics
	
	// Internal counters for rate calculations
	tradeCounter       int64
	aggregationCounter int64
	redisCounter       int64
	
	// Time tracking for rates
	startTime          time.Time
	lastRateUpdate     time.Time
	
	// Processing time tracking
	tradeTimes         []time.Duration
	aggregationTimes   []time.Duration
	redisTimes         []time.Duration
	
	// Max samples for average calculations
	maxSamples         int
}

// NewCollector creates a new metrics collector
func NewCollector() interfaces.MetricsCollector {
	now := time.Now()
	return &Collector{
		tradeMetrics: &TradeMetrics{
			LastProcessedAt: now,
		},
		aggregationMetrics: &AggregationMetrics{
			LatencyByWindow:  make(map[string]time.Duration),
			LastCalculatedAt: now,
		},
		redisMetrics: &RedisMetrics{
			LatencyByOp:     make(map[string]time.Duration),
			LastOperationAt: now,
		},
		systemMetrics: &SystemMetrics{
			LastUpdatedAt: now,
		},
		startTime:      now,
		lastRateUpdate: now,
		maxSamples:     1000, // Keep last 1000 samples for averages
	}
}

// Initialize initializes the metrics collector
func (c *Collector) Initialize() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	// Initialize time slices
	c.tradeTimes = make([]time.Duration, 0, c.maxSamples)
	c.aggregationTimes = make([]time.Duration, 0, c.maxSamples)
	c.redisTimes = make([]time.Duration, 0, c.maxSamples)
	
	// Start background goroutine to update system metrics
	go c.updateSystemMetricsPeriodically()
	
	return nil
}

// RecordTradeProcessed records trade processing metrics
func (c *Collector) RecordTradeProcessed(tokenAddress string, processingTime time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	atomic.AddInt64(&c.tradeCounter, 1)
	c.tradeMetrics.TotalProcessed = atomic.LoadInt64(&c.tradeCounter)
	c.tradeMetrics.LastProcessedAt = time.Now()
	
	// Add processing time to samples
	c.tradeTimes = append(c.tradeTimes, processingTime)
	if len(c.tradeTimes) > c.maxSamples {
		c.tradeTimes = c.tradeTimes[1:]
	}
	
	// Calculate average processing time
	c.tradeMetrics.AvgProcessingTime = c.calculateAverage(c.tradeTimes)
	
	// Update processing rate
	c.updateProcessingRate()
}

// RecordAggregationCalculated records aggregation calculation metrics
func (c *Collector) RecordAggregationCalculated(tokenAddress, timeframe string, calculationTime time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	atomic.AddInt64(&c.aggregationCounter, 1)
	c.aggregationMetrics.TotalCalculations = atomic.LoadInt64(&c.aggregationCounter)
	c.aggregationMetrics.LastCalculatedAt = time.Now()
	
	// Add calculation time to samples
	c.aggregationTimes = append(c.aggregationTimes, calculationTime)
	if len(c.aggregationTimes) > c.maxSamples {
		c.aggregationTimes = c.aggregationTimes[1:]
	}
	
	// Calculate average latency
	c.aggregationMetrics.AvgLatency = c.calculateAverage(c.aggregationTimes)
	
	// Update latency by window
	c.aggregationMetrics.LatencyByWindow[timeframe] = calculationTime
	
	// Update calculation rate
	c.updateCalculationRate()
}

// RecordRedisOperation records Redis operation metrics
func (c *Collector) RecordRedisOperation(operation string, duration time.Duration, success bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	atomic.AddInt64(&c.redisCounter, 1)
	c.redisMetrics.TotalOperations = atomic.LoadInt64(&c.redisCounter)
	c.redisMetrics.LastOperationAt = time.Now()
	
	if success {
		atomic.AddInt64(&c.redisMetrics.SuccessfulOps, 1)
	} else {
		atomic.AddInt64(&c.redisMetrics.FailedOps, 1)
	}
	
	// Calculate success rate
	total := c.redisMetrics.TotalOperations
	if total > 0 {
		c.redisMetrics.SuccessRate = float64(c.redisMetrics.SuccessfulOps) / float64(total) * 100
	}
	
	// Add operation time to samples
	c.redisTimes = append(c.redisTimes, duration)
	if len(c.redisTimes) > c.maxSamples {
		c.redisTimes = c.redisTimes[1:]
	}
	
	// Calculate average latency
	c.redisMetrics.AvgLatency = c.calculateAverage(c.redisTimes)
	
	// Update latency by operation
	c.redisMetrics.LatencyByOp[operation] = duration
}

// RecordActiveTokens records active token count
func (c *Collector) RecordActiveTokens(count int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	c.systemMetrics.ActiveTokens = count
	c.systemMetrics.LastUpdatedAt = time.Now()
}

// RecordWorkerCount records worker count
func (c *Collector) RecordWorkerCount(count int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	c.systemMetrics.WorkerCount = count
	c.systemMetrics.LastUpdatedAt = time.Now()
}

// RecordMemoryUsage records memory usage
func (c *Collector) RecordMemoryUsage(bytes int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	c.systemMetrics.MemoryUsage = bytes
	c.systemMetrics.LastUpdatedAt = time.Now()
}

// GetMetrics returns current metrics
func (c *Collector) GetMetrics() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	// Update system metrics before returning
	c.updateSystemMetrics()
	
	return map[string]interface{}{
		"trade_processing":      c.tradeMetrics,
		"aggregation_calculation": c.aggregationMetrics,
		"redis_operations":      c.redisMetrics,
		"system_resources":      c.systemMetrics,
		"uptime_seconds":        time.Since(c.startTime).Seconds(),
	}
}

// Helper methods

// calculateAverage calculates average duration from a slice
func (c *Collector) calculateAverage(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	
	var total time.Duration
	for _, d := range durations {
		total += d
	}
	
	return total / time.Duration(len(durations))
}

// updateProcessingRate updates trade processing rate
func (c *Collector) updateProcessingRate() {
	now := time.Now()
	elapsed := now.Sub(c.startTime).Seconds()
	if elapsed > 0 {
		c.tradeMetrics.ProcessingRate = float64(c.tradeMetrics.TotalProcessed) / elapsed
	}
}

// updateCalculationRate updates aggregation calculation rate
func (c *Collector) updateCalculationRate() {
	now := time.Now()
	elapsed := now.Sub(c.startTime).Seconds()
	if elapsed > 0 {
		c.aggregationMetrics.CalculationRate = float64(c.aggregationMetrics.TotalCalculations) / elapsed
	}
}

// updateSystemMetrics updates system resource metrics
func (c *Collector) updateSystemMetrics() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	
	c.systemMetrics.GoroutineCount = runtime.NumGoroutine()
	c.systemMetrics.MemoryAllocated = int64(m.Alloc)
	c.systemMetrics.MemorySystem = int64(m.Sys)
	c.systemMetrics.GCPauseTotal = int64(m.PauseTotalNs)
}

// updateSystemMetricsPeriodically runs in background to update system metrics
func (c *Collector) updateSystemMetricsPeriodically() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	
	for range ticker.C {
		c.mu.Lock()
		c.updateSystemMetrics()
		c.mu.Unlock()
	}
}