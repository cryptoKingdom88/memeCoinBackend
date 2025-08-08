package interfaces

import (
	"context"
	"time"

	"aggregatorService/config"
	"aggregatorService/models"
	"github.com/cryptoKingdom88/memeCoinBackend/shared/packet"
)

// RedisManager interface defines Redis operations
type RedisManager interface {
	// Connection management
	Connect(ctx context.Context) error
	Close() error
	Ping(ctx context.Context) error

	// Token data operations
	UpdateTokenData(ctx context.Context, tokenAddress string, data *models.TokenData) error
	GetTokenData(ctx context.Context, tokenAddress string) (*models.TokenData, error)
	
	// Trade operations
	AddTrade(ctx context.Context, tokenAddress string, trade models.TradeData) error
	GetTrades(ctx context.Context, tokenAddress string, start, end int64) ([]models.TradeData, error)
	
	// Index operations
	SetIndex(ctx context.Context, tokenAddress, timeframe string, index int64) error
	GetIndex(ctx context.Context, tokenAddress, timeframe string) (int64, error)
	
	// Aggregate operations
	SetAggregate(ctx context.Context, tokenAddress, timeframe string, aggregate *models.AggregateData) error
	GetAggregate(ctx context.Context, tokenAddress, timeframe string) (*models.AggregateData, error)
	
	// TTL and maintenance
	SetTTL(ctx context.Context, tokenAddress string, duration time.Duration) error
	GetActiveTokens(ctx context.Context) ([]string, error)
	UpdateLastUpdate(ctx context.Context, tokenAddress string, timestamp time.Time) error
	GetLastUpdate(ctx context.Context, tokenAddress string) (time.Time, error)
	
	// Atomic operations
	ExecuteAtomicUpdate(ctx context.Context, script string, keys []string, args []interface{}) error
}

// SlidingWindowCalculator interface defines sliding window operations
type SlidingWindowCalculator interface {
	// Initialize calculator with time windows
	Initialize(timeWindows []config.TimeWindow) error
	
	// Update sliding windows with new trade
	UpdateWindows(
		ctx context.Context,
		trades []models.TradeData,
		indices map[string]int64,
		aggregates map[string]*models.AggregateData,
		newTrade models.TradeData,
	) (map[string]int64, map[string]*models.AggregateData, error)
	
	// Calculate aggregations for a specific time window
	CalculateWindow(
		ctx context.Context,
		trades []models.TradeData,
		timeWindow config.TimeWindow,
		currentTime time.Time,
	) (*models.AggregateData, int64, error)
	
	// Find expired trades for a time window
	FindExpiredTrades(
		trades []models.TradeData,
		currentIndex int64,
		timeWindow config.TimeWindow,
		currentTime time.Time,
	) ([]models.TradeData, int64, error)
	
	// Get time window by name
	GetTimeWindowByName(name string) (config.TimeWindow, bool)
	
	// Get all time window names
	GetAllTimeWindowNames() []string
}

// TokenProcessor interface defines per-token processing operations
type TokenProcessor interface {
	// Initialize processor for a token
	Initialize(tokenAddress string, redisManager RedisManager, calculator SlidingWindowCalculator) error
	
	// Process a single trade
	ProcessTrade(ctx context.Context, trade models.TradeData) error
	
	// Get current aggregation data
	GetAggregates(ctx context.Context) (map[string]*models.AggregateData, error)
	
	// Perform manual aggregation (for maintenance)
	PerformManualAggregation(ctx context.Context) error
	
	// Cleanup and shutdown
	Shutdown(ctx context.Context) error
}

// BlockAggregator interface defines block-level trade aggregation
type BlockAggregator interface {
	// Initialize aggregator
	Initialize(redisManager RedisManager, calculator SlidingWindowCalculator, workerPool WorkerPool, kafkaProducer KafkaProducer) error
	
	// Process multiple trades (grouped by block)
	ProcessTrades(ctx context.Context, trades []packet.TokenTradeHistory) error
	
	// Get or create token processor
	GetTokenProcessor(tokenAddress string) (TokenProcessor, error)
	
	// Send aggregate results to Kafka
	SendAggregateResults(ctx context.Context, tokenAddress string) error
	SendBatchAggregateResults(ctx context.Context, tokenAddresses []string) error
	SendAllActiveTokenAggregates(ctx context.Context) error
	
	// Schedule periodic aggregate publishing
	SchedulePeriodicAggregatePublishing(ctx context.Context, interval time.Duration)
	
	// Shutdown all processors
	Shutdown(ctx context.Context) error
}

// WorkerPool interface defines worker pool operations
type WorkerPool interface {
	// Initialize worker pool
	Initialize(maxWorkers int) error
	
	// Submit job to worker pool
	Submit(job func()) error
	
	// Get current worker count
	GetWorkerCount() int
	
	// Shutdown worker pool
	Shutdown(ctx context.Context) error
}

// KafkaConsumer interface defines Kafka consumer operations
type KafkaConsumer interface {
	// Initialize consumer
	Initialize(brokers []string, topic, consumerGroup string) error
	
	// Start consuming messages
	Start(ctx context.Context, processor BlockAggregator) error
	
	// Stop consuming
	Stop() error
}

// MaintenanceService interface defines background maintenance operations
type MaintenanceService interface {
	// Initialize maintenance service
	Initialize(redisManager RedisManager, interval, staleThreshold time.Duration) error
	
	// Start maintenance service
	Start(ctx context.Context) error
	
	// Scan for stale tokens
	ScanStaleTokens(ctx context.Context) ([]string, error)
	
	// Perform manual aggregation for a token
	PerformManualAggregation(ctx context.Context, tokenAddress string) error
	
	// Stop maintenance service
	Stop() error
}

// MetricsCollector interface defines metrics collection operations
type MetricsCollector interface {
	// Initialize metrics collector
	Initialize() error
	
	// Record trade processing metrics
	RecordTradeProcessed(tokenAddress string, processingTime time.Duration)
	RecordAggregationCalculated(tokenAddress, timeframe string, calculationTime time.Duration)
	RecordRedisOperation(operation string, duration time.Duration, success bool)
	
	// Record system metrics
	RecordActiveTokens(count int)
	RecordWorkerCount(count int)
	RecordMemoryUsage(bytes int64)
	
	// Get current metrics
	GetMetrics() map[string]interface{}
}

// KafkaProducer interface defines Kafka producer operations for aggregate data
type KafkaProducer interface {
	// Initialize producer
	Initialize(brokers []string) error
	
	// Send aggregate data to Kafka
	SendAggregateData(ctx context.Context, aggregateData *packet.TokenAggregateData) error
	
	// Send batch of aggregate data
	SendBatchAggregateData(ctx context.Context, aggregateDataList []*packet.TokenAggregateData) error
	
	// Health check
	HealthCheck(ctx context.Context) error
	
	// Get producer statistics
	GetStats() map[string]interface{}
	
	// Close producer
	Close() error
}