# Design Document

## Overview

The Real-time Token Trade Aggregation System is designed to process high-frequency trading data and provide real-time aggregated metrics across multiple time windows. The system uses an efficient sliding window algorithm with Redis-based storage to handle tens of thousands of trades per day while maintaining sub-second response times.

## Architecture

### High-Level Architecture

```
Kafka Topic → Block Aggregator → Token Processor Pool → Redis Storage
     ↓              ↓                    ↓                   ↓
trade-info    Group by Token    Per-Token Goroutines    Sliding Windows
                    ↓                    ↓                   ↓
              Initial Stats      Index-based Calc      Time-series Data
```

### Component Interaction Flow

1. **Kafka Consumer** receives trade data from `trade-info` topic
2. **Block Aggregator** groups trades by token for initial processing
3. **Token Processor Pool** spawns/manages per-token goroutines
4. **Sliding Window Calculator** performs incremental aggregation
5. **Redis Manager** handles atomic data operations
6. **Background Maintenance** ensures data consistency

## Components and Interfaces

### 1. Kafka Consumer Component

```go
type KafkaConsumer struct {
    reader    *kafka.Reader
    processor *BlockAggregator
}

func (kc *KafkaConsumer) Start(ctx context.Context) error
func (kc *KafkaConsumer) processMessage(msg kafka.Message) error
```

**Responsibilities:**
- Consume trade messages from Kafka
- Parse JSON into trade structs
- Forward to block aggregator

### 2. Block Aggregator Component

```go
type BlockAggregator struct {
    tokenProcessors map[string]*TokenProcessor
    processorMutex  sync.RWMutex
    workerPool      *WorkerPool
}

func (ba *BlockAggregator) ProcessTrades(trades []packet.TokenTradeHistory)
func (ba *BlockAggregator) getOrCreateProcessor(tokenAddress string) *TokenProcessor
```

**Responsibilities:**
- Group incoming trades by token address
- Manage token processor lifecycle
- Distribute work to token processors

### 3. Token Processor Component

```go
type TokenProcessor struct {
    tokenAddress string
    redisClient  *redis.Client
    tradeBuffer  []TradeData
    indices      map[TimeWindow]int64
    aggregates   map[TimeWindow]*AggregateData
    lastUpdate   time.Time
    mutex        sync.Mutex
}

func (tp *TokenProcessor) ProcessTrade(trade packet.TokenTradeHistory)
func (tp *TokenProcessor) calculateAggregations() error
func (tp *TokenProcessor) updateSlidingWindows() error
```

**Responsibilities:**
- Process trades for specific token
- Maintain sliding window indices
- Calculate incremental aggregations
- Update Redis atomically

### 4. Sliding Window Calculator

```go
type SlidingWindowCalculator struct {
    timeWindows []TimeWindow
}

type TimeWindow struct {
    Duration time.Duration
    Name     string
}

func (swc *SlidingWindowCalculator) UpdateWindows(
    trades []TradeData, 
    indices map[TimeWindow]int64,
    aggregates map[TimeWindow]*AggregateData,
    newTrade TradeData
) error
```

**Responsibilities:**
- Implement sliding window algorithm
- Find expired data indices
- Calculate incremental updates
- Optimize for unchanged indices

### 5. Redis Manager Component

```go
type RedisManager struct {
    client   *redis.Client
    pipeline redis.Pipeliner
}

func (rm *RedisManager) UpdateTokenData(tokenAddress string, data *TokenData) error
func (rm *RedisManager) GetTokenData(tokenAddress string) (*TokenData, error)
func (rm *RedisManager) SetTTL(tokenAddress string, duration time.Duration) error
func (rm *RedisManager) ExecuteAtomicUpdate(script string, keys []string, args []interface{}) error
```

**Responsibilities:**
- Manage Redis connections and pipelines
- Execute atomic operations via Lua scripts
- Handle TTL management
- Provide data persistence layer

### 6. Background Maintenance Service

```go
type MaintenanceService struct {
    redisManager *RedisManager
    interval     time.Duration
    staleThreshold time.Duration
}

func (ms *MaintenanceService) Start(ctx context.Context)
func (ms *MaintenanceService) scanStaleTokens() ([]string, error)
func (ms *MaintenanceService) performManualAggregation(tokenAddress string) error
```

**Responsibilities:**
- Periodic scanning of active tokens
- Identify stale aggregations
- Perform manual calculations
- Update last_update timestamps

## Data Models

### Trade Data Structure

```go
type TradeData struct {
    Token        string    `json:"token"`
    Wallet       string    `json:"wallet"`
    SellBuy      string    `json:"sell_buy"`      // "buy" or "sell"
    NativeAmount float64   `json:"native_amount"` // Volume in native currency
    TokenAmount  float64   `json:"token_amount"`  // Token quantity
    PriceUsd     float64   `json:"price_usd"`     // USD price
    TransTime    time.Time `json:"trans_time"`    // Transaction timestamp
    TxHash       string    `json:"tx_hash"`       // Transaction hash
}
```

### Aggregate Data Structure

```go
type AggregateData struct {
    SellCount    int64   `json:"sell_count"`
    BuyCount     int64   `json:"buy_count"`
    SellVolume   float64 `json:"sell_volume"`   // USD volume
    BuyVolume    float64 `json:"buy_volume"`    // USD volume
    TotalVolume  float64 `json:"total_volume"`  // USD volume
    PriceChange  float64 `json:"price_change"`  // Price change %
    StartPrice   float64 `json:"start_price"`   // Window start price
    EndPrice     float64 `json:"end_price"`     // Current/end price
    HighPrice    float64 `json:"high_price"`    // Highest price in window
    LowPrice     float64 `json:"low_price"`     // Lowest price in window
    LastUpdate   time.Time `json:"last_update"` // Last calculation time
}
```

### Redis Data Schema

```
Key Pattern: token:{address}:trades
Type: List
Value: JSON serialized TradeData
TTL: 1 hour (3600 seconds)

Key Pattern: token:{address}:index_1min
Type: String  
Value: Index position (int64) for 1-minute window

Key Pattern: token:{address}:index_5min
Type: String
Value: Index position (int64) for 5-minute window

Key Pattern: token:{address}:index_15min
Type: String
Value: Index position (int64) for 15-minute window

Key Pattern: token:{address}:index_30min
Type: String
Value: Index position (int64) for 30-minute window

Key Pattern: token:{address}:index_1hour
Type: String
Value: Index position (int64) for 1-hour window

Key Pattern: token:{address}:agg_1min
Type: String
Value: JSON serialized AggregateData for 1-minute window

Key Pattern: token:{address}:agg_5min
Type: String
Value: JSON serialized AggregateData for 5-minute window

Key Pattern: token:{address}:agg_15min
Type: String
Value: JSON serialized AggregateData for 15-minute window

Key Pattern: token:{address}:agg_30min
Type: String
Value: JSON serialized AggregateData for 30-minute window

Key Pattern: token:{address}:agg_1hour
Type: String
Value: JSON serialized AggregateData for 1-hour window

Key Pattern: token:{address}:last_update
Type: String
Value: RFC3339 timestamp
```

## Error Handling

### Error Categories and Strategies

1. **Kafka Connection Errors**
   - Retry with exponential backoff
   - Circuit breaker pattern
   - Graceful degradation

2. **Redis Operation Errors**
   - Atomic operation rollback
   - Retry with jitter
   - Fallback to in-memory calculation

3. **Data Parsing Errors**
   - Log and skip invalid messages
   - Maintain processing continuity
   - Emit error metrics

4. **Goroutine Panics**
   - Recover and restart token processor
   - Log panic details
   - Preserve other token processing

### Consistency Guarantees

- **Atomic Updates**: Use Redis Lua scripts for multi-key operations
- **Idempotency**: Handle duplicate trade processing
- **Recovery**: Rebuild state from Redis on startup
- **Validation**: Verify data integrity during calculations

## Testing Strategy

### Unit Testing
- Individual component testing with mocks
- Sliding window algorithm validation
- Redis operation testing with test containers
- Error handling scenario coverage

### Integration Testing
- End-to-end trade processing flow
- Redis data consistency verification
- Kafka consumer integration
- Performance benchmarking

### Load Testing
- High-frequency trade simulation
- Concurrent token processing
- Memory usage profiling
- Latency measurement under load

### Chaos Testing
- Redis connection failures
- Kafka broker outages
- Goroutine crash scenarios
- Network partition handling

## Performance Considerations

### Optimization Strategies

1. **Redis Pipeline Usage**
   - Batch multiple operations
   - Reduce network round trips
   - Improve throughput

2. **Memory Management**
   - Object pooling for frequent allocations
   - Efficient data structures
   - Garbage collection tuning

3. **Concurrency Optimization**
   - Lock-free algorithms where possible
   - Minimize critical sections
   - Worker pool sizing

4. **Algorithm Efficiency**
   - O(1) sliding window updates
   - Index-based data access
   - Lazy evaluation for unchanged windows

### Expected Performance Metrics

- **Throughput**: 50,000+ trades/second
- **Latency**: <10ms per trade processing
- **Memory**: <100MB per 1000 active tokens
- **CPU**: <50% utilization under normal load

## Monitoring and Observability

### Key Metrics

- Trades processed per second
- Aggregation calculation latency
- Active token count
- Redis operation performance
- Goroutine count and health
- Memory usage patterns

### Alerting Thresholds

- Processing latency >100ms
- Error rate >1%
- Memory usage >80%
- Redis connection failures
- Goroutine leak detection

### Logging Strategy

- Structured logging with context
- Trade processing traceability
- Error categorization and correlation
- Performance profiling data