# 🚀 Kafka Integration for Aggregate Data Publishing

## 📋 Overview

The aggregatorService now publishes aggregate results to the `aggregate-info` Kafka topic. This feature is fully integrated into the current architecture following senior engineer-level structured design principles.

## 🏗️ Architecture Components

### 1. **Enhanced Data Structures**

#### Updated `shared/packet/packet.go`:
```go
type TokenAggregateItem struct {
    TimeWindow   string  `json:"time_window"`   // e.g., "1min", "5min", "15min"
    SellCount    int     `json:"sell_count"`
    BuyCount     int     `json:"buy_count"`
    TotalTrades  int     `json:"total_trades"`
    SellVolume   string  `json:"sell_volume"`   // Total sell volume
    BuyVolume    string  `json:"buy_volume"`    // Total buy volume
    TotalVolume  string  `json:"total_volume"`  // Total volume
    VolumeUsd    string  `json:"volume_usd"`    // Total volume in USD
    PriceChange  float64 `json:"price_change"`  // Price change percentage
    OpenPrice    string  `json:"open_price"`    // Opening price
    ClosePrice   string  `json:"close_price"`   // Closing price
    Timestamp    string  `json:"timestamp"`     // Window end timestamp
}

type TokenAggregateData struct {
    Token         string                `json:"token"`
    Symbol        string                `json:"symbol,omitempty"`
    Name          string                `json:"name,omitempty"`
    AggregateData []TokenAggregateItem  `json:"aggregate_data"`
    GeneratedAt   string                `json:"generated_at"`
    Version       string                `json:"version"`
}
```

### 2. **Kafka Producer Interface**

#### New `interfaces.KafkaProducer`:
```go
type KafkaProducer interface {
    Initialize(brokers []string) error
    SendAggregateData(ctx context.Context, aggregateData *packet.TokenAggregateData) error
    SendBatchAggregateData(ctx context.Context, aggregateDataList []*packet.TokenAggregateData) error
    HealthCheck(ctx context.Context) error
    GetStats() map[string]interface{}
    Close() error
}
```

### 3. **Kafka Producer Implementation**

#### New `kafka/producer.go`:
- **Topic**: `aggregate-info`
- **Partitioning**: By token address for consistent routing
- **Batching**: 50 messages per batch with 100ms timeout
- **Compression**: Enabled for better throughput
- **Headers**: Content-type, version, and token metadata
- **Error Handling**: Comprehensive logging and statistics
- **Performance Monitoring**: Latency tracking and throughput metrics

### 4. **Enhanced BlockAggregator**

#### Updated `processor/block_aggregator.go`:
- **KafkaProducer Integration**: Added as dependency in Initialize method
- **New Methods**:
  - `SendAggregateResults(ctx, tokenAddress)` - Send single token aggregates
  - `SendBatchAggregateResults(ctx, tokenAddresses)` - Send multiple token aggregates
  - `SendAllActiveTokenAggregates(ctx)` - Send all active token aggregates
  - `SchedulePeriodicAggregatePublishing(ctx, interval)` - Periodic publishing
  - `convertToPacketFormat()` - Convert internal data to packet format

## 🔄 Data Flow

```
1. Trade Data → Kafka Consumer → BlockAggregator
2. BlockAggregator → TokenProcessor → Redis (Aggregation)
3. Periodic Timer → BlockAggregator → KafkaProducer → aggregate-info topic
```

## 📊 Message Format

### Sample Kafka Message:
```json
{
  "token": "0x0000000000000000000000000000000000000001",
  "symbol": "",
  "name": "",
  "aggregate_data": [
    {
      "time_window": "1min",
      "sell_count": 5,
      "buy_count": 8,
      "total_trades": 13,
      "sell_volume": "1250.500000",
      "buy_volume": "2100.750000",
      "total_volume": "3351.250000",
      "volume_usd": "3351.250000",
      "price_change": 2.5,
      "open_price": "0.000118",
      "close_price": "0.000125",
      "timestamp": "2025-01-08T10:30:00Z"
    },
    {
      "time_window": "5min",
      "sell_count": 25,
      "buy_count": 32,
      "total_trades": 57,
      "sell_volume": "5200.250000",
      "buy_volume": "7800.500000",
      "total_volume": "13000.750000",
      "volume_usd": "13000.750000",
      "price_change": 5.2,
      "open_price": "0.000115",
      "close_price": "0.000125",
      "timestamp": "2025-01-08T10:30:00Z"
    }
  ],
  "generated_at": "2025-01-08T10:30:15Z",
  "version": "1.0"
}
```

## ⚙️ Configuration

### Environment Variables:
```bash
# Existing Kafka Configuration
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC=trade-info
KAFKA_CONSUMER_GROUP=aggregator-service

# New Aggregate Publishing Configuration
AGGREGATE_PUBLISH_INTERVAL=30s  # How often to publish aggregate data
```

### Kafka Topics:
- **Input**: `trade-info` (existing) - Raw trade data
- **Output**: `aggregate-info` (new) - Aggregated data

## 🚀 Features

### 1. **Intelligent Publishing**
- **Periodic Publishing**: Every 30 seconds (configurable)
- **Batch Processing**: Multiple tokens in single batch for efficiency
- **Active Token Filtering**: Only publishes data for tokens with recent activity

### 2. **Performance Optimizations**
- **Batching**: Up to 50 messages per batch
- **Compression**: Snappy compression for reduced bandwidth
- **Partitioning**: By token address for consistent consumer routing
- **Connection Pooling**: Reuses connections for better performance

### 3. **Reliability Features**
- **Error Handling**: Comprehensive error logging and recovery
- **Health Checks**: Built-in health monitoring
- **Graceful Shutdown**: Proper cleanup on service shutdown
- **Statistics Tracking**: Performance metrics and monitoring

### 4. **Data Quality**
- **Type Safety**: Proper type conversions between internal and packet formats
- **Validation**: Data validation before publishing
- **Versioning**: Message versioning for backward compatibility
- **Metadata**: Rich message headers for routing and filtering

## 📈 Monitoring & Metrics

### Producer Statistics:
```json
{
  "is_running": true,
  "brokers": ["localhost:9092"],
  "topic": "aggregate-info",
  "messages_sent": 1250,
  "messages_error": 3,
  "bytes_sent": 2500000,
  "last_sent_time": "2025-01-08T10:30:15Z",
  "average_latency": 15.5,
  "writer_stats": {
    "writes": 125,
    "messages": 1250,
    "bytes": 2500000,
    "errors": 3,
    "batch_time": "85ms",
    "write_time": "12ms"
  }
}
```

## 🔧 Integration Points

### 1. **Service Initialization**
```go
// main.go
kafkaProducer := kafka.NewProducer()
kafkaProducer.Initialize(cfg.KafkaBrokers)

blockAggregator.Initialize(redisManager, calculator, workerPool, kafkaProducer)
```

### 2. **Periodic Publishing**
```go
// Starts automatically with 30-second interval
blockAggregator.SchedulePeriodicAggregatePublishing(ctx, 30*time.Second)
```

### 3. **Manual Publishing**
```go
// Send specific token
err := blockAggregator.SendAggregateResults(ctx, tokenAddress)

// Send multiple tokens
err := blockAggregator.SendBatchAggregateResults(ctx, tokenAddresses)

// Send all active tokens
err := blockAggregator.SendAllActiveTokenAggregates(ctx)
```

## 🧪 Testing

### 1. **Start the Service**:
```bash
cd aggregatorService
./aggregatorService
```

### 2. **Monitor Kafka Topic**:
```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic aggregate-info --from-beginning
```

### 3. **Generate Test Data**:
```bash
cd testCollectorService
./scripts/quick_test.sh
```

### 4. **Expected Output**:
- Aggregate data published every 30 seconds
- JSON messages with time window aggregations
- Performance logs showing successful publishing

## 🎯 Benefits

### 1. **Scalability**
- **Decoupled Architecture**: Consumers can process aggregates independently
- **Horizontal Scaling**: Multiple consumers can process different partitions
- **Load Distribution**: Partitioning by token address distributes load evenly

### 2. **Real-time Analytics**
- **Low Latency**: 30-second publishing interval for near real-time data
- **Multiple Time Windows**: 1min, 5min, 15min, 30min, 1hour aggregations
- **Rich Metrics**: Volume, price changes, trade counts per window

### 3. **Operational Excellence**
- **Monitoring**: Built-in metrics and health checks
- **Reliability**: Error handling and graceful degradation
- **Maintainability**: Clean interfaces and separation of concerns
- **Performance**: Optimized batching and compression

## 🔮 Future Enhancements

1. **Schema Registry Integration**: For better message evolution
2. **Dead Letter Queue**: For failed message handling
3. **Metrics Dashboard**: Grafana integration for monitoring
4. **Alert System**: Automated alerts for publishing failures
5. **Token Metadata**: Integration with token info service for symbol/name
6. **Custom Time Windows**: Dynamic time window configuration
7. **Data Retention**: Configurable message retention policies

## 📝 Summary

This implementation is fully integrated into the current aggregatorService architecture with the following characteristics:

- ✅ **Enterprise-grade**: Production-ready stability and performance
- ✅ **Scalable**: Horizontally scalable architecture
- ✅ **Maintainable**: Clean interfaces and separation of concerns
- ✅ **Observable**: Comprehensive monitoring and logging
- ✅ **Performant**: Optimized batch processing and compression
- ✅ **Reliable**: Error handling and graceful shutdown

Aggregate results are now available through the `aggregate-info` topic for real-time consumption by other services.