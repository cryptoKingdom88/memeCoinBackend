# Token Trade Aggregator Service

A Go service that consumes token trade history data from Kafka topics and performs real-time aggregation of trading metrics.

## Features

- **Real-time Aggregation**: Processes token trade data and generates aggregated metrics
- **Configurable Intervals**: Adjustable aggregation time windows
- **Trade Metrics**: Calculates volume, price, and trade count statistics
- **Kafka Integration**: Consumes from trade-info topic with dedicated consumer group
- **Graceful Shutdown**: Handles termination signals and processes remaining data

## Aggregation Metrics

For each token, the service calculates:

- **Total Volume**: Combined buy and sell volume in USD
- **Buy/Sell Volume**: Separate volume calculations for each trade type
- **Trade Counts**: Total, buy, and sell trade counts
- **Price Metrics**: Last price, high price, low price in the period
- **Time Window**: Start and end time of the aggregation period

## Configuration

Configure the service using environment variables in `.env` file:

```env
# Kafka Configuration
KAFKA_BROKERS=localhost:9092

# Aggregation Configuration (in seconds)
AGGREGATION_INTERVAL=60
```

## Setup

1. **Install Dependencies**:

   ```bash
   go mod tidy
   ```

2. **Configure Environment**:

   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

3. **Run Service**:
   ```bash
   go run main.go
   ```

## Kafka Topics

- `trade-info`: Token trade history messages (input)

## JSON Message Format

### Trade History Message (Input)

```json
{
  "token": "0x1234...",
  "wallet": "0x5678...",
  "sell_buy": "buy",
  "native_amount": "1.5",
  "token_amount": "100",
  "price_usd": "0.015",
  "trans_time": "2024-01-01T00:00:00Z",
  "tx_hash": "0xabcd..."
}
```

## Architecture

```
Kafka Topic → JSON Parser → Aggregation Buffer → Metrics Calculator → Output
     ↓             ↓              ↓                    ↓              ↓
trade-info   TradeHistory   Timer-based         TradeAggregation   Logs/API
```

## Consumer Group

- Uses `aggregator-trade-group` consumer group (separate from dbSaveService)
- Allows parallel processing with database save service

## Aggregation Output

Currently outputs aggregated metrics to logs. Future implementations can:

- Send to another Kafka topic
- Store in cache (Redis)
- Expose via REST API
- Send to monitoring systems

## Logging

The service provides comprehensive logging:

- ✅ Success operations
- ❌ Error conditions
- 🚀 Service startup
- 🛑 Service shutdown
- 📝 Buffer operations
- 🔄 Aggregation processing
- 📊 Aggregation results
