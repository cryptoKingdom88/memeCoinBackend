# Test Collector Service

A test service that generates random token trade data and sends it to Kafka for testing the Aggregator Service.

## Features

- **20 Fixed Token Addresses**: Generates consistent token addresses for testing
- **Random Trade Generation**: Creates realistic trade data with price movements
- **Configurable Parameters**: Adjustable trade frequency, amounts, and test duration
- **Real-time Statistics**: Shows generation rates and current token prices
- **Kafka Integration**: Sends data to the same topic used by the aggregator service

## Configuration

Copy `.env.example` to `.env` and adjust settings:

```bash
cp .env.example .env
```

### Key Parameters

- `TOKEN_COUNT=20`: Number of different tokens to simulate
- `TRADES_PER_TOKEN=15`: Base number of trades per token per batch
- `TRADE_VARIATION=10`: Random variation (±10, so 5-25 trades per token)
- `BATCH_INTERVAL=200ms`: Time between batches (200ms as requested)
- `TEST_DURATION=5m`: How long to run the test

### Trade Simulation

- **Price Movement**: Realistic random walk with ±5% changes per trade
- **Buy/Sell Ratio**: Configurable probability (default 50/50)
- **Amount Range**: Configurable min/max trade amounts
- **Wallet Variety**: Uses 100 different wallet addresses for diversity

## Running the Test

1. **Start Kafka** (if not already running):
```bash
# Using Docker
docker run -d --name kafka -p 9092:9092 apache/kafka:latest

# Or use your existing Kafka setup
```

2. **Build and run the test service**:
```bash
cd testCollectorService
go mod tidy
go build -o testCollectorService
./testCollectorService
```

3. **Start the Aggregator Service** (in another terminal):
```bash
cd aggregatorService
./aggregatorService
```

4. **Monitor the results**:
   - Test service will show generation statistics every 5 seconds
   - Aggregator service will show batch processing completion logs
   - Check Redis for aggregated data

## Expected Output

### Test Service Output:
```
Starting Test Collector Service
Configuration:
  Kafka Brokers: [localhost:9092]
  Token Count: 20
  Trades Per Token: 15 ± 10
  Batch Interval: 200ms
  ...

Statistics: 3000 trades in 10 batches (150.0 trades/sec, 5.0 batches/sec)
Sample token prices:
  0x0000000000000000000000000000000000000001: $0.123456
  0x0000000000000000000000000000000000000002: $2.345678
  ...
```

### Aggregator Service Output:
```
{"level":"INFO","message":"Batch processing completed","total_trades":300,"unique_tokens":20,"processing_time":"25.3ms","trades_per_sec":11857.7}
```

## Test Scenarios

### High Volume Test
```bash
export TRADES_PER_TOKEN=25
export TRADE_VARIATION=15
export BATCH_INTERVAL=100ms
```

### Long Duration Test
```bash
export TEST_DURATION=30m
export BATCH_INTERVAL=500ms
```

### Price Volatility Test
```bash
export MIN_PRICE=0.0001
export MAX_PRICE=100.0
export BUY_PROBABILITY=0.3  # More selling pressure
```

## Monitoring

The test generates approximately:
- **Base Load**: 20 tokens × 15 trades × 5 batches/sec = 1,500 trades/sec
- **With Variation**: 20 tokens × (5-25) trades × 5 batches/sec = 500-2,500 trades/sec
- **Per Token**: 75-125 trades per token per second

This should provide a good stress test for the aggregator service's sliding window calculations.

## Token Addresses Used

The service generates 20 fixed token addresses:
- `0x0000000000000000000000000000000000000001`
- `0x0000000000000000000000000000000000000002`
- ...
- `0x0000000000000000000000000000000000000014` (20th token)

These addresses are consistent across runs for predictable testing.