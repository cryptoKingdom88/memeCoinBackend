# Test Collector Service

A test service that generates random token trade data and sends it to Kafka for testing the Aggregator Service.

## Features

- **Progressive Token Launches**: Starts with a few tokens and launches new ones over time
- **Realistic Token Launch Simulation**: Generates token info with names, symbols, and metadata
- **Random Trade Generation**: Creates realistic trade data with price movements
- **Configurable Parameters**: Adjustable trade frequency, amounts, and test duration
- **Real-time Statistics**: Shows generation rates and current token prices
- **Dual Kafka Topics**: Sends token launches to `token-info` and trades to `trade-info`

## Configuration

Copy `.env.example` to `.env` and adjust settings:

```bash
cp .env.example .env
```

### Key Parameters

- `TOKEN_COUNT=20`: Total number of different tokens to simulate
- `INITIAL_TOKENS=3`: Number of tokens to start with
- `TOKEN_LAUNCH_INTERVAL=10s`: Time between new token launches
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
  Token Count: 20 (starting with 3)
  Token Launch Interval: 10s
  Trades Per Token: 15 ± 10
  Batch Interval: 200ms
  ...

🚀 New token launched: DogeCoin (DOGCOI) - Active tokens: 4/20
🚀 New token launched: PepeMars (PEPMAR) - Active tokens: 5/20

Statistics: 1500 trades in 10 batches (150.0 trades/sec, 5.0 batches/sec) - Active tokens: 5/20
Sample active token prices:
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

## Progressive Token Launch Simulation

The service simulates a realistic token launch scenario:

1. **Initial Phase**: Starts with `INITIAL_TOKENS` (default: 3) active tokens
2. **Launch Phase**: Every `TOKEN_LAUNCH_INTERVAL` (default: 10s), launches a new token
3. **Mature Phase**: Once all tokens are launched, continues trading with all tokens

### Trade Volume Progression

- **Start**: 3 tokens × 15 trades × 5 batches/sec = 225 trades/sec
- **Mid-test**: 10 tokens × 15 trades × 5 batches/sec = 750 trades/sec  
- **Full capacity**: 20 tokens × 15 trades × 5 batches/sec = 1,500 trades/sec
- **With variation**: Up to 20 tokens × 25 trades × 5 batches/sec = 2,500 trades/sec

This progressive approach provides:
- **Realistic simulation** of new token launches
- **Gradual load increase** for testing system scalability
- **Token lifecycle testing** from launch to mature trading

## Token Information Generated

Each launched token includes:
- **Unique address**: `0x0000...0001`, `0x0000...0002`, etc.
- **Meme-style names**: "DogeCoin", "PepeMars", "ShibaFloki", etc.
- **Symbols**: Derived from names (e.g., "DOGCOI", "PEPMAR")
- **Metadata**: Initial price and launch information
- **Supply**: Random between 1B-10B tokens
- **Launch timestamp**: Exact time of token creation

## Quick Test

Run a 60-second test with progressive token launches:

```bash
./scripts/quick_test.sh
```

This will:
- Start with 2 tokens
- Launch a new token every 5 seconds
- Run for 60 seconds total
- Show token launch events with 🚀 emoji