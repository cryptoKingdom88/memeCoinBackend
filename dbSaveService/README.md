# DB Save Service

A Go service that consumes token information and trade history data from Kafka topics, parses JSON messages, and batch inserts them into PostgreSQL database at configurable intervals.

## Features

- **JSON Parsing**: Automatically parses Kafka messages into `TokenInfo` and `TokenTradeHistory` structs
- **Batch Processing**: Configurable batch interval for efficient database operations
- **PostgreSQL Integration**: Uses PostgreSQL with snake_case field names and auto-increment IDs
- **Graceful Shutdown**: Handles termination signals and processes remaining data before shutdown
- **Error Handling**: Comprehensive error handling and logging
- **Duplicate Prevention**: Uses UPSERT for token info and conflict handling for trade history

## Database Schema

### token_info Table
- `id` (SERIAL PRIMARY KEY): Auto-increment ID
- `token`: Token contract address
- `symbol`: Token symbol
- `name`: Token name
- `meta_info`: Token metadata
- `total_supply`: Total token supply
- `create_time`: Token creation timestamp

### token_trade_history Table
- `id` (SERIAL PRIMARY KEY): Auto-increment ID
- `token`: Token contract address
- `wallet`: Wallet address
- `sell_buy`: Trade type (sell/buy)
- `native_amount`: Native currency amount
- `token_amount`: Token amount
- `price_usd`: USD price
- `trans_time`: Transaction timestamp
- `tx_hash`: Transaction hash (unique)

## Configuration

Configure the service using environment variables in `.env` file:

```env
# Kafka Configuration
KAFKA_BROKERS=localhost:9092

# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=password
DB_NAME=memecoin_db

# Batch Processing (in seconds)
BATCH_INTERVAL=30
```

## Setup

1. **Install Dependencies**:
   ```bash
   go mod tidy
   ```

2. **Setup PostgreSQL**:
   ```bash
   # Create database
   createdb memecoin_db
   
   # Run initialization script
   psql -d memecoin_db -f sql/init.sql
   ```

3. **Configure Environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

4. **Run Service**:
   ```bash
   go run main.go
   ```

## Kafka Topics

- `token-info`: Token information messages
- `trade-info`: Token trade history messages

## JSON Message Format

### Token Info Message
```json
{
  "token": "0x1234...",
  "symbol": "TOKEN",
  "name": "Token Name",
  "meta_info": "Additional metadata",
  "total_supply": "1000000",
  "create_time": "2024-01-01T00:00:00Z"
}
```

### Trade History Message
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
Kafka Topics → JSON Parser → Model Converter → Batch Processor → PostgreSQL
     ↓              ↓              ↓               ↓              ↓
token-info    packet.TokenInfo → DBTokenInfo → Buffer Queue → token_info
trade-info  packet.TradeHistory → DBTradeHistory → Timer-based → token_trade_history
```

### Data Flow
1. **Kafka Consumer**: Receives JSON messages from topics
2. **JSON Parser**: Parses messages into `packet` structs (shared between microservices)
3. **Model Converter**: Converts `packet` structs to database-specific models with snake_case fields
4. **Batch Processor**: Buffers converted models and processes them at configured intervals
5. **Database**: Stores data in PostgreSQL with proper schema

## Logging

The service provides comprehensive logging:
- ✅ Success operations
- ❌ Error conditions
- 🚀 Service startup
- 🛑 Service shutdown
- 📝 Buffer operations
- 🔄 Batch processing

## Error Handling

- **JSON Parse Errors**: Logged and skipped, service continues
- **Database Errors**: Logged per record, batch continues
- **Connection Errors**: Service attempts reconnection
- **Duplicate Data**: Handled via UPSERT/conflict resolution