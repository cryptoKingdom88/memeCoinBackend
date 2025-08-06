# Aggregator Service

Real-time token trade data aggregation service.

## Overview

The Aggregator Service consumes token trade data from Kafka in real-time, calculates aggregated data for various time windows, and stores the results in Redis.

## Key Features

- **Real-time Trade Processing**: Consumes trade data from Kafka in real-time
- **Sliding Window Aggregation**: Calculates aggregated data for 1min, 5min, 15min, 30min, and 1hour windows
- **Redis Storage**: Efficiently stores aggregated data in Redis
- **Performance Optimization**: Memory usage, GC tuning, and concurrency optimization
- **Monitoring**: Metrics collection and performance monitoring via HTTP endpoints
- **Maintenance**: Automatic data cleanup and recovery functionality

## Architecture

```
Kafka → Consumer → Block Aggregator → Token Processors → Redis
                                   ↓
                              Metrics Collector
                                   ↓
                              Performance Monitor
```

## Configuration

Configure the service using environment variables. See `.env.example` for reference.

### Key Settings

- **Kafka**: Broker addresses, topic, consumer group
- **Redis**: Connection info, pool size, timeouts
- **Time Windows**: Time units for aggregation
- **Performance Tuning**: GC settings, memory limits, concurrency settings
- **Logging**: Log level configuration (DEBUG, INFO, WARN, ERROR, FATAL)

## Running the Service

```bash
# Set up environment variables
cp .env.example .env
# Edit .env file to adjust settings

# Build the service
go build -o aggregatorService

# Run the service
./aggregatorService
```

## Monitoring & Metrics

The service provides HTTP endpoints for monitoring and operational insights:

### Health Check Endpoints
- `GET /health` - Overall service health status
- `GET /health/live` - Liveness probe (is the service running?)
- `GET /health/ready` - Readiness probe (is the service ready to accept traffic?)

### Metrics Endpoints
- `GET /metrics` - Prometheus format metrics for monitoring systems
- `GET /metrics/json` - JSON format metrics for debugging
- `GET /performance` - Performance statistics and tuning information

### Performance Management Endpoints
- `POST /performance/gc` - Force garbage collection
- `POST /performance/optimize/throughput` - Optimize for high throughput
- `POST /performance/optimize/latency` - Optimize for low latency

### What Metrics Are Used For

**Operational Monitoring:**
- Track service health and availability
- Monitor processing performance (trades/second)
- Detect bottlenecks and performance issues
- Alert on service failures or degradation

**Performance Optimization:**
- Memory usage and garbage collection metrics
- Processing latency and throughput statistics
- Redis operation performance
- Worker pool utilization

**Business Intelligence:**
- Total trades processed
- Active token count
- Processing volume trends
- System capacity planning

**Integration with Monitoring Systems:**
- Prometheus/Grafana dashboards
- Alerting systems (PagerDuty, Slack)
- Log aggregation (ELK stack)
- APM tools (DataDog, New Relic)

## Performance Optimization

For detailed performance optimization information, see [PERFORMANCE.md](PERFORMANCE.md).

### Performance Testing

```bash
# Run performance test script
./scripts/performance_test.sh
```

## Logging

The service uses structured JSON logging. Control log output volume through log levels:

- `DEBUG`: All debug information (development use)
- `INFO`: General information messages (default)
- `WARN`: Warnings and important events only (production recommended)
- `ERROR`: Error messages only
- `FATAL`: Fatal errors only

### Batch Processing Completion Logs

When a complete batch of trades from Kafka is processed, the following information is logged:

```json
{
  "level": "INFO",
  "message": "Batch processing completed",
  "total_trades": 150,
  "unique_tokens": 12,
  "processing_time": "25.3ms",
  "trades_per_sec": 5928.5
}
```

### Log Level Configuration

```bash
# Production environment (minimal logs)
export LOG_LEVEL=WARN

# Development environment (detailed logs)
export LOG_LEVEL=DEBUG
```

## Development

### Running Tests

```bash
go test ./...
```

### Log Testing

```bash
# Test log level configuration
./test_logs.sh
```

### Code Structure

- `main.go` - Main service entry point
- `config/` - Configuration management
- `kafka/` - Kafka consumer
- `processor/` - Trade data processing
- `aggregation/` - Sliding window aggregation
- `redis/` - Redis connection and data storage
- `metrics/` - Metrics collection and HTTP server
- `performance/` - Performance tuning and monitoring
- `maintenance/` - Maintenance service
- `logging/` - Structured logging

## License

This project is distributed under the MIT License.