# Performance Optimization Guide

This document describes the performance optimization features available in the Aggregator Service.

## Overview

The Aggregator Service includes comprehensive performance tuning capabilities designed to optimize for different workload patterns:

- **Memory optimization** with automatic garbage collection tuning
- **Concurrency optimization** with goroutine pool management
- **Redis connection optimization** with pipeline and connection tuning
- **Real-time performance monitoring** with automatic adjustments

## Configuration

### Environment Variables

The following environment variables control performance tuning:

#### Redis Performance
```bash
REDIS_MAX_RETRIES=3              # Maximum retry attempts for Redis operations
REDIS_RETRY_DELAY=100ms          # Delay between retry attempts
REDIS_PIPELINE_SIZE=100          # Number of commands to batch in pipeline
REDIS_READ_TIMEOUT=3s            # Read timeout for Redis operations
REDIS_WRITE_TIMEOUT=3s           # Write timeout for Redis operations
REDIS_IDLE_TIMEOUT=5m            # Idle connection timeout
REDIS_MAX_CONN_AGE=30m           # Maximum connection age before renewal
```

#### Memory Optimization
```bash
GC_TARGET_PERCENTAGE=100         # Garbage collection target percentage
MEMORY_LIMIT_MB=512              # Soft memory limit in megabytes
TRADE_BUFFER_SIZE=1000           # Size of trade processing buffers
```

#### Concurrency Tuning
```bash
PROCESSOR_CHANNEL_SIZE=1000      # Channel buffer size for processors
WORKER_QUEUE_SIZE=10000          # Worker pool queue size
BATCH_PROCESSING_SIZE=100        # Batch size for processing operations
MAX_WORKERS=100                  # Maximum number of worker goroutines
MAX_TOKEN_PROCESSORS=1000        # Maximum number of token processors
```

## Performance Monitoring

### Automatic Monitoring

The service includes a background performance monitor that:

- Collects system metrics every 10 seconds
- Performs automatic tuning every 60 seconds
- Detects performance issues and logs warnings
- Adjusts GC settings based on CPU usage

### Manual Performance Operations

#### HTTP Endpoints

The service exposes several HTTP endpoints for performance management:

##### Get Performance Statistics
```bash
GET /performance
```
Returns current performance statistics including memory usage, GC stats, and runtime information.

##### Force Garbage Collection
```bash
POST /performance/gc
```
Forces immediate garbage collection and returns before/after statistics.

##### Optimize for High Throughput
```bash
POST /performance/optimize/throughput
```
Adjusts settings to optimize for high-throughput scenarios:
- Increases GC target to reduce GC frequency
- Optimizes for sustained high load

##### Optimize for Low Latency
```bash
POST /performance/optimize/latency
```
Adjusts settings to optimize for low-latency scenarios:
- Decreases GC target for more frequent cleanup
- Optimizes for consistent response times

## Performance Tuning Strategies

### High Throughput Workloads

For workloads with high trade volumes (>10,000 trades/second):

1. **Increase GC target**: Set `GC_TARGET_PERCENTAGE=200` to reduce GC frequency
2. **Increase buffer sizes**: Set `TRADE_BUFFER_SIZE=2000` and `WORKER_QUEUE_SIZE=20000`
3. **Optimize Redis**: Increase `REDIS_PIPELINE_SIZE=200` for better batching
4. **Use throughput optimization**: Call `POST /performance/optimize/throughput`

Example configuration:
```bash
GC_TARGET_PERCENTAGE=200
TRADE_BUFFER_SIZE=2000
WORKER_QUEUE_SIZE=20000
REDIS_PIPELINE_SIZE=200
MAX_WORKERS=200
```

### Low Latency Workloads

For workloads requiring consistent low latency (<10ms):

1. **Decrease GC target**: Set `GC_TARGET_PERCENTAGE=50` for frequent cleanup
2. **Reduce buffer sizes**: Set `TRADE_BUFFER_SIZE=500` to minimize queuing
3. **Optimize timeouts**: Reduce `REDIS_READ_TIMEOUT=1s` and `REDIS_WRITE_TIMEOUT=1s`
4. **Use latency optimization**: Call `POST /performance/optimize/latency`

Example configuration:
```bash
GC_TARGET_PERCENTAGE=50
TRADE_BUFFER_SIZE=500
REDIS_READ_TIMEOUT=1s
REDIS_WRITE_TIMEOUT=1s
BATCH_PROCESSING_SIZE=50
```

### Memory-Constrained Environments

For environments with limited memory:

1. **Set memory limit**: Configure `MEMORY_LIMIT_MB=256`
2. **Aggressive GC**: Set `GC_TARGET_PERCENTAGE=50`
3. **Reduce buffers**: Lower `TRADE_BUFFER_SIZE=500` and `WORKER_QUEUE_SIZE=5000`
4. **Limit processors**: Set `MAX_TOKEN_PROCESSORS=500`

Example configuration:
```bash
MEMORY_LIMIT_MB=256
GC_TARGET_PERCENTAGE=50
TRADE_BUFFER_SIZE=500
WORKER_QUEUE_SIZE=5000
MAX_TOKEN_PROCESSORS=500
```

## Monitoring and Alerting

### Key Metrics to Monitor

1. **Memory Usage**
   - `heap_alloc_mb`: Current heap allocation
   - `heap_inuse_mb`: Heap memory in use
   - `gc_cpu_fraction`: CPU time spent in GC

2. **Performance Metrics**
   - `num_goroutines`: Current goroutine count
   - `active_tokens`: Number of active token processors
   - `worker_count`: Current worker pool size

3. **Redis Performance**
   - `redis_latency_avg_ms`: Average Redis operation latency
   - `redis_success_rate`: Redis operation success rate

### Alert Thresholds

Recommended alert thresholds:

- **High Memory Usage**: `heap_inuse_mb > 400`
- **High GC CPU**: `gc_cpu_fraction > 0.1`
- **Goroutine Leak**: `num_goroutines > 5000`
- **High Redis Latency**: `redis_latency_avg_ms > 50`

## Testing Performance

Use the provided performance testing script:

```bash
./scripts/performance_test.sh
```

This script will:
1. Check service health
2. Get current performance statistics
3. Test performance optimization endpoints
4. Demonstrate GC optimization
5. Show before/after metrics

## Troubleshooting

### High Memory Usage

If memory usage is consistently high:

1. Check for goroutine leaks: `GET /performance`
2. Force garbage collection: `POST /performance/gc`
3. Reduce buffer sizes in configuration
4. Enable memory limit: `MEMORY_LIMIT_MB=<value>`

### High Latency

If processing latency is high:

1. Optimize for latency: `POST /performance/optimize/latency`
2. Reduce batch sizes: `BATCH_PROCESSING_SIZE=50`
3. Increase worker count: `MAX_WORKERS=200`
4. Check Redis performance metrics

### High CPU Usage

If CPU usage is consistently high:

1. Check GC CPU fraction in metrics
2. Increase GC target: `GC_TARGET_PERCENTAGE=200`
3. Optimize for throughput: `POST /performance/optimize/throughput`
4. Monitor goroutine count for leaks

## Best Practices

1. **Start with defaults** and measure performance before tuning
2. **Monitor continuously** using the built-in metrics
3. **Tune incrementally** - change one parameter at a time
4. **Test under load** to validate optimizations
5. **Document changes** and their impact on performance
6. **Use appropriate optimization** for your workload pattern
7. **Set up alerting** on key performance metrics