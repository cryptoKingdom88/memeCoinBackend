#!/bin/bash

# Quick Test Script - 30 second test with high frequency

echo "=== Quick Test (30 seconds, high frequency) ==="

# Set test parameters for quick test
export TOKEN_COUNT=20
export TRADES_PER_TOKEN=20
export TRADE_VARIATION=15
export BATCH_INTERVAL=100ms
export TEST_DURATION=30s
export MIN_PRICE=0.01
export MAX_PRICE=5.0
export MIN_AMOUNT=50.0
export MAX_AMOUNT=5000.0
export BUY_PROBABILITY=0.5

echo "Configuration:"
echo "  Duration: 30 seconds"
echo "  Batch Interval: 100ms (10 batches/sec)"
echo "  Trades per batch: ~20 tokens × (5-35) trades = 100-700 trades"
echo "  Expected total: ~3,000-21,000 trades"
echo

# Build and run
go build -o testCollectorService
if [ $? -ne 0 ]; then
    echo "❌ Build failed"
    exit 1
fi

echo "Starting quick test..."
./testCollectorService