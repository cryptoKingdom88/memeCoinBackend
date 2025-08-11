#!/bin/bash

# Quick Test Script - 60 second test with progressive token launches

echo "=== Quick Test (60 seconds, progressive token launches) ==="

# Set test parameters for quick test
export TOKEN_COUNT=10
export TRADES_PER_TOKEN=15
export TRADE_VARIATION=10
export BATCH_INTERVAL=200ms
export TEST_DURATION=60s
export TOKEN_LAUNCH_INTERVAL=5s
export INITIAL_TOKENS=2
export MIN_PRICE=0.01
export MAX_PRICE=5.0
export MIN_AMOUNT=50.0
export MAX_AMOUNT=5000.0
export BUY_PROBABILITY=0.5

echo "Configuration:"
echo "  Duration: 60 seconds"
echo "  Total tokens: 10 (starting with 2)"
echo "  Token launch interval: 5 seconds"
echo "  Batch interval: 200ms (5 batches/sec)"
echo "  Trades per token per batch: 5-25 trades"
echo "  Expected progression:"
echo "    0-5s:   2 tokens active"
echo "    5-10s:  3 tokens active"
echo "    10-15s: 4 tokens active"
echo "    ..."
echo "    40-45s: 10 tokens active"
echo "    45-60s: All 10 tokens active"
echo

# Build and run
go build -o testCollectorService
if [ $? -ne 0 ]; then
    echo "❌ Build failed"
    exit 1
fi

echo "Starting progressive token launch test..."
echo "Watch for 🚀 token launch messages every 5 seconds!"
echo
./testCollectorService