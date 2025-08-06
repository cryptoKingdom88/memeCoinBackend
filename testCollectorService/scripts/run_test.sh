#!/bin/bash

# Test Collector Service Runner Script

echo "=== Test Collector Service Runner ==="
echo

# Check if Kafka is running
echo "Checking Kafka connectivity..."
if ! nc -z localhost 9092; then
    echo "❌ Kafka is not running on localhost:9092"
    echo "Please start Kafka first:"
    echo "  docker run -d --name kafka -p 9092:9092 apache/kafka:latest"
    exit 1
fi
echo "✅ Kafka is running"

# Build the service
echo "Building test collector service..."
go build -o testCollectorService
if [ $? -ne 0 ]; then
    echo "❌ Build failed"
    exit 1
fi
echo "✅ Build successful"

# Set default environment if .env doesn't exist
if [ ! -f .env ]; then
    echo "Creating .env from .env.example..."
    cp .env.example .env
fi

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# Show configuration
echo
echo "=== Test Configuration ==="
echo "Kafka Brokers: ${KAFKA_BROKERS:-localhost:9092}"
echo "Kafka Topic: ${KAFKA_TOPIC:-trade-info}"
echo "Token Count: ${TOKEN_COUNT:-20}"
echo "Trades Per Token: ${TRADES_PER_TOKEN:-15} ± ${TRADE_VARIATION:-10}"
echo "Batch Interval: ${BATCH_INTERVAL:-200ms}"
echo "Test Duration: ${TEST_DURATION:-5m}"
echo "Price Range: $${MIN_PRICE:-0.001} - $${MAX_PRICE:-10.0}"
echo

# Ask for confirmation
read -p "Start the test with these settings? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Test cancelled"
    exit 0
fi

# Run the test
echo "Starting test collector service..."
echo "Press Ctrl+C to stop the test early"
echo
./testCollectorService