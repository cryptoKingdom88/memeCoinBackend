#!/bin/bash

# Performance Testing Script for Aggregator Service
# This script demonstrates the performance optimization features

METRICS_URL="http://localhost:8080"

echo "=== Aggregator Service Performance Testing ==="
echo

# Function to make HTTP requests and format JSON output
make_request() {
    local url=$1
    local method=${2:-GET}
    echo "Making $method request to: $url"
    if [ "$method" = "POST" ]; then
        curl -s -X POST "$url" | jq '.' 2>/dev/null || curl -s -X POST "$url"
    else
        curl -s "$url" | jq '.' 2>/dev/null || curl -s "$url"
    fi
    echo
}

# Check if service is running
echo "1. Checking service health..."
make_request "$METRICS_URL/health"

echo "2. Checking service readiness..."
make_request "$METRICS_URL/health/ready"

echo "3. Getting current performance statistics..."
make_request "$METRICS_URL/performance"

echo "4. Getting current metrics..."
make_request "$METRICS_URL/metrics/json"

echo "5. Forcing garbage collection..."
make_request "$METRICS_URL/performance/gc" "POST"

echo "6. Optimizing for high throughput..."
make_request "$METRICS_URL/performance/optimize/throughput" "POST"

echo "7. Getting performance stats after throughput optimization..."
make_request "$METRICS_URL/performance"

echo "8. Optimizing for low latency..."
make_request "$METRICS_URL/performance/optimize/latency" "POST"

echo "9. Getting performance stats after latency optimization..."
make_request "$METRICS_URL/performance"

echo "10. Final health check..."
make_request "$METRICS_URL/health"

echo "=== Performance testing completed ==="