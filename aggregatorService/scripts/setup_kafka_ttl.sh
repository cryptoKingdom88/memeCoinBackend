#!/bin/bash

# Kafka Topic TTL Setup Script
# Sets message retention to 20 seconds for real-time data

KAFKA_BROKERS=${KAFKA_BROKERS:-"localhost:9092"}
RETENTION_MS=20000  # 20 seconds

echo "Setting up Kafka topics with 20-second TTL..."

# Create or modify topics with TTL
kafka-topics --bootstrap-server $KAFKA_BROKERS \
    --alter --topic trade-info \
    --config retention.ms=$RETENTION_MS \
    --config segment.ms=5000 \
    --config min.cleanable.dirty.ratio=0.01

kafka-topics --bootstrap-server $KAFKA_BROKERS \
    --alter --topic aggregate-info \
    --config retention.ms=$RETENTION_MS \
    --config segment.ms=5000 \
    --config min.cleanable.dirty.ratio=0.01

kafka-topics --bootstrap-server $KAFKA_BROKERS \
    --alter --topic token-info \
    --config retention.ms=$RETENTION_MS \
    --config segment.ms=5000 \
    --config min.cleanable.dirty.ratio=0.01

echo "✅ Kafka topics configured with 20-second TTL"
echo "Messages will be automatically deleted after 20 seconds"

# Verify configuration
echo ""
echo "Verifying topic configurations:"
kafka-topics --bootstrap-server $KAFKA_BROKERS --describe --topic trade-info
kafka-topics --bootstrap-server $KAFKA_BROKERS --describe --topic aggregate-info
kafka-topics --bootstrap-server $KAFKA_BROKERS --describe --topic token-info