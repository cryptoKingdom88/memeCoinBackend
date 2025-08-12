#!/bin/bash

# Create Kafka topics with 20-second TTL
# Run this script after starting Kafka

KAFKA_BROKERS=${KAFKA_BROKERS:-"localhost:9092"}
RETENTION_MS=20000  # 20 seconds
SEGMENT_MS=5000     # 5 seconds (smaller segments for faster cleanup)

echo "Creating Kafka topics with 20-second TTL..."

# Function to create topic with TTL
create_topic_with_ttl() {
    local topic_name=$1
    local partitions=${2:-3}
    local replication=${3:-1}
    
    echo "Creating topic: $topic_name"
    
    # Create topic
    docker exec kafka kafka-topics \
        --bootstrap-server localhost:9092 \
        --create \
        --topic $topic_name \
        --partitions $partitions \
        --replication-factor $replication \
        --if-not-exists \
        --config retention.ms=$RETENTION_MS \
        --config segment.ms=$SEGMENT_MS \
        --config min.cleanable.dirty.ratio=0.01 \
        --config delete.retention.ms=1000
    
    if [ $? -eq 0 ]; then
        echo "✅ Topic $topic_name created successfully"
    else
        echo "❌ Failed to create topic $topic_name"
    fi
}

# Create topics
create_topic_with_ttl "token-info" 3 1
create_topic_with_ttl "trade-info" 3 1  
create_topic_with_ttl "aggregate-info" 3 1

echo ""
echo "🎯 All topics configured with:"
echo "  - Retention: 20 seconds"
echo "  - Segment size: 5 seconds"
echo "  - Cleanup policy: delete"
echo ""

# Verify topics
echo "Verifying topic configurations:"
docker exec kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --describe \
    --topic token-info,trade-info,aggregate-info

echo ""
echo "✅ Kafka topics are ready!"
echo "Messages will automatically expire after 20 seconds"