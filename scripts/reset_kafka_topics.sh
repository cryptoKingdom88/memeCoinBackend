#!/bin/bash

# Reset Kafka topics by deleting and recreating them
# This will clear all cached messages

KAFKA_BROKERS=${KAFKA_BROKERS:-"localhost:9092"}
RETENTION_MS=20000  # 20 seconds
SEGMENT_MS=5000     # 5 seconds

echo "🧹 Resetting Kafka topics to clear cache..."

# Topics to reset
TOPICS=("token-info" "trade-info" "aggregate-info")

# Function to delete topic
delete_topic() {
    local topic_name=$1
    echo "Deleting topic: $topic_name"
    
    docker exec kafka kafka-topics \
        --bootstrap-server localhost:9092 \
        --delete \
        --topic $topic_name 2>/dev/null
    
    if [ $? -eq 0 ]; then
        echo "✅ Topic $topic_name deleted"
    else
        echo "⚠️  Topic $topic_name may not exist or already deleted"
    fi
}

# Function to create topic with TTL
create_topic_with_ttl() {
    local topic_name=$1
    local partitions=${2:-3}
    local replication=${3:-1}
    
    echo "Creating topic: $topic_name with TTL"
    
    docker exec kafka kafka-topics \
        --bootstrap-server localhost:9092 \
        --create \
        --topic $topic_name \
        --partitions $partitions \
        --replication-factor $replication \
        --config retention.ms=$RETENTION_MS \
        --config segment.ms=$SEGMENT_MS \
        --config min.cleanable.dirty.ratio=0.01 \
        --config delete.retention.ms=1000 \
        --config cleanup.policy=delete
    
    if [ $? -eq 0 ]; then
        echo "✅ Topic $topic_name created with TTL"
    else
        echo "❌ Failed to create topic $topic_name"
    fi
}

# Check if Kafka is running
echo "Checking Kafka connection..."
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "❌ Kafka is not running or not accessible"
    echo "Please start Kafka first: docker-compose up -d"
    exit 1
fi

echo "✅ Kafka is running"
echo ""

# Delete all topics
echo "🗑️  Deleting existing topics..."
for topic in "${TOPICS[@]}"; do
    delete_topic $topic
done

echo ""
echo "⏳ Waiting 5 seconds for topic deletion to complete..."
sleep 5

# Recreate all topics with TTL
echo ""
echo "🔄 Recreating topics with 20-second TTL..."
for topic in "${TOPICS[@]}"; do
    create_topic_with_ttl $topic 3 1
done

echo ""
echo "📋 Verifying topic configurations:"
docker exec kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --describe \
    --topic token-info,trade-info,aggregate-info

echo ""
echo "🎯 Reset complete! All topics now have:"
echo "  - Retention: 20 seconds"
echo "  - Segment size: 5 seconds"
echo "  - Cleanup policy: delete"
echo "  - All old messages cleared"
echo ""
echo "✅ Kafka topics are ready for fresh data!"