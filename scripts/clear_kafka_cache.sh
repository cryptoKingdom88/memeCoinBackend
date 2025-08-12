#!/bin/bash

# Quick Kafka cache clear script
# Forces immediate cleanup of old messages

echo "🧹 Clearing Kafka cache..."

# Force log cleanup for all topics
TOPICS=("token-info" "trade-info" "aggregate-info")

for topic in "${TOPICS[@]}"; do
    echo "Clearing cache for topic: $topic"
    
    # Trigger log cleanup by setting very short retention temporarily
    docker exec kafka kafka-configs \
        --bootstrap-server localhost:9092 \
        --entity-type topics \
        --entity-name $topic \
        --alter \
        --add-config retention.ms=1000 2>/dev/null
    
    sleep 2
    
    # Restore 20-second retention
    docker exec kafka kafka-configs \
        --bootstrap-server localhost:9092 \
        --entity-type topics \
        --entity-name $topic \
        --alter \
        --add-config retention.ms=20000 2>/dev/null
    
    echo "✅ Cache cleared for $topic"
done

echo ""
echo "🎯 Cache clearing complete!"
echo "Old messages should be cleaned up within a few seconds"