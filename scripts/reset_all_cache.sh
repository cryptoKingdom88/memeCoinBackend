#!/bin/bash

# Reset all caches: Kafka topics + Redis + Service restart

echo "🧹 Resetting ALL caches (Kafka + Redis)..."

# 1. Clear Redis cache
echo "🔴 Clearing Redis cache..."
docker exec redis redis-cli FLUSHALL
if [ $? -eq 0 ]; then
    echo "✅ Redis cache cleared"
else
    echo "⚠️  Redis may not be running"
fi

# 2. Reset Kafka topics
echo ""
echo "🟡 Resetting Kafka topics..."
./scripts/reset_kafka_topics.sh

# 3. Show current status
echo ""
echo "📊 Current status:"
echo "  Redis keys: $(docker exec redis redis-cli DBSIZE 2>/dev/null || echo 'Redis not accessible')"
echo "  Kafka topics:"
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null || echo "  Kafka not accessible"

echo ""
echo "🎯 Complete cache reset finished!"
echo "All services are ready for fresh data"
echo ""
echo "💡 Next steps:"
echo "  1. Restart your services if they're running"
echo "  2. Start fresh data collection"