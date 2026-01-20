#!/bin/bash
set -e

echo "Creating Kafka topics..."

KAFKA_CONTAINER="kafka-1"
BOOTSTRAP_SERVER="localhost:9092"

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
until docker exec $KAFKA_CONTAINER kafka-broker-api-versions --bootstrap-server $BOOTSTRAP_SERVER > /dev/null 2>&1; do
    echo "Kafka not ready yet, waiting..."
    sleep 5
done
echo "Kafka is ready!"

# Create topics
create_topic() {
    local topic_name=$1
    local partitions=$2
    local replication=$3

    echo "Creating topic: $topic_name"
    docker exec $KAFKA_CONTAINER kafka-topics \
        --create \
        --if-not-exists \
        --bootstrap-server $BOOTSTRAP_SERVER \
        --topic $topic_name \
        --partitions $partitions \
        --replication-factor $replication \
        --config retention.ms=604800000 \
        --config compression.type=snappy
}

# Raw data topics
create_topic "youtube-raw" 3 3
create_topic "twitter-raw" 3 3
create_topic "tiktok-raw" 3 3
create_topic "naver-shopping-raw" 3 3
create_topic "google-trends-raw" 3 3

# Processed data topics
create_topic "processed-metrics" 5 3
create_topic "sentiment-results" 3 3
create_topic "trending-alerts" 1 3
create_topic "cross-platform-analysis" 3 3

echo ""
echo "Listing all topics:"
docker exec $KAFKA_CONTAINER kafka-topics --list --bootstrap-server $BOOTSTRAP_SERVER

echo ""
echo "Topics created successfully!"
