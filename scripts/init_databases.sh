#!/bin/bash
set -e

echo "Initializing databases..."

# Wait for PostgreSQL
echo "Waiting for PostgreSQL to be ready..."
until docker exec postgres pg_isready -U wkdgh -d commerce_analytics > /dev/null 2>&1; do
    echo "PostgreSQL not ready yet, waiting..."
    sleep 5
done
echo "PostgreSQL is ready!"

# Run PostgreSQL migrations
echo "Running PostgreSQL migrations..."
docker exec -i postgres psql -U wkdgh -d commerce_analytics < database/migrations/001_create_tables.sql
docker exec -i postgres psql -U wkdgh -d commerce_analytics < database/migrations/002_create_indexes.sql
docker exec -i postgres psql -U wkdgh -d commerce_analytics < database/migrations/003_create_views.sql
echo "PostgreSQL migrations complete!"

# Wait for ClickHouse
echo "Waiting for ClickHouse to be ready..."
until docker exec clickhouse clickhouse-client --query "SELECT 1" > /dev/null 2>&1; do
    echo "ClickHouse not ready yet, waiting..."
    sleep 5
done
echo "ClickHouse is ready!"

# Run ClickHouse init
echo "Running ClickHouse initialization..."
docker exec -i clickhouse clickhouse-client --multiquery < database/clickhouse/init.sql
echo "ClickHouse initialization complete!"

# Wait for Elasticsearch
echo "Waiting for Elasticsearch to be ready..."
until curl -s http://localhost:9200/_cluster/health > /dev/null 2>&1; do
    echo "Elasticsearch not ready yet, waiting..."
    sleep 5
done
echo "Elasticsearch is ready!"

# Create Elasticsearch indices
echo "Creating Elasticsearch indices..."
curl -X PUT "localhost:9200/logs-producers" -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "properties": {
      "@timestamp": { "type": "date" },
      "level": { "type": "keyword" },
      "logger": { "type": "keyword" },
      "message": { "type": "text" },
      "platform": { "type": "keyword" },
      "error": { "type": "text" }
    }
  }
}
' 2>/dev/null || true

curl -X PUT "localhost:9200/logs-streaming" -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "properties": {
      "@timestamp": { "type": "date" },
      "level": { "type": "keyword" },
      "logger": { "type": "keyword" },
      "message": { "type": "text" },
      "batch_id": { "type": "keyword" },
      "records_processed": { "type": "long" }
    }
  }
}
' 2>/dev/null || true

curl -X PUT "localhost:9200/content-search" -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "properties": {
      "@timestamp": { "type": "date" },
      "platform": { "type": "keyword" },
      "content_id": { "type": "keyword" },
      "title": { "type": "text", "analyzer": "korean" },
      "description": { "type": "text", "analyzer": "korean" },
      "keywords": { "type": "keyword" },
      "sentiment": { "type": "keyword" },
      "sentiment_score": { "type": "float" },
      "views": { "type": "long" },
      "likes": { "type": "long" },
      "comments": { "type": "long" }
    }
  },
  "settings": {
    "analysis": {
      "analyzer": {
        "korean": {
          "type": "custom",
          "tokenizer": "standard",
          "filter": ["lowercase"]
        }
      }
    }
  }
}
' 2>/dev/null || true

echo ""
echo "All databases initialized successfully!"
