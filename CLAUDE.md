# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Real-time multi-platform commerce analytics pipeline that collects data from YouTube, Twitter, TikTok, Naver Shopping, and Google Trends, processes it with Kafka + Spark Streaming, and generates media commerce insights.

## Build and Run Commands

```bash
# Initial setup
make setup                    # Create .env, install dependencies

# Start/Stop services
make start                    # Start all Docker services
make stop                     # Stop all services
make restart                  # Restart services
make clean                    # Remove containers and volumes

# Kafka operations
make topics                   # Create Kafka topics
make topics-list              # List all topics

# Database operations
make db-init                  # Initialize all databases
make db-migrate               # Run PostgreSQL migrations

# Run producers
make producers                # Start all producers
make producer-youtube         # Start YouTube producer only
make producer-twitter         # Start Twitter producer only

# Run streaming
make streaming                # Start Spark Streaming job
make streaming-local          # Run streaming locally (python -m streaming.main)

# Testing
make test                     # Run all tests
make test-unit                # Run unit tests only
pytest tests/unit/test_producers.py -v  # Run specific test file
pytest tests/unit/test_producers.py::TestBaseProducer -v  # Run specific class

# Linting
make lint                     # Run flake8 and black check
make format                   # Auto-format with black

# Monitoring
make health                   # Check all service health
```

## Architecture

```
producers/           → Kafka topics → streaming/          → Sinks
├── youtube_producer     youtube-raw     ├── transformations/   ├── PostgreSQL
├── twitter_producer     twitter-raw     │   ├── parsers.py     ├── ClickHouse
├── tiktok_producer      tiktok-raw      │   ├── aggregations   ├── Redis
├── naver_producer       naver-raw       │   └── enrichment     ├── Elasticsearch
└── trends_producer      trends-raw      ├── ml/                └── Slack
                                         │   ├── sentiment
                                         │   ├── trend_detection
                                         │   └── keyword_extraction
                                         └── main.py
```

## Key Patterns

### Producers
- All producers inherit from `BaseProducer` (`producers/base_producer.py`)
- Each producer implements `fetch_data()` and `transform_data()` methods
- Use `QuotaManager` for API rate limiting
- Kafka messages use JSON serialization with schema: `{message_id, platform, timestamp, ingestion_time, data}`

### Streaming
- Entry point: `streaming/main.py` (CommerceAnalyticsStreaming class)
- Schemas defined in `streaming/schemas/`
- All platforms have dedicated parsers in `streaming/transformations/parsers.py`
- Unified view created for cross-platform analysis
- Watermark: 10 minutes for late data handling

### ML Components
- Sentiment: Korean BERT model (klue/bert-base) with simple keyword fallback
- Trend detection: Z-score based (threshold: 3.0)
- Keywords: TF-IDF with Korean stopword filtering

### Sinks
- PostgreSQL: Aggregated metrics, trending content
- ClickHouse: Time-series data, raw events
- Redis: Real-time rankings with TTL
- Elasticsearch: Full-text search
- Slack: Alert notifications for trending content

## Configuration Files

- `config/platforms.yaml` - Platform API settings (channels, keywords, intervals)
- `config/kafka/topics.json` - Kafka topic definitions
- `config/spark/spark-defaults.conf` - Spark configuration
- `.env` - Environment variables (API keys, database credentials)

## Database Schemas

- PostgreSQL migrations: `database/migrations/001_create_tables.sql`, `002_create_indexes.sql`, `003_create_views.sql`
- ClickHouse init: `database/clickhouse/init.sql`

## Docker Services

Core services with their ports:
- Kafka (3 brokers): 9092, 9093, 9094
- Kafka UI: 8080
- PostgreSQL: 5432
- ClickHouse: 8123 (HTTP), 9000 (native)
- Redis: 6379
- Elasticsearch: 9200
- Prometheus: 9090
- Grafana: 3000
- Superset: 8088
- Spark Master: 7077, UI: 8081

## Environment Variables

Required API keys in `.env`:
- `YOUTUBE_API_KEY`
- `TWITTER_BEARER_TOKEN`
- `NAVER_CLIENT_ID`, `NAVER_CLIENT_SECRET`
- `SLACK_WEBHOOK_URL` (optional, for alerts)

## Testing

Test fixtures in `tests/conftest.py` provide:
- `kafka_config`, `mock_kafka_producer`
- `sample_youtube_data`, `sample_twitter_data`, etc.
- `spark_session` (requires PySpark)
- Mock API clients for all platforms
