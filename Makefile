.PHONY: help setup start stop restart logs clean test lint build

# Default target
help:
	@echo "Real-time Commerce Analytics Pipeline"
	@echo ""
	@echo "Usage:"
	@echo "  make setup          - Initial setup (create .env, install deps)"
	@echo "  make start          - Start all services"
	@echo "  make stop           - Stop all services"
	@echo "  make restart        - Restart all services"
	@echo "  make logs           - View logs"
	@echo "  make clean          - Remove containers and volumes"
	@echo ""
	@echo "Development:"
	@echo "  make test           - Run tests"
	@echo "  make lint           - Run linter"
	@echo "  make build          - Build Docker images"
	@echo ""
	@echo "Services:"
	@echo "  make start-infra    - Start infrastructure only"
	@echo "  make start-kafka    - Start Kafka cluster"
	@echo "  make start-dbs      - Start databases"
	@echo "  make start-spark    - Start Spark cluster"
	@echo ""
	@echo "Producers:"
	@echo "  make producers      - Start all producers"
	@echo "  make producer-youtube   - Start YouTube producer"
	@echo "  make producer-twitter   - Start Twitter producer"
	@echo "  make producer-tiktok    - Start TikTok producer"
	@echo "  make producer-naver     - Start Naver Shopping producer"
	@echo "  make producer-trends    - Start Google Trends producer"
	@echo ""
	@echo "Streaming:"
	@echo "  make streaming      - Start Spark Streaming job"
	@echo ""
	@echo "Kafka:"
	@echo "  make topics         - Create Kafka topics"
	@echo "  make topics-list    - List Kafka topics"
	@echo "  make topics-delete  - Delete all topics"
	@echo ""
	@echo "Database:"
	@echo "  make db-init        - Initialize databases"
	@echo "  make db-migrate     - Run migrations"
	@echo ""
	@echo "Monitoring:"
	@echo "  make health         - Check service health"

# Setup
setup:
	@echo "Setting up the project..."
	@if [ ! -f .env ]; then cp .env.example .env; echo "Created .env file"; fi
	@pip install -r requirements.txt
	@echo "Setup complete!"

# Docker Compose commands
start:
	docker-compose up -d
	@echo "All services started!"
	@echo "Kafka UI: http://localhost:8080"
	@echo "Grafana: http://localhost:3000"
	@echo "Superset: http://localhost:8088"
	@echo "Spark UI: http://localhost:8081"

stop:
	docker-compose down

restart:
	docker-compose restart

logs:
	docker-compose logs -f

logs-kafka:
	docker-compose logs -f kafka-1 kafka-2 kafka-3

logs-spark:
	docker-compose logs -f spark-master spark-worker-1 spark-worker-2

clean:
	docker-compose down -v --remove-orphans
	@echo "Cleaned up containers and volumes"

# Infrastructure
start-infra:
	docker-compose up -d zookeeper kafka-1 kafka-2 kafka-3 kafka-ui postgres clickhouse redis elasticsearch prometheus grafana superset

start-kafka:
	docker-compose up -d zookeeper kafka-1 kafka-2 kafka-3 kafka-ui

start-dbs:
	docker-compose up -d postgres clickhouse redis elasticsearch

start-spark:
	docker-compose up -d spark-master spark-worker-1 spark-worker-2

# Kafka Topics
topics:
	@./scripts/create_topics.sh

topics-list:
	docker exec kafka-1 kafka-topics --list --bootstrap-server localhost:9092

topics-delete:
	@echo "Deleting all topics..."
	docker exec kafka-1 kafka-topics --delete --topic youtube-raw --bootstrap-server localhost:9092 || true
	docker exec kafka-1 kafka-topics --delete --topic twitter-raw --bootstrap-server localhost:9092 || true
	docker exec kafka-1 kafka-topics --delete --topic tiktok-raw --bootstrap-server localhost:9092 || true
	docker exec kafka-1 kafka-topics --delete --topic naver-shopping-raw --bootstrap-server localhost:9092 || true
	docker exec kafka-1 kafka-topics --delete --topic google-trends-raw --bootstrap-server localhost:9092 || true

# Database
db-init:
	@./scripts/init_databases.sh

db-migrate:
	@echo "Running PostgreSQL migrations..."
	@docker exec -i postgres psql -U admin -d commerce_analytics < database/migrations/001_create_tables.sql
	@docker exec -i postgres psql -U admin -d commerce_analytics < database/migrations/002_create_indexes.sql
	@docker exec -i postgres psql -U admin -d commerce_analytics < database/migrations/003_create_views.sql

# Producers
producers:
	@./scripts/start_producers.sh

producer-youtube:
	python -m producers.youtube_producer

producer-twitter:
	python -m producers.twitter_producer

producer-tiktok:
	python -m producers.tiktok_producer

producer-naver:
	python -m producers.naver_shopping_producer

producer-trends:
	python -m producers.google_trends_producer

# Streaming
streaming:
	@./scripts/start_streaming.sh

streaming-local:
	python -m streaming.main

# Testing
test:
	pytest tests/ -v --cov=producers --cov=streaming --cov-report=html

test-unit:
	pytest tests/unit/ -v

test-integration:
	pytest tests/integration/ -v

test-load:
	locust -f tests/load/locustfile.py

# Linting
lint:
	@echo "Running linter..."
	@flake8 producers/ streaming/ --max-line-length=120
	@black producers/ streaming/ --check

format:
	black producers/ streaming/

# Health Check
health:
	@./scripts/health_check.sh

# Build
build:
	docker-compose build

# Consumer for testing
consume-youtube:
	docker exec kafka-1 kafka-console-consumer --topic youtube-raw --from-beginning --bootstrap-server localhost:9092

consume-twitter:
	docker exec kafka-1 kafka-console-consumer --topic twitter-raw --from-beginning --bootstrap-server localhost:9092

consume-tiktok:
	docker exec kafka-1 kafka-console-consumer --topic tiktok-raw --from-beginning --bootstrap-server localhost:9092

consume-naver:
	docker exec kafka-1 kafka-console-consumer --topic naver-shopping-raw --from-beginning --bootstrap-server localhost:9092

consume-trends:
	docker exec kafka-1 kafka-console-consumer --topic google-trends-raw --from-beginning --bootstrap-server localhost:9092
