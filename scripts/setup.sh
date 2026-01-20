#!/bin/bash
set -e

echo "=========================================="
echo "Real-time Commerce Analytics Setup"
echo "=========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check Docker
echo -e "${YELLOW}Checking Docker...${NC}"
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Docker is not installed. Please install Docker first.${NC}"
    exit 1
fi
echo -e "${GREEN}Docker is installed.${NC}"

# Check Docker Compose
echo -e "${YELLOW}Checking Docker Compose...${NC}"
if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}Docker Compose is not installed. Please install Docker Compose first.${NC}"
    exit 1
fi
echo -e "${GREEN}Docker Compose is installed.${NC}"

# Create .env file if not exists
if [ ! -f .env ]; then
    echo -e "${YELLOW}Creating .env file from .env.example...${NC}"
    cp .env.example .env
    echo -e "${GREEN}.env file created. Please update it with your API keys.${NC}"
else
    echo -e "${GREEN}.env file already exists.${NC}"
fi

# Create logs directory
mkdir -p logs/{producers,streaming,kafka}

# Create data directory for local development
mkdir -p data/{checkpoints,models}

# Install Python dependencies
echo -e "${YELLOW}Installing Python dependencies...${NC}"
pip install -r requirements.txt

# Start infrastructure
echo -e "${YELLOW}Starting Docker infrastructure...${NC}"
docker-compose up -d zookeeper

# Wait for Zookeeper
echo -e "${YELLOW}Waiting for Zookeeper to be ready...${NC}"
sleep 10

# Start Kafka brokers
docker-compose up -d kafka-1 kafka-2 kafka-3
echo -e "${YELLOW}Waiting for Kafka brokers to be ready...${NC}"
sleep 20

# Create Kafka topics
echo -e "${YELLOW}Creating Kafka topics...${NC}"
./scripts/create_topics.sh

# Start databases
docker-compose up -d postgres clickhouse redis elasticsearch
echo -e "${YELLOW}Waiting for databases to be ready...${NC}"
sleep 15

# Initialize databases
echo -e "${YELLOW}Initializing databases...${NC}"
./scripts/init_databases.sh

# Start monitoring
docker-compose up -d prometheus grafana

# Start Superset
docker-compose up -d superset

# Start Spark
docker-compose up -d spark-master spark-worker-1 spark-worker-2

# Start Kafka UI
docker-compose up -d kafka-ui

echo ""
echo "=========================================="
echo -e "${GREEN}Setup Complete!${NC}"
echo "=========================================="
echo ""
echo "Services available at:"
echo "  - Kafka UI: http://localhost:8080"
echo "  - Grafana: http://localhost:3000 (admin/admin)"
echo "  - Superset: http://localhost:8088 (admin/admin)"
echo "  - Spark UI: http://localhost:8081"
echo "  - Prometheus: http://localhost:9090"
echo ""
echo "Next steps:"
echo "  1. Update .env file with your API keys"
echo "  2. Run 'make producers' to start data collection"
echo "  3. Run 'make streaming' to start Spark streaming"
echo ""
