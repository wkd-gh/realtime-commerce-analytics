#!/bin/bash

echo "=========================================="
echo "Health Check - Commerce Analytics"
echo "=========================================="

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

check_service() {
    local name=$1
    local check_cmd=$2

    printf "%-20s" "$name:"
    if eval $check_cmd > /dev/null 2>&1; then
        echo -e "${GREEN}OK${NC}"
        return 0
    else
        echo -e "${RED}FAILED${NC}"
        return 1
    fi
}

echo ""
echo "Docker Services:"
echo "----------------"
check_service "Zookeeper" "docker exec zookeeper nc -z localhost 2181"
check_service "Kafka-1" "docker exec kafka-1 kafka-broker-api-versions --bootstrap-server localhost:9092"
check_service "Kafka-2" "docker exec kafka-2 kafka-broker-api-versions --bootstrap-server localhost:9093"
check_service "Kafka-3" "docker exec kafka-3 kafka-broker-api-versions --bootstrap-server localhost:9094"
check_service "PostgreSQL" "docker exec postgres pg_isready -U admin"
check_service "ClickHouse" "docker exec clickhouse clickhouse-client --query 'SELECT 1'"
check_service "Redis" "docker exec redis redis-cli ping"
check_service "Elasticsearch" "curl -s http://localhost:9200/_cluster/health"
check_service "Prometheus" "curl -s http://localhost:9090/-/healthy"
check_service "Grafana" "curl -s http://localhost:3000/api/health"
check_service "Superset" "curl -s http://localhost:8088/health"
check_service "Spark Master" "curl -s http://localhost:8081"
check_service "Kafka UI" "curl -s http://localhost:8080"

echo ""
echo "Kafka Topics:"
echo "-------------"
docker exec kafka-1 kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null | while read topic; do
    echo "  - $topic"
done

echo ""
echo "Kafka Consumer Groups:"
echo "----------------------"
docker exec kafka-1 kafka-consumer-groups --list --bootstrap-server localhost:9092 2>/dev/null | while read group; do
    echo "  - $group"
done

echo ""
echo "Producer Processes:"
echo "-------------------"
for pid_file in logs/producers/*.pid; do
    if [ -f "$pid_file" ]; then
        producer=$(basename $pid_file .pid)
        pid=$(cat $pid_file)
        printf "%-20s" "$producer:"
        if ps -p $pid > /dev/null 2>&1; then
            echo -e "${GREEN}Running (PID: $pid)${NC}"
        else
            echo -e "${RED}Stopped${NC}"
        fi
    fi
done

echo ""
echo "Disk Usage:"
echo "-----------"
docker system df

echo ""
echo "=========================================="
echo "Health check complete!"
echo "=========================================="
