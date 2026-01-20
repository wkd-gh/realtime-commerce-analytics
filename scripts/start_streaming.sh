#!/bin/bash
set -e

echo "Starting Spark Streaming job..."

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '#' | xargs)
fi

# Create logs directory
mkdir -p logs/streaming

# Spark submit configuration
SPARK_MASTER=${SPARK_MASTER_URL:-"spark://localhost:7077"}
SPARK_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

# Submit the streaming job
docker exec spark-master spark-submit \
    --master $SPARK_MASTER \
    --packages $SPARK_PACKAGES \
    --conf spark.sql.shuffle.partitions=12 \
    --conf spark.streaming.stopGracefullyOnShutdown=true \
    --conf spark.sql.streaming.checkpointLocation=/opt/spark/checkpoints \
    --executor-memory 2g \
    --executor-cores 2 \
    --total-executor-cores 4 \
    /opt/spark-apps/main.py

echo "Spark Streaming job submitted!"
