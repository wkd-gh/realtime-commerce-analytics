#!/usr/bin/env python3
"""
Simple streaming consumer - reads from Kafka and writes to databases
"""
import json
import os
import time
from datetime import datetime
from kafka import KafkaConsumer
import psycopg2
from clickhouse_driver import Client as ClickHouseClient
import redis
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092,localhost:9093,localhost:9094')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'commerce_analytics')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'wkdgh')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', '357tmdgml!!')
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'wkdgh')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '357tmdgml!!')
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')

print("🚀 Starting Simple Streaming Consumer...")
print(f"Kafka: {KAFKA_BOOTSTRAP}")
print(f"PostgreSQL: {POSTGRES_HOST}:{POSTGRES_DB}")
print(f"ClickHouse: {CLICKHOUSE_HOST}")

# Initialize connections
print("📡 Connecting to databases...")
pg_conn = psycopg2.connect(
    host=POSTGRES_HOST,
    database=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD
)
pg_cursor = pg_conn.cursor()

ch_client = ClickHouseClient(
    host=CLICKHOUSE_HOST,
    user=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database='commerce_analytics'
)
redis_client = redis.Redis(host=REDIS_HOST, decode_responses=True)

print("✅ Database connections established")

# Create Kafka consumer
print("📥 Creating Kafka consumer...")
consumer = KafkaConsumer(
    'youtube-raw',
    bootstrap_servers=KAFKA_BOOTSTRAP.split(','),
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='simple-streaming-consumer',
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

print("✅ Kafka consumer ready")
print("🎬 Processing messages...\n")

message_count = 0
batch_count = 0
BATCH_SIZE = 50

clickhouse_batch = []
start_time = time.time()

try:
    for message in consumer:
        try:
            data = message.value
            platform = data.get('platform', 'unknown')
            timestamp = data.get('timestamp')
            content_data = data.get('data', {})

            message_count += 1

            # Extract fields
            content_id = content_data.get('content_id', '')
            title = content_data.get('title', '')
            views = content_data.get('views', 0)
            likes = content_data.get('likes', 0)
            comments = content_data.get('comments', 0)

            # Store in ClickHouse (batch)
            clickhouse_batch.append({
                'event_time': timestamp or datetime.now().isoformat(),
                'message_id': data.get('message_id', ''),
                'platform': platform,
                'content_type': content_data.get('content_type', ''),
                'content_id': content_id,
                'title': title[:500] if title else '',
                'text': content_data.get('description', '')[:1000] if content_data.get('description') else '',
                'author_id': content_data.get('channel_id', ''),
                'author_name': content_data.get('channel_title', ''),
                'views': views,
                'likes': likes,
                'comments': comments,
                'shares': 0,
                'engagement_rate': (likes + comments) / (views + 1) * 100 if views else 0,
                'sentiment': 'neutral',
                'sentiment_score': 0.5,
                'brand': None,
                'category': None,
                'keywords': [],
                'hashtags': [],
                'url': content_data.get('url', ''),
            })

            # Update Redis real-time metrics
            redis_key = f"realtime:{platform}:views"
            redis_client.zadd(redis_key, {content_id: views})
            redis_client.expire(redis_key, 3600)  # 1 hour TTL

            # Batch insert to ClickHouse
            if len(clickhouse_batch) >= BATCH_SIZE:
                try:
                    ch_client.execute(
                        'INSERT INTO raw_events VALUES',
                        clickhouse_batch
                    )
                    batch_count += 1
                    clickhouse_batch = []

                    # Update PostgreSQL aggregates
                    pg_cursor.execute("""
                        INSERT INTO platform_metrics (window_start, window_end, platform, total_views, total_likes, total_comments, total_content, created_at)
                        VALUES (NOW() - INTERVAL '5 minutes', NOW(), %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (platform, window_start) DO UPDATE SET
                            total_views = platform_metrics.total_views + EXCLUDED.total_views,
                            total_likes = platform_metrics.total_likes + EXCLUDED.total_likes,
                            total_comments = platform_metrics.total_comments + EXCLUDED.total_comments,
                            total_content = platform_metrics.total_content + EXCLUDED.total_content
                    """, (platform, views, likes, comments, 1))
                    pg_conn.commit()

                    elapsed = time.time() - start_time
                    rate = message_count / elapsed if elapsed > 0 else 0
                    print(f"✨ Processed {message_count} messages ({batch_count} batches) | Rate: {rate:.2f} msg/s | Platform: {platform}")

                except Exception as e:
                    print(f"❌ Batch insert error: {e}")
                    clickhouse_batch = []
                    pg_conn.rollback()

        except Exception as e:
            print(f"❌ Error processing message: {e}")
            continue

except KeyboardInterrupt:
    print("\n⏸️  Shutting down...")

    # Final batch insert
    if clickhouse_batch:
        try:
            ch_client.execute(
                'INSERT INTO raw_events VALUES',
                clickhouse_batch
            )
            print(f"✅ Inserted final batch of {len(clickhouse_batch)} messages")
        except Exception as e:
            print(f"❌ Final batch error: {e}")

    pg_conn.close()
    consumer.close()
    print(f"\n📊 Total messages processed: {message_count}")
    print("👋 Goodbye!")
