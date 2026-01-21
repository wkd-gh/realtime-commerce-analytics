#!/usr/bin/env python3
"""
Process all Kafka messages and load into databases
"""
import json
import os
from datetime import datetime
from dateutil import parser as date_parser
from kafka import KafkaConsumer
import psycopg2
from clickhouse_driver import Client as ClickHouseClient
import redis
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

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
ES_HOST = os.getenv('ELASTICSEARCH_HOST', 'localhost')

print("🚀 Processing Kafka Messages to Databases")
print(f"📊 Kafka: {KAFKA_BOOTSTRAP}")

# Connect to databases
print("📡 Connecting to databases...")
pg_conn = psycopg2.connect(
    host=POSTGRES_HOST, database=POSTGRES_DB,
    user=POSTGRES_USER, password=POSTGRES_PASSWORD
)
pg_cursor = pg_conn.cursor()

ch_client = ClickHouseClient(
    host=CLICKHOUSE_HOST, user=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD, database='commerce_analytics'
)

redis_client = redis.Redis(host=REDIS_HOST, decode_responses=True)
es_client = Elasticsearch([f'http://{ES_HOST}:9200'])

print("✅ All database connections established\n")

# Create Kafka consumer
consumer = KafkaConsumer(
    'youtube-raw',
    bootstrap_servers=KAFKA_BOOTSTRAP.split(','),
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='data-processor',
    auto_offset_reset='earliest',
    enable_auto_commit=False
)

print("📥 Starting to process messages...")
print("-" * 60)

ch_batch = []
es_batch = []
message_count = 0
error_count = 0
BATCH_SIZE = 100

for message in consumer:
    try:
        data = message.value
        platform = data.get('platform', 'unknown')
        timestamp = data.get('timestamp', datetime.now().isoformat())
        content_data = data.get('data', {})

        message_count += 1

        # Extract fields
        content_id = content_data.get('content_id', '')
        title = content_data.get('title', '')[:500]
        description = content_data.get('description', '')[:1000]
        views = int(content_data.get('views', 0))
        likes = int(content_data.get('likes', 0))
        comments = int(content_data.get('comments', 0))

        # Parse timestamp to datetime object
        if timestamp:
            try:
                event_time = date_parser.parse(timestamp)
            except:
                event_time = datetime.now()
        else:
            event_time = datetime.now()

        # ClickHouse batch
        ch_batch.append((
            event_time,
            data.get('message_id', ''),
            platform,
            content_data.get('content_type', 'video'),
            content_id,
            title,
            description,
            content_data.get('channel_id', ''),
            content_data.get('channel_title', ''),
            views, likes, comments, 0,
            round((likes + comments) / max(views, 1) * 100, 2),
            'neutral', 0.5, None, None,
            [], [],
            content_data.get('url', '')
        ))

        # Elasticsearch batch
        es_doc = {
            'timestamp': timestamp,
            'platform': platform,
            'content_id': content_id,
            'title': title,
            'description': description,
            'views': views,
            'likes': likes,
            'comments': comments,
            'sentiment': 'neutral',
            'keywords': []
        }
        es_batch.append({
            '_index': 'content-search',
            '_id': f"{platform}_{content_id}",
            '_source': es_doc
        })

        # Redis - trending
        redis_client.zadd(f'trending:{platform}', {content_id: views})
        redis_client.expire(f'trending:{platform}', 3600)

        # Batch processing
        if len(ch_batch) >= BATCH_SIZE:
            # ClickHouse insert
            ch_client.execute('''
                INSERT INTO raw_events
                (event_time, message_id, platform, content_type, content_id, title, text,
                 author_id, author_name, views, likes, comments, shares, engagement_rate,
                 sentiment, sentiment_score, brand, category, keywords, hashtags, url)
                VALUES
            ''', ch_batch
            )

            # Elasticsearch bulk
            from elasticsearch.helpers import bulk
            bulk(es_client, es_batch)

            # PostgreSQL aggregate
            pg_cursor.execute("""
                INSERT INTO platform_metrics
                (window_start, window_end, platform, total_views, total_likes, total_comments, total_content, created_at)
                VALUES (NOW() - INTERVAL '5 minutes', NOW(), %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (platform, window_start) DO UPDATE SET
                    total_views = platform_metrics.total_views + EXCLUDED.total_views,
                    total_likes = platform_metrics.total_likes + EXCLUDED.total_likes,
                    total_comments = platform_metrics.total_comments + EXCLUDED.total_comments,
                    total_content = platform_metrics.total_content + EXCLUDED.total_content
            """, (platform, sum(v[9] for v in ch_batch), sum(v[10] for v in ch_batch),
                  sum(v[11] for v in ch_batch), len(ch_batch)))
            pg_conn.commit()

            print(f"✨ Processed {message_count} messages | Inserted batch of {len(ch_batch)}")
            ch_batch = []
            es_batch = []

            consumer.commit()

    except Exception as e:
        error_count += 1
        print(f"❌ Error processing message {message_count}: {e}")
        continue

    # Stop after processing all available messages
    if message_count >= 14313:
        break

# Final batch
if ch_batch:
    try:
        ch_client.execute('''
            INSERT INTO raw_events
            (event_time, message_id, platform, content_type, content_id, title, text,
             author_id, author_name, views, likes, comments, shares, engagement_rate,
             sentiment, sentiment_score, brand, category, keywords, hashtags, url)
            VALUES
        ''', ch_batch)
        from elasticsearch.helpers import bulk
        bulk(es_client, es_batch)
        print(f"✅ Final batch: {len(ch_batch)} messages")
    except Exception as e:
        print(f"❌ Final batch error: {e}")

pg_conn.close()
consumer.close()

print("\n" + "=" * 60)
print(f"📊 SUMMARY")
print(f"   Total messages processed: {message_count}")
print(f"   Errors: {error_count}")
print(f"   Success rate: {(message_count-error_count)/message_count*100:.1f}%")
print("=" * 60)
print("\n✅ Data loading completed!")
print("\n🔍 Check your data:")
print("   ClickHouse: SELECT COUNT(*) FROM raw_events")
print("   Elasticsearch: curl http://localhost:9200/content-search/_count")
print("   PostgreSQL: SELECT * FROM platform_metrics LIMIT 5")
