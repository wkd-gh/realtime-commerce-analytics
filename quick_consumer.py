#!/usr/bin/env python3
"""
Quick consumer - reads from Kafka and writes to PostgreSQL only
"""
import json
import os
import time
from datetime import datetime, timedelta
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092,localhost:9093,localhost:9094')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'commerce_analytics')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'admin')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'password')

print("🚀 Starting Quick Consumer...")
print(f"Kafka: {KAFKA_BOOTSTRAP}")
print(f"PostgreSQL: {POSTGRES_HOST}:{POSTGRES_DB}")

# Initialize PostgreSQL connection
print("📡 Connecting to PostgreSQL...")
pg_conn = psycopg2.connect(
    host=POSTGRES_HOST,
    database=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD
)
pg_cursor = pg_conn.cursor()

print("✅ PostgreSQL connection established")

# Create Kafka consumer
print("📥 Creating Kafka consumer...")
consumer = KafkaConsumer(
    'youtube-raw',
    bootstrap_servers=KAFKA_BOOTSTRAP.split(','),
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='quick-consumer',
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

print("✅ Kafka consumer ready")
print("🎬 Processing messages...\n")

message_count = 0
batch_data = {}
BATCH_SIZE = 50

start_time = time.time()

try:
    for message in consumer:
        try:
            data = message.value
            platform = data.get('platform', 'unknown')
            timestamp = data.get('timestamp')
            content_data = data.get('data', {})

            message_count += 1

            # Extract metrics
            views = content_data.get('view_count', 0) or content_data.get('views', 0)
            likes = content_data.get('like_count', 0) or content_data.get('likes', 0)
            comments = content_data.get('comment_count', 0) or content_data.get('comments', 0)

            # Aggregate by platform
            if platform not in batch_data:
                batch_data[platform] = {
                    'views': 0,
                    'likes': 0,
                    'comments': 0,
                    'content_count': 0
                }

            batch_data[platform]['views'] += views
            batch_data[platform]['likes'] += likes
            batch_data[platform]['comments'] += comments
            batch_data[platform]['content_count'] += 1

            # Process batch
            if message_count % BATCH_SIZE == 0:
                try:
                    # Calculate window
                    window_start = datetime.now() - timedelta(minutes=5)
                    window_end = datetime.now()

                    for plat, metrics in batch_data.items():
                        # Insert into platform_metrics
                        pg_cursor.execute("""
                            INSERT INTO platform_metrics (
                                window_start, window_end, platform,
                                total_views, total_likes, total_comments,
                                avg_engagement_rate, total_content,
                                created_at
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        """, (
                            window_start,
                            window_end,
                            plat,
                            metrics['views'],
                            metrics['likes'],
                            metrics['comments'],
                            ((metrics['likes'] + metrics['comments']) / (metrics['views'] + 1) * 100) if metrics['views'] > 0 else 0,
                            metrics['content_count']
                        ))

                    pg_conn.commit()

                    # Add some trending content
                    if content_data.get('title'):
                        try:
                            pg_cursor.execute("""
                                INSERT INTO trending_content (
                                    platform, content_id, rank, title,
                                    total_views, total_likes,
                                    engagement_rate, buzz_score,
                                    window_start, window_end, detected_at
                                )
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                            """, (
                                platform,
                                content_data.get('video_id') or content_data.get('content_id', f'content_{message_count}'),
                                1,
                                content_data.get('title', '')[:500],
                                views,
                                likes,
                                ((likes + comments) / (views + 1) * 100) if views > 0 else 0,
                                (views * 0.6 + likes * 0.3 + comments * 0.1) / 1000,
                                window_start,
                                window_end
                            ))
                            pg_conn.commit()
                        except Exception as tc_err:
                            # Ignore trending content errors
                            pg_conn.rollback()

                    elapsed = time.time() - start_time
                    rate = message_count / elapsed if elapsed > 0 else 0
                    print(f"✨ Processed {message_count} messages | Rate: {rate:.2f} msg/s")
                    print(f"   Platforms: {list(batch_data.keys())}")

                    # Reset batch
                    batch_data = {}

                except Exception as e:
                    print(f"❌ Batch insert error: {e}")
                    import traceback
                    traceback.print_exc()
                    pg_conn.rollback()
                    batch_data = {}

        except Exception as e:
            print(f"❌ Error processing message: {e}")
            continue

except KeyboardInterrupt:
    print("\n⏸️  Shutting down...")

    # Process remaining data
    if batch_data:
        try:
            window_start = datetime.now() - timedelta(minutes=5)
            window_end = datetime.now()

            for plat, metrics in batch_data.items():
                pg_cursor.execute("""
                    INSERT INTO platform_metrics (
                        window_start, window_end, platform,
                        total_views, total_likes, total_comments,
                        avg_engagement_rate, total_content,
                        created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """, (
                    window_start,
                    window_end,
                    plat,
                    metrics['views'],
                    metrics['likes'],
                    metrics['comments'],
                    ((metrics['likes'] + metrics['comments']) / (metrics['views'] + 1) * 100) if metrics['views'] > 0 else 0,
                    metrics['content_count']
                ))
            pg_conn.commit()
            print(f"✅ Inserted final batch")
        except Exception as e:
            print(f"❌ Final batch error: {e}")

    pg_conn.close()
    consumer.close()
    print(f"\n📊 Total messages processed: {message_count}")
    print("👋 Goodbye!")
