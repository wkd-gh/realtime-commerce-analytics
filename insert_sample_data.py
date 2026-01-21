#!/usr/bin/env python3
"""
Insert sample data for Grafana dashboard demo
"""
import psycopg2
from datetime import datetime, timedelta
import random

# Configuration
POSTGRES_HOST = 'localhost'
POSTGRES_DB = 'commerce_analytics'
POSTGRES_USER = 'wkdgh'
POSTGRES_PASSWORD = '357tmdgml!!'

print("🔄 Inserting sample data...")

# Connect to PostgreSQL
conn = psycopg2.connect(
    host=POSTGRES_HOST,
    database=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD
)
cursor = conn.cursor()

# Clear old data
cursor.execute("DELETE FROM platform_metrics WHERE window_start < NOW() - INTERVAL '24 hours'")
cursor.execute("DELETE FROM trending_content WHERE window_start < NOW() - INTERVAL '24 hours'")
conn.commit()

# Generate data for last 6 hours
platforms = ['youtube', 'twitter', 'tiktok', 'naver_shopping']
now = datetime.now()

print("📊 Generating platform metrics...")
for i in range(72):  # 72 x 5 minutes = 6 hours
    window_start = now - timedelta(minutes=5 * (72 - i))
    window_end = window_start + timedelta(minutes=5)

    for platform in platforms:
        base_views = random.randint(10000, 100000)
        base_likes = int(base_views * random.uniform(0.01, 0.05))
        base_comments = int(base_views * random.uniform(0.001, 0.01))

        cursor.execute("""
            INSERT INTO platform_metrics (
                window_start, window_end, platform,
                total_views, total_likes, total_comments, total_shares,
                avg_engagement_rate, total_content, max_views, unique_authors
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            window_start,
            window_end,
            platform,
            base_views,
            base_likes,
            base_comments,
            int(base_views * random.uniform(0.0005, 0.002)),
            (base_likes + base_comments) / (base_views + 1) * 100,
            random.randint(10, 50),
            int(base_views * random.uniform(0.3, 0.6)),
            random.randint(5, 30)
        ))

conn.commit()
print(f"✅ Inserted {72 * len(platforms)} platform metrics records")

# Generate trending content
print("🔥 Generating trending content...")
sample_titles = [
    "신제품 리뷰 - 완전 대박!",
    "언박싱 영상 - 이거 실화?",
    "추천 제품 TOP 10",
    "가성비 최고 제품 찾았습니다",
    "이달의 베스트 상품",
    "핫딜 정보 공유",
    "쿠팡 추천템 모음",
    "네이버 쇼핑 꿀템",
    "틱톡 바이럴 상품",
    "트위터 화제의 제품"
]

for i in range(20):
    window_start = now - timedelta(minutes=random.randint(10, 360))
    window_end = window_start + timedelta(minutes=5)
    platform = random.choice(platforms)

    views = random.randint(50000, 500000)
    likes = int(views * random.uniform(0.02, 0.08))
    comments = int(views * random.uniform(0.005, 0.02))

    cursor.execute("""
        INSERT INTO trending_content (
            window_start, window_end, platform, rank,
            content_id, title, author_name,
            total_views, total_likes, engagement_rate,
            buzz_score, z_score, url
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        window_start,
        window_end,
        platform,
        i % 10 + 1,
        f"content_{platform}_{i}",
        random.choice(sample_titles),
        f"Creator_{random.randint(1, 100)}",
        views,
        likes,
        (likes + comments) / (views + 1) * 100,
        (views * 0.6 + likes * 0.3 + comments * 0.1) / 1000,
        random.uniform(2.0, 5.0),
        f"https://{platform}.com/video/{i}"
    ))

conn.commit()
print(f"✅ Inserted 20 trending content records")

# Verify data
cursor.execute("SELECT COUNT(*) FROM platform_metrics WHERE window_start >= NOW() - INTERVAL '6 hours'")
metrics_count = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM trending_content WHERE window_start >= NOW() - INTERVAL '6 hours'")
trending_count = cursor.fetchone()[0]

print(f"\n📈 Summary:")
print(f"   Platform metrics: {metrics_count} records")
print(f"   Trending content: {trending_count} records")
print(f"   Time range: Last 6 hours")

conn.close()
print("\n✅ Sample data inserted successfully!")
print("🔄 Refresh your Grafana dashboard to see the data")
