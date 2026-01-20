"""
Load testing configuration using Locust.

Run with: locust -f tests/load/locustfile.py
"""
import json
import random
import time
from datetime import datetime
from locust import HttpUser, task, between


class KafkaProducerUser(HttpUser):
    """
    Simulates Kafka producer load for testing.

    Note: This simulates the HTTP endpoints that would be exposed
    for testing purposes. In production, producers directly use
    the Kafka protocol.
    """

    wait_time = between(0.5, 2)

    def on_start(self):
        """Initialize test data."""
        self.platforms = ['youtube', 'twitter', 'tiktok', 'naver_shopping', 'google_trends']
        self.sample_titles = [
            "최고의 제품 리뷰",
            "신제품 언박싱",
            "구매 후기",
            "베스트셀러 추천",
            "할인 정보",
        ]
        self.sample_hashtags = [
            "#쿠팡추천", "#올리브영", "#베스트셀러", "#핫딜", "#리뷰"
        ]

    def generate_youtube_data(self):
        """Generate sample YouTube data."""
        return {
            'content_type': 'video',
            'content_id': f'yt_{random.randint(1000000, 9999999)}',
            'channel_id': f'UC{random.randint(10000, 99999)}',
            'channel_title': f'Channel_{random.randint(1, 100)}',
            'title': random.choice(self.sample_titles),
            'description': 'Test description',
            'views': random.randint(100, 1000000),
            'likes': random.randint(10, 10000),
            'comments': random.randint(1, 1000),
            'published_at': datetime.utcnow().isoformat(),
        }

    def generate_twitter_data(self):
        """Generate sample Twitter data."""
        return {
            'content_type': 'tweet',
            'content_id': str(random.randint(1000000000, 9999999999)),
            'text': f'{random.choice(self.sample_titles)} {random.choice(self.sample_hashtags)}',
            'author_id': str(random.randint(100000, 999999)),
            'author_username': f'user_{random.randint(1, 1000)}',
            'likes': random.randint(0, 1000),
            'retweets': random.randint(0, 500),
            'replies': random.randint(0, 100),
            'impressions': random.randint(100, 50000),
            'published_at': datetime.utcnow().isoformat(),
        }

    def generate_tiktok_data(self):
        """Generate sample TikTok data."""
        return {
            'content_type': 'video',
            'content_id': f'tt_{random.randint(1000000, 9999999)}',
            'description': f'{random.choice(self.sample_titles)} {random.choice(self.sample_hashtags)}',
            'author_id': str(random.randint(100000, 999999)),
            'author_username': f'tiktoker_{random.randint(1, 500)}',
            'views': random.randint(1000, 5000000),
            'likes': random.randint(100, 100000),
            'comments': random.randint(10, 5000),
            'shares': random.randint(1, 1000),
            'duration': random.randint(15, 180),
            'published_at': datetime.utcnow().isoformat(),
        }

    def generate_naver_data(self):
        """Generate sample Naver Shopping data."""
        return {
            'content_type': 'product',
            'content_id': f'nv_{random.randint(1000000, 9999999)}',
            'title': random.choice(self.sample_titles),
            'low_price': random.randint(5000, 500000),
            'high_price': random.randint(500000, 1000000),
            'discount_rate': random.uniform(0, 50),
            'mall_name': f'Mall_{random.randint(1, 100)}',
            'brand': f'Brand_{random.randint(1, 50)}',
            'category1': '생활/건강',
            'total_results': random.randint(100, 10000),
        }

    @task(3)
    def produce_youtube_data(self):
        """Simulate YouTube data production."""
        data = self.generate_youtube_data()
        self.client.post(
            "/api/produce/youtube",
            json=data,
            name="Produce YouTube"
        )

    @task(3)
    def produce_twitter_data(self):
        """Simulate Twitter data production."""
        data = self.generate_twitter_data()
        self.client.post(
            "/api/produce/twitter",
            json=data,
            name="Produce Twitter"
        )

    @task(2)
    def produce_tiktok_data(self):
        """Simulate TikTok data production."""
        data = self.generate_tiktok_data()
        self.client.post(
            "/api/produce/tiktok",
            json=data,
            name="Produce TikTok"
        )

    @task(2)
    def produce_naver_data(self):
        """Simulate Naver Shopping data production."""
        data = self.generate_naver_data()
        self.client.post(
            "/api/produce/naver",
            json=data,
            name="Produce Naver"
        )

    @task(1)
    def get_metrics(self):
        """Get current metrics."""
        self.client.get("/api/metrics", name="Get Metrics")

    @task(1)
    def get_trending(self):
        """Get trending content."""
        platform = random.choice(self.platforms[:4])
        self.client.get(
            f"/api/trending/{platform}",
            name="Get Trending"
        )


class DashboardUser(HttpUser):
    """
    Simulates dashboard user load.
    """

    wait_time = between(2, 5)

    @task(5)
    def view_dashboard(self):
        """View main dashboard."""
        self.client.get("/dashboard", name="View Dashboard")

    @task(3)
    def view_platform_metrics(self):
        """View platform metrics."""
        platforms = ['youtube', 'twitter', 'tiktok', 'naver_shopping']
        platform = random.choice(platforms)
        self.client.get(
            f"/api/metrics/{platform}",
            name="View Platform Metrics"
        )

    @task(2)
    def view_trending(self):
        """View trending content."""
        self.client.get("/api/trending", name="View Trending")

    @task(2)
    def view_brand_metrics(self):
        """View brand metrics."""
        brands = ['Apple', 'Samsung', 'Nike', 'Dyson', 'Coupang']
        brand = random.choice(brands)
        self.client.get(
            f"/api/brands/{brand}",
            name="View Brand Metrics"
        )

    @task(1)
    def view_sentiment(self):
        """View sentiment analysis."""
        self.client.get("/api/sentiment", name="View Sentiment")

    @task(1)
    def search_content(self):
        """Search content."""
        keywords = ['아이폰', '쿠팡', '올리브영', '베스트', '추천']
        keyword = random.choice(keywords)
        self.client.get(
            f"/api/search?q={keyword}",
            name="Search Content"
        )
