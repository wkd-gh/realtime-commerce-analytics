"""
Pytest configuration and fixtures.
"""
import os
import sys
import pytest
from unittest.mock import MagicMock, patch

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def kafka_config():
    """Kafka configuration for testing."""
    return {
        'bootstrap_servers': 'localhost:9092',
    }


@pytest.fixture
def mock_kafka_producer():
    """Mock Kafka producer."""
    with patch('kafka.KafkaProducer') as mock:
        producer = MagicMock()
        mock.return_value = producer
        producer.send.return_value.get.return_value = MagicMock(
            topic='test-topic',
            partition=0,
            offset=1,
        )
        yield producer


@pytest.fixture
def sample_youtube_data():
    """Sample YouTube video data."""
    return {
        'content_type': 'video',
        'content_id': 'abc123xyz',
        'channel_id': 'UCtest123',
        'channel_title': 'Test Channel',
        'title': '테스트 비디오 - 최고의 제품 리뷰',
        'description': '이것은 테스트 설명입니다.',
        'published_at': '2024-01-15T10:00:00Z',
        'thumbnail_url': 'https://example.com/thumb.jpg',
        'views': 10000,
        'likes': 500,
        'comments': 50,
        'url': 'https://www.youtube.com/watch?v=abc123xyz',
        'search_keyword': '제품 리뷰',
        'fetched_at': '2024-01-15T12:00:00Z',
    }


@pytest.fixture
def sample_twitter_data():
    """Sample Twitter tweet data."""
    return {
        'content_type': 'tweet',
        'content_id': '123456789',
        'text': '#쿠팡추천 이 제품 정말 좋아요! 강추합니다 @coupang',
        'author_id': '987654321',
        'author_username': 'test_user',
        'author_verified': False,
        'author_followers': 1000,
        'likes': 50,
        'retweets': 10,
        'replies': 5,
        'quotes': 2,
        'impressions': 5000,
        'hashtags': ['쿠팡추천'],
        'mentions': ['coupang'],
        'language': 'ko',
        'published_at': '2024-01-15T10:00:00Z',
        'url': 'https://twitter.com/test_user/status/123456789',
        'fetched_at': '2024-01-15T12:00:00Z',
    }


@pytest.fixture
def sample_tiktok_data():
    """Sample TikTok video data."""
    return {
        'content_type': 'video',
        'content_id': 'tiktok123',
        'description': '쿠팡에서 산 템 #tiktokmademebuyit #쿠팡추천',
        'author_id': 'user123',
        'author_username': 'tiktok_creator',
        'author_nickname': '틱톡크리에이터',
        'author_verified': True,
        'author_followers': 50000,
        'views': 100000,
        'likes': 5000,
        'comments': 200,
        'shares': 50,
        'duration': 30,
        'hashtag': 'tiktokmademebuyit',
        'published_at': '2024-01-15T10:00:00Z',
        'url': 'https://www.tiktok.com/@tiktok_creator/video/tiktok123',
        'fetched_at': '2024-01-15T12:00:00Z',
    }


@pytest.fixture
def sample_naver_data():
    """Sample Naver Shopping product data."""
    return {
        'content_type': 'product',
        'content_id': 'naver_prod_123',
        'title': '인기 상품 - 베스트셀러 제품',
        'link': 'https://shopping.naver.com/product/123',
        'image_url': 'https://example.com/product.jpg',
        'low_price': 29900,
        'high_price': 39900,
        'discount_rate': 25.0,
        'mall_name': '테스트몰',
        'brand': 'TestBrand',
        'category1': '생활/건강',
        'category2': '건강식품',
        'search_keyword': '인기상품',
        'total_results': 1500,
        'fetched_at': '2024-01-15T12:00:00Z',
    }


@pytest.fixture
def spark_session():
    """Create a Spark session for testing."""
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .master("local[2]") \
            .appName("test") \
            .config("spark.driver.memory", "1g") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
        yield spark
        spark.stop()
    except ImportError:
        pytest.skip("PySpark not installed")


@pytest.fixture
def mock_youtube_api():
    """Mock YouTube API client."""
    with patch('googleapiclient.discovery.build') as mock:
        client = MagicMock()
        mock.return_value = client
        yield client


@pytest.fixture
def mock_twitter_api():
    """Mock Twitter API client."""
    with patch('tweepy.Client') as mock:
        client = MagicMock()
        mock.return_value = client
        yield client


@pytest.fixture
def mock_redis():
    """Mock Redis client."""
    with patch('redis.Redis') as mock:
        client = MagicMock()
        mock.return_value = client
        yield client


@pytest.fixture
def mock_elasticsearch():
    """Mock Elasticsearch client."""
    with patch('elasticsearch.Elasticsearch') as mock:
        client = MagicMock()
        mock.return_value = client
        yield client
