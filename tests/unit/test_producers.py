"""
Unit tests for producers.
"""
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime


class TestBaseProducer:
    """Tests for BaseProducer class."""

    def test_create_message_adds_metadata(self, kafka_config, mock_kafka_producer):
        """Test that create_message adds required metadata."""
        from producers.base_producer import BaseProducer

        class TestProducer(BaseProducer):
            def fetch_data(self):
                return []

            def transform_data(self, raw_data):
                return raw_data

        producer = TestProducer(kafka_config, 'test-topic', 'test')

        data = {'key': 'value'}
        message = producer._create_message(data)

        assert 'message_id' in message
        assert 'platform' in message
        assert 'timestamp' in message
        assert 'ingestion_time' in message
        assert 'data' in message
        assert message['platform'] == 'test'
        assert message['data'] == data

    def test_get_message_key_with_content_id(self, kafka_config, mock_kafka_producer):
        """Test message key generation with content_id."""
        from producers.base_producer import BaseProducer

        class TestProducer(BaseProducer):
            def fetch_data(self):
                return []

            def transform_data(self, raw_data):
                return raw_data

        producer = TestProducer(kafka_config, 'test-topic', 'test')

        message = {'data': {'content_id': '12345'}}
        key = producer._get_message_key(message)

        assert key == 'test:12345'

    def test_send_to_kafka_success(self, kafka_config, mock_kafka_producer):
        """Test successful message sending."""
        from producers.base_producer import BaseProducer

        class TestProducer(BaseProducer):
            def fetch_data(self):
                return []

            def transform_data(self, raw_data):
                return raw_data

        producer = TestProducer(kafka_config, 'test-topic', 'test')

        result = producer.send_to_kafka({'test': 'data'})
        assert result is True


class TestYouTubeProducer:
    """Tests for YouTubeProducer class."""

    def test_transform_video_data(self, kafka_config, mock_kafka_producer, mock_youtube_api):
        """Test YouTube video data transformation."""
        from producers.youtube_producer import YouTubeProducer

        producer = YouTubeProducer(
            kafka_config=kafka_config,
            api_key='test_key',
        )

        raw_data = {
            'type': 'channel_video',
            'video': {
                'snippet': {
                    'resourceId': {'videoId': 'test123'},
                    'channelId': 'channel123',
                    'channelTitle': 'Test Channel',
                    'title': '테스트 비디오',
                    'description': '테스트 설명',
                    'publishedAt': '2024-01-15T10:00:00Z',
                    'thumbnails': {'high': {'url': 'https://example.com/thumb.jpg'}},
                }
            },
            'video_details': {
                'id': 'test123',
                'statistics': {
                    'viewCount': '10000',
                    'likeCount': '500',
                    'commentCount': '50',
                }
            }
        }

        transformed = producer._transform_video(raw_data)

        assert transformed['content_type'] == 'video'
        assert transformed['content_id'] == 'test123'
        assert transformed['title'] == '테스트 비디오'
        assert transformed['views'] == 10000
        assert transformed['likes'] == 500
        assert transformed['comments'] == 50

    def test_transform_comment_data(self, kafka_config, mock_kafka_producer, mock_youtube_api):
        """Test YouTube comment data transformation."""
        from producers.youtube_producer import YouTubeProducer

        producer = YouTubeProducer(
            kafka_config=kafka_config,
            api_key='test_key',
        )

        raw_data = {
            'type': 'comment',
            'video_id': 'video123',
            'comment': {
                'id': 'comment123',
                'snippet': {
                    'topLevelComment': {
                        'snippet': {
                            'authorDisplayName': 'Test User',
                            'authorChannelId': {'value': 'user123'},
                            'textDisplay': '좋은 영상이에요!',
                            'likeCount': 10,
                            'publishedAt': '2024-01-15T11:00:00Z',
                        }
                    }
                }
            }
        }

        transformed = producer._transform_comment(raw_data)

        assert transformed['content_type'] == 'comment'
        assert transformed['content_id'] == 'comment123'
        assert transformed['video_id'] == 'video123'
        assert transformed['author'] == 'Test User'
        assert transformed['text'] == '좋은 영상이에요!'


class TestTwitterProducer:
    """Tests for TwitterProducer class."""

    def test_transform_tweet_data(self, kafka_config, mock_kafka_producer, mock_twitter_api):
        """Test Twitter tweet data transformation."""
        from producers.twitter_producer import TwitterProducer

        producer = TwitterProducer(
            kafka_config=kafka_config,
            bearer_token='test_token',
        )

        # Create mock tweet and user objects
        tweet = MagicMock()
        tweet.id = 123456789
        tweet.text = '테스트 트윗 #테스트'
        tweet.author_id = 987654321
        tweet.conversation_id = 123456789
        tweet.lang = 'ko'
        tweet.source = 'Twitter Web App'
        tweet.created_at = datetime(2024, 1, 15, 10, 0, 0)
        tweet.public_metrics = {
            'like_count': 50,
            'retweet_count': 10,
            'reply_count': 5,
            'quote_count': 2,
            'impression_count': 5000,
        }
        tweet.entities = {
            'hashtags': [{'tag': '테스트'}],
            'mentions': [],
            'urls': [],
        }

        user = MagicMock()
        user.id = 987654321
        user.username = 'test_user'
        user.verified = False
        user.public_metrics = {
            'followers_count': 1000,
            'following_count': 500,
        }

        raw_data = {
            'tweet': tweet,
            'user': user,
            'query': '#테스트',
        }

        transformed = producer.transform_data(raw_data)

        assert transformed['content_type'] == 'tweet'
        assert transformed['content_id'] == '123456789'
        assert transformed['author_username'] == 'test_user'
        assert transformed['likes'] == 50
        assert transformed['retweets'] == 10


class TestDataValidator:
    """Tests for DataValidator class."""

    def test_validate_youtube_video(self, sample_youtube_data):
        """Test YouTube video data validation."""
        from producers.utils.validators import DataValidator

        result = DataValidator.validate(sample_youtube_data, 'youtube_video')

        assert result is not None
        assert result['content_id'] == sample_youtube_data['content_id']

    def test_validate_twitter_tweet(self, sample_twitter_data):
        """Test Twitter tweet data validation."""
        from producers.utils.validators import DataValidator

        result = DataValidator.validate(sample_twitter_data, 'twitter_tweet')

        assert result is not None
        assert result['content_id'] == sample_twitter_data['content_id']

    def test_validate_invalid_data(self):
        """Test validation of invalid data."""
        from producers.utils.validators import DataValidator

        invalid_data = {'invalid': 'data'}
        result = DataValidator.validate(invalid_data, 'youtube_video', strict=False)

        assert result is None

    def test_sanitize_text(self):
        """Test text sanitization."""
        from producers.utils.validators import DataValidator

        # Test null byte removal
        text = "Hello\x00World"
        sanitized = DataValidator.sanitize_text(text)
        assert '\x00' not in sanitized

        # Test whitespace normalization
        text = "Hello   World\n\nTest"
        sanitized = DataValidator.sanitize_text(text)
        assert sanitized == "Hello World Test"

        # Test truncation
        long_text = "a" * 10000
        sanitized = DataValidator.sanitize_text(long_text, max_length=100)
        assert len(sanitized) <= 103  # 100 + "..."


class TestQuotaManager:
    """Tests for QuotaManager class."""

    def test_can_make_request_within_quota(self):
        """Test request allowed within quota."""
        from producers.utils.quota_manager import QuotaManager

        manager = QuotaManager()
        assert manager.can_make_request('youtube', cost=1) is True

    def test_record_request_updates_quota(self):
        """Test that recording a request updates quota."""
        from producers.utils.quota_manager import QuotaManager

        manager = QuotaManager()
        initial_remaining = manager.get_remaining('youtube')

        manager.record_request('youtube', cost=1)
        new_remaining = manager.get_remaining('youtube')

        assert new_remaining == initial_remaining - 1

    def test_get_all_status(self):
        """Test getting status for all platforms."""
        from producers.utils.quota_manager import QuotaManager

        manager = QuotaManager()
        status = manager.get_all_status()

        assert 'youtube' in status
        assert 'twitter' in status
        assert 'limit' in status['youtube']
        assert 'used' in status['youtube']
        assert 'remaining' in status['youtube']
