"""
Data validators for producer data.
"""
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, field_validator, model_validator

logger = logging.getLogger(__name__)


class BaseContentData(BaseModel):
    """Base model for content data validation."""
    content_type: str
    content_id: str
    fetched_at: str

    @field_validator('content_id')
    @classmethod
    def validate_content_id(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError('content_id cannot be empty')
        return v.strip()

    @field_validator('fetched_at')
    @classmethod
    def validate_fetched_at(cls, v: str) -> str:
        try:
            datetime.fromisoformat(v.replace('Z', '+00:00'))
        except ValueError:
            raise ValueError('fetched_at must be a valid ISO format datetime')
        return v


class YouTubeVideoData(BaseContentData):
    """Validation model for YouTube video data."""
    content_type: str = Field(default='video')
    channel_id: Optional[str] = None
    channel_title: Optional[str] = None
    title: str
    description: Optional[str] = None
    published_at: Optional[str] = None
    thumbnail_url: Optional[str] = None
    views: int = Field(ge=0, default=0)
    likes: int = Field(ge=0, default=0)
    comments: int = Field(ge=0, default=0)
    url: str

    @field_validator('url')
    @classmethod
    def validate_url(cls, v: str) -> str:
        if not v.startswith('https://www.youtube.com/'):
            raise ValueError('Invalid YouTube URL')
        return v


class YouTubeCommentData(BaseContentData):
    """Validation model for YouTube comment data."""
    content_type: str = Field(default='comment')
    video_id: str
    author: Optional[str] = None
    author_channel_id: Optional[str] = None
    text: str
    likes: int = Field(ge=0, default=0)
    published_at: Optional[str] = None


class TwitterTweetData(BaseContentData):
    """Validation model for Twitter tweet data."""
    content_type: str = Field(default='tweet')
    text: str
    author_id: str
    author_username: Optional[str] = None
    author_verified: bool = False
    author_followers: int = Field(ge=0, default=0)
    likes: int = Field(ge=0, default=0)
    retweets: int = Field(ge=0, default=0)
    replies: int = Field(ge=0, default=0)
    hashtags: List[str] = Field(default_factory=list)
    mentions: List[str] = Field(default_factory=list)
    language: Optional[str] = None
    published_at: Optional[str] = None
    url: str


class TikTokVideoData(BaseContentData):
    """Validation model for TikTok video data."""
    content_type: str = Field(default='video')
    description: Optional[str] = None
    author_id: Optional[str] = None
    author_username: Optional[str] = None
    author_nickname: Optional[str] = None
    author_verified: bool = False
    views: int = Field(ge=0, default=0)
    likes: int = Field(ge=0, default=0)
    comments: int = Field(ge=0, default=0)
    shares: int = Field(ge=0, default=0)
    duration: int = Field(ge=0, default=0)
    hashtag: Optional[str] = None
    published_at: Optional[str] = None
    url: str


class NaverProductData(BaseContentData):
    """Validation model for Naver Shopping product data."""
    content_type: str = Field(default='product')
    title: str
    link: str
    image_url: Optional[str] = None
    low_price: int = Field(ge=0)
    high_price: int = Field(ge=0, default=0)
    discount_rate: float = Field(ge=0, le=100, default=0)
    mall_name: Optional[str] = None
    brand: Optional[str] = None
    category1: Optional[str] = None
    category2: Optional[str] = None
    category3: Optional[str] = None
    category4: Optional[str] = None

    @model_validator(mode='after')
    def validate_prices(self) -> 'NaverProductData':
        if self.high_price > 0 and self.high_price < self.low_price:
            raise ValueError('high_price must be >= low_price')
        return self


class GoogleTrendsData(BaseContentData):
    """Validation model for Google Trends data."""
    content_type: str
    geo: str = Field(default='KR')

    @field_validator('content_type')
    @classmethod
    def validate_content_type(cls, v: str) -> str:
        valid_types = ['interest_over_time', 'related_queries', 'trending_searches']
        if v not in valid_types:
            raise ValueError(f'content_type must be one of {valid_types}')
        return v


class DataValidator:
    """
    Validator class for producer data.

    Provides validation methods for different content types
    with support for both strict and lenient modes.
    """

    VALIDATORS = {
        'youtube_video': YouTubeVideoData,
        'youtube_comment': YouTubeCommentData,
        'twitter_tweet': TwitterTweetData,
        'tiktok_video': TikTokVideoData,
        'naver_product': NaverProductData,
        'google_trends': GoogleTrendsData,
    }

    @classmethod
    def validate(
        cls,
        data: Dict[str, Any],
        content_type: str,
        strict: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        Validate data against the appropriate schema.

        Args:
            data: Data dictionary to validate
            content_type: Type of content (youtube_video, twitter_tweet, etc.)
            strict: If True, raise exception on validation error

        Returns:
            Validated data dict or None if validation fails
        """
        validator_class = cls.VALIDATORS.get(content_type)

        if not validator_class:
            if strict:
                raise ValueError(f"Unknown content type: {content_type}")
            logger.warning(f"No validator for content type: {content_type}")
            return data

        try:
            validated = validator_class(**data)
            return validated.model_dump()
        except Exception as e:
            if strict:
                raise
            logger.warning(f"Validation error for {content_type}: {e}")
            return None

    @classmethod
    def validate_batch(
        cls,
        data_list: List[Dict[str, Any]],
        content_type: str,
        strict: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Validate a batch of data items.

        Args:
            data_list: List of data dictionaries
            content_type: Type of content
            strict: If True, raise exception on validation error

        Returns:
            List of validated data dicts (invalid items excluded in non-strict mode)
        """
        results = []
        errors = 0

        for data in data_list:
            validated = cls.validate(data, content_type, strict)
            if validated:
                results.append(validated)
            else:
                errors += 1

        if errors > 0:
            logger.info(f"Validated {len(results)}/{len(data_list)} items, {errors} errors")

        return results

    @staticmethod
    def sanitize_text(text: str, max_length: int = 5000) -> str:
        """
        Sanitize text content.

        Args:
            text: Text to sanitize
            max_length: Maximum allowed length

        Returns:
            Sanitized text
        """
        if not text:
            return ''

        # Remove null bytes
        text = text.replace('\x00', '')

        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text)

        # Truncate if needed
        if len(text) > max_length:
            text = text[:max_length] + '...'

        return text.strip()

    @staticmethod
    def validate_url(url: str) -> bool:
        """
        Validate a URL.

        Args:
            url: URL to validate

        Returns:
            True if URL is valid
        """
        url_pattern = re.compile(
            r'^https?://'
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'
            r'localhost|'
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
            r'(?::\d+)?'
            r'(?:/?|[/?]\S+)$', re.IGNORECASE
        )
        return bool(url_pattern.match(url))

    @staticmethod
    def validate_timestamp(timestamp: str) -> bool:
        """
        Validate an ISO format timestamp.

        Args:
            timestamp: Timestamp string

        Returns:
            True if timestamp is valid
        """
        try:
            datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            return True
        except (ValueError, AttributeError):
            return False
