"""
Schema definitions for streaming data.
"""
from .youtube_schema import YOUTUBE_SCHEMA, YOUTUBE_VIDEO_SCHEMA, YOUTUBE_COMMENT_SCHEMA
from .twitter_schema import TWITTER_SCHEMA
from .tiktok_schema import TIKTOK_SCHEMA
from .naver_schema import NAVER_SCHEMA
from .common_schema import (
    KAFKA_MESSAGE_SCHEMA,
    UNIFIED_CONTENT_SCHEMA,
    AGGREGATED_METRICS_SCHEMA,
)

__all__ = [
    'YOUTUBE_SCHEMA',
    'YOUTUBE_VIDEO_SCHEMA',
    'YOUTUBE_COMMENT_SCHEMA',
    'TWITTER_SCHEMA',
    'TIKTOK_SCHEMA',
    'NAVER_SCHEMA',
    'KAFKA_MESSAGE_SCHEMA',
    'UNIFIED_CONTENT_SCHEMA',
    'AGGREGATED_METRICS_SCHEMA',
]
