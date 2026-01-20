"""
Producers package for multi-platform data collection.
"""
from .base_producer import BaseProducer
from .youtube_producer import YouTubeProducer
from .twitter_producer import TwitterProducer
from .tiktok_producer import TikTokProducer
from .naver_shopping_producer import NaverShoppingProducer
from .google_trends_producer import GoogleTrendsProducer

__all__ = [
    'BaseProducer',
    'YouTubeProducer',
    'TwitterProducer',
    'TikTokProducer',
    'NaverShoppingProducer',
    'GoogleTrendsProducer',
]
