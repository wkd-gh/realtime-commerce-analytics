"""
API Client Factory for managing platform API connections.
"""
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class APIClientFactory:
    """
    Factory class for creating and managing API clients.

    Provides centralized configuration and lazy initialization
    of platform-specific API clients.
    """

    _instances: Dict[str, Any] = {}

    @classmethod
    def get_youtube_client(cls, api_key: str):
        """
        Get or create YouTube API client.

        Args:
            api_key: YouTube Data API key

        Returns:
            YouTube API client
        """
        if 'youtube' not in cls._instances:
            from googleapiclient.discovery import build
            cls._instances['youtube'] = build('youtube', 'v3', developerKey=api_key)
            logger.info("Created YouTube API client")

        return cls._instances['youtube']

    @classmethod
    def get_twitter_client(cls, bearer_token: str):
        """
        Get or create Twitter API client.

        Args:
            bearer_token: Twitter API Bearer Token

        Returns:
            Twitter API client
        """
        if 'twitter' not in cls._instances:
            import tweepy
            cls._instances['twitter'] = tweepy.Client(bearer_token=bearer_token)
            logger.info("Created Twitter API client")

        return cls._instances['twitter']

    @classmethod
    def get_tiktok_api(cls):
        """
        Get or create TikTok API instance.

        Returns:
            TikTok API instance
        """
        if 'tiktok' not in cls._instances:
            from tiktokapipy.api import TikTokAPI
            cls._instances['tiktok'] = TikTokAPI()
            logger.info("Created TikTok API client")

        return cls._instances['tiktok']

    @classmethod
    def get_naver_session(cls, client_id: str, client_secret: str):
        """
        Get or create Naver API session with headers.

        Args:
            client_id: Naver API Client ID
            client_secret: Naver API Client Secret

        Returns:
            Requests session with Naver headers
        """
        if 'naver' not in cls._instances:
            import requests
            session = requests.Session()
            session.headers.update({
                'X-Naver-Client-Id': client_id,
                'X-Naver-Client-Secret': client_secret,
            })
            cls._instances['naver'] = session
            logger.info("Created Naver API session")

        return cls._instances['naver']

    @classmethod
    def get_pytrends(cls, hl: str = 'ko', tz: int = 540):
        """
        Get or create pytrends instance.

        Args:
            hl: Host language
            tz: Timezone offset

        Returns:
            pytrends TrendReq instance
        """
        if 'pytrends' not in cls._instances:
            from pytrends.request import TrendReq
            cls._instances['pytrends'] = TrendReq(hl=hl, tz=tz)
            logger.info("Created pytrends instance")

        return cls._instances['pytrends']

    @classmethod
    def clear_all(cls):
        """Clear all cached client instances."""
        cls._instances.clear()
        logger.info("Cleared all API client instances")

    @classmethod
    def clear(cls, platform: str):
        """
        Clear a specific client instance.

        Args:
            platform: Platform name to clear
        """
        if platform in cls._instances:
            del cls._instances[platform]
            logger.info(f"Cleared {platform} API client instance")
