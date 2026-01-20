"""
TikTok Data Producer for collecting video data.
"""
import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from .base_producer import BaseProducer

logger = logging.getLogger(__name__)


class TikTokProducer(BaseProducer):
    """
    Producer for TikTok video data.

    Uses tiktokapipy for data collection:
    - Videos by hashtag
    - Video metadata and statistics
    - Creator information
    """

    def __init__(
        self,
        kafka_config: Dict[str, Any],
        hashtags: Optional[List[str]] = None,
        max_videos_per_hashtag: int = 30,
        rate_limit_delay: float = 2.0,
    ):
        """
        Initialize TikTok producer.

        Args:
            kafka_config: Kafka configuration
            hashtags: List of hashtags to track
            max_videos_per_hashtag: Maximum videos per hashtag
            rate_limit_delay: Delay between requests in seconds
        """
        super().__init__(
            kafka_config=kafka_config,
            topic_name='tiktok-raw',
            platform_name='tiktok',
        )

        self.hashtags = hashtags or []
        self.max_videos_per_hashtag = max_videos_per_hashtag
        self.rate_limit_delay = rate_limit_delay

        # Lazy import to handle optional dependency
        self._api = None

        logger.info(
            f"Initialized TikTok producer with {len(self.hashtags)} hashtags"
        )

    def _get_api(self):
        """Get or initialize TikTok API client."""
        if self._api is None:
            try:
                from tiktokapipy.api import TikTokAPI
                self._api = TikTokAPI()
            except ImportError:
                logger.error("tiktokapipy not installed. Run: pip install tiktokapipy")
                raise
        return self._api

    def _get_hashtag_videos(self, hashtag: str) -> List[Dict]:
        """
        Get videos for a hashtag.

        Args:
            hashtag: Hashtag to search (without #)

        Returns:
            List of video data
        """
        results = []

        try:
            api = self._get_api()

            # Clean hashtag (remove # if present)
            hashtag = hashtag.lstrip('#')

            with api:
                tag = api.challenge(hashtag)

                if not tag:
                    logger.warning(f"Hashtag not found: {hashtag}")
                    return []

                video_count = 0
                for video in tag.videos:
                    if video_count >= self.max_videos_per_hashtag:
                        break

                    results.append({
                        'video': video,
                        'hashtag': hashtag,
                        'challenge_info': {
                            'id': tag.id,
                            'title': tag.title,
                            'view_count': getattr(tag, 'view_count', 0),
                        }
                    })

                    video_count += 1
                    time.sleep(self.rate_limit_delay)

            logger.info(f"Fetched {len(results)} videos for #{hashtag}")
            return results

        except Exception as e:
            logger.error(f"TikTok hashtag error for #{hashtag}: {e}")
            return []

    def fetch_data(self) -> List[Dict[str, Any]]:
        """
        Fetch data from TikTok.

        Returns:
            List of raw TikTok data
        """
        all_data = []

        for hashtag in self.hashtags:
            videos = self._get_hashtag_videos(hashtag)
            all_data.extend(videos)

            # Rate limiting between hashtags
            time.sleep(self.rate_limit_delay)

        logger.info(f"Fetched {len(all_data)} items from TikTok")
        return all_data

    def transform_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform raw TikTok data to standardized format.

        Args:
            raw_data: Raw TikTok API response data

        Returns:
            Standardized data dictionary
        """
        video = raw_data.get('video')
        challenge_info = raw_data.get('challenge_info', {})

        # Extract statistics safely
        stats = getattr(video, 'stats', None) or {}
        if hasattr(stats, '__dict__'):
            stats = stats.__dict__

        # Extract author information
        author = getattr(video, 'author', None)
        author_stats = {}
        if author:
            author_stats = getattr(author, 'stats', None) or {}
            if hasattr(author_stats, '__dict__'):
                author_stats = author_stats.__dict__

        # Extract music information
        music = getattr(video, 'music', None)
        music_info = None
        if music:
            music_info = {
                'id': getattr(music, 'id', None),
                'title': getattr(music, 'title', None),
                'author': getattr(music, 'author', None),
            }

        return {
            'content_type': 'video',
            'content_id': str(getattr(video, 'id', '')),
            'description': getattr(video, 'desc', ''),
            'author_id': str(getattr(author, 'id', '')) if author else None,
            'author_username': getattr(author, 'unique_id', None) if author else None,
            'author_nickname': getattr(author, 'nickname', None) if author else None,
            'author_verified': getattr(author, 'verified', False) if author else False,
            'author_followers': author_stats.get('follower_count', 0),
            'author_following': author_stats.get('following_count', 0),
            'author_likes': author_stats.get('heart_count', 0),
            'views': stats.get('play_count', 0),
            'likes': stats.get('digg_count', 0),
            'comments': stats.get('comment_count', 0),
            'shares': stats.get('share_count', 0),
            'duration': getattr(video, 'duration', 0),
            'music': music_info,
            'hashtag': raw_data.get('hashtag'),
            'challenge_id': challenge_info.get('id'),
            'challenge_views': challenge_info.get('view_count', 0),
            'published_at': datetime.fromtimestamp(
                getattr(video, 'create_time', 0)
            ).isoformat() if getattr(video, 'create_time', 0) else None,
            'url': f"https://www.tiktok.com/@{getattr(author, 'unique_id', 'user') if author else 'user'}/video/{getattr(video, 'id', '')}",
            'fetched_at': datetime.utcnow().isoformat(),
        }


def main():
    """Main entry point for TikTok producer."""
    import yaml
    from dotenv import load_dotenv

    load_dotenv()

    # Load configuration
    with open('config/platforms.yaml', 'r') as f:
        config = yaml.safe_load(f)

    tiktok_config = config.get('tiktok', {})

    kafka_config = {
        'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    }

    # Initialize and run producer
    producer = TikTokProducer(
        kafka_config=kafka_config,
        hashtags=tiktok_config.get('hashtags', []),
        max_videos_per_hashtag=tiktok_config.get('max_videos_per_hashtag', 30),
        rate_limit_delay=tiktok_config.get('rate_limit_delay', 2),
    )

    try:
        producer.start(interval_seconds=tiktok_config.get('polling_interval', 120))
    except KeyboardInterrupt:
        producer.stop()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
