"""
YouTube Data Producer for collecting video and comment data.
"""
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from .base_producer import BaseProducer, API_QUOTA_REMAINING

logger = logging.getLogger(__name__)


class YouTubeProducer(BaseProducer):
    """
    Producer for YouTube video and comment data.

    Uses the YouTube Data API v3 to collect:
    - Video metadata (title, description, statistics)
    - Comments and replies
    - Channel information
    """

    def __init__(
        self,
        kafka_config: Dict[str, Any],
        api_key: str,
        channels: Optional[List[str]] = None,
        keywords: Optional[List[str]] = None,
        max_results: int = 50,
    ):
        """
        Initialize YouTube producer.

        Args:
            kafka_config: Kafka configuration
            api_key: YouTube Data API key
            channels: List of channel IDs to track
            keywords: List of keywords to search
            max_results: Maximum results per API call
        """
        super().__init__(
            kafka_config=kafka_config,
            topic_name='youtube-raw',
            platform_name='youtube',
        )

        self.api_key = api_key
        self.channels = channels or []
        self.keywords = keywords or []
        self.max_results = max_results

        # Initialize YouTube API client
        self.youtube = build('youtube', 'v3', developerKey=api_key)

        # Track API quota usage (daily limit: 10,000 units)
        self.quota_used = 0
        self.quota_limit = 10000

        logger.info(
            f"Initialized YouTube producer with {len(self.channels)} channels "
            f"and {len(self.keywords)} keywords"
        )

    def _update_quota(self, units: int):
        """Update API quota usage."""
        self.quota_used += units
        remaining = self.quota_limit - self.quota_used
        API_QUOTA_REMAINING.labels(platform='youtube').set(remaining)

    def _search_videos(self, query: str) -> List[Dict]:
        """
        Search for videos by keyword.

        Args:
            query: Search query string

        Returns:
            List of video search results
        """
        try:
            request = self.youtube.search().list(
                part='snippet',
                q=query,
                type='video',
                order='date',
                maxResults=self.max_results,
                relevanceLanguage='ko',
                regionCode='KR',
            )
            response = request.execute()
            self._update_quota(100)  # search.list costs 100 units

            return response.get('items', [])

        except HttpError as e:
            logger.error(f"YouTube search error: {e}")
            return []

    def _get_video_details(self, video_ids: List[str]) -> List[Dict]:
        """
        Get detailed video information.

        Args:
            video_ids: List of video IDs

        Returns:
            List of video details
        """
        if not video_ids:
            return []

        try:
            request = self.youtube.videos().list(
                part='snippet,statistics,contentDetails',
                id=','.join(video_ids),
            )
            response = request.execute()
            self._update_quota(1)  # videos.list costs 1 unit per video

            return response.get('items', [])

        except HttpError as e:
            logger.error(f"YouTube video details error: {e}")
            return []

    def _get_channel_videos(self, channel_id: str) -> List[Dict]:
        """
        Get latest videos from a channel.

        Args:
            channel_id: YouTube channel ID

        Returns:
            List of video items
        """
        try:
            # First, get the uploads playlist ID
            request = self.youtube.channels().list(
                part='contentDetails',
                id=channel_id,
            )
            response = request.execute()
            self._update_quota(1)

            if not response.get('items'):
                return []

            uploads_playlist_id = (
                response['items'][0]['contentDetails']
                ['relatedPlaylists']['uploads']
            )

            # Get videos from uploads playlist
            request = self.youtube.playlistItems().list(
                part='snippet',
                playlistId=uploads_playlist_id,
                maxResults=self.max_results,
            )
            response = request.execute()
            self._update_quota(1)

            return response.get('items', [])

        except HttpError as e:
            logger.error(f"YouTube channel videos error: {e}")
            return []

    def _get_video_comments(self, video_id: str, max_comments: int = 100) -> List[Dict]:
        """
        Get comments for a video.

        Args:
            video_id: YouTube video ID
            max_comments: Maximum number of comments to fetch

        Returns:
            List of comment items
        """
        try:
            request = self.youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=min(max_comments, 100),
                order='relevance',
            )
            response = request.execute()
            self._update_quota(1)

            return response.get('items', [])

        except HttpError as e:
            # Comments might be disabled
            if 'commentsDisabled' in str(e):
                logger.debug(f"Comments disabled for video {video_id}")
            else:
                logger.error(f"YouTube comments error: {e}")
            return []

    def fetch_data(self) -> List[Dict[str, Any]]:
        """
        Fetch data from YouTube API.

        Returns:
            List of raw YouTube data
        """
        all_data = []
        video_ids = set()

        # Fetch from tracked channels
        for channel_id in self.channels:
            videos = self._get_channel_videos(channel_id)
            for video in videos:
                video_id = video['snippet']['resourceId']['videoId']
                video_ids.add(video_id)
                all_data.append({
                    'type': 'channel_video',
                    'channel_id': channel_id,
                    'video': video,
                })

        # Search by keywords
        for keyword in self.keywords:
            search_results = self._search_videos(keyword)
            for item in search_results:
                video_id = item['id']['videoId']
                video_ids.add(video_id)
                all_data.append({
                    'type': 'search_result',
                    'keyword': keyword,
                    'video': item,
                })

        # Get detailed video information
        video_details = self._get_video_details(list(video_ids))
        video_details_map = {v['id']: v for v in video_details}

        # Get comments for videos
        for video_id in list(video_ids)[:10]:  # Limit to 10 videos for quota
            comments = self._get_video_comments(video_id)
            for comment in comments:
                all_data.append({
                    'type': 'comment',
                    'video_id': video_id,
                    'comment': comment,
                })

        # Enrich data with video details
        for item in all_data:
            if item['type'] in ('channel_video', 'search_result'):
                video_id = (
                    item['video']['snippet'].get('resourceId', {}).get('videoId')
                    or item['video'].get('id', {}).get('videoId')
                )
                if video_id and video_id in video_details_map:
                    item['video_details'] = video_details_map[video_id]

        logger.info(f"Fetched {len(all_data)} items from YouTube")
        return all_data

    def transform_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform raw YouTube data to standardized format.

        Args:
            raw_data: Raw YouTube API response data

        Returns:
            Standardized data dictionary
        """
        data_type = raw_data.get('type')

        if data_type == 'comment':
            return self._transform_comment(raw_data)
        else:
            return self._transform_video(raw_data)

    def _transform_video(self, raw_data: Dict) -> Dict:
        """Transform video data."""
        video = raw_data.get('video', {})
        details = raw_data.get('video_details', {})
        snippet = video.get('snippet', {})
        statistics = details.get('statistics', {})

        video_id = (
            snippet.get('resourceId', {}).get('videoId')
            or video.get('id', {}).get('videoId')
            or details.get('id')
        )

        return {
            'content_type': 'video',
            'content_id': video_id,
            'channel_id': snippet.get('channelId'),
            'channel_title': snippet.get('channelTitle'),
            'title': snippet.get('title'),
            'description': snippet.get('description', '')[:500],
            'published_at': snippet.get('publishedAt'),
            'thumbnail_url': snippet.get('thumbnails', {}).get('high', {}).get('url'),
            'views': int(statistics.get('viewCount', 0)),
            'likes': int(statistics.get('likeCount', 0)),
            'comments': int(statistics.get('commentCount', 0)),
            'url': f"https://www.youtube.com/watch?v={video_id}",
            'search_keyword': raw_data.get('keyword'),
            'fetched_at': datetime.utcnow().isoformat(),
        }

    def _transform_comment(self, raw_data: Dict) -> Dict:
        """Transform comment data."""
        comment = raw_data.get('comment', {})
        snippet = comment.get('snippet', {}).get('topLevelComment', {}).get('snippet', {})

        return {
            'content_type': 'comment',
            'content_id': comment.get('id'),
            'video_id': raw_data.get('video_id'),
            'author': snippet.get('authorDisplayName'),
            'author_channel_id': snippet.get('authorChannelId', {}).get('value'),
            'text': snippet.get('textDisplay'),
            'likes': snippet.get('likeCount', 0),
            'published_at': snippet.get('publishedAt'),
            'fetched_at': datetime.utcnow().isoformat(),
        }


def main():
    """Main entry point for YouTube producer."""
    import yaml
    from dotenv import load_dotenv

    load_dotenv()

    # Load configuration
    with open('config/platforms.yaml', 'r') as f:
        config = yaml.safe_load(f)

    youtube_config = config.get('youtube', {})

    kafka_config = {
        'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    }

    # Initialize and run producer
    producer = YouTubeProducer(
        kafka_config=kafka_config,
        api_key=os.getenv('YOUTUBE_API_KEY'),
        channels=[c['channel_id'] for c in youtube_config.get('channels', [])],
        keywords=youtube_config.get('keywords', []),
        max_results=youtube_config.get('max_results_per_request', 50),
    )

    try:
        producer.start(interval_seconds=youtube_config.get('polling_interval', 60))
    except KeyboardInterrupt:
        producer.stop()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
