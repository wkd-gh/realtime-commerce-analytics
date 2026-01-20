"""
Twitter Data Producer for collecting tweet data.
"""
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import tweepy

from .base_producer import BaseProducer, API_QUOTA_REMAINING

logger = logging.getLogger(__name__)


class TwitterProducer(BaseProducer):
    """
    Producer for Twitter tweet data.

    Uses the Twitter API v2 to collect:
    - Tweets by keyword search
    - Tweets by hashtag
    - User metrics
    """

    def __init__(
        self,
        kafka_config: Dict[str, Any],
        bearer_token: str,
        keywords: Optional[List[str]] = None,
        users: Optional[List[str]] = None,
        max_results: int = 100,
    ):
        """
        Initialize Twitter producer.

        Args:
            kafka_config: Kafka configuration
            bearer_token: Twitter API Bearer Token
            keywords: List of keywords/hashtags to track
            users: List of usernames to track
            max_results: Maximum results per API call
        """
        super().__init__(
            kafka_config=kafka_config,
            topic_name='twitter-raw',
            platform_name='twitter',
        )

        self.bearer_token = bearer_token
        self.keywords = keywords or []
        self.users = users or []
        self.max_results = min(max_results, 100)  # API limit

        # Initialize Twitter API client
        self.client = tweepy.Client(bearer_token=bearer_token)

        # Rate limit tracking (Free tier: 450 requests per 15 minutes)
        self.requests_made = 0
        self.rate_limit = 450

        logger.info(
            f"Initialized Twitter producer with {len(self.keywords)} keywords "
            f"and {len(self.users)} users"
        )

    def _update_rate_limit(self):
        """Update rate limit tracking."""
        self.requests_made += 1
        remaining = self.rate_limit - self.requests_made
        API_QUOTA_REMAINING.labels(platform='twitter').set(remaining)

    def _search_tweets(self, query: str) -> List[Dict]:
        """
        Search for recent tweets.

        Args:
            query: Search query string

        Returns:
            List of tweet data
        """
        try:
            response = self.client.search_recent_tweets(
                query=query,
                max_results=self.max_results,
                tweet_fields=[
                    'created_at', 'public_metrics', 'source',
                    'lang', 'conversation_id', 'entities'
                ],
                user_fields=['username', 'public_metrics', 'verified'],
                expansions=['author_id'],
            )
            self._update_rate_limit()

            if not response.data:
                return []

            # Create user lookup
            users_map = {}
            if response.includes and 'users' in response.includes:
                users_map = {u.id: u for u in response.includes['users']}

            results = []
            for tweet in response.data:
                user = users_map.get(tweet.author_id)
                results.append({
                    'tweet': tweet,
                    'user': user,
                    'query': query,
                })

            return results

        except tweepy.TweepyException as e:
            logger.error(f"Twitter search error: {e}")
            return []

    def _get_user_tweets(self, username: str) -> List[Dict]:
        """
        Get recent tweets from a user.

        Args:
            username: Twitter username

        Returns:
            List of tweet data
        """
        try:
            # Get user ID first
            user_response = self.client.get_user(
                username=username,
                user_fields=['public_metrics', 'verified', 'description']
            )
            self._update_rate_limit()

            if not user_response.data:
                return []

            user = user_response.data

            # Get user's tweets
            tweets_response = self.client.get_users_tweets(
                id=user.id,
                max_results=self.max_results,
                tweet_fields=[
                    'created_at', 'public_metrics', 'source',
                    'lang', 'conversation_id', 'entities'
                ],
            )
            self._update_rate_limit()

            if not tweets_response.data:
                return []

            return [{
                'tweet': tweet,
                'user': user,
                'source': 'user_timeline',
            } for tweet in tweets_response.data]

        except tweepy.TweepyException as e:
            logger.error(f"Twitter user tweets error: {e}")
            return []

    def fetch_data(self) -> List[Dict[str, Any]]:
        """
        Fetch data from Twitter API.

        Returns:
            List of raw Twitter data
        """
        all_data = []

        # Search by keywords
        for keyword in self.keywords:
            # Build query with Korean language filter
            query = f"{keyword} lang:ko -is:retweet"
            tweets = self._search_tweets(query)
            all_data.extend(tweets)

        # Get tweets from tracked users
        for username in self.users:
            tweets = self._get_user_tweets(username)
            all_data.extend(tweets)

        logger.info(f"Fetched {len(all_data)} items from Twitter")
        return all_data

    def transform_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform raw Twitter data to standardized format.

        Args:
            raw_data: Raw Twitter API response data

        Returns:
            Standardized data dictionary
        """
        tweet = raw_data.get('tweet')
        user = raw_data.get('user')

        # Extract metrics
        tweet_metrics = tweet.public_metrics or {}
        user_metrics = user.public_metrics if user else {}

        # Extract hashtags
        hashtags = []
        if tweet.entities and 'hashtags' in tweet.entities:
            hashtags = [h['tag'] for h in tweet.entities['hashtags']]

        # Extract mentions
        mentions = []
        if tweet.entities and 'mentions' in tweet.entities:
            mentions = [m['username'] for m in tweet.entities['mentions']]

        # Extract URLs
        urls = []
        if tweet.entities and 'urls' in tweet.entities:
            urls = [u.get('expanded_url', u.get('url')) for u in tweet.entities['urls']]

        return {
            'content_type': 'tweet',
            'content_id': str(tweet.id),
            'conversation_id': str(tweet.conversation_id) if tweet.conversation_id else None,
            'text': tweet.text,
            'author_id': str(tweet.author_id),
            'author_username': user.username if user else None,
            'author_verified': user.verified if user else False,
            'author_followers': user_metrics.get('followers_count', 0),
            'author_following': user_metrics.get('following_count', 0),
            'likes': tweet_metrics.get('like_count', 0),
            'retweets': tweet_metrics.get('retweet_count', 0),
            'replies': tweet_metrics.get('reply_count', 0),
            'quotes': tweet_metrics.get('quote_count', 0),
            'impressions': tweet_metrics.get('impression_count', 0),
            'hashtags': hashtags,
            'mentions': mentions,
            'urls': urls,
            'language': tweet.lang,
            'source': tweet.source,
            'published_at': tweet.created_at.isoformat() if tweet.created_at else None,
            'url': f"https://twitter.com/{user.username if user else 'i'}/status/{tweet.id}",
            'search_keyword': raw_data.get('query'),
            'fetched_at': datetime.utcnow().isoformat(),
        }


def main():
    """Main entry point for Twitter producer."""
    import yaml
    from dotenv import load_dotenv

    load_dotenv()

    # Load configuration
    with open('config/platforms.yaml', 'r') as f:
        config = yaml.safe_load(f)

    twitter_config = config.get('twitter', {})

    kafka_config = {
        'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    }

    # Initialize and run producer
    producer = TwitterProducer(
        kafka_config=kafka_config,
        bearer_token=os.getenv('TWITTER_BEARER_TOKEN'),
        keywords=twitter_config.get('keywords', []),
        users=twitter_config.get('users_to_track', []),
        max_results=twitter_config.get('max_results_per_request', 100),
    )

    try:
        producer.start(interval_seconds=twitter_config.get('polling_interval', 30))
    except KeyboardInterrupt:
        producer.stop()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
