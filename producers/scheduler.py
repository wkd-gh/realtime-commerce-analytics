"""
APScheduler-based scheduler for producer jobs.
"""
import logging
import os
import signal
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from prometheus_client import Counter, start_http_server

from .youtube_producer import YouTubeProducer
from .twitter_producer import TwitterProducer
from .tiktok_producer import TikTokProducer
from .naver_shopping_producer import NaverShoppingProducer
from .google_trends_producer import GoogleTrendsProducer

logger = logging.getLogger(__name__)

# Prometheus metrics
JOB_EXECUTIONS = Counter(
    'scheduler_job_executions_total',
    'Total job executions',
    ['platform', 'status']
)


class ProducerScheduler:
    """
    Scheduler for managing all producer jobs.

    Uses APScheduler to run producers at configured intervals.
    """

    def __init__(self, config_path: str = 'config/platforms.yaml'):
        """
        Initialize the scheduler.

        Args:
            config_path: Path to platforms configuration file
        """
        self.config_path = config_path
        self.scheduler = BackgroundScheduler()
        self.producers: Dict[str, Any] = {}

        # Load configuration
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        # Kafka configuration
        self.kafka_config = {
            'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        }

        # Set up signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        logger.info("Initialized producer scheduler")

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, shutting down...")
        self.stop()
        sys.exit(0)

    def _job_listener(self, event):
        """Listen for job events."""
        job_id = event.job_id
        platform = job_id.replace('_producer', '')

        if event.exception:
            JOB_EXECUTIONS.labels(platform=platform, status='error').inc()
            logger.error(f"Job {job_id} failed: {event.exception}")
        else:
            JOB_EXECUTIONS.labels(platform=platform, status='success').inc()
            logger.debug(f"Job {job_id} executed successfully")

    def _create_youtube_producer(self) -> Optional[YouTubeProducer]:
        """Create YouTube producer instance."""
        youtube_config = self.config.get('youtube', {})

        if not youtube_config.get('enabled', True):
            return None

        api_key = os.getenv('YOUTUBE_API_KEY')
        if not api_key:
            logger.warning("YouTube API key not set, skipping YouTube producer")
            return None

        return YouTubeProducer(
            kafka_config=self.kafka_config,
            api_key=api_key,
            channels=[c['channel_id'] for c in youtube_config.get('channels', [])],
            keywords=youtube_config.get('keywords', []),
            max_results=youtube_config.get('max_results_per_request', 50),
        )

    def _create_twitter_producer(self) -> Optional[TwitterProducer]:
        """Create Twitter producer instance."""
        twitter_config = self.config.get('twitter', {})

        if not twitter_config.get('enabled', True):
            return None

        bearer_token = os.getenv('TWITTER_BEARER_TOKEN')
        if not bearer_token:
            logger.warning("Twitter bearer token not set, skipping Twitter producer")
            return None

        return TwitterProducer(
            kafka_config=self.kafka_config,
            bearer_token=bearer_token,
            keywords=twitter_config.get('keywords', []),
            users=twitter_config.get('users_to_track', []),
            max_results=twitter_config.get('max_results_per_request', 100),
        )

    def _create_tiktok_producer(self) -> Optional[TikTokProducer]:
        """Create TikTok producer instance."""
        tiktok_config = self.config.get('tiktok', {})

        if not tiktok_config.get('enabled', True):
            return None

        return TikTokProducer(
            kafka_config=self.kafka_config,
            hashtags=tiktok_config.get('hashtags', []),
            max_videos_per_hashtag=tiktok_config.get('max_videos_per_hashtag', 30),
            rate_limit_delay=tiktok_config.get('rate_limit_delay', 2),
        )

    def _create_naver_producer(self) -> Optional[NaverShoppingProducer]:
        """Create Naver Shopping producer instance."""
        naver_config = self.config.get('naver_shopping', {})

        if not naver_config.get('enabled', True):
            return None

        client_id = os.getenv('NAVER_CLIENT_ID')
        client_secret = os.getenv('NAVER_CLIENT_SECRET')

        if not client_id or not client_secret:
            logger.warning("Naver credentials not set, skipping Naver producer")
            return None

        return NaverShoppingProducer(
            kafka_config=self.kafka_config,
            client_id=client_id,
            client_secret=client_secret,
            keywords=naver_config.get('keywords', []),
            categories=naver_config.get('categories', []),
            max_results=naver_config.get('max_results_per_request', 100),
        )

    def _create_trends_producer(self) -> Optional[GoogleTrendsProducer]:
        """Create Google Trends producer instance."""
        trends_config = self.config.get('google_trends', {})

        if not trends_config.get('enabled', True):
            return None

        return GoogleTrendsProducer(
            kafka_config=self.kafka_config,
            keywords=trends_config.get('keywords', []),
            geo=trends_config.get('geo', 'KR'),
            timeframe=trends_config.get('timeframe', 'now 1-H'),
        )

    def _run_producer(self, platform: str):
        """
        Run a producer's fetch cycle.

        Args:
            platform: Platform name
        """
        producer = self.producers.get(platform)
        if producer:
            try:
                count = producer.run_once()
                logger.info(f"[{platform}] Processed {count} messages")
            except Exception as e:
                logger.error(f"[{platform}] Error: {e}")
                raise

    def setup(self):
        """Set up all producers and schedule jobs."""
        logger.info("Setting up producers...")

        # Create producers
        producers_config = [
            ('youtube', self._create_youtube_producer, 'youtube'),
            ('twitter', self._create_twitter_producer, 'twitter'),
            ('tiktok', self._create_tiktok_producer, 'tiktok'),
            ('naver_shopping', self._create_naver_producer, 'naver_shopping'),
            ('google_trends', self._create_trends_producer, 'google_trends'),
        ]

        for name, creator, config_key in producers_config:
            try:
                producer = creator()
                if producer:
                    self.producers[name] = producer
                    interval = self.config.get(config_key, {}).get('polling_interval', 60)

                    # Schedule the job
                    self.scheduler.add_job(
                        func=self._run_producer,
                        trigger=IntervalTrigger(seconds=interval),
                        args=[name],
                        id=f'{name}_producer',
                        name=f'{name.title()} Producer',
                        replace_existing=True,
                    )
                    logger.info(f"Scheduled {name} producer with interval {interval}s")

            except Exception as e:
                logger.error(f"Failed to create {name} producer: {e}")

        # Add job listener
        self.scheduler.add_listener(
            self._job_listener,
            EVENT_JOB_EXECUTED | EVENT_JOB_ERROR
        )

        logger.info(f"Set up {len(self.producers)} producers")

    def start(self, metrics_port: int = 8000):
        """
        Start the scheduler.

        Args:
            metrics_port: Port for Prometheus metrics server
        """
        # Start Prometheus metrics server
        start_http_server(metrics_port)
        logger.info(f"Started Prometheus metrics server on port {metrics_port}")

        # Start scheduler
        self.scheduler.start()
        logger.info("Started scheduler")

        # Run initial fetch for all producers
        logger.info("Running initial fetch for all producers...")
        for platform in self.producers:
            try:
                self._run_producer(platform)
            except Exception as e:
                logger.error(f"Initial fetch failed for {platform}: {e}")

        # Keep the main thread alive
        try:
            while True:
                import time
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            self.stop()

    def stop(self):
        """Stop the scheduler and all producers."""
        logger.info("Stopping scheduler...")

        # Shutdown scheduler
        self.scheduler.shutdown(wait=False)

        # Stop all producers
        for name, producer in self.producers.items():
            try:
                producer.stop()
                logger.info(f"Stopped {name} producer")
            except Exception as e:
                logger.error(f"Error stopping {name} producer: {e}")

        logger.info("Scheduler stopped")

    def get_status(self) -> Dict[str, Any]:
        """
        Get scheduler status.

        Returns:
            Status dictionary
        """
        jobs = []
        for job in self.scheduler.get_jobs():
            jobs.append({
                'id': job.id,
                'name': job.name,
                'next_run': job.next_run_time.isoformat() if job.next_run_time else None,
            })

        return {
            'running': self.scheduler.running,
            'producers': list(self.producers.keys()),
            'jobs': jobs,
        }


def main():
    """Main entry point for the scheduler."""
    from dotenv import load_dotenv

    load_dotenv()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    )

    scheduler = ProducerScheduler()
    scheduler.setup()
    scheduler.start()


if __name__ == '__main__':
    main()
