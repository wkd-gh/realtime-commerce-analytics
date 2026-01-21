"""
Configuration for Spark Streaming application.
"""
import os
from dataclasses import dataclass, field
from typing import List, Optional

from pydantic_settings import BaseSettings


class StreamingConfig(BaseSettings):
    """Streaming application configuration."""

    # Spark settings
    spark_master: str = "spark://spark-master:7077"
    spark_app_name: str = "commerce-analytics-streaming"
    spark_executor_memory: str = "2g"
    spark_executor_cores: int = 2

    # Kafka settings
    kafka_bootstrap_servers: str = "kafka-1:29092,kafka-2:29093,kafka-3:29094"
    kafka_starting_offsets: str = "latest"
    kafka_max_offsets_per_trigger: int = 10000

    # Topics
    youtube_topic: str = "youtube-raw"
    twitter_topic: str = "twitter-raw"
    tiktok_topic: str = "tiktok-raw"
    naver_topic: str = "naver-shopping-raw"
    trends_topic: str = "google-trends-raw"
    processed_topic: str = "processed-metrics"
    sentiment_topic: str = "sentiment-results"
    trending_topic: str = "trending-alerts"

    # Streaming settings
    trigger_interval: str = "10 seconds"
    watermark_delay: str = "10 minutes"
    checkpoint_location: str = "/opt/spark/checkpoints"

    # Window settings
    window_1m: str = "1 minute"
    window_5m: str = "5 minutes"
    window_15m: str = "15 minutes"
    window_1h: str = "1 hour"

    # Database settings
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_db: str = "commerce_analytics"
    postgres_user: str = "admin"
    postgres_password: str = "password"

    clickhouse_host: str = "clickhouse"
    clickhouse_port: int = 9000
    clickhouse_db: str = "commerce_analytics"
    clickhouse_user: str = "admin"
    clickhouse_password: str = "password"

    redis_host: str = "redis"
    redis_port: int = 6379
    redis_db: int = 0

    elasticsearch_host: str = "elasticsearch"
    elasticsearch_port: int = 9200

    # Slack settings
    slack_webhook_url: Optional[str] = None

    # ML settings
    sentiment_model: str = "klue/bert-base"
    sentiment_batch_size: int = 32
    trending_zscore_threshold: float = 3.0

    class Config:
        env_prefix = ""
        env_file = ".env"
        extra = "ignore"

    @property
    def postgres_url(self) -> str:
        """Get PostgreSQL JDBC URL."""
        return f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    @property
    def postgres_properties(self) -> dict:
        """Get PostgreSQL connection properties."""
        return {
            "user": self.postgres_user,
            "password": self.postgres_password,
            "driver": "org.postgresql.Driver",
        }

    @property
    def clickhouse_url(self) -> str:
        """Get ClickHouse JDBC URL."""
        return f"jdbc:clickhouse://{self.clickhouse_host}:{self.clickhouse_port}/{self.clickhouse_db}"

    @property
    def all_source_topics(self) -> List[str]:
        """Get all source topic names."""
        return [
            self.youtube_topic,
            self.twitter_topic,
            self.tiktok_topic,
            self.naver_topic,
            self.trends_topic,
        ]


# Global config instance
_config: Optional[StreamingConfig] = None


def get_config() -> StreamingConfig:
    """Get or create the global config instance."""
    global _config
    if _config is None:
        _config = StreamingConfig()
    return _config
