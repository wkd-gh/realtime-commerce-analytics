"""
Redis sink for real-time caching and rankings.
"""
import json
import logging
from typing import Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

logger = logging.getLogger(__name__)


class RedisSink:
    """
    Redis sink for caching real-time rankings and metrics.

    Supports various Redis data structures:
    - Sorted sets for rankings
    - Hashes for metrics
    - Strings for simple values
    """

    def __init__(
        self,
        host: str = "redis",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        default_ttl: int = 3600,
    ):
        """
        Initialize Redis sink.

        Args:
            host: Redis host
            port: Redis port
            db: Redis database number
            password: Redis password (optional)
            default_ttl: Default TTL in seconds
        """
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.default_ttl = default_ttl
        self._client = None

    def _get_client(self):
        """Lazy load Redis client."""
        if self._client is None:
            import redis
            self._client = redis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                decode_responses=True,
            )
        return self._client

    def set_value(
        self,
        key: str,
        value: any,
        ttl: Optional[int] = None,
    ) -> bool:
        """
        Set a simple key-value pair.

        Args:
            key: Redis key
            value: Value to store
            ttl: TTL in seconds (optional)

        Returns:
            True if successful
        """
        try:
            client = self._get_client()
            ttl = ttl or self.default_ttl

            if isinstance(value, (dict, list)):
                value = json.dumps(value)

            client.setex(key, ttl, value)
            return True

        except Exception as e:
            logger.error(f"Failed to set Redis value: {e}")
            return False

    def update_ranking(
        self,
        key: str,
        member: str,
        score: float,
        ttl: Optional[int] = None,
    ) -> bool:
        """
        Update a sorted set ranking.

        Args:
            key: Redis key for sorted set
            member: Member to add/update
            score: Score for ranking
            ttl: TTL in seconds (optional)

        Returns:
            True if successful
        """
        try:
            client = self._get_client()
            client.zadd(key, {member: score})

            if ttl:
                client.expire(key, ttl)

            return True

        except Exception as e:
            logger.error(f"Failed to update Redis ranking: {e}")
            return False

    def update_hash(
        self,
        key: str,
        mapping: Dict,
        ttl: Optional[int] = None,
    ) -> bool:
        """
        Update a hash with multiple fields.

        Args:
            key: Redis key for hash
            mapping: Field-value mapping
            ttl: TTL in seconds (optional)

        Returns:
            True if successful
        """
        try:
            client = self._get_client()

            # Convert non-string values to JSON
            processed = {}
            for k, v in mapping.items():
                if isinstance(v, (dict, list)):
                    processed[k] = json.dumps(v)
                else:
                    processed[k] = str(v)

            client.hset(key, mapping=processed)

            if ttl:
                client.expire(key, ttl)

            return True

        except Exception as e:
            logger.error(f"Failed to update Redis hash: {e}")
            return False

    def create_foreach_batch_writer(
        self,
        key_prefix: str,
        score_column: str,
        id_column: str,
        ttl: int = 3600,
    ):
        """
        Create a foreachBatch writer for rankings.

        Args:
            key_prefix: Prefix for Redis keys
            score_column: Column to use for scores
            id_column: Column to use for member IDs
            ttl: TTL in seconds

        Returns:
            foreachBatch function
        """
        host = self.host
        port = self.port
        db = self.db
        password = self.password

        def write_to_redis(batch_df: DataFrame, batch_id: int):
            """Write batch to Redis."""
            if batch_df.isEmpty():
                return

            try:
                import redis

                client = redis.Redis(
                    host=host,
                    port=port,
                    db=db,
                    password=password,
                    decode_responses=True,
                )

                rows = batch_df.collect()

                # Group by platform for separate rankings
                platform_data = {}
                for row in rows:
                    row_dict = row.asDict()
                    platform = row_dict.get("platform", "all")

                    if platform not in platform_data:
                        platform_data[platform] = []

                    platform_data[platform].append({
                        "id": str(row_dict.get(id_column, "")),
                        "score": float(row_dict.get(score_column, 0)),
                        "data": row_dict,
                    })

                # Write rankings for each platform
                for platform, items in platform_data.items():
                    key = f"{key_prefix}:{platform}"

                    # Update sorted set
                    for item in items:
                        client.zadd(key, {item["id"]: item["score"]})

                        # Store item details in hash
                        detail_key = f"{key_prefix}:detail:{item['id']}"
                        client.hset(detail_key, mapping={
                            k: json.dumps(v) if isinstance(v, (dict, list)) else str(v)
                            for k, v in item["data"].items()
                        })
                        client.expire(detail_key, ttl)

                    # Trim to top 100
                    client.zremrangebyrank(key, 0, -101)
                    client.expire(key, ttl)

                logger.info(f"Batch {batch_id}: Updated Redis rankings")

            except Exception as e:
                logger.error(f"Batch {batch_id}: Failed to write to Redis: {e}")

        return write_to_redis

    def write_stream(
        self,
        df: DataFrame,
        key_prefix: str,
        score_column: str,
        id_column: str,
        checkpoint_location: str,
        trigger_interval: str = "30 seconds",
        ttl: int = 3600,
    ) -> StreamingQuery:
        """
        Write streaming DataFrame to Redis.

        Args:
            df: Streaming DataFrame
            key_prefix: Prefix for Redis keys
            score_column: Column to use for scores
            id_column: Column for member IDs
            checkpoint_location: Checkpoint directory
            trigger_interval: Trigger processing interval
            ttl: TTL in seconds

        Returns:
            StreamingQuery object
        """
        return df.writeStream \
            .foreachBatch(self.create_foreach_batch_writer(
                key_prefix, score_column, id_column, ttl
            )) \
            .outputMode("append") \
            .trigger(processingTime=trigger_interval) \
            .option("checkpointLocation", f"{checkpoint_location}/{key_prefix}_redis") \
            .start()

    @staticmethod
    def write_trending_rankings(
        df: DataFrame,
        sink: "RedisSink",
        checkpoint_location: str,
    ) -> StreamingQuery:
        """
        Write trending content rankings to Redis.

        Args:
            df: Trending content DataFrame
            sink: RedisSink instance
            checkpoint_location: Checkpoint directory

        Returns:
            StreamingQuery object
        """
        return sink.write_stream(
            df,
            key_prefix="trending",
            score_column="buzz_score",
            id_column="content_id",
            checkpoint_location=checkpoint_location,
            trigger_interval="30 seconds",
            ttl=3600,
        )

    @staticmethod
    def write_engagement_rankings(
        df: DataFrame,
        sink: "RedisSink",
        checkpoint_location: str,
    ) -> StreamingQuery:
        """
        Write engagement rankings to Redis.

        Args:
            df: Engagement metrics DataFrame
            sink: RedisSink instance
            checkpoint_location: Checkpoint directory

        Returns:
            StreamingQuery object
        """
        return sink.write_stream(
            df,
            key_prefix="engagement",
            score_column="engagement_rate",
            id_column="content_id",
            checkpoint_location=checkpoint_location,
            trigger_interval="1 minute",
            ttl=1800,
        )
