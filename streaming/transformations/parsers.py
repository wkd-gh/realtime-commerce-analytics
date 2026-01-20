"""
Data parsers for transforming raw Kafka messages.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, from_json, get_json_object, when, lit, coalesce,
    to_timestamp, current_timestamp, expr, array, size,
    split, regexp_extract, lower, trim
)
from pyspark.sql.types import StringType, LongType, DoubleType, ArrayType

from ..schemas import (
    YOUTUBE_SCHEMA,
    TWITTER_SCHEMA,
    TIKTOK_SCHEMA,
    NAVER_SCHEMA,
    KAFKA_MESSAGE_SCHEMA,
)


class DataParser:
    """
    Parser for transforming raw Kafka messages into structured DataFrames.
    """

    @staticmethod
    def parse_kafka_message(df: DataFrame) -> DataFrame:
        """
        Parse raw Kafka messages into structured format.

        Args:
            df: Raw Kafka DataFrame with key/value columns

        Returns:
            Parsed DataFrame with message envelope
        """
        return df.select(
            col("key").cast(StringType()).alias("kafka_key"),
            from_json(col("value").cast(StringType()), KAFKA_MESSAGE_SCHEMA).alias("message"),
            col("timestamp").alias("kafka_timestamp"),
            col("topic"),
            col("partition"),
            col("offset"),
        ).select(
            "kafka_key",
            "message.*",
            "kafka_timestamp",
            "topic",
            "partition",
            "offset",
        )

    @staticmethod
    def parse_youtube_data(df: DataFrame) -> DataFrame:
        """
        Parse YouTube data from Kafka messages.

        Args:
            df: DataFrame with parsed Kafka messages

        Returns:
            DataFrame with YouTube-specific columns
        """
        youtube_df = df.filter(col("platform") == "youtube")

        return youtube_df.select(
            col("message_id"),
            col("platform"),
            col("timestamp").alias("event_time"),
            from_json(col("data"), YOUTUBE_SCHEMA).alias("content"),
        ).select(
            "message_id",
            "platform",
            to_timestamp("event_time").alias("event_time"),
            "content.*",
        ).withColumn(
            "views", coalesce(col("views"), lit(0))
        ).withColumn(
            "likes", coalesce(col("likes"), lit(0))
        ).withColumn(
            "comments", coalesce(col("comments"), lit(0))
        ).withColumn(
            "engagement_rate",
            when(col("views") > 0,
                 (col("likes") + col("comments")) / col("views") * 100
            ).otherwise(0.0)
        )

    @staticmethod
    def parse_twitter_data(df: DataFrame) -> DataFrame:
        """
        Parse Twitter data from Kafka messages.

        Args:
            df: DataFrame with parsed Kafka messages

        Returns:
            DataFrame with Twitter-specific columns
        """
        twitter_df = df.filter(col("platform") == "twitter")

        return twitter_df.select(
            col("message_id"),
            col("platform"),
            col("timestamp").alias("event_time"),
            from_json(col("data"), TWITTER_SCHEMA).alias("content"),
        ).select(
            "message_id",
            "platform",
            to_timestamp("event_time").alias("event_time"),
            "content.*",
        ).withColumn(
            "total_engagement",
            coalesce(col("likes"), lit(0)) +
            coalesce(col("retweets"), lit(0)) +
            coalesce(col("replies"), lit(0)) +
            coalesce(col("quotes"), lit(0))
        ).withColumn(
            "engagement_rate",
            when(col("impressions") > 0,
                 col("total_engagement") / col("impressions") * 100
            ).otherwise(0.0)
        )

    @staticmethod
    def parse_tiktok_data(df: DataFrame) -> DataFrame:
        """
        Parse TikTok data from Kafka messages.

        Args:
            df: DataFrame with parsed Kafka messages

        Returns:
            DataFrame with TikTok-specific columns
        """
        tiktok_df = df.filter(col("platform") == "tiktok")

        return tiktok_df.select(
            col("message_id"),
            col("platform"),
            col("timestamp").alias("event_time"),
            from_json(col("data"), TIKTOK_SCHEMA).alias("content"),
        ).select(
            "message_id",
            "platform",
            to_timestamp("event_time").alias("event_time"),
            "content.*",
        ).withColumn(
            "views", coalesce(col("views"), lit(0))
        ).withColumn(
            "likes", coalesce(col("likes"), lit(0))
        ).withColumn(
            "comments", coalesce(col("comments"), lit(0))
        ).withColumn(
            "shares", coalesce(col("shares"), lit(0))
        ).withColumn(
            "engagement_rate",
            when(col("views") > 0,
                 (col("likes") + col("comments") + col("shares")) / col("views") * 100
            ).otherwise(0.0)
        )

    @staticmethod
    def parse_naver_data(df: DataFrame) -> DataFrame:
        """
        Parse Naver Shopping data from Kafka messages.

        Args:
            df: DataFrame with parsed Kafka messages

        Returns:
            DataFrame with Naver-specific columns
        """
        naver_df = df.filter(col("platform") == "naver_shopping")

        return naver_df.select(
            col("message_id"),
            col("platform"),
            col("timestamp").alias("event_time"),
            from_json(col("data"), NAVER_SCHEMA).alias("content"),
        ).select(
            "message_id",
            "platform",
            to_timestamp("event_time").alias("event_time"),
            "content.*",
        ).withColumn(
            "price_range",
            when(col("high_price") > col("low_price"),
                 col("high_price") - col("low_price")
            ).otherwise(0)
        )

    @staticmethod
    def parse_trends_data(df: DataFrame) -> DataFrame:
        """
        Parse Google Trends data from Kafka messages.

        Args:
            df: DataFrame with parsed Kafka messages

        Returns:
            DataFrame with Trends-specific columns
        """
        trends_df = df.filter(col("platform") == "google_trends")

        return trends_df.select(
            col("message_id"),
            col("platform"),
            col("timestamp").alias("event_time"),
            col("data"),
        ).select(
            "message_id",
            "platform",
            to_timestamp("event_time").alias("event_time"),
            get_json_object(col("data"), "$.content_type").alias("content_type"),
            get_json_object(col("data"), "$.content_id").alias("content_id"),
            get_json_object(col("data"), "$.keyword").alias("keyword"),
            get_json_object(col("data"), "$.geo").alias("geo"),
            col("data").alias("raw_data"),
        )

    @staticmethod
    def create_unified_view(
        youtube_df: DataFrame,
        twitter_df: DataFrame,
        tiktok_df: DataFrame,
        naver_df: DataFrame,
    ) -> DataFrame:
        """
        Create a unified view across all platforms.

        Args:
            youtube_df: Parsed YouTube DataFrame
            twitter_df: Parsed Twitter DataFrame
            tiktok_df: Parsed TikTok DataFrame
            naver_df: Parsed Naver DataFrame

        Returns:
            Unified DataFrame with common columns
        """
        # Normalize YouTube data
        youtube_unified = youtube_df.select(
            col("message_id"),
            col("platform"),
            col("content_type"),
            col("content_id"),
            col("title"),
            col("description").alias("text"),
            col("channel_id").alias("author_id"),
            col("channel_title").alias("author_name"),
            lit(None).cast(LongType()).alias("author_followers"),
            col("views"),
            col("likes"),
            col("comments"),
            lit(0).cast(LongType()).alias("shares"),
            col("engagement_rate"),
            col("url"),
            col("event_time"),
            current_timestamp().alias("processing_time"),
        )

        # Normalize Twitter data
        twitter_unified = twitter_df.select(
            col("message_id"),
            col("platform"),
            col("content_type"),
            col("content_id"),
            lit(None).alias("title"),
            col("text"),
            col("author_id"),
            col("author_username").alias("author_name"),
            col("author_followers"),
            coalesce(col("impressions"), lit(0)).alias("views"),
            col("likes"),
            col("replies").alias("comments"),
            col("retweets").alias("shares"),
            col("engagement_rate"),
            col("url"),
            col("event_time"),
            current_timestamp().alias("processing_time"),
        )

        # Normalize TikTok data
        tiktok_unified = tiktok_df.select(
            col("message_id"),
            col("platform"),
            col("content_type"),
            col("content_id"),
            lit(None).alias("title"),
            col("description").alias("text"),
            col("author_id"),
            coalesce(col("author_nickname"), col("author_username")).alias("author_name"),
            col("author_followers"),
            col("views"),
            col("likes"),
            col("comments"),
            col("shares"),
            col("engagement_rate"),
            col("url"),
            col("event_time"),
            current_timestamp().alias("processing_time"),
        )

        # Normalize Naver data
        naver_unified = naver_df.select(
            col("message_id"),
            col("platform"),
            col("content_type"),
            col("content_id"),
            col("title"),
            lit(None).alias("text"),
            lit(None).alias("author_id"),
            col("mall_name").alias("author_name"),
            lit(None).cast(LongType()).alias("author_followers"),
            col("total_results").alias("views"),
            lit(0).cast(LongType()).alias("likes"),
            lit(0).cast(LongType()).alias("comments"),
            lit(0).cast(LongType()).alias("shares"),
            lit(0.0).alias("engagement_rate"),
            col("link").alias("url"),
            col("event_time"),
            current_timestamp().alias("processing_time"),
        )

        # Union all platforms
        return youtube_unified.union(twitter_unified).union(tiktok_unified).union(naver_unified)
