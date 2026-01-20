"""
Common schema definitions for streaming data.
"""
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, TimestampType, BooleanType, ArrayType, MapType
)


# Schema for Kafka message envelope
KAFKA_MESSAGE_SCHEMA = StructType([
    StructField("message_id", StringType(), False),
    StructField("platform", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("ingestion_time", DoubleType(), False),
    StructField("data", StringType(), False),  # JSON string of platform-specific data
])


# Unified content schema for cross-platform analysis
UNIFIED_CONTENT_SCHEMA = StructType([
    StructField("message_id", StringType(), False),
    StructField("platform", StringType(), False),
    StructField("content_type", StringType(), False),
    StructField("content_id", StringType(), False),
    StructField("title", StringType(), True),
    StructField("text", StringType(), True),
    StructField("author_id", StringType(), True),
    StructField("author_name", StringType(), True),
    StructField("author_followers", LongType(), True),
    StructField("views", LongType(), True),
    StructField("likes", LongType(), True),
    StructField("comments", LongType(), True),
    StructField("shares", LongType(), True),
    StructField("engagement_rate", DoubleType(), True),
    StructField("url", StringType(), True),
    StructField("keywords", ArrayType(StringType()), True),
    StructField("hashtags", ArrayType(StringType()), True),
    StructField("brand", StringType(), True),
    StructField("category", StringType(), True),
    StructField("published_at", TimestampType(), True),
    StructField("event_time", TimestampType(), False),
    StructField("processing_time", TimestampType(), False),
])


# Aggregated metrics schema
AGGREGATED_METRICS_SCHEMA = StructType([
    StructField("window_start", TimestampType(), False),
    StructField("window_end", TimestampType(), False),
    StructField("platform", StringType(), False),
    StructField("content_type", StringType(), True),
    StructField("total_content", LongType(), False),
    StructField("total_views", LongType(), False),
    StructField("total_likes", LongType(), False),
    StructField("total_comments", LongType(), False),
    StructField("total_shares", LongType(), False),
    StructField("avg_engagement_rate", DoubleType(), False),
    StructField("max_views", LongType(), False),
    StructField("top_content_id", StringType(), True),
    StructField("unique_authors", LongType(), False),
])


# Sentiment result schema
SENTIMENT_SCHEMA = StructType([
    StructField("content_id", StringType(), False),
    StructField("platform", StringType(), False),
    StructField("text", StringType(), True),
    StructField("sentiment", StringType(), False),  # positive, negative, neutral
    StructField("sentiment_score", DoubleType(), False),
    StructField("confidence", DoubleType(), False),
    StructField("processed_at", TimestampType(), False),
])


# Trending content schema
TRENDING_SCHEMA = StructType([
    StructField("content_id", StringType(), False),
    StructField("platform", StringType(), False),
    StructField("title", StringType(), True),
    StructField("views", LongType(), False),
    StructField("likes", LongType(), False),
    StructField("engagement_rate", DoubleType(), False),
    StructField("z_score", DoubleType(), False),
    StructField("buzz_score", DoubleType(), False),
    StructField("is_trending", BooleanType(), False),
    StructField("detected_at", TimestampType(), False),
    StructField("url", StringType(), True),
])


# Cross-platform analysis schema
CROSS_PLATFORM_SCHEMA = StructType([
    StructField("brand", StringType(), False),
    StructField("window_start", TimestampType(), False),
    StructField("window_end", TimestampType(), False),
    StructField("youtube_views", LongType(), True),
    StructField("youtube_engagement", DoubleType(), True),
    StructField("twitter_mentions", LongType(), True),
    StructField("twitter_sentiment", DoubleType(), True),
    StructField("tiktok_views", LongType(), True),
    StructField("tiktok_engagement", DoubleType(), True),
    StructField("naver_search_count", LongType(), True),
    StructField("naver_avg_price", DoubleType(), True),
    StructField("google_trends_score", DoubleType(), True),
    StructField("total_reach", LongType(), True),
    StructField("cross_platform_score", DoubleType(), True),
])
