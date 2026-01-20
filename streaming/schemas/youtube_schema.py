"""
Schema definitions for YouTube data.
"""
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    TimestampType, BooleanType, ArrayType
)


# Schema for YouTube video data
YOUTUBE_VIDEO_SCHEMA = StructType([
    StructField("content_type", StringType(), False),
    StructField("content_id", StringType(), False),
    StructField("channel_id", StringType(), True),
    StructField("channel_title", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("thumbnail_url", StringType(), True),
    StructField("views", LongType(), True),
    StructField("likes", LongType(), True),
    StructField("comments", LongType(), True),
    StructField("url", StringType(), True),
    StructField("search_keyword", StringType(), True),
    StructField("fetched_at", StringType(), True),
])


# Schema for YouTube comment data
YOUTUBE_COMMENT_SCHEMA = StructType([
    StructField("content_type", StringType(), False),
    StructField("content_id", StringType(), False),
    StructField("video_id", StringType(), True),
    StructField("author", StringType(), True),
    StructField("author_channel_id", StringType(), True),
    StructField("text", StringType(), True),
    StructField("likes", LongType(), True),
    StructField("published_at", StringType(), True),
    StructField("fetched_at", StringType(), True),
])


# Combined YouTube schema (used for parsing Kafka messages)
YOUTUBE_SCHEMA = StructType([
    StructField("content_type", StringType(), False),
    StructField("content_id", StringType(), False),
    StructField("channel_id", StringType(), True),
    StructField("channel_title", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("thumbnail_url", StringType(), True),
    StructField("views", LongType(), True),
    StructField("likes", LongType(), True),
    StructField("comments", LongType(), True),
    StructField("url", StringType(), True),
    StructField("search_keyword", StringType(), True),
    StructField("video_id", StringType(), True),
    StructField("author", StringType(), True),
    StructField("author_channel_id", StringType(), True),
    StructField("text", StringType(), True),
    StructField("fetched_at", StringType(), True),
])
