"""
Schema definitions for TikTok data.
"""
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    TimestampType, BooleanType, MapType
)


# Schema for TikTok music info
TIKTOK_MUSIC_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("author", StringType(), True),
])


# Schema for TikTok video data
TIKTOK_SCHEMA = StructType([
    StructField("content_type", StringType(), False),
    StructField("content_id", StringType(), False),
    StructField("description", StringType(), True),
    StructField("author_id", StringType(), True),
    StructField("author_username", StringType(), True),
    StructField("author_nickname", StringType(), True),
    StructField("author_verified", BooleanType(), True),
    StructField("author_followers", LongType(), True),
    StructField("author_following", LongType(), True),
    StructField("author_likes", LongType(), True),
    StructField("views", LongType(), True),
    StructField("likes", LongType(), True),
    StructField("comments", LongType(), True),
    StructField("shares", LongType(), True),
    StructField("duration", IntegerType(), True),
    StructField("music", TIKTOK_MUSIC_SCHEMA, True),
    StructField("hashtag", StringType(), True),
    StructField("challenge_id", StringType(), True),
    StructField("challenge_views", LongType(), True),
    StructField("published_at", StringType(), True),
    StructField("url", StringType(), True),
    StructField("fetched_at", StringType(), True),
])
