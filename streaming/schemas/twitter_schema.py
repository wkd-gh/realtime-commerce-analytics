"""
Schema definitions for Twitter data.
"""
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    TimestampType, BooleanType, ArrayType
)


# Schema for Twitter tweet data
TWITTER_SCHEMA = StructType([
    StructField("content_type", StringType(), False),
    StructField("content_id", StringType(), False),
    StructField("conversation_id", StringType(), True),
    StructField("text", StringType(), True),
    StructField("author_id", StringType(), True),
    StructField("author_username", StringType(), True),
    StructField("author_verified", BooleanType(), True),
    StructField("author_followers", LongType(), True),
    StructField("author_following", LongType(), True),
    StructField("likes", LongType(), True),
    StructField("retweets", LongType(), True),
    StructField("replies", LongType(), True),
    StructField("quotes", LongType(), True),
    StructField("impressions", LongType(), True),
    StructField("hashtags", ArrayType(StringType()), True),
    StructField("mentions", ArrayType(StringType()), True),
    StructField("urls", ArrayType(StringType()), True),
    StructField("language", StringType(), True),
    StructField("source", StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("url", StringType(), True),
    StructField("search_keyword", StringType(), True),
    StructField("fetched_at", StringType(), True),
])
