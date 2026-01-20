"""
Schema definitions for Naver Shopping data.
"""
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, TimestampType
)


# Schema for Naver Shopping product data
NAVER_SCHEMA = StructType([
    StructField("content_type", StringType(), False),
    StructField("content_id", StringType(), False),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("link", StringType(), True),
    StructField("image_url", StringType(), True),
    StructField("low_price", LongType(), True),
    StructField("high_price", LongType(), True),
    StructField("discount_rate", DoubleType(), True),
    StructField("mall_name", StringType(), True),
    StructField("product_type", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("maker", StringType(), True),
    StructField("category1", StringType(), True),
    StructField("category2", StringType(), True),
    StructField("category3", StringType(), True),
    StructField("category4", StringType(), True),
    StructField("search_keyword", StringType(), True),
    StructField("search_category", StringType(), True),
    StructField("total_results", LongType(), True),
    StructField("fetched_at", StringType(), True),
])
