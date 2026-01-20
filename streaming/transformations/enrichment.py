"""
Data enrichment transformations for streaming data.
"""
import re
from typing import List, Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, udf, when, lit, array, array_contains, lower, regexp_extract,
    split, explode, trim, coalesce, current_timestamp, concat_ws,
    size, element_at
)
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, DoubleType


class DataEnrichment:
    """
    Data enrichment transformations for adding derived fields.
    """

    # Brand keyword mappings
    BRAND_KEYWORDS = {
        "Apple": ["애플", "아이폰", "맥북", "에어팟", "apple", "iphone", "macbook", "airpods"],
        "Samsung": ["삼성", "갤럭시", "삼성전자", "samsung", "galaxy"],
        "Nike": ["나이키", "에어맥스", "조던", "nike", "airmax", "jordan"],
        "Dyson": ["다이슨", "에어랩", "dyson", "airwrap"],
        "Olive Young": ["올리브영", "올영", "oliveyoung"],
        "Coupang": ["쿠팡", "coupang", "로켓배송"],
        "Musinsa": ["무신사", "musinsa"],
    }

    # Category keyword mappings
    CATEGORY_KEYWORDS = {
        "Electronics": ["전자", "가전", "IT", "테크", "스마트폰", "노트북", "태블릿"],
        "Beauty": ["뷰티", "화장품", "스킨케어", "메이크업", "코스메틱"],
        "Fashion": ["패션", "의류", "옷", "신발", "가방", "액세서리"],
        "Food": ["음식", "식품", "맛집", "요리", "푸드"],
        "Lifestyle": ["라이프스타일", "인테리어", "홈리빙", "리빙"],
    }

    @classmethod
    def extract_brand(cls, text: str) -> Optional[str]:
        """
        Extract brand name from text.

        Args:
            text: Text to analyze

        Returns:
            Brand name or None
        """
        if not text:
            return None

        text_lower = text.lower()
        for brand, keywords in cls.BRAND_KEYWORDS.items():
            for keyword in keywords:
                if keyword.lower() in text_lower:
                    return brand
        return None

    @classmethod
    def extract_category(cls, text: str) -> Optional[str]:
        """
        Extract category from text.

        Args:
            text: Text to analyze

        Returns:
            Category name or None
        """
        if not text:
            return None

        text_lower = text.lower()
        for category, keywords in cls.CATEGORY_KEYWORDS.items():
            for keyword in keywords:
                if keyword.lower() in text_lower:
                    return category
        return None

    @classmethod
    def extract_hashtags(cls, text: str) -> List[str]:
        """
        Extract hashtags from text.

        Args:
            text: Text to analyze

        Returns:
            List of hashtags
        """
        if not text:
            return []

        # Match both Korean and English hashtags
        pattern = r'#[\w\u3131-\u3163\uac00-\ud7a3]+'
        return re.findall(pattern, text)

    @classmethod
    def extract_mentions(cls, text: str) -> List[str]:
        """
        Extract mentions from text.

        Args:
            text: Text to analyze

        Returns:
            List of mentions
        """
        if not text:
            return []

        pattern = r'@[\w]+'
        return re.findall(pattern, text)

    @staticmethod
    def add_brand_column(df: DataFrame) -> DataFrame:
        """
        Add brand column based on text content.

        Args:
            df: DataFrame with text content

        Returns:
            DataFrame with brand column
        """
        # Create UDF for brand extraction
        extract_brand_udf = udf(DataEnrichment.extract_brand, StringType())

        # Combine title and text for analysis
        return df.withColumn(
            "combined_text",
            concat_ws(" ",
                coalesce(col("title"), lit("")),
                coalesce(col("text"), lit(""))
            )
        ).withColumn(
            "brand",
            extract_brand_udf(col("combined_text"))
        ).drop("combined_text")

    @staticmethod
    def add_category_column(df: DataFrame) -> DataFrame:
        """
        Add category column based on text content.

        Args:
            df: DataFrame with text content

        Returns:
            DataFrame with category column
        """
        extract_category_udf = udf(DataEnrichment.extract_category, StringType())

        return df.withColumn(
            "combined_text",
            concat_ws(" ",
                coalesce(col("title"), lit("")),
                coalesce(col("text"), lit(""))
            )
        ).withColumn(
            "category",
            extract_category_udf(col("combined_text"))
        ).drop("combined_text")

    @staticmethod
    def add_hashtags_column(df: DataFrame) -> DataFrame:
        """
        Add hashtags column extracted from text.

        Args:
            df: DataFrame with text content

        Returns:
            DataFrame with hashtags column
        """
        extract_hashtags_udf = udf(DataEnrichment.extract_hashtags, ArrayType(StringType()))

        return df.withColumn(
            "extracted_hashtags",
            extract_hashtags_udf(
                concat_ws(" ",
                    coalesce(col("title"), lit("")),
                    coalesce(col("text"), lit(""))
                )
            )
        ).withColumn(
            "hashtags",
            when(col("hashtags").isNotNull(), col("hashtags"))
            .otherwise(col("extracted_hashtags"))
        ).drop("extracted_hashtags")

    @staticmethod
    def calculate_buzz_score(df: DataFrame) -> DataFrame:
        """
        Calculate buzz score based on engagement metrics.

        Buzz Score = Views (40%) + Engagement (30%) + Recency (30%)

        Args:
            df: DataFrame with engagement metrics

        Returns:
            DataFrame with buzz_score column
        """
        # Normalize views (log scale to handle large numbers)
        df = df.withColumn(
            "normalized_views",
            when(col("views") > 0,
                 (col("views").cast("double").alias("v") + 1).cast("double")
            ).otherwise(1.0)
        )

        # Normalize engagement rate (cap at 100)
        df = df.withColumn(
            "normalized_engagement",
            when(col("engagement_rate") > 100, 100.0)
            .otherwise(coalesce(col("engagement_rate"), lit(0.0)))
        )

        # Calculate buzz score (simplified without recency for streaming)
        return df.withColumn(
            "buzz_score",
            (col("normalized_views") * 0.4) +
            (col("normalized_engagement") * 0.6)
        ).drop("normalized_views", "normalized_engagement")

    @staticmethod
    def enrich_content(df: DataFrame) -> DataFrame:
        """
        Apply all enrichment transformations.

        Args:
            df: Unified DataFrame

        Returns:
            Enriched DataFrame
        """
        df = DataEnrichment.add_brand_column(df)
        df = DataEnrichment.add_category_column(df)
        df = DataEnrichment.add_hashtags_column(df)
        df = DataEnrichment.calculate_buzz_score(df)

        return df.withColumn(
            "enriched_at",
            current_timestamp()
        )

    @staticmethod
    def add_influencer_tier(df: DataFrame) -> DataFrame:
        """
        Add influencer tier based on follower count.

        Args:
            df: DataFrame with author_followers column

        Returns:
            DataFrame with influencer_tier column
        """
        return df.withColumn(
            "influencer_tier",
            when(col("author_followers") >= 1000000, "mega")
            .when(col("author_followers") >= 100000, "macro")
            .when(col("author_followers") >= 10000, "micro")
            .when(col("author_followers") >= 1000, "nano")
            .otherwise("regular")
        )

    @staticmethod
    def add_content_length_features(df: DataFrame) -> DataFrame:
        """
        Add content length features.

        Args:
            df: DataFrame with text content

        Returns:
            DataFrame with length features
        """
        return df.withColumn(
            "title_length",
            when(col("title").isNotNull(), size(split(col("title"), " ")))
            .otherwise(0)
        ).withColumn(
            "text_length",
            when(col("text").isNotNull(), size(split(col("text"), " ")))
            .otherwise(0)
        ).withColumn(
            "hashtag_count",
            when(col("hashtags").isNotNull(), size(col("hashtags")))
            .otherwise(0)
        )
