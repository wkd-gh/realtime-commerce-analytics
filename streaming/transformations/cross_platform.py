"""
Cross-platform analysis transformations.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, window, count, sum as spark_sum, avg, max as spark_max,
    min as spark_min, first, when, lit, coalesce, collect_list,
    struct, current_timestamp, expr
)


class CrossPlatformAnalyzer:
    """
    Analyzer for cross-platform content analysis.

    Provides methods for:
    - Brand analysis across platforms
    - Correlation analysis
    - Time lag analysis
    """

    @staticmethod
    def aggregate_by_brand(
        df: DataFrame,
        window_duration: str = "15 minutes",
        watermark_delay: str = "10 minutes",
    ) -> DataFrame:
        """
        Aggregate metrics by brand across all platforms.

        Args:
            df: Enriched unified DataFrame
            window_duration: Window size
            watermark_delay: Watermark for late data

        Returns:
            DataFrame with brand-level aggregations
        """
        # Filter to records with identified brands
        brand_df = df.filter(col("brand").isNotNull())

        return brand_df.withWatermark("event_time", watermark_delay).groupBy(
            window(col("event_time"), window_duration),
            col("brand"),
        ).agg(
            count("*").alias("total_mentions"),
            spark_sum("views").alias("total_reach"),
            spark_sum("likes").alias("total_likes"),
            avg("engagement_rate").alias("avg_engagement"),
            collect_list(
                struct(
                    col("platform"),
                    col("content_id"),
                    col("views"),
                    col("engagement_rate")
                )
            ).alias("platform_details"),
        ).select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "brand",
            "total_mentions",
            "total_reach",
            "total_likes",
            "avg_engagement",
            "platform_details",
        )

    @staticmethod
    def platform_breakdown_by_brand(
        df: DataFrame,
        window_duration: str = "15 minutes",
        watermark_delay: str = "10 minutes",
    ) -> DataFrame:
        """
        Get platform-specific metrics for each brand.

        Args:
            df: Enriched unified DataFrame
            window_duration: Window size
            watermark_delay: Watermark for late data

        Returns:
            DataFrame with brand-platform breakdown
        """
        brand_df = df.filter(col("brand").isNotNull())

        return brand_df.withWatermark("event_time", watermark_delay).groupBy(
            window(col("event_time"), window_duration),
            col("brand"),
            col("platform"),
        ).agg(
            count("*").alias("content_count"),
            spark_sum("views").alias("views"),
            spark_sum("likes").alias("likes"),
            spark_sum("comments").alias("comments"),
            avg("engagement_rate").alias("engagement_rate"),
            avg("buzz_score").alias("avg_buzz_score"),
        ).select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "brand",
            "platform",
            "content_count",
            "views",
            "likes",
            "comments",
            "engagement_rate",
            "avg_buzz_score",
        )

    @staticmethod
    def create_cross_platform_view(
        youtube_agg: DataFrame,
        twitter_agg: DataFrame,
        tiktok_agg: DataFrame,
        naver_agg: DataFrame,
        trends_agg: DataFrame = None,
    ) -> DataFrame:
        """
        Create a cross-platform view for brand comparison.

        Args:
            youtube_agg: YouTube aggregated by brand
            twitter_agg: Twitter aggregated by brand
            tiktok_agg: TikTok aggregated by brand
            naver_agg: Naver aggregated by brand
            trends_agg: Google Trends aggregated (optional)

        Returns:
            Cross-platform comparison DataFrame
        """
        # Join YouTube and Twitter
        joined = youtube_agg.alias("yt").join(
            twitter_agg.alias("tw"),
            (col("yt.brand") == col("tw.brand")) &
            (col("yt.window_start") == col("tw.window_start")),
            "full_outer"
        ).select(
            coalesce(col("yt.brand"), col("tw.brand")).alias("brand"),
            coalesce(col("yt.window_start"), col("tw.window_start")).alias("window_start"),
            coalesce(col("yt.window_end"), col("tw.window_end")).alias("window_end"),
            col("yt.views").alias("youtube_views"),
            col("yt.engagement_rate").alias("youtube_engagement"),
            col("tw.content_count").alias("twitter_mentions"),
            col("tw.engagement_rate").alias("twitter_engagement"),
        )

        # Join TikTok
        joined = joined.alias("j1").join(
            tiktok_agg.alias("tt"),
            (col("j1.brand") == col("tt.brand")) &
            (col("j1.window_start") == col("tt.window_start")),
            "full_outer"
        ).select(
            coalesce(col("j1.brand"), col("tt.brand")).alias("brand"),
            coalesce(col("j1.window_start"), col("tt.window_start")).alias("window_start"),
            coalesce(col("j1.window_end"), col("tt.window_end")).alias("window_end"),
            col("j1.youtube_views"),
            col("j1.youtube_engagement"),
            col("j1.twitter_mentions"),
            col("j1.twitter_engagement"),
            col("tt.views").alias("tiktok_views"),
            col("tt.engagement_rate").alias("tiktok_engagement"),
        )

        # Join Naver
        joined = joined.alias("j2").join(
            naver_agg.alias("nv"),
            (col("j2.brand") == col("nv.brand")) &
            (col("j2.window_start") == col("nv.window_start")),
            "full_outer"
        ).select(
            coalesce(col("j2.brand"), col("nv.brand")).alias("brand"),
            coalesce(col("j2.window_start"), col("nv.window_start")).alias("window_start"),
            coalesce(col("j2.window_end"), col("nv.window_end")).alias("window_end"),
            coalesce(col("j2.youtube_views"), lit(0)).alias("youtube_views"),
            coalesce(col("j2.youtube_engagement"), lit(0.0)).alias("youtube_engagement"),
            coalesce(col("j2.twitter_mentions"), lit(0)).alias("twitter_mentions"),
            coalesce(col("j2.twitter_engagement"), lit(0.0)).alias("twitter_engagement"),
            coalesce(col("j2.tiktok_views"), lit(0)).alias("tiktok_views"),
            coalesce(col("j2.tiktok_engagement"), lit(0.0)).alias("tiktok_engagement"),
            coalesce(col("nv.content_count"), lit(0)).alias("naver_search_count"),
        )

        # Calculate total reach and cross-platform score
        return joined.withColumn(
            "total_reach",
            col("youtube_views") + col("tiktok_views")
        ).withColumn(
            "platform_count",
            (when(col("youtube_views") > 0, 1).otherwise(0) +
             when(col("twitter_mentions") > 0, 1).otherwise(0) +
             when(col("tiktok_views") > 0, 1).otherwise(0) +
             when(col("naver_search_count") > 0, 1).otherwise(0))
        ).withColumn(
            "cross_platform_score",
            (
                coalesce(col("youtube_engagement"), lit(0.0)) +
                coalesce(col("twitter_engagement"), lit(0.0)) +
                coalesce(col("tiktok_engagement"), lit(0.0))
            ) / 3 * col("platform_count")
        ).withColumn(
            "analyzed_at",
            current_timestamp()
        )

    @staticmethod
    def analyze_category_trends(
        df: DataFrame,
        window_duration: str = "15 minutes",
        watermark_delay: str = "10 minutes",
    ) -> DataFrame:
        """
        Analyze trends by category across platforms.

        Args:
            df: Enriched unified DataFrame
            window_duration: Window size
            watermark_delay: Watermark for late data

        Returns:
            Category trend DataFrame
        """
        category_df = df.filter(col("category").isNotNull())

        return category_df.withWatermark("event_time", watermark_delay).groupBy(
            window(col("event_time"), window_duration),
            col("category"),
            col("platform"),
        ).agg(
            count("*").alias("content_count"),
            spark_sum("views").alias("total_views"),
            avg("engagement_rate").alias("avg_engagement"),
            avg("buzz_score").alias("avg_buzz_score"),
            spark_max("buzz_score").alias("max_buzz_score"),
        ).select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "category",
            "platform",
            "content_count",
            "total_views",
            "avg_engagement",
            "avg_buzz_score",
            "max_buzz_score",
        )

    @staticmethod
    def calculate_platform_share(
        df: DataFrame,
        window_duration: str = "1 hour",
        watermark_delay: str = "10 minutes",
    ) -> DataFrame:
        """
        Calculate platform share of voice for each brand.

        Args:
            df: Enriched unified DataFrame
            window_duration: Window size
            watermark_delay: Watermark for late data

        Returns:
            Platform share DataFrame
        """
        brand_df = df.filter(col("brand").isNotNull())

        # Get total by brand
        brand_total = brand_df.withWatermark("event_time", watermark_delay).groupBy(
            window(col("event_time"), window_duration),
            col("brand"),
        ).agg(
            spark_sum("views").alias("brand_total_views"),
        )

        # Get platform breakdown
        platform_breakdown = brand_df.withWatermark("event_time", watermark_delay).groupBy(
            window(col("event_time"), window_duration),
            col("brand"),
            col("platform"),
        ).agg(
            spark_sum("views").alias("platform_views"),
        )

        # Join and calculate share
        return platform_breakdown.alias("pb").join(
            brand_total.alias("bt"),
            (col("pb.brand") == col("bt.brand")) &
            (col("pb.window") == col("bt.window")),
            "inner"
        ).select(
            col("pb.window.start").alias("window_start"),
            col("pb.window.end").alias("window_end"),
            col("pb.brand"),
            col("pb.platform"),
            col("pb.platform_views"),
            col("bt.brand_total_views"),
            (col("pb.platform_views") / col("bt.brand_total_views") * 100).alias("platform_share_pct"),
        )
