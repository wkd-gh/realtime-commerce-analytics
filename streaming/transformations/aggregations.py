"""
Window aggregation transformations for streaming data.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, window, count, sum as spark_sum, avg, max as spark_max,
    min as spark_min, first, last, collect_list, collect_set,
    approx_count_distinct, stddev, expr, when, lit,
    struct, row_number
)
from pyspark.sql.window import Window


class WindowAggregations:
    """
    Window-based aggregations for streaming data.

    Implements tumbling window aggregations for various time intervals
    (1 minute, 5 minutes, 15 minutes, 1 hour).
    """

    @staticmethod
    def aggregate_by_platform(
        df: DataFrame,
        window_duration: str = "5 minutes",
        watermark_delay: str = "10 minutes",
    ) -> DataFrame:
        """
        Aggregate metrics by platform within a time window.

        Args:
            df: Unified DataFrame with all platforms
            window_duration: Window size
            watermark_delay: Watermark for late data

        Returns:
            Aggregated DataFrame
        """
        return df.withWatermark("event_time", watermark_delay).groupBy(
            window(col("event_time"), window_duration),
            col("platform"),
        ).agg(
            count("*").alias("total_content"),
            spark_sum("views").alias("total_views"),
            spark_sum("likes").alias("total_likes"),
            spark_sum("comments").alias("total_comments"),
            spark_sum("shares").alias("total_shares"),
            avg("engagement_rate").alias("avg_engagement_rate"),
            spark_max("views").alias("max_views"),
            approx_count_distinct("author_id").alias("unique_authors"),
        ).select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("platform"),
            col("total_content"),
            col("total_views"),
            col("total_likes"),
            col("total_comments"),
            col("total_shares"),
            col("avg_engagement_rate"),
            col("max_views"),
            col("unique_authors"),
        )

    @staticmethod
    def aggregate_by_content_type(
        df: DataFrame,
        window_duration: str = "5 minutes",
        watermark_delay: str = "10 minutes",
    ) -> DataFrame:
        """
        Aggregate metrics by platform and content type.

        Args:
            df: Unified DataFrame
            window_duration: Window size
            watermark_delay: Watermark for late data

        Returns:
            Aggregated DataFrame
        """
        return df.withWatermark("event_time", watermark_delay).groupBy(
            window(col("event_time"), window_duration),
            col("platform"),
            col("content_type"),
        ).agg(
            count("*").alias("content_count"),
            spark_sum("views").alias("total_views"),
            spark_sum("likes").alias("total_likes"),
            avg("engagement_rate").alias("avg_engagement"),
            spark_max("engagement_rate").alias("max_engagement"),
        ).select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "platform",
            "content_type",
            "content_count",
            "total_views",
            "total_likes",
            "avg_engagement",
            "max_engagement",
        )

    @staticmethod
    def calculate_engagement_metrics(
        df: DataFrame,
        window_duration: str = "1 minute",
        watermark_delay: str = "10 minutes",
    ) -> DataFrame:
        """
        Calculate detailed engagement metrics per window.

        Args:
            df: Unified DataFrame
            window_duration: Window size
            watermark_delay: Watermark for late data

        Returns:
            DataFrame with engagement metrics
        """
        return df.withWatermark("event_time", watermark_delay).groupBy(
            window(col("event_time"), window_duration),
            col("platform"),
            col("content_id"),
        ).agg(
            first("title").alias("title"),
            first("author_name").alias("author_name"),
            spark_sum("views").alias("total_views"),
            spark_sum("likes").alias("total_likes"),
            spark_sum("comments").alias("total_comments"),
            spark_sum("shares").alias("total_shares"),
            first("url").alias("url"),
        ).withColumn(
            "engagement_rate",
            when(col("total_views") > 0,
                 (col("total_likes") + col("total_comments") + col("total_shares")) /
                 col("total_views") * 100
            ).otherwise(0.0)
        ).select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "platform",
            "content_id",
            "title",
            "author_name",
            "total_views",
            "total_likes",
            "total_comments",
            "total_shares",
            "engagement_rate",
            "url",
        )

    @staticmethod
    def calculate_growth_rate(
        df: DataFrame,
        window_duration: str = "5 minutes",
        slide_duration: str = "1 minute",
        watermark_delay: str = "10 minutes",
    ) -> DataFrame:
        """
        Calculate growth rate using sliding windows.

        Args:
            df: Unified DataFrame
            window_duration: Window size
            slide_duration: Slide interval
            watermark_delay: Watermark for late data

        Returns:
            DataFrame with growth rate metrics
        """
        # Current window aggregation
        current = df.withWatermark("event_time", watermark_delay).groupBy(
            window(col("event_time"), window_duration, slide_duration),
            col("platform"),
        ).agg(
            spark_sum("views").alias("current_views"),
            spark_sum("likes").alias("current_likes"),
            count("*").alias("current_count"),
        )

        # Use window function to get previous values
        window_spec = Window.partitionBy("platform").orderBy("window.start")

        return current.withColumn(
            "prev_views",
            first("current_views").over(
                window_spec.rowsBetween(-1, -1)
            )
        ).withColumn(
            "prev_likes",
            first("current_likes").over(
                window_spec.rowsBetween(-1, -1)
            )
        ).withColumn(
            "views_growth_rate",
            when(col("prev_views") > 0,
                 (col("current_views") - col("prev_views")) / col("prev_views") * 100
            ).otherwise(0.0)
        ).withColumn(
            "likes_growth_rate",
            when(col("prev_likes") > 0,
                 (col("current_likes") - col("prev_likes")) / col("prev_likes") * 100
            ).otherwise(0.0)
        ).select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "platform",
            "current_views",
            "current_likes",
            "current_count",
            "views_growth_rate",
            "likes_growth_rate",
        )

    @staticmethod
    def top_content_by_window(
        df: DataFrame,
        window_duration: str = "5 minutes",
        watermark_delay: str = "10 minutes",
        top_n: int = 10,
    ) -> DataFrame:
        """
        Get top content by views within each window.

        Args:
            df: Unified DataFrame
            window_duration: Window size
            watermark_delay: Watermark for late data
            top_n: Number of top items to return

        Returns:
            DataFrame with top content
        """
        # Aggregate by content
        content_agg = df.withWatermark("event_time", watermark_delay).groupBy(
            window(col("event_time"), window_duration),
            col("platform"),
            col("content_id"),
        ).agg(
            first("title").alias("title"),
            first("author_name").alias("author_name"),
            spark_sum("views").alias("views"),
            spark_sum("likes").alias("likes"),
            avg("engagement_rate").alias("engagement_rate"),
            first("url").alias("url"),
        )

        # Rank by views within each platform/window
        window_spec = Window.partitionBy("window", "platform").orderBy(
            col("views").desc()
        )

        return content_agg.withColumn(
            "rank", row_number().over(window_spec)
        ).filter(
            col("rank") <= top_n
        ).select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "platform",
            "rank",
            "content_id",
            "title",
            "author_name",
            "views",
            "likes",
            "engagement_rate",
            "url",
        )

    @staticmethod
    def multi_window_aggregation(
        df: DataFrame,
        watermark_delay: str = "10 minutes",
    ) -> dict:
        """
        Perform aggregations across multiple window sizes.

        Args:
            df: Unified DataFrame
            watermark_delay: Watermark for late data

        Returns:
            Dictionary of DataFrames for each window size
        """
        windows = {
            "1m": "1 minute",
            "5m": "5 minutes",
            "15m": "15 minutes",
            "1h": "1 hour",
        }

        results = {}
        for key, duration in windows.items():
            results[key] = WindowAggregations.aggregate_by_platform(
                df, window_duration=duration, watermark_delay=watermark_delay
            )

        return results
