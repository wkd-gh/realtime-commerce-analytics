"""
Trend detection for streaming data.
"""
import logging
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, avg, stddev, when, lit, window, count, sum as spark_sum,
    max as spark_max, current_timestamp, row_number, lag, lead,
    percent_rank, dense_rank
)
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


class TrendDetector:
    """
    Trend detection using statistical methods.

    Implements Z-score based anomaly detection and
    trend analysis for streaming data.
    """

    def __init__(
        self,
        zscore_threshold: float = 3.0,
        min_data_points: int = 10,
    ):
        """
        Initialize trend detector.

        Args:
            zscore_threshold: Z-score threshold for trending (default 3.0)
            min_data_points: Minimum data points needed for analysis
        """
        self.zscore_threshold = zscore_threshold
        self.min_data_points = min_data_points

    @staticmethod
    def calculate_zscore(
        df: DataFrame,
        value_column: str,
        partition_by: str = "platform",
        window_duration: str = "1 hour",
        watermark_delay: str = "10 minutes",
    ) -> DataFrame:
        """
        Calculate Z-score for values within a time window.

        Args:
            df: Input DataFrame with value column
            value_column: Column name for values to analyze
            partition_by: Column to partition by
            window_duration: Window size for calculating statistics
            watermark_delay: Watermark for late data

        Returns:
            DataFrame with z_score column
        """
        # Calculate statistics within window
        stats = df.withWatermark("event_time", watermark_delay).groupBy(
            window(col("event_time"), window_duration),
            col(partition_by),
        ).agg(
            avg(value_column).alias("mean_value"),
            stddev(value_column).alias("stddev_value"),
            count("*").alias("data_count"),
        )

        # Join back and calculate z-score
        return df.alias("main").join(
            stats.alias("stats"),
            (col("main.platform") == col("stats." + partition_by)) &
            (col("main.event_time") >= col("stats.window.start")) &
            (col("main.event_time") < col("stats.window.end")),
            "left"
        ).withColumn(
            "z_score",
            when(
                (col("stddev_value") > 0) & (col("data_count") >= 10),
                (col(value_column) - col("mean_value")) / col("stddev_value")
            ).otherwise(0.0)
        ).select(
            col("main.*"),
            col("z_score"),
            col("mean_value"),
            col("stddev_value"),
        )

    @staticmethod
    def detect_trending_content(
        df: DataFrame,
        zscore_threshold: float = 3.0,
        value_column: str = "views",
    ) -> DataFrame:
        """
        Detect trending content based on Z-score.

        Args:
            df: DataFrame with z_score column
            zscore_threshold: Threshold for marking as trending
            value_column: Value column used for detection

        Returns:
            DataFrame with is_trending column
        """
        return df.withColumn(
            "is_trending",
            when(col("z_score") > zscore_threshold, True).otherwise(False)
        ).withColumn(
            "trend_level",
            when(col("z_score") > 5.0, "viral")
            .when(col("z_score") > 4.0, "hot")
            .when(col("z_score") > zscore_threshold, "trending")
            .otherwise("normal")
        ).withColumn(
            "detected_at",
            current_timestamp()
        )

    @staticmethod
    def calculate_buzz_score(
        df: DataFrame,
        views_weight: float = 0.4,
        engagement_weight: float = 0.3,
        recency_weight: float = 0.3,
    ) -> DataFrame:
        """
        Calculate buzz score combining multiple factors.

        Buzz Score = Views (40%) + Engagement (30%) + Recency (30%)

        Args:
            df: Input DataFrame
            views_weight: Weight for views (default 0.4)
            engagement_weight: Weight for engagement (default 0.3)
            recency_weight: Weight for recency (default 0.3)

        Returns:
            DataFrame with buzz_score column
        """
        # Define window for percentile ranking
        views_window = Window.partitionBy("platform").orderBy("views")
        engagement_window = Window.partitionBy("platform").orderBy("engagement_rate")

        return df.withColumn(
            "views_percentile",
            percent_rank().over(views_window)
        ).withColumn(
            "engagement_percentile",
            percent_rank().over(engagement_window)
        ).withColumn(
            "buzz_score",
            (col("views_percentile") * views_weight) +
            (col("engagement_percentile") * engagement_weight) +
            (lit(0.5) * recency_weight)  # Simplified recency
        ).drop("views_percentile", "engagement_percentile")

    @staticmethod
    def detect_anomalies(
        df: DataFrame,
        value_columns: list,
        window_duration: str = "1 hour",
        watermark_delay: str = "10 minutes",
        threshold: float = 3.0,
    ) -> DataFrame:
        """
        Detect anomalies in multiple metrics.

        Args:
            df: Input DataFrame
            value_columns: List of columns to check for anomalies
            window_duration: Window size
            watermark_delay: Watermark for late data
            threshold: Z-score threshold

        Returns:
            DataFrame with anomaly flags
        """
        result_df = df

        for column in value_columns:
            # Calculate statistics
            stats = df.withWatermark("event_time", watermark_delay).groupBy(
                window(col("event_time"), window_duration),
                col("platform"),
            ).agg(
                avg(column).alias(f"{column}_mean"),
                stddev(column).alias(f"{column}_stddev"),
            )

            # Join and calculate anomaly
            result_df = result_df.alias("main").join(
                stats.alias("stats"),
                (col("main.platform") == col("stats.platform")) &
                (col("main.event_time") >= col("stats.window.start")) &
                (col("main.event_time") < col("stats.window.end")),
                "left"
            ).withColumn(
                f"{column}_zscore",
                when(
                    col(f"{column}_stddev") > 0,
                    (col(column) - col(f"{column}_mean")) / col(f"{column}_stddev")
                ).otherwise(0.0)
            ).withColumn(
                f"{column}_anomaly",
                when(
                    (col(f"{column}_zscore") > threshold) |
                    (col(f"{column}_zscore") < -threshold),
                    True
                ).otherwise(False)
            ).select(
                col("main.*"),
                col(f"{column}_zscore"),
                col(f"{column}_anomaly"),
            )

        return result_df

    @staticmethod
    def calculate_growth_rate(
        df: DataFrame,
        value_column: str = "views",
        partition_by: list = None,
    ) -> DataFrame:
        """
        Calculate growth rate compared to previous period.

        Args:
            df: Input DataFrame
            value_column: Column to calculate growth for
            partition_by: Columns to partition by

        Returns:
            DataFrame with growth_rate column
        """
        partition_by = partition_by or ["platform", "content_id"]

        window_spec = Window.partitionBy(*partition_by).orderBy("event_time")

        return df.withColumn(
            f"prev_{value_column}",
            lag(col(value_column), 1).over(window_spec)
        ).withColumn(
            "growth_rate",
            when(
                col(f"prev_{value_column}") > 0,
                (col(value_column) - col(f"prev_{value_column}")) /
                col(f"prev_{value_column}") * 100
            ).otherwise(0.0)
        ).withColumn(
            "is_growing",
            when(col("growth_rate") > 0, True).otherwise(False)
        ).drop(f"prev_{value_column}")

    @staticmethod
    def rank_trending_content(
        df: DataFrame,
        top_n: int = 20,
        window_duration: str = "15 minutes",
        watermark_delay: str = "10 minutes",
    ) -> DataFrame:
        """
        Rank trending content within windows.

        Args:
            df: DataFrame with trending content
            top_n: Number of top items to keep
            window_duration: Window size
            watermark_delay: Watermark for late data

        Returns:
            DataFrame with ranked trending content
        """
        # Aggregate by content within window
        aggregated = df.withWatermark("event_time", watermark_delay).groupBy(
            window(col("event_time"), window_duration),
            col("platform"),
            col("content_id"),
        ).agg(
            spark_sum("views").alias("total_views"),
            spark_sum("likes").alias("total_likes"),
            avg("buzz_score").alias("avg_buzz_score"),
            avg("z_score").alias("avg_zscore"),
            spark_max("is_trending").alias("is_trending"),
        )

        # Rank by buzz score
        rank_window = Window.partitionBy("window", "platform").orderBy(
            col("avg_buzz_score").desc()
        )

        return aggregated.withColumn(
            "rank",
            row_number().over(rank_window)
        ).filter(
            col("rank") <= top_n
        ).select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "platform",
            "rank",
            "content_id",
            "total_views",
            "total_likes",
            "avg_buzz_score",
            "avg_zscore",
            "is_trending",
        ).withColumn(
            "ranked_at",
            current_timestamp()
        )
