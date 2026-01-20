"""
Main entry point for Spark Streaming application.
"""
import logging
import os
import signal
import sys
from typing import Dict, List

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp

from .config import get_config, StreamingConfig
from .schemas import KAFKA_MESSAGE_SCHEMA
from .transformations import DataParser, WindowAggregations, DataEnrichment, CrossPlatformAnalyzer
from .ml import SentimentAnalyzer, TrendDetector, KeywordExtractor
from .sinks import PostgresSink, ClickHouseSink, RedisSink, ElasticsearchSink, SlackSink

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)


class CommerceAnalyticsStreaming:
    """
    Main streaming application for commerce analytics.

    Orchestrates the entire streaming pipeline:
    1. Read from Kafka topics
    2. Parse and transform data
    3. Apply ML models
    4. Aggregate metrics
    5. Write to multiple sinks
    """

    def __init__(self, config: StreamingConfig = None):
        """
        Initialize streaming application.

        Args:
            config: StreamingConfig instance
        """
        self.config = config or get_config()
        self.spark = None
        self.queries: List = []
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        """Set up graceful shutdown handlers."""
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.stop()
        sys.exit(0)

    def create_spark_session(self) -> SparkSession:
        """
        Create and configure Spark session.

        Returns:
            SparkSession instance
        """
        logger.info("Creating Spark session...")

        self.spark = SparkSession.builder \
            .appName(self.config.spark_app_name) \
            .config("spark.sql.shuffle.partitions", "12") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.sql.streaming.checkpointLocation", self.config.checkpoint_location) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")

        logger.info(f"Spark session created: {self.spark.sparkContext.applicationId}")
        return self.spark

    def read_kafka_stream(self, topics: List[str] = None):
        """
        Read streaming data from Kafka topics.

        Args:
            topics: List of topic names (default: all source topics)

        Returns:
            Streaming DataFrame
        """
        topics = topics or self.config.all_source_topics

        logger.info(f"Reading from Kafka topics: {topics}")

        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config.kafka_bootstrap_servers) \
            .option("subscribe", ",".join(topics)) \
            .option("startingOffsets", self.config.kafka_starting_offsets) \
            .option("maxOffsetsPerTrigger", self.config.kafka_max_offsets_per_trigger) \
            .option("failOnDataLoss", "false") \
            .load()

    def process_stream(self, raw_stream):
        """
        Process the raw Kafka stream.

        Args:
            raw_stream: Raw Kafka DataFrame

        Returns:
            Dictionary of processed DataFrames
        """
        logger.info("Processing stream...")

        # Parse Kafka messages
        parsed = DataParser.parse_kafka_message(raw_stream)

        # Parse platform-specific data
        youtube_df = DataParser.parse_youtube_data(parsed)
        twitter_df = DataParser.parse_twitter_data(parsed)
        tiktok_df = DataParser.parse_tiktok_data(parsed)
        naver_df = DataParser.parse_naver_data(parsed)

        # Create unified view
        unified_df = DataParser.create_unified_view(
            youtube_df, twitter_df, tiktok_df, naver_df
        )

        # Enrich data
        enriched_df = DataEnrichment.enrich_content(unified_df)

        # Add sentiment (using simple method for performance)
        sentiment_df = SentimentAnalyzer.add_simple_sentiment(enriched_df, "text")

        # Extract keywords
        keywords_df = KeywordExtractor.add_keywords_column(sentiment_df, "text")

        return {
            "youtube": youtube_df,
            "twitter": twitter_df,
            "tiktok": tiktok_df,
            "naver": naver_df,
            "unified": unified_df,
            "enriched": enriched_df,
            "sentiment": sentiment_df,
            "keywords": keywords_df,
        }

    def create_aggregations(self, processed: Dict):
        """
        Create windowed aggregations.

        Args:
            processed: Dictionary of processed DataFrames

        Returns:
            Dictionary of aggregated DataFrames
        """
        logger.info("Creating aggregations...")

        enriched_df = processed["keywords"]

        # Platform aggregations
        platform_metrics_5m = WindowAggregations.aggregate_by_platform(
            enriched_df,
            window_duration="5 minutes",
            watermark_delay=self.config.watermark_delay,
        )

        platform_metrics_1h = WindowAggregations.aggregate_by_platform(
            enriched_df,
            window_duration="1 hour",
            watermark_delay=self.config.watermark_delay,
        )

        # Engagement metrics
        engagement_metrics = WindowAggregations.calculate_engagement_metrics(
            enriched_df,
            window_duration="1 minute",
            watermark_delay=self.config.watermark_delay,
        )

        # Top content
        top_content = WindowAggregations.top_content_by_window(
            enriched_df,
            window_duration="15 minutes",
            watermark_delay=self.config.watermark_delay,
            top_n=20,
        )

        # Cross-platform analysis
        brand_metrics = CrossPlatformAnalyzer.aggregate_by_brand(
            enriched_df,
            window_duration="15 minutes",
            watermark_delay=self.config.watermark_delay,
        )

        # Keyword aggregations
        keyword_trends = KeywordExtractor.aggregate_keywords(
            enriched_df,
            window_duration="15 minutes",
            watermark_delay=self.config.watermark_delay,
        )

        return {
            "platform_metrics_5m": platform_metrics_5m,
            "platform_metrics_1h": platform_metrics_1h,
            "engagement_metrics": engagement_metrics,
            "top_content": top_content,
            "brand_metrics": brand_metrics,
            "keyword_trends": keyword_trends,
        }

    def detect_trends(self, processed: Dict, aggregations: Dict):
        """
        Detect trending content.

        Args:
            processed: Dictionary of processed DataFrames
            aggregations: Dictionary of aggregated DataFrames

        Returns:
            Trending content DataFrame
        """
        logger.info("Detecting trends...")

        enriched_df = processed["keywords"]

        # Calculate Z-scores
        with_zscore = TrendDetector.calculate_zscore(
            enriched_df,
            value_column="views",
            partition_by="platform",
            window_duration="1 hour",
            watermark_delay=self.config.watermark_delay,
        )

        # Detect trending
        trending = TrendDetector.detect_trending_content(
            with_zscore,
            zscore_threshold=self.config.trending_zscore_threshold,
        )

        # Calculate buzz scores
        with_buzz = TrendDetector.calculate_buzz_score(trending)

        # Rank trending content
        ranked_trending = TrendDetector.rank_trending_content(
            with_buzz,
            top_n=20,
            window_duration="15 minutes",
            watermark_delay=self.config.watermark_delay,
        )

        return ranked_trending

    def setup_sinks(self):
        """
        Set up output sinks.

        Returns:
            Dictionary of sink instances
        """
        logger.info("Setting up sinks...")

        return {
            "postgres": PostgresSink(
                host=self.config.postgres_host,
                port=self.config.postgres_port,
                database=self.config.postgres_db,
                user=self.config.postgres_user,
                password=self.config.postgres_password,
            ),
            "clickhouse": ClickHouseSink(
                host=self.config.clickhouse_host,
                port=self.config.clickhouse_port,
                database=self.config.clickhouse_db,
                user=self.config.clickhouse_user,
                password=self.config.clickhouse_password,
            ),
            "redis": RedisSink(
                host=self.config.redis_host,
                port=self.config.redis_port,
                db=self.config.redis_db,
            ),
            "elasticsearch": ElasticsearchSink(
                host=self.config.elasticsearch_host,
                port=self.config.elasticsearch_port,
            ),
            "slack": SlackSink(
                webhook_url=self.config.slack_webhook_url or "",
            ) if self.config.slack_webhook_url else None,
        }

    def start_output_streams(self, processed: Dict, aggregations: Dict, trending, sinks: Dict):
        """
        Start all output streaming queries.

        Args:
            processed: Processed DataFrames
            aggregations: Aggregated DataFrames
            trending: Trending content DataFrame
            sinks: Sink instances
        """
        logger.info("Starting output streams...")

        checkpoint_base = self.config.checkpoint_location

        # PostgreSQL outputs
        self.queries.append(
            sinks["postgres"].write_stream(
                aggregations["platform_metrics_5m"],
                table_name="platform_metrics",
                checkpoint_location=checkpoint_base,
                trigger_interval="30 seconds",
            )
        )

        self.queries.append(
            sinks["postgres"].write_stream(
                aggregations["brand_metrics"],
                table_name="brand_metrics",
                checkpoint_location=checkpoint_base,
                trigger_interval="1 minute",
            )
        )

        # Redis outputs (rankings)
        self.queries.append(
            sinks["redis"].write_stream(
                trending,
                key_prefix="trending",
                score_column="avg_buzz_score",
                id_column="content_id",
                checkpoint_location=checkpoint_base,
                trigger_interval="30 seconds",
            )
        )

        # Elasticsearch outputs (search)
        self.queries.append(
            sinks["elasticsearch"].write_stream(
                processed["enriched"],
                index_prefix="content-search",
                checkpoint_location=checkpoint_base,
                id_field="content_id",
                trigger_interval="30 seconds",
            )
        )

        # Slack alerts (if configured)
        if sinks["slack"]:
            self.queries.append(
                sinks["slack"].write_stream(
                    trending,
                    checkpoint_location=checkpoint_base,
                    zscore_threshold=self.config.trending_zscore_threshold,
                    trigger_interval="1 minute",
                )
            )

        # Console output for debugging (optional)
        if os.getenv("DEBUG_OUTPUT", "false").lower() == "true":
            self.queries.append(
                aggregations["platform_metrics_5m"].writeStream
                .format("console")
                .outputMode("append")
                .trigger(processingTime="30 seconds")
                .start()
            )

        logger.info(f"Started {len(self.queries)} output streams")

    def run(self):
        """
        Run the streaming application.
        """
        logger.info("Starting Commerce Analytics Streaming...")

        try:
            # Create Spark session
            self.create_spark_session()

            # Read from Kafka
            raw_stream = self.read_kafka_stream()

            # Process stream
            processed = self.process_stream(raw_stream)

            # Create aggregations
            aggregations = self.create_aggregations(processed)

            # Detect trends
            trending = self.detect_trends(processed, aggregations)

            # Setup sinks
            sinks = self.setup_sinks()

            # Start output streams
            self.start_output_streams(processed, aggregations, trending, sinks)

            logger.info("All streams started. Waiting for termination...")

            # Wait for all queries
            for query in self.queries:
                query.awaitTermination()

        except Exception as e:
            logger.error(f"Streaming application failed: {e}")
            raise
        finally:
            self.stop()

    def stop(self):
        """
        Stop all streaming queries gracefully.
        """
        logger.info("Stopping streaming application...")

        for query in self.queries:
            try:
                query.stop()
            except Exception as e:
                logger.warning(f"Error stopping query: {e}")

        if self.spark:
            self.spark.stop()

        logger.info("Streaming application stopped")


def main():
    """Main entry point."""
    from dotenv import load_dotenv
    load_dotenv()

    app = CommerceAnalyticsStreaming()
    app.run()


if __name__ == "__main__":
    main()
