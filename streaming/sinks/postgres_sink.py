"""
PostgreSQL sink for streaming data.
"""
import logging
from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

logger = logging.getLogger(__name__)


class PostgresSink:
    """
    PostgreSQL sink for streaming aggregated metrics.

    Uses foreachBatch to write micro-batches to PostgreSQL.
    """

    def __init__(
        self,
        host: str = "postgres",
        port: int = 5432,
        database: str = "commerce_analytics",
        user: str = "admin",
        password: str = "password",
    ):
        """
        Initialize PostgreSQL sink.

        Args:
            host: PostgreSQL host
            port: PostgreSQL port
            database: Database name
            user: Username
            password: Password
        """
        self.jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
        self.properties = {
            "user": user,
            "password": password,
            "driver": "org.postgresql.Driver",
        }

    def write_batch(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "append",
    ) -> None:
        """
        Write a batch DataFrame to PostgreSQL.

        Args:
            df: DataFrame to write
            table_name: Target table name
            mode: Write mode (append, overwrite)
        """
        try:
            df.write.jdbc(
                url=self.jdbc_url,
                table=table_name,
                mode=mode,
                properties=self.properties,
            )
            logger.debug(f"Wrote batch to {table_name}")
        except Exception as e:
            logger.error(f"Failed to write to PostgreSQL: {e}")
            raise

    def create_foreach_batch_writer(
        self,
        table_name: str,
        mode: str = "append",
    ):
        """
        Create a foreachBatch writer function.

        Args:
            table_name: Target table name
            mode: Write mode

        Returns:
            foreachBatch function
        """
        jdbc_url = self.jdbc_url
        properties = self.properties

        def write_to_postgres(batch_df: DataFrame, batch_id: int):
            """Write batch to PostgreSQL."""
            if batch_df.isEmpty():
                return

            try:
                batch_df.write.jdbc(
                    url=jdbc_url,
                    table=table_name,
                    mode=mode,
                    properties=properties,
                )
                logger.info(f"Batch {batch_id}: Wrote {batch_df.count()} rows to {table_name}")
            except Exception as e:
                logger.error(f"Batch {batch_id}: Failed to write to {table_name}: {e}")

        return write_to_postgres

    def write_stream(
        self,
        df: DataFrame,
        table_name: str,
        checkpoint_location: str,
        trigger_interval: str = "10 seconds",
        output_mode: str = "append",
    ) -> StreamingQuery:
        """
        Write streaming DataFrame to PostgreSQL.

        Args:
            df: Streaming DataFrame
            table_name: Target table name
            checkpoint_location: Checkpoint directory
            trigger_interval: Trigger processing interval
            output_mode: Output mode (append, complete, update)

        Returns:
            StreamingQuery object
        """
        return df.writeStream \
            .foreachBatch(self.create_foreach_batch_writer(table_name)) \
            .outputMode(output_mode) \
            .trigger(processingTime=trigger_interval) \
            .option("checkpointLocation", f"{checkpoint_location}/{table_name}") \
            .start()

    @staticmethod
    def write_aggregated_metrics(
        df: DataFrame,
        sink: "PostgresSink",
        checkpoint_location: str,
    ) -> StreamingQuery:
        """
        Write aggregated metrics to PostgreSQL.

        Args:
            df: Aggregated metrics DataFrame
            sink: PostgresSink instance
            checkpoint_location: Checkpoint directory

        Returns:
            StreamingQuery object
        """
        return sink.write_stream(
            df,
            table_name="platform_metrics",
            checkpoint_location=checkpoint_location,
            trigger_interval="30 seconds",
            output_mode="append",
        )

    @staticmethod
    def write_trending_content(
        df: DataFrame,
        sink: "PostgresSink",
        checkpoint_location: str,
    ) -> StreamingQuery:
        """
        Write trending content to PostgreSQL.

        Args:
            df: Trending content DataFrame
            sink: PostgresSink instance
            checkpoint_location: Checkpoint directory

        Returns:
            StreamingQuery object
        """
        return sink.write_stream(
            df,
            table_name="trending_content",
            checkpoint_location=checkpoint_location,
            trigger_interval="1 minute",
            output_mode="append",
        )

    @staticmethod
    def write_sentiment_results(
        df: DataFrame,
        sink: "PostgresSink",
        checkpoint_location: str,
    ) -> StreamingQuery:
        """
        Write sentiment analysis results to PostgreSQL.

        Args:
            df: Sentiment results DataFrame
            sink: PostgresSink instance
            checkpoint_location: Checkpoint directory

        Returns:
            StreamingQuery object
        """
        return sink.write_stream(
            df,
            table_name="sentiment_analysis",
            checkpoint_location=checkpoint_location,
            trigger_interval="30 seconds",
            output_mode="append",
        )
