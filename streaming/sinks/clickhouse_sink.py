"""
ClickHouse sink for time-series data.
"""
import logging
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, Row
from pyspark.sql.streaming import StreamingQuery

logger = logging.getLogger(__name__)


class ClickHouseSink:
    """
    ClickHouse sink for high-volume time-series data.

    Uses native ClickHouse driver for optimal performance.
    """

    def __init__(
        self,
        host: str = "clickhouse",
        port: int = 9000,
        database: str = "commerce_analytics",
        user: str = "admin",
        password: str = "password",
    ):
        """
        Initialize ClickHouse sink.

        Args:
            host: ClickHouse host
            port: ClickHouse native port
            database: Database name
            user: Username
            password: Password
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self._client = None

    def _get_client(self):
        """Lazy load ClickHouse client."""
        if self._client is None:
            from clickhouse_driver import Client
            self._client = Client(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
            )
        return self._client

    def write_batch(
        self,
        data: List[Dict],
        table_name: str,
    ) -> int:
        """
        Write a batch of data to ClickHouse.

        Args:
            data: List of dictionaries to insert
            table_name: Target table name

        Returns:
            Number of rows inserted
        """
        if not data:
            return 0

        try:
            client = self._get_client()

            # Get columns from first row
            columns = list(data[0].keys())
            values = [[row[col] for col in columns] for row in data]

            client.execute(
                f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES",
                values,
            )

            logger.debug(f"Inserted {len(data)} rows into {table_name}")
            return len(data)

        except Exception as e:
            logger.error(f"Failed to write to ClickHouse: {e}")
            raise

    def create_foreach_batch_writer(
        self,
        table_name: str,
    ):
        """
        Create a foreachBatch writer function.

        Args:
            table_name: Target table name

        Returns:
            foreachBatch function
        """
        host = self.host
        port = self.port
        database = self.database
        user = self.user
        password = self.password

        def write_to_clickhouse(batch_df: DataFrame, batch_id: int):
            """Write batch to ClickHouse."""
            if batch_df.isEmpty():
                return

            try:
                from clickhouse_driver import Client

                # Convert to list of dicts
                rows = batch_df.collect()
                data = [row.asDict() for row in rows]

                if not data:
                    return

                # Get columns
                columns = list(data[0].keys())
                values = [[row[col] for col in columns] for row in data]

                client = Client(
                    host=host,
                    port=port,
                    database=database,
                    user=user,
                    password=password,
                )

                client.execute(
                    f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES",
                    values,
                )

                logger.info(f"Batch {batch_id}: Wrote {len(data)} rows to ClickHouse {table_name}")

            except Exception as e:
                logger.error(f"Batch {batch_id}: Failed to write to ClickHouse: {e}")

        return write_to_clickhouse

    def write_stream(
        self,
        df: DataFrame,
        table_name: str,
        checkpoint_location: str,
        trigger_interval: str = "10 seconds",
        output_mode: str = "append",
    ) -> StreamingQuery:
        """
        Write streaming DataFrame to ClickHouse.

        Args:
            df: Streaming DataFrame
            table_name: Target table name
            checkpoint_location: Checkpoint directory
            trigger_interval: Trigger processing interval
            output_mode: Output mode

        Returns:
            StreamingQuery object
        """
        return df.writeStream \
            .foreachBatch(self.create_foreach_batch_writer(table_name)) \
            .outputMode(output_mode) \
            .trigger(processingTime=trigger_interval) \
            .option("checkpointLocation", f"{checkpoint_location}/{table_name}_ch") \
            .start()

    @staticmethod
    def write_raw_events(
        df: DataFrame,
        sink: "ClickHouseSink",
        checkpoint_location: str,
    ) -> StreamingQuery:
        """
        Write raw events to ClickHouse for time-series analysis.

        Args:
            df: Raw events DataFrame
            sink: ClickHouseSink instance
            checkpoint_location: Checkpoint directory

        Returns:
            StreamingQuery object
        """
        return sink.write_stream(
            df,
            table_name="raw_events",
            checkpoint_location=checkpoint_location,
            trigger_interval="10 seconds",
        )

    @staticmethod
    def write_metrics_timeseries(
        df: DataFrame,
        sink: "ClickHouseSink",
        checkpoint_location: str,
    ) -> StreamingQuery:
        """
        Write metrics time-series to ClickHouse.

        Args:
            df: Metrics DataFrame
            sink: ClickHouseSink instance
            checkpoint_location: Checkpoint directory

        Returns:
            StreamingQuery object
        """
        return sink.write_stream(
            df,
            table_name="metrics_timeseries",
            checkpoint_location=checkpoint_location,
            trigger_interval="30 seconds",
        )
