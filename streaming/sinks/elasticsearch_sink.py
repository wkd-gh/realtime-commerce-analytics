"""
Elasticsearch sink for search and logging.
"""
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

logger = logging.getLogger(__name__)


class ElasticsearchSink:
    """
    Elasticsearch sink for full-text search and logging.

    Supports:
    - Content indexing for search
    - Log aggregation
    - Analytics data
    """

    def __init__(
        self,
        host: str = "elasticsearch",
        port: int = 9200,
        scheme: str = "http",
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        """
        Initialize Elasticsearch sink.

        Args:
            host: Elasticsearch host
            port: Elasticsearch port
            scheme: HTTP or HTTPS
            username: Username (optional)
            password: Password (optional)
        """
        self.host = host
        self.port = port
        self.scheme = scheme
        self.username = username
        self.password = password
        self._client = None

    def _get_client(self):
        """Lazy load Elasticsearch client."""
        if self._client is None:
            from elasticsearch import Elasticsearch

            auth = None
            if self.username and self.password:
                auth = (self.username, self.password)

            self._client = Elasticsearch(
                [f"{self.scheme}://{self.host}:{self.port}"],
                basic_auth=auth,
            )
        return self._client

    def index_document(
        self,
        index: str,
        document: Dict,
        doc_id: Optional[str] = None,
    ) -> bool:
        """
        Index a single document.

        Args:
            index: Index name
            document: Document to index
            doc_id: Document ID (optional)

        Returns:
            True if successful
        """
        try:
            client = self._get_client()

            # Add timestamp if not present
            if "@timestamp" not in document:
                document["@timestamp"] = datetime.utcnow().isoformat()

            if doc_id:
                client.index(index=index, id=doc_id, document=document)
            else:
                client.index(index=index, document=document)

            return True

        except Exception as e:
            logger.error(f"Failed to index document: {e}")
            return False

    def bulk_index(
        self,
        index: str,
        documents: List[Dict],
        id_field: Optional[str] = None,
    ) -> int:
        """
        Bulk index multiple documents.

        Args:
            index: Index name
            documents: List of documents
            id_field: Field to use as document ID (optional)

        Returns:
            Number of documents indexed
        """
        if not documents:
            return 0

        try:
            from elasticsearch.helpers import bulk

            client = self._get_client()

            actions = []
            for doc in documents:
                # Add timestamp
                if "@timestamp" not in doc:
                    doc["@timestamp"] = datetime.utcnow().isoformat()

                action = {
                    "_index": index,
                    "_source": doc,
                }

                if id_field and id_field in doc:
                    action["_id"] = str(doc[id_field])

                actions.append(action)

            success, failed = bulk(client, actions, raise_on_error=False)
            logger.debug(f"Indexed {success} documents, {len(failed)} failed")

            return success

        except Exception as e:
            logger.error(f"Bulk index failed: {e}")
            return 0

    def create_foreach_batch_writer(
        self,
        index_prefix: str,
        id_field: Optional[str] = None,
        use_daily_index: bool = True,
    ):
        """
        Create a foreachBatch writer function.

        Args:
            index_prefix: Prefix for index name
            id_field: Field to use as document ID
            use_daily_index: Whether to use daily index pattern

        Returns:
            foreachBatch function
        """
        host = self.host
        port = self.port
        scheme = self.scheme
        username = self.username
        password = self.password

        def write_to_elasticsearch(batch_df: DataFrame, batch_id: int):
            """Write batch to Elasticsearch."""
            if batch_df.isEmpty():
                return

            try:
                from elasticsearch import Elasticsearch
                from elasticsearch.helpers import bulk

                auth = None
                if username and password:
                    auth = (username, password)

                client = Elasticsearch(
                    [f"{scheme}://{host}:{port}"],
                    basic_auth=auth,
                )

                # Determine index name
                if use_daily_index:
                    date_str = datetime.utcnow().strftime("%Y.%m.%d")
                    index_name = f"{index_prefix}-{date_str}"
                else:
                    index_name = index_prefix

                # Convert to documents
                rows = batch_df.collect()
                actions = []

                for row in rows:
                    doc = row.asDict()

                    # Add timestamp
                    if "@timestamp" not in doc:
                        doc["@timestamp"] = datetime.utcnow().isoformat()

                    action = {
                        "_index": index_name,
                        "_source": doc,
                    }

                    if id_field and id_field in doc:
                        action["_id"] = str(doc[id_field])

                    actions.append(action)

                if actions:
                    success, failed = bulk(client, actions, raise_on_error=False)
                    logger.info(f"Batch {batch_id}: Indexed {success} docs to {index_name}")

            except Exception as e:
                logger.error(f"Batch {batch_id}: Failed to write to Elasticsearch: {e}")

        return write_to_elasticsearch

    def write_stream(
        self,
        df: DataFrame,
        index_prefix: str,
        checkpoint_location: str,
        id_field: Optional[str] = None,
        trigger_interval: str = "30 seconds",
        use_daily_index: bool = True,
    ) -> StreamingQuery:
        """
        Write streaming DataFrame to Elasticsearch.

        Args:
            df: Streaming DataFrame
            index_prefix: Prefix for index name
            checkpoint_location: Checkpoint directory
            id_field: Field to use as document ID
            trigger_interval: Trigger processing interval
            use_daily_index: Whether to use daily index pattern

        Returns:
            StreamingQuery object
        """
        return df.writeStream \
            .foreachBatch(self.create_foreach_batch_writer(
                index_prefix, id_field, use_daily_index
            )) \
            .outputMode("append") \
            .trigger(processingTime=trigger_interval) \
            .option("checkpointLocation", f"{checkpoint_location}/{index_prefix}_es") \
            .start()

    @staticmethod
    def write_content_search(
        df: DataFrame,
        sink: "ElasticsearchSink",
        checkpoint_location: str,
    ) -> StreamingQuery:
        """
        Write content to Elasticsearch for search.

        Args:
            df: Content DataFrame
            sink: ElasticsearchSink instance
            checkpoint_location: Checkpoint directory

        Returns:
            StreamingQuery object
        """
        return sink.write_stream(
            df,
            index_prefix="content-search",
            checkpoint_location=checkpoint_location,
            id_field="content_id",
            trigger_interval="30 seconds",
            use_daily_index=False,
        )

    @staticmethod
    def write_logs(
        df: DataFrame,
        sink: "ElasticsearchSink",
        checkpoint_location: str,
    ) -> StreamingQuery:
        """
        Write logs to Elasticsearch.

        Args:
            df: Log DataFrame
            sink: ElasticsearchSink instance
            checkpoint_location: Checkpoint directory

        Returns:
            StreamingQuery object
        """
        return sink.write_stream(
            df,
            index_prefix="logs-streaming",
            checkpoint_location=checkpoint_location,
            trigger_interval="10 seconds",
            use_daily_index=True,
        )
