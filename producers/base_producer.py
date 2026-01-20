"""
Base Producer class for Kafka message production.
"""
import json
import logging
import time
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError
from prometheus_client import Counter, Histogram, Gauge

logger = logging.getLogger(__name__)


# Prometheus metrics
MESSAGES_SENT = Counter(
    'producer_messages_sent_total',
    'Total messages sent to Kafka',
    ['platform', 'topic']
)

MESSAGES_FAILED = Counter(
    'producer_messages_failed_total',
    'Total messages failed to send',
    ['platform', 'topic']
)

MESSAGE_LATENCY = Histogram(
    'producer_message_latency_seconds',
    'Message production latency',
    ['platform'],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
)

API_QUOTA_REMAINING = Gauge(
    'producer_api_quota_remaining',
    'Remaining API quota',
    ['platform']
)


class BaseProducer(ABC):
    """
    Abstract base class for Kafka producers.

    Each platform-specific producer should inherit from this class
    and implement the fetch_data and transform_data methods.
    """

    def __init__(
        self,
        kafka_config: Dict[str, Any],
        topic_name: str,
        platform_name: str,
        batch_size: int = 100,
        linger_ms: int = 10,
    ):
        """
        Initialize the base producer.

        Args:
            kafka_config: Kafka configuration dictionary
            topic_name: Kafka topic to produce to
            platform_name: Name of the platform (youtube, twitter, etc.)
            batch_size: Number of messages to batch before sending
            linger_ms: Time to wait for batching in milliseconds
        """
        self.topic_name = topic_name
        self.platform_name = platform_name
        self.batch_size = batch_size

        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_config.get('bootstrap_servers', 'localhost:9092'),
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            retry_backoff_ms=1000,
            batch_size=batch_size * 1024,  # Convert to bytes
            linger_ms=linger_ms,
            compression_type='snappy',
            max_in_flight_requests_per_connection=5,
        )

        self._running = False
        self._message_buffer: List[Dict] = []

        logger.info(f"Initialized {platform_name} producer for topic {topic_name}")

    @abstractmethod
    def fetch_data(self) -> List[Dict[str, Any]]:
        """
        Fetch data from the platform API.

        Returns:
            List of raw data dictionaries from the platform
        """
        pass

    @abstractmethod
    def transform_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform raw platform data to a standardized format.

        Args:
            raw_data: Raw data from the platform API

        Returns:
            Transformed data dictionary
        """
        pass

    def _create_message(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a Kafka message with metadata.

        Args:
            data: Transformed data dictionary

        Returns:
            Message dictionary with metadata
        """
        return {
            'message_id': str(uuid.uuid4()),
            'platform': self.platform_name,
            'timestamp': datetime.utcnow().isoformat(),
            'ingestion_time': time.time(),
            'data': data,
        }

    def _get_message_key(self, data: Dict[str, Any]) -> Optional[str]:
        """
        Get the message key for partitioning.

        Args:
            data: Message data

        Returns:
            Message key string or None
        """
        # Use content_id as key for partitioning
        content_id = data.get('data', {}).get('content_id')
        if content_id:
            return f"{self.platform_name}:{content_id}"
        return None

    def send_to_kafka(self, data: Dict[str, Any]) -> bool:
        """
        Send a single message to Kafka.

        Args:
            data: Data dictionary to send

        Returns:
            True if successful, False otherwise
        """
        try:
            message = self._create_message(data)
            key = self._get_message_key(message)

            start_time = time.time()

            future = self.producer.send(
                self.topic_name,
                key=key,
                value=message
            )

            # Wait for send to complete
            record_metadata = future.get(timeout=10)

            latency = time.time() - start_time
            MESSAGE_LATENCY.labels(platform=self.platform_name).observe(latency)
            MESSAGES_SENT.labels(platform=self.platform_name, topic=self.topic_name).inc()

            logger.debug(
                f"Sent message to {record_metadata.topic}:{record_metadata.partition} "
                f"offset={record_metadata.offset}"
            )

            return True

        except KafkaError as e:
            MESSAGES_FAILED.labels(platform=self.platform_name, topic=self.topic_name).inc()
            logger.error(f"Failed to send message: {e}")
            return False

    def send_batch(self, data_list: List[Dict[str, Any]]) -> int:
        """
        Send a batch of messages to Kafka.

        Args:
            data_list: List of data dictionaries to send

        Returns:
            Number of successfully sent messages
        """
        success_count = 0

        for data in data_list:
            if self.send_to_kafka(data):
                success_count += 1

        self.producer.flush()

        logger.info(
            f"Sent batch of {success_count}/{len(data_list)} messages "
            f"to {self.topic_name}"
        )

        return success_count

    def run_once(self) -> int:
        """
        Run a single fetch-transform-send cycle.

        Returns:
            Number of messages processed
        """
        try:
            # Fetch data from the platform
            raw_data_list = self.fetch_data()

            if not raw_data_list:
                logger.debug(f"No data fetched from {self.platform_name}")
                return 0

            # Transform and send each item
            transformed_data = []
            for raw_data in raw_data_list:
                try:
                    data = self.transform_data(raw_data)
                    if data:
                        transformed_data.append(data)
                except Exception as e:
                    logger.warning(f"Failed to transform data: {e}")

            # Send batch to Kafka
            if transformed_data:
                return self.send_batch(transformed_data)

            return 0

        except Exception as e:
            logger.error(f"Error in run_once: {e}")
            return 0

    def start(self, interval_seconds: int = 60):
        """
        Start the producer in a continuous loop.

        Args:
            interval_seconds: Seconds between fetch cycles
        """
        self._running = True
        logger.info(f"Starting {self.platform_name} producer with interval {interval_seconds}s")

        while self._running:
            try:
                count = self.run_once()
                logger.info(f"Processed {count} messages from {self.platform_name}")

            except Exception as e:
                logger.error(f"Error in producer loop: {e}")

            # Wait for next interval
            time.sleep(interval_seconds)

    def stop(self):
        """Stop the producer."""
        self._running = False
        self.producer.flush()
        self.producer.close()
        logger.info(f"Stopped {self.platform_name} producer")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
