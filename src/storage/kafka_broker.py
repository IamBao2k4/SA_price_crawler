"""
Kafka message broker implementation
"""
import json
import logging
from typing import List, Callable
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

from ..core.interfaces import IMessageBroker
from ..core.models import KlineMessage
from ..config.settings import KafkaConfig

logger = logging.getLogger(__name__)


class KafkaMessageBroker(IMessageBroker):
    """Kafka implementation of message broker"""

    def __init__(self, config: KafkaConfig, mode: str = 'producer'):
        self.config = config
        self.mode = mode
        self.producer = None
        self.consumer = None

        if mode == 'producer':
            self._init_producer()
        elif mode == 'consumer':
            self._init_consumer()

    def _init_producer(self):
        """Initialize Kafka producer"""
        logger.info(f"Connecting to Kafka (producer): {self.config.bootstrap_servers}")

        try:
            logger.info("Creating KafkaProducer instance...")
            self.producer = KafkaProducer(
                bootstrap_servers=self.config.bootstrap_servers.split(','),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks=self.config.acks,
                retries=self.config.retries,
                compression_type=self.config.compression_type,
                batch_size=self.config.batch_size,
                linger_ms=self.config.linger_ms,
                request_timeout_ms=self.config.request_timeout_ms
            )
            logger.info("Kafka producer connected successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise

    def _init_consumer(self):
        """Initialize Kafka consumer"""
        logger.info(f"Connecting to Kafka (consumer): {self.config.bootstrap_servers}")

        # Topics will be subscribed when consume() is called
        logger.info("✅ Kafka consumer ready")

    def publish(self, topic: str, key: str, message: KlineMessage) -> bool:
        """Publish message to Kafka"""
        if not self.producer:
            raise RuntimeError("Producer not initialized")

        try:
            future = self.producer.send(
                topic,
                key=key,
                value=message.to_dict()
            )
            # future.get(timeout=10)  # Uncomment for synchronous
            return True
        except KafkaError as e:
            logger.error(f"Kafka publish error: {e}")
            return False

    def consume(self, topics: List[str], callback: Callable) -> None:
        """Consume messages from Kafka"""
        logger.info(f"Subscribing to topics: {', '.join(topics)}")

        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=self.config.bootstrap_servers.split(','),
            group_id=self.config.group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
            max_poll_records=self.config.max_poll_records
        )

        logger.info("✅ Subscribed to Kafka topics")

        # Consume messages
        try:
            for message in self.consumer:
                callback(message.value)
        except KeyboardInterrupt:
            logger.info("Consumer interrupted")
        finally:
            self.close()

    def flush(self):
        """Flush pending messages"""
        if self.producer:
            self.producer.flush()

    def close(self) -> None:
        """Close connections"""
        if self.producer:
            logger.info("Closing Kafka producer...")
            self.producer.flush()
            self.producer.close()
            logger.info("✅ Producer closed")

        if self.consumer:
            logger.info("Closing Kafka consumer...")
            self.consumer.close()
            logger.info("✅ Consumer closed")
