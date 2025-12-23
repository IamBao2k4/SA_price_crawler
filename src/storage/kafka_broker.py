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
            # Wait for acknowledgment to ensure message is sent
            record_metadata = future.get(timeout=10)
            logger.debug(
                f"Message sent to {record_metadata.topic} "
                f"partition {record_metadata.partition} "
                f"offset {record_metadata.offset}"
            )
            return True
        except KafkaError as e:
            logger.error(f"Kafka publish error: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error publishing to Kafka: {e}")
            return False

    def consume(self, topics: List[str], callback: Callable) -> None:
        """Consume messages from Kafka"""
        logger.info(f"Subscribing to topics: {', '.join(topics)}")

        # Create consumer without auto-subscribing
        self.consumer = KafkaConsumer(
            bootstrap_servers=self.config.bootstrap_servers.split(','),
            group_id=self.config.group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
            max_poll_records=self.config.max_poll_records
        )

        # Subscribe to topics
        self.consumer.subscribe(topics)
        logger.info("✅ Subscribed to Kafka topics")

        # Wait for partition assignment using poll
        logger.info("Waiting for partition assignment...")
        max_retries = 10
        retry_count = 0

        while retry_count < max_retries:
            # Poll with timeout to trigger rebalance
            records = self.consumer.poll(timeout_ms=1000)

            partitions = self.consumer.assignment()
            if partitions:
                logger.info(f"✅ Assigned partitions: {partitions}")

                # Log consumer position
                for partition in partitions:
                    position = self.consumer.position(partition)
                    logger.info(f"Partition {partition.topic}:{partition.partition} position: {position}")

                # Process any records from this poll
                for topic_partition, messages in records.items():
                    for message in messages:
                        callback(message.value)

                break

            retry_count += 1
            logger.info(f"Waiting for partitions... (attempt {retry_count}/{max_retries})")

        if not self.consumer.assignment():
            logger.error("❌ No partitions assigned after waiting. Check Kafka configuration.")
            return

        # Continue consuming messages
        try:
            logger.info("Starting message consumption loop...")
            message_count = 0

            while True:
                records = self.consumer.poll(timeout_ms=1000)

                for topic_partition, messages in records.items():
                    for message in messages:
                        message_count += 1
                        if message_count % 100 == 0:
                            logger.info(f"Consumed {message_count} messages so far...")
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
