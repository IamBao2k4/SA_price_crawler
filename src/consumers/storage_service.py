"""
Storage Service - Consumes from message broker and stores to database
"""
import time
import logging
from datetime import datetime
from typing import List

from ..core.interfaces import IMessageBroker, IStorage
from ..config.settings import Settings

logger = logging.getLogger(__name__)


class StorageService:
    """Service to consume messages and store to database"""

    def __init__(
        self,
        message_broker: IMessageBroker,
        storage: IStorage,
        settings: Settings
    ):
        self.message_broker = message_broker
        self.storage = storage
        self.settings = settings

        # Statistics
        self.stats = {
            'total_consumed': 0,
            'total_saved': 0,
            'total_errors': 0,
            'batch_count': 0
        }

    def run(self, intervals: List[str] = None):
        """Run continuous consumption"""
        if intervals is None:
            intervals = self.settings.active_intervals

        # Build topic list
        topics = [
            f"{self.settings.kafka.topic_prefix}.{interval}"
            for interval in intervals
        ]

        logger.info("="*70)
        logger.info(f"Storage Service Started - {datetime.now()}")
        logger.info("="*70)
        logger.info(f"Topics: {', '.join(topics)}")
        logger.info(f"Batch size: {self.settings.mongodb.batch_size}")
        logger.info(f"Batch timeout: {self.settings.mongodb.batch_timeout}s")
        logger.info("="*70)

        # Batch processing
        batch = []
        last_batch_time = time.time()

        def process_message(message: dict):
            """Callback for each message"""
            nonlocal batch, last_batch_time

            self.stats['total_consumed'] += 1
            batch.append(message)

            # Process batch if size reached or timeout
            current_time = time.time()
            should_process = (
                len(batch) >= self.settings.mongodb.batch_size or
                (current_time - last_batch_time) >= self.settings.mongodb.batch_timeout
            )

            if should_process and batch:
                self._process_batch(batch)
                batch = []
                last_batch_time = current_time

        try:
            # Consume messages
            self.message_broker.consume(topics, process_message)

        except KeyboardInterrupt:
            logger.info("\nReceived interrupt signal")
            # Process remaining batch
            if batch:
                logger.info(f"Processing final batch of {len(batch)} messages...")
                self._process_batch(batch)
        finally:
            self.message_broker.close()
            self.storage.close()
            self._print_stats()

    def _process_batch(self, batch: List[dict]):
        """Process a batch of messages"""
        logger.info(
            f"[{datetime.now().strftime('%H:%M:%S')}] "
            f"Processing batch of {len(batch)} messages..."
        )

        saved = self.storage.save_batch(batch)

        if saved > 0:
            self.stats['total_saved'] += saved
            self.stats['batch_count'] += 1
            logger.info(f"✅ Saved {saved} documents")

            # Print stats every 10 batches
            if self.stats['batch_count'] % 10 == 0:
                self._print_stats()
        else:
            self.stats['total_errors'] += len(batch)
            logger.error("❌ Batch save failed")

    def _print_stats(self):
        """Print consumption statistics"""
        logger.info("\n" + "="*70)
        logger.info("STATISTICS")
        logger.info("="*70)
        logger.info(f"Total consumed: {self.stats['total_consumed']:,}")
        logger.info(f"Total saved: {self.stats['total_saved']:,}")
        logger.info(f"Total batches: {self.stats['batch_count']:,}")
        logger.info(f"Errors: {self.stats['total_errors']:,}")
        logger.info("="*70 + "\n")
