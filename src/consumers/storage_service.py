"""
Storage Service - Consumes from message broker and stores to database
"""
import asyncio
import time
import logging
import threading
from datetime import datetime
from typing import List, Optional

from ..core.interfaces import IMessageBroker, IStorage, INatsPublisher
from ..config.settings import Settings

logger = logging.getLogger(__name__)


class StorageService:
    """Service to consume messages and store to database"""

    def __init__(
        self,
        message_broker: IMessageBroker,
        storage: IStorage,
        settings: Settings,
        nats_publisher: Optional[INatsPublisher] = None
    ):
        self.message_broker = message_broker
        self.storage = storage
        self.nats_publisher = nats_publisher
        self.settings = settings

        self._nats_loop: Optional[asyncio.AbstractEventLoop] = None
        self._nats_thread: Optional[threading.Thread] = None

        # Statistics
        self.stats = {
            'total_consumed': 0,
            'total_saved': 0,
            'total_published': 0,
            'total_errors': 0,
            'batch_count': 0
        }

    def _start_nats_loop(self):
        """Start persistent event loop for NATS in background thread"""
        if self.nats_publisher is None:
            return

        def run_loop():
            self._nats_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._nats_loop)

            try:
                self._nats_loop.run_until_complete(self.nats_publisher.connect())
                logger.info("NATS connected in background thread")
            except Exception as e:
                logger.error(f"Failed to connect NATS in background thread: {e}")
                return

            self._nats_loop.run_forever()

        self._nats_thread = threading.Thread(target=run_loop, daemon=True)
        self._nats_thread.start()

        time.sleep(1)
        logger.info("NATS background thread started")

    def _stop_nats_loop(self):
        """Stop the NATS event loop"""
        if self._nats_loop and self._nats_loop.is_running():
            async def close_nats():
                if self.nats_publisher:
                    await self.nats_publisher.close()

            future = asyncio.run_coroutine_threadsafe(close_nats(), self._nats_loop)
            try:
                future.result(timeout=5)
            except Exception as e:
                logger.error(f"Error closing NATS: {e}")

            self._nats_loop.call_soon_threadsafe(self._nats_loop.stop)

        if self._nats_thread:
            self._nats_thread.join(timeout=5)
            logger.info("NATS background thread stopped")

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

        # Start NATS in background thread with persistent connection
        self._start_nats_loop()

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
            self._stop_nats_loop()
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
            logger.info(f"Saved {saved} documents to MongoDB")

            if self.nats_publisher and self._nats_loop and self._nats_loop.is_running():
                future = asyncio.run_coroutine_threadsafe(
                    self._publish_to_nats(batch),
                    self._nats_loop
                )
                try:
                    published = future.result(timeout=30)
                    self.stats['total_published'] += published
                    logger.info(f"Published {published} messages to NATS")
                except Exception as e:
                    logger.error(f"Error publishing to NATS: {e}")

            if self.stats['batch_count'] % 10 == 0:
                self._print_stats()
        else:
            self.stats['total_errors'] += len(batch)
            logger.error("Batch save failed")

    async def _publish_to_nats(self, batch: List[dict]) -> int:
        """
        Publish batch to NATS subjects following pattern: candles.{symbol}.{interval}
        
        Returns:
            Number of successfully published messages
        """
        published_count = 0
        
        for message in batch:
            try:
                from ..core.models import Kline
                
                # Convert message dict to Kline
                kline = Kline(
                    symbol=message['symbol'],
                    interval=message['interval'],
                    open_time=message['open_time'],
                    open=message['open'],
                    high=message['high'],
                    low=message['low'],
                    close=message['close'],
                    volume=message['volume'],
                    close_time=message['close_time'],
                    quote_asset_volume=message['quote_asset_volume'],
                    number_of_trades=message['number_of_trades'],
                    taker_buy_base_asset_volume=message['taker_buy_base_asset_volume'],
                    taker_buy_quote_asset_volume=message['taker_buy_quote_asset_volume']
                )
                
                # Publish to NATS
                success = await self.nats_publisher.publish_kline(kline)
                if success:
                    published_count += 1
                    
            except Exception as e:
                logger.error(f"Failed to publish message to NATS: {e}")
                
        return published_count

    def _print_stats(self):
        """Print consumption statistics"""
        logger.info("\n" + "="*70)
        logger.info("STATISTICS")
        logger.info("="*70)
        logger.info(f"Total consumed: {self.stats['total_consumed']:,}")
        logger.info(f"Total saved: {self.stats['total_saved']:,}")
        logger.info(f"Total published: {self.stats['total_published']:,}")
        logger.info(f"Total batches: {self.stats['batch_count']:,}")
        logger.info(f"Errors: {self.stats['total_errors']:,}")
        logger.info("="*70 + "\n")
