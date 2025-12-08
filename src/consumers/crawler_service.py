"""
Crawler Service - Orchestrates crawling and publishing
"""
import time
import logging
from datetime import datetime
from typing import List, Dict

from ..core.interfaces import IDataSource, IMessageBroker
from ..config.settings import Settings
from ..utils.symbols import SymbolsReader

logger = logging.getLogger(__name__)


class CrawlerService:
    """Service to crawl data and publish to message broker"""

    def __init__(
        self,
        data_source: IDataSource,
        message_broker: IMessageBroker,
        settings: Settings
    ):
        self.data_source = data_source
        self.message_broker = message_broker
        self.settings = settings

        # Track last update times
        self.last_update: Dict[str, Dict[str, float]] = {}

    def run(self, intervals: List[str] = None):
        """Run continuous crawling"""
        if intervals is None:
            intervals = self.settings.active_intervals

        symbols = SymbolsReader.read_from_file(
            self.settings.crawler.symbols_file
        )

        logger.info("="*70)
        logger.info(f"Crawler Service Started - {datetime.now()}")
        logger.info("="*70)
        logger.info(f"Symbols: {len(symbols)}")
        logger.info(f"Intervals: {', '.join(intervals)}")
        logger.info("="*70)

        # Initialize tracking
        for interval in intervals:
            self.last_update[interval] = {}

        try:
            while True:
                self._crawl_cycle(symbols, intervals)
                logger.info(f"\n[{datetime.now().strftime('%H:%M:%S')}] Cycle completed. Sleeping 60s...\n")
                time.sleep(60)

        except KeyboardInterrupt:
            logger.info("\nReceived interrupt signal")
        finally:
            self.message_broker.close()
            self.data_source.close()

    def _crawl_cycle(self, symbols: List[str], intervals: List[str]):
        """Single crawl cycle"""
        current_time = time.time()

        for symbol in symbols:
            for interval in intervals:
                if self._should_crawl(symbol, interval, current_time):
                    self._crawl_and_publish(symbol, interval, current_time)
                    time.sleep(self.settings.binance.rate_limit_delay)

        # Flush messages
        self.message_broker.flush()

    def _should_crawl(self, symbol: str, interval: str, current_time: float) -> bool:
        """Check if should crawl this symbol-interval"""
        config = self.settings.crawler.intervals[interval]

        if symbol not in self.last_update[interval]:
            return True

        elapsed = current_time - self.last_update[interval][symbol]
        return elapsed >= config.update_every_seconds

    def _crawl_and_publish(self, symbol: str, interval: str, current_time: float):
        """Crawl data and publish to message broker"""
        config = self.settings.crawler.intervals[interval]

        logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Crawling {symbol} {interval}...")

        # Fetch klines
        klines = self.data_source.fetch_klines(symbol, interval, config.limit)

        if not klines:
            logger.error(f"Failed to fetch {symbol} {interval}")
            return

        # Publish to message broker
        topic = f"{self.settings.kafka.topic_prefix}.{interval}"
        published = 0

        for kline in klines:
            key = f"{symbol}:{interval}:{kline.open_time}"
            if self.message_broker.publish(topic, key, kline):
                published += 1

        logger.info(f"✅ Published {published} candles to Kafka")

        # Update tracking
        self.last_update[interval][symbol] = current_time
