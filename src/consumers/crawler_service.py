"""
Crawler Service - Orchestrates crawling and publishing
"""
import time
import logging
import threading
from datetime import datetime
from typing import List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

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

        # Concurrency and rate-limit control
        self.max_workers: int = getattr(self.settings.crawler, 'max_workers', 4)
        self._request_lock = threading.Lock()
        self._last_request_time: float = 0.0

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
                # cycle_started = time.time()
                self._crawl_cycle(symbols, intervals)

                next_due = self._compute_next_due(symbols, intervals)
                now = time.time()
                sleep_for = max(0, min(next_due - now, 1.0))  

                # logger.debug(
                #     f"\n[{datetime.now().strftime('%H:%M:%S')}] Cycle completed in "
                #     f"{now - cycle_started:.2f}s. Sleeping {sleep_for:.2f}s...\n"
                # )
                time.sleep(sleep_for)

        except KeyboardInterrupt:
            logger.info("\nReceived interrupt signal")
        finally:
            self.message_broker.close()
            self.data_source.close()

    def _crawl_cycle(self, symbols: List[str], intervals: List[str]):
        """Single crawl cycle with bounded parallel fetches"""
        current_time = time.time()
        due_tasks: List[Tuple[str, str]] = []

        for symbol in symbols:
            for interval in intervals:
                if self._should_crawl(symbol, interval, current_time):
                    due_tasks.append((symbol, interval))

        if not due_tasks:
            return

        logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Crawling {len(due_tasks)} symbol-interval pairs...")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [
                executor.submit(self._crawl_and_publish, symbol, interval)
                for symbol, interval in due_tasks
            ]

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Worker error: {e}")

        # Flush messages after workers complete
        self.message_broker.flush()
        logger.info(f"Completed crawling {len(due_tasks)} pairs")

    def _compute_next_due(self, symbols: List[str], intervals: List[str]) -> float:
        """Find the earliest next allowed crawl time across symbols/intervals"""
        next_due = float('inf')

        for symbol in symbols:
            for interval in intervals:
                config = self.settings.crawler.intervals[interval]
                last = self.last_update[interval].get(symbol)
                candidate = 0 if last is None else last + config.update_every_seconds
                if candidate < next_due:
                    next_due = candidate

        if next_due == float('inf'):
            return time.time()
        return next_due

    def _should_crawl(self, symbol: str, interval: str, current_time: float) -> bool:
        """Check if should crawl this symbol-interval"""
        config = self.settings.crawler.intervals[interval]

        if symbol not in self.last_update[interval]:
            return True

        elapsed = current_time - self.last_update[interval][symbol]
        return elapsed >= config.update_every_seconds

    def _respect_rate_limit(self):
        """Global rate-limit guard to keep spacing between requests"""
        with self._request_lock:
            now = time.time()
            elapsed = now - self._last_request_time
            delay = self.settings.binance.rate_limit_delay
            if elapsed < delay:
                time.sleep(delay - elapsed)
                now = time.time()
            self._last_request_time = now

    def _crawl_and_publish(self, symbol: str, interval: str):
        """Crawl data and publish to message broker"""
        self._respect_rate_limit()
        current_time = time.time()
        config = self.settings.crawler.intervals[interval]

        # logger.debug(f"[{datetime.now().strftime('%H:%M:%S')}] Crawling {symbol} {interval}...")

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

        logger.debug(f"Published {published} candles to topic={topic}")
        # Update tracking
        self.last_update[interval][symbol] = current_time
