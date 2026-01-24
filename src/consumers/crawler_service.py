"""
Crawler Service - Orchestrates crawling and publishing
"""
import time
import logging
import threading
from datetime import datetime
from typing import List, Dict, Tuple, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque

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

        # Deduplication cache
        self._published_candles: Set[str] = set()
        self._published_queue: deque = deque(maxlen=50000)  
        self._dedup_lock = threading.Lock()
        self._total_fetched = 0
        self._total_published = 0
        self._total_duplicates = 0

        # Concurrency and rate-limit control
        self.max_workers: int = getattr(self.settings.crawler, 'max_workers', 4)
        self._request_lock = threading.Lock()
        self._last_request_time: float = 0.0

    def get_dedup_stats(self) -> Dict[str, int]:
        """Get deduplication statistics"""
        with self._dedup_lock:
            return {
                'total_fetched': self._total_fetched,
                'total_published': self._total_published,
                'total_duplicates': self._total_duplicates,
                'cache_size': len(self._published_candles),
                'dedup_rate': (
                    self._total_duplicates / self._total_fetched * 100
                    if self._total_fetched > 0 else 0
                )
            }

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
        logger.info(f"Deduplication: ENABLED (cache size: 50,000)")
        logger.info("="*70)

        # Initialize tracking
        for interval in intervals:
            self.last_update[interval] = {}

        cycle_count = 0
        try:
            while True:
                # cycle_started = time.time()
                self._crawl_cycle(symbols, intervals)
                cycle_count += 1

                # Log stats every 10 cycles
                if cycle_count % 10 == 0:
                    stats = self.get_dedup_stats()
                    logger.info(
                        f"\n{'='*70}\n"
                        f"Deduplication Stats (after {cycle_count} cycles):\n"
                        f"  Total fetched: {stats['total_fetched']:,}\n"
                        f"  Total published: {stats['total_published']:,}\n"
                        f"  Total duplicates: {stats['total_duplicates']:,}\n"
                        f"  Dedup rate: {stats['dedup_rate']:.2f}%\n"
                        f"  Cache size: {stats['cache_size']:,}\n"
                        f"{'='*70}\n"
                    )

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
            # Print final stats
            stats = self.get_dedup_stats()
            logger.info(
                f"\n{'='*70}\n"
                f"Final Deduplication Stats:\n"
                f"  Total fetched: {stats['total_fetched']:,}\n"
                f"  Total published: {stats['total_published']:,}\n"
                f"  Total duplicates: {stats['total_duplicates']:,}\n"
                f"  Dedup rate: {stats['dedup_rate']:.2f}%\n"
                f"{'='*70}\n"
            )
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

    def _is_candle_published(self, candle_id: str) -> bool:
        """Thread-safe check if candle was already published"""
        with self._dedup_lock:
            return candle_id in self._published_candles

    def _mark_candle_published(self, candle_id: str):
        """Thread-safe mark candle as published with LRU cleanup"""
        with self._dedup_lock:
            if candle_id not in self._published_candles:
                self._published_candles.add(candle_id)
                self._published_queue.append(candle_id)

                if len(self._published_queue) == self._published_queue.maxlen:
                    cleanup_count = self._published_queue.maxlen // 10
                    for _ in range(cleanup_count):
                        old_id = self._published_queue.popleft()
                        self._published_candles.discard(old_id)
                    self._published_queue.append(candle_id)

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

        # Publish to message broker with deduplication
        topic = f"{self.settings.kafka.topic_prefix}.{interval}"
        published = 0
        duplicates = 0
        fetched = len(klines)

        for kline in klines:
            candle_id = f"{symbol}:{interval}:{kline.open_time}"

            if self._is_candle_published(candle_id):
                duplicates += 1
                continue

            # Publish new candle
            key = f"{symbol}:{interval}:{kline.open_time}"
            if self.message_broker.publish(topic, key, kline):
                self._mark_candle_published(candle_id)
                published += 1

        with self._dedup_lock:
            self._total_fetched += fetched
            self._total_published += published
            self._total_duplicates += duplicates

        if duplicates > 0:
            logger.debug(
                f"{symbol} {interval}: fetched={fetched}, published={published}, "
                f"duplicates={duplicates} ({duplicates/fetched*100:.1f}%)"
            )
        else:
            logger.debug(f"{symbol} {interval}: published {published} candles")

        # Update tracking
        self.last_update[interval][symbol] = current_time
