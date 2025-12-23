"""
Fetch 90 days historical data for top 20 symbols
Date range: 2024-09-22 to 2024-12-21 (90 days)
"""
import sys
import time
import logging
from datetime import datetime, timedelta
from typing import List
from pathlib import Path

# Add src to path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from src.producers.binance import BinanceDataSource
from src.storage.mongodb_storage import MongoDBStorage
from src.config.settings import Settings
from src.utils.validation import check_env_or_exit

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HistoricalDataFetcher:
    """Fetch historical data for multiple symbols and intervals"""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.data_source = BinanceDataSource(settings.binance)
        self.storage = MongoDBStorage(settings.mongodb)

        # Statistics
        self.stats = {
            'total_candles': 0,
            'total_saved': 0,
            'failed_requests': 0,
            'symbols_completed': 0
        }

    def calculate_limit_for_interval(self, interval: str, days: int = 90) -> int:
        """Calculate how many candles for given interval and days"""
        minutes_per_candle = {
            '1m': 1,
            '5m': 5,
            '15m': 15,
            '30m': 30,
            '1h': 60,
            '2h': 120,
            '4h': 240,
            '6h': 360,
            '12h': 720,
            '1d': 1440,
            '1w': 10080
        }

        minutes_in_days = days * 24 * 60
        candles = minutes_in_days // minutes_per_candle[interval]

        # Binance limit is 1000 per request
        return min(candles, 1000)

    def fetch_symbol_interval(
        self,
        symbol: str,
        interval: str,
        days: int = 90
    ) -> List[dict]:
        """Fetch all data for a symbol-interval pair"""

        logger.info(f"Fetching {symbol} {interval} for {days} days...")

        # Calculate end time (now) and start time (90 days ago)
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days)

        all_candles = []
        current_end_time = end_time

        # Fetch in batches (max 1000 per request)
        limit = self.calculate_limit_for_interval(interval, days)
        batch_count = 0

        while current_end_time > start_time:
            batch_count += 1

            # Fetch batch
            klines = self.data_source.fetch_klines(
                symbol=symbol,
                interval=interval,
                limit=limit,
                end_time=int(current_end_time.timestamp() * 1000)
            )

            if not klines:
                logger.warning(f"No data returned for {symbol} {interval} batch {batch_count}")
                break

            # Convert to documents
            documents = [kline.to_dict() for kline in klines]
            all_candles.extend(documents)

            # Update end time for next batch (use earliest candle time)
            earliest_candle = min(klines, key=lambda k: k.open_time)
            current_end_time = datetime.fromtimestamp(earliest_candle.open_time / 1000)

            logger.info(
                f"  Batch {batch_count}: fetched {len(klines)} candles, "
                f"total so far: {len(all_candles)}, "
                f"oldest: {current_end_time.strftime('%Y-%m-%d %H:%M')}"
            )

            # Check if we've reached the start time
            if current_end_time <= start_time:
                break

            # Rate limiting
            time.sleep(self.settings.binance.rate_limit_delay)

        # Filter candles to exact date range
        filtered_candles = [
            candle for candle in all_candles
            if start_time.timestamp() * 1000 <= candle['open_time'] <= end_time.timestamp() * 1000
        ]

        logger.info(
            f"✅ {symbol} {interval}: fetched {len(all_candles)} candles, "
            f"filtered to {len(filtered_candles)} in date range"
        )

        self.stats['total_candles'] += len(filtered_candles)
        return filtered_candles

    def fetch_and_save_symbol(
        self,
        symbol: str,
        intervals: List[str],
        days: int = 90
    ):
        """Fetch and save all intervals for one symbol"""

        logger.info("=" * 70)
        logger.info(f"Processing {symbol}")
        logger.info("=" * 70)

        for interval in intervals:
            try:
                # Fetch data
                candles = self.fetch_symbol_interval(symbol, interval, days)

                if candles:
                    # Save to MongoDB
                    saved = self.storage.save_batch(candles)
                    self.stats['total_saved'] += saved
                    logger.info(f"✅ Saved {saved} documents to MongoDB")
                else:
                    logger.warning(f"No candles to save for {symbol} {interval}")

            except Exception as e:
                logger.error(f"❌ Error fetching {symbol} {interval}: {e}")
                self.stats['failed_requests'] += 1

        self.stats['symbols_completed'] += 1

    def fetch_all(
        self,
        symbols: List[str],
        intervals: List[str],
        days: int = 90
    ):
        """Fetch data for all symbols and intervals"""

        logger.info("=" * 70)
        logger.info("HISTORICAL DATA FETCHER")
        logger.info("=" * 70)
        logger.info(f"Symbols: {len(symbols)}")
        logger.info(f"Intervals: {', '.join(intervals)}")
        logger.info(f"Date range: {days} days")
        logger.info(f"End date: {datetime.now().strftime('%Y-%m-%d')}")
        logger.info(f"Start date: {(datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')}")
        logger.info("=" * 70)

        start_time = time.time()

        for i, symbol in enumerate(symbols, 1):
            logger.info(f"\n[{i}/{len(symbols)}] Processing {symbol}...")
            self.fetch_and_save_symbol(symbol, intervals, days)

        elapsed = time.time() - start_time

        # Print final statistics
        self.print_statistics(elapsed)

    def print_statistics(self, elapsed_seconds: float):
        """Print final statistics"""

        logger.info("\n" + "=" * 70)
        logger.info("FINAL STATISTICS")
        logger.info("=" * 70)
        logger.info(f"Symbols completed: {self.stats['symbols_completed']}")
        logger.info(f"Total candles fetched: {self.stats['total_candles']:,}")
        logger.info(f"Total documents saved: {self.stats['total_saved']:,}")
        logger.info(f"Failed requests: {self.stats['failed_requests']}")
        logger.info(f"Elapsed time: {elapsed_seconds/60:.1f} minutes")
        logger.info(f"Average: {self.stats['total_candles']/elapsed_seconds:.1f} candles/second")

        # Estimate data size
        avg_doc_size = 200  # bytes per document
        total_size_mb = (self.stats['total_saved'] * avg_doc_size) / (1024 * 1024)
        logger.info(f"Estimated data size: {total_size_mb:.1f} MB")
        logger.info("=" * 70)

    def close(self):
        """Cleanup resources"""
        self.data_source.close()
        self.storage.close()


def load_symbols(file_path: str = "symbols_top20.txt") -> List[str]:
    """Load symbols from file"""
    with open(file_path, 'r') as f:
        return [line.strip() for line in f if line.strip()]


def main():
    """Main execution"""

    # Validate environment
    check_env_or_exit()

    # Load settings
    settings = Settings()

    # Load symbols
    symbols = load_symbols(settings.crawler.symbols_file)
    logger.info(f"✅ Loaded {len(symbols)} symbols")

    # Define intervals (all available)
    intervals = ['1m', '5m', '15m', '1h', '4h', '1d']

    # Create fetcher
    fetcher = HistoricalDataFetcher(settings)

    try:
        # Fetch all data
        fetcher.fetch_all(
            symbols=symbols,
            intervals=intervals,
            days=90
        )
    except KeyboardInterrupt:
        logger.info("\n⚠️ Interrupted by user")
    finally:
        fetcher.close()
        logger.info("✅ Cleanup completed")


if __name__ == "__main__":
    main()
