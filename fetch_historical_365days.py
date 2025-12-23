"""
Fetch long-term historical data (365 days or more) for top 20 symbols
Optimized for large data fetches with:
- Flexible days parameter
- Progress tracking and resume capability
- Robust retry mechanism
- Memory-efficient batch processing

Usage:
    python fetch_historical_365days.py --days 365 --interval 4h
    python fetch_historical_365days.py --days 180 --interval 4h --symbols BTCUSDT,ETHUSDT
    python fetch_historical_365days.py --resume  # Resume interrupted fetch
"""
import sys
import time
import json
import argparse
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Dict
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
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    handlers=[
        logging.FileHandler('fetch_historical_365days.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class LongTermHistoricalFetcher:
    """Fetch long-term historical data with progress tracking"""

    def __init__(self, interval: str = '4h'):
        # Validate environment first
        check_env_or_exit()

        # Load settings
        self.settings = Settings()
        self.interval = interval

        # Initialize connections
        logger.info("🔌 Initializing connections...")
        self.data_source = BinanceDataSource(self.settings.binance)
        self.storage = MongoDBStorage(self.settings.mongodb)

        # Retry settings
        self.max_retries = 5
        self.retry_delay = 3  # seconds

        # Progress tracking file
        self.progress_file = Path('fetch_progress.json')

        # Stats
        self.stats = {
            'symbols_completed': 0,
            'symbols_skipped': 0,
            'total_candles': 0,
            'total_saved': 0,
            'total_retries': 0,
            'failed_symbols': [],
            'start_time': None,
            'last_symbol': None
        }

        logger.info("✅ Connections ready")

    def calculate_expected_candles(self, days: int, interval: str) -> int:
        """Calculate expected number of candles for given period"""
        candles_per_day = {
            '1m': 1440,
            '5m': 288,
            '15m': 96,
            '30m': 48,
            '1h': 24,
            '2h': 12,
            '4h': 6,
            '6h': 4,
            '12h': 2,
            '1d': 1
        }
        return days * candles_per_day.get(interval, 6)

    def load_progress(self) -> Dict:
        """Load progress from previous run"""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r') as f:
                    progress = json.load(f)
                    logger.info(f"📂 Loaded progress: {progress['completed_symbols']} symbols completed")
                    return progress
            except Exception as e:
                logger.warning(f"⚠️  Could not load progress: {e}")
        return {'completed_symbols': [], 'failed_symbols': []}

    def save_progress(self, symbol: str, success: bool = True):
        """Save progress to file"""
        try:
            progress = self.load_progress()

            if success:
                if symbol not in progress['completed_symbols']:
                    progress['completed_symbols'].append(symbol)
            else:
                if symbol not in progress['failed_symbols']:
                    progress['failed_symbols'].append(symbol)

            progress['last_updated'] = datetime.now().isoformat()
            progress['total_candles'] = self.stats['total_candles']
            progress['total_saved'] = self.stats['total_saved']

            with open(self.progress_file, 'w') as f:
                json.dump(progress, f, indent=2)

        except Exception as e:
            logger.warning(f"⚠️  Could not save progress: {e}")

    def fetch_with_retry(
        self,
        symbol: str,
        limit: int,
        end_time: int,
        attempt: int = 1
    ) -> Optional[List]:
        """Fetch data with exponential backoff retry"""

        try:
            klines = self.data_source.fetch_klines(
                symbol=symbol,
                interval=self.interval,
                limit=limit,
                end_time=end_time
            )
            return klines

        except Exception as e:
            error_msg = str(e)

            if attempt < self.max_retries:
                self.stats['total_retries'] += 1

                # Exponential backoff: 3s, 6s, 12s, 24s, 48s
                wait_time = self.retry_delay * (2 ** (attempt - 1))

                logger.warning(
                    f"⚠️  Retry {attempt}/{self.max_retries} for {symbol}: {error_msg[:100]}"
                )
                logger.info(f"⏳ Waiting {wait_time}s before retry...")

                time.sleep(wait_time)
                return self.fetch_with_retry(symbol, limit, end_time, attempt + 1)
            else:
                logger.error(f"❌ Max retries exceeded for {symbol}")
                raise

    def fetch_symbol(self, symbol: str, days: int) -> List[dict]:
        """Fetch historical data for one symbol"""

        logger.info(f"\n{'='*70}")
        logger.info(f"📊 Fetching {symbol} - {self.interval} interval - {days} days")
        logger.info(f"{'='*70}")

        # Calculate time range
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days)

        expected_candles = self.calculate_expected_candles(days, self.interval)

        logger.info(f"📅 From: {start_time.strftime('%Y-%m-%d %H:%M')}")
        logger.info(f"📅 To:   {end_time.strftime('%Y-%m-%d %H:%M')}")
        logger.info(f"🎯 Expected: ~{expected_candles} candles")

        all_candles = []
        current_end_time = end_time
        batch_count = 0

        # Fetch in batches of 1000 (Binance limit)
        while len(all_candles) < expected_candles and current_end_time > start_time:
            batch_count += 1
            remaining = expected_candles - len(all_candles)
            limit = min(1000, remaining + 100)  # Fetch extra to ensure coverage

            logger.info(
                f"📦 Batch {batch_count}: fetching up to {limit} candles "
                f"(total: {len(all_candles)}/{expected_candles})..."
            )

            try:
                # Fetch with retry
                klines = self.fetch_with_retry(
                    symbol=symbol,
                    limit=limit,
                    end_time=int(current_end_time.timestamp() * 1000)
                )

                if not klines:
                    logger.warning(f"⚠️  No data returned for batch {batch_count}")
                    break

                # Convert to dict
                documents = [kline.to_dict() for kline in klines]

                # Filter duplicates by open_time
                existing_times = {c['open_time'] for c in all_candles}
                new_candles = [
                    doc for doc in documents
                    if doc['open_time'] not in existing_times
                ]

                all_candles.extend(new_candles)

                # Update end time for next batch
                earliest = min(klines, key=lambda k: k.open_time)
                current_end_time = datetime.fromtimestamp(earliest.open_time / 1000)

                logger.info(
                    f"   ✓ Got {len(klines)} candles ({len(new_candles)} new) | "
                    f"Total: {len(all_candles)}/{expected_candles} | "
                    f"Oldest: {current_end_time.strftime('%Y-%m-%d %H:%M')}"
                )

                # Check if we've gone too far back
                if current_end_time <= start_time:
                    logger.info(f"✓ Reached start time, stopping fetch")
                    break

                # Adaptive rate limiting (slower for many batches)
                if batch_count > 10:
                    time.sleep(1.0)  # Slower for large fetches
                else:
                    time.sleep(0.5)  # Normal rate

            except Exception as e:
                logger.error(f"❌ Failed to fetch batch {batch_count}: {e}")
                raise

        # Filter to exact date range
        start_ts = start_time.timestamp() * 1000
        end_ts = end_time.timestamp() * 1000

        filtered = [
            c for c in all_candles
            if start_ts <= c['open_time'] <= end_ts
        ]

        # Sort by open_time to ensure chronological order
        filtered.sort(key=lambda x: x['open_time'])

        logger.info(f"\n📊 Summary for {symbol}:")
        logger.info(f"   Raw fetched:  {len(all_candles)}")
        logger.info(f"   Filtered:     {len(filtered)}")
        logger.info(f"   Expected:     ~{expected_candles}")

        coverage = (len(filtered) / expected_candles * 100) if expected_candles > 0 else 0
        logger.info(f"   Coverage:     {coverage:.1f}%")

        self.stats['total_candles'] += len(filtered)
        return filtered

    def save_with_retry(self, candles: List[dict], attempt: int = 1) -> int:
        """Save to MongoDB with retry"""

        try:
            saved = self.storage.save_batch(candles)
            return saved

        except Exception as e:
            if attempt < self.max_retries:
                self.stats['total_retries'] += 1
                wait_time = self.retry_delay * attempt

                logger.warning(f"⚠️  Save retry {attempt}/{self.max_retries}: {str(e)[:100]}")
                logger.info(f"⏳ Waiting {wait_time}s...")

                time.sleep(wait_time)
                return self.save_with_retry(candles, attempt + 1)
            else:
                logger.error(f"❌ Max save retries exceeded")
                raise

    def process_symbol(self, symbol: str, days: int) -> bool:
        """Fetch and save one symbol. Returns True if successful."""

        try:
            # Fetch
            candles = self.fetch_symbol(symbol, days)

            if candles:
                # Save in chunks for large datasets (avoid memory issues)
                chunk_size = 5000
                total_saved = 0

                if len(candles) > chunk_size:
                    logger.info(f"💾 Saving {len(candles)} candles in chunks of {chunk_size}...")

                    for i in range(0, len(candles), chunk_size):
                        chunk = candles[i:i + chunk_size]
                        saved = self.save_with_retry(chunk)
                        total_saved += saved
                        logger.info(f"   Chunk {i//chunk_size + 1}: saved {saved} documents")
                else:
                    logger.info(f"💾 Saving {len(candles)} candles to MongoDB...")
                    total_saved = self.save_with_retry(candles)

                self.stats['total_saved'] += total_saved
                logger.info(f"✅ Total saved: {total_saved} documents")
            else:
                logger.warning(f"⚠️  No candles to save for {symbol}")

            self.stats['symbols_completed'] += 1
            self.stats['last_symbol'] = symbol

            # Save progress
            self.save_progress(symbol, success=True)

            logger.info(f"✅ {symbol} completed!")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to process {symbol}: {e}")
            self.stats['failed_symbols'].append(symbol)
            self.save_progress(symbol, success=False)
            return False

    def fetch_all(self, symbols: List[str], days: int, resume: bool = False):
        """Fetch all symbols"""

        # Load progress if resuming
        completed_symbols = []
        if resume:
            progress = self.load_progress()
            completed_symbols = progress.get('completed_symbols', [])

            if completed_symbols:
                logger.info(f"🔄 Resuming fetch. {len(completed_symbols)} symbols already completed.")
                symbols = [s for s in symbols if s not in completed_symbols]
                self.stats['symbols_skipped'] = len(completed_symbols)

        if not symbols:
            logger.info("✅ All symbols already completed!")
            return

        expected_candles_per_symbol = self.calculate_expected_candles(days, self.interval)

        logger.info("\n" + "="*70)
        logger.info(f"🚀 LONG-TERM HISTORICAL DATA FETCHER")
        logger.info("="*70)
        logger.info(f"Symbols:     {len(symbols)} (skipped: {self.stats['symbols_skipped']})")
        logger.info(f"Interval:    {self.interval}")
        logger.info(f"Period:      {days} days")
        logger.info(f"Expected:    ~{expected_candles_per_symbol} candles per symbol")
        logger.info(f"Total data:  ~{expected_candles_per_symbol * len(symbols):,} candles")
        logger.info(f"Resume mode: {'Yes' if resume else 'No'}")
        logger.info("="*70)

        start_time = time.time()
        self.stats['start_time'] = datetime.now().isoformat()

        for i, symbol in enumerate(symbols, 1):
            total_progress = i + self.stats['symbols_skipped']
            total_symbols = len(symbols) + self.stats['symbols_skipped']

            logger.info(f"\n🎯 [{total_progress}/{total_symbols}] Processing {symbol}...")

            success = self.process_symbol(symbol, days)

            # Print running stats
            if i % 5 == 0:  # Every 5 symbols
                elapsed = time.time() - start_time
                avg_time = elapsed / i
                remaining = len(symbols) - i
                eta_seconds = avg_time * remaining
                eta_minutes = eta_seconds / 60

                logger.info(f"\n⏱️  Progress Update:")
                logger.info(f"   Completed:  {i}/{len(symbols)} symbols")
                logger.info(f"   Avg time:   {avg_time:.1f}s per symbol")
                logger.info(f"   ETA:        {eta_minutes:.1f} minutes")

        elapsed = time.time() - start_time
        self.print_stats(elapsed)

    def print_stats(self, elapsed: float):
        """Print final statistics"""

        logger.info("\n" + "="*70)
        logger.info("📊 FINAL STATISTICS")
        logger.info("="*70)
        logger.info(f"✅ Completed:     {self.stats['symbols_completed']} symbols")
        logger.info(f"⏭️  Skipped:       {self.stats['symbols_skipped']} symbols")
        logger.info(f"📈 Total candles: {self.stats['total_candles']:,}")
        logger.info(f"💾 Total saved:   {self.stats['total_saved']:,}")
        logger.info(f"🔄 Total retries: {self.stats['total_retries']}")

        if self.stats['failed_symbols']:
            logger.info(f"❌ Failed:        {len(self.stats['failed_symbols'])} symbols")
            for sym in self.stats['failed_symbols']:
                logger.info(f"   - {sym}")
        else:
            logger.info(f"❌ Failed:        0 symbols (100% success!)")

        logger.info(f"\n⏱️  Time:          {elapsed/60:.1f} minutes ({elapsed/3600:.2f} hours)")

        if elapsed > 0 and self.stats['symbols_completed'] > 0:
            logger.info(f"⚡ Speed:         {self.stats['total_candles']/elapsed:.1f} candles/sec")
            logger.info(f"🔢 Avg time:      {elapsed/self.stats['symbols_completed']:.1f} sec per symbol")

        # Estimate data size
        size_mb = (self.stats['total_saved'] * 200) / (1024 * 1024)
        logger.info(f"💽 Data size:     ~{size_mb:.1f} MB")
        logger.info("="*70)

        # Save final stats
        try:
            stats_file = Path('fetch_stats.json')
            with open(stats_file, 'w') as f:
                json.dump(self.stats, f, indent=2)
            logger.info(f"📄 Stats saved to: {stats_file}")
        except Exception as e:
            logger.warning(f"⚠️  Could not save stats: {e}")

    def close(self):
        """Cleanup"""
        self.data_source.close()
        self.storage.close()
        logger.info("✅ Connections closed")


def load_symbols(file_path: str = "symbols_top20.txt", specific_symbols: Optional[str] = None) -> List[str]:
    """Load symbols from file or command line"""

    if specific_symbols:
        # Parse comma-separated symbols
        symbols = [s.strip() for s in specific_symbols.split(',') if s.strip()]
        logger.info(f"📋 Using {len(symbols)} specific symbols: {', '.join(symbols)}")
        return symbols

    # Load from file
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"Symbols file not found: {file_path}")

    with open(path, 'r') as f:
        symbols = [line.strip() for line in f if line.strip()]

    logger.info(f"📋 Loaded {len(symbols)} symbols from {file_path}")
    return symbols


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Fetch long-term historical data from Binance',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch 365 days of 4h candles for all symbols
  python fetch_historical_365days.py --days 365 --interval 4h

  # Fetch 180 days for specific symbols
  python fetch_historical_365days.py --days 180 --symbols BTCUSDT,ETHUSDT

  # Resume interrupted fetch
  python fetch_historical_365days.py --resume --days 365

  # Fetch 1 year of 1h candles
  python fetch_historical_365days.py --days 365 --interval 1h
        """
    )

    parser.add_argument(
        '--days',
        type=int,
        default=365,
        help='Number of days to fetch (default: 365)'
    )

    parser.add_argument(
        '--interval',
        type=str,
        default='4h',
        choices=['1m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '12h', '1d', '1w'],
        help='Candle interval (default: 4h)'
    )

    parser.add_argument(
        '--symbols',
        type=str,
        help='Comma-separated list of symbols (default: load from symbols_top20.txt)'
    )

    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume from previous interrupted fetch'
    )

    parser.add_argument(
        '--clear-progress',
        action='store_true',
        help='Clear progress file and start fresh'
    )

    return parser.parse_args()


def main():
    """Main execution"""

    # Parse arguments
    args = parse_args()

    logger.info("🎬 Starting long-term historical data fetcher...")
    logger.info(f"   Days:     {args.days}")
    logger.info(f"   Interval: {args.interval}")
    logger.info(f"   Resume:   {args.resume}")

    # Clear progress if requested
    if args.clear_progress:
        progress_file = Path('fetch_progress.json')
        if progress_file.exists():
            progress_file.unlink()
            logger.info("🗑️  Progress file cleared")

    fetcher = None

    try:
        # Create fetcher (this validates env and connects)
        fetcher = LongTermHistoricalFetcher(interval=args.interval)

        # Load symbols
        symbols = load_symbols(specific_symbols=args.symbols)

        # Fetch all
        fetcher.fetch_all(symbols, days=args.days, resume=args.resume)

        logger.info("\n🎉 All done!")

    except KeyboardInterrupt:
        logger.info("\n⚠️  Interrupted by user")
        logger.info("💡 Tip: Use --resume flag to continue from where you left off")
    except Exception as e:
        logger.error(f"\n❌ Fatal error: {e}", exc_info=True)
    finally:
        if fetcher:
            fetcher.close()
        logger.info("👋 Goodbye!")


if __name__ == "__main__":
    main()
