"""
MongoDB storage implementation
"""
import time
import logging
from typing import List
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

from ..core.interfaces import IStorage
from ..core.models import Kline
from ..config.settings import MongoDBConfig

logger = logging.getLogger(__name__)


class MongoDBStorage(IStorage):
    """MongoDB implementation of storage"""

    def __init__(self, config: MongoDBConfig):
        self.config = config
        logger.info(f"Connecting to MongoDB: {config.uri}")

        self.client = MongoClient(config.uri)
        self.db = self.client[config.database]
        self.collection = self.db[config.collection]

        self._ensure_indexes()
        logger.info("✅ MongoDB connected")

    def _ensure_indexes(self):
        """Create indexes if not exist"""
        try:
            # Unique index
            self.collection.create_index(
                [('symbol', 1), ('interval', 1), ('open_time', 1)],
                unique=True,
                name='symbol_interval_opentime_unique'
            )

            # Query index
            self.collection.create_index(
                [('symbol', 1), ('interval', 1), ('open_time', -1)],
                name='symbol_interval_opentime_desc'
            )

            self.collection.create_index(
                [('open_time', -1)],
                name='opentime_desc'
            )

            logger.info("✅ MongoDB indexes ensured")
        except Exception as e:
            logger.warning(f"Index creation warning: {e}")

    def save_batch(self, klines: List[dict]) -> int:
        """Save batch of klines using upsert"""
        if not klines:
            return 0

        operations = []

        for kline in klines:
            # Add processed timestamp
            kline['processed_at'] = int(time.time() * 1000)

            operations.append(
                UpdateOne(
                    {
                        'symbol': kline['symbol'],
                        'interval': kline['interval'],
                        'open_time': kline['open_time']
                    },
                    {'$set': kline},
                    upsert=True
                )
            )

        try:
            result = self.collection.bulk_write(operations, ordered=False)
            total_saved = result.upserted_count + result.modified_count

            logger.debug(
                f"Saved batch: {result.upserted_count} inserted, "
                f"{result.modified_count} updated"
            )

            return total_saved

        except BulkWriteError as e:
            errors = len(e.details.get('writeErrors', []))
            logger.error(f"Bulk write errors: {errors}")
            return 0

    def close(self) -> None:
        """Close connection"""
        logger.info("Closing MongoDB connection...")
        self.client.close()
        logger.info("✅ MongoDB closed")
