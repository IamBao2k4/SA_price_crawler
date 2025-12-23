"""
Application settings and configuration
"""
import os
from dataclasses import dataclass, field
from typing import Dict, List
from dotenv import load_dotenv

# Load .env file
load_dotenv()


@dataclass
class BinanceConfig:
    """Binance API configuration"""
    base_url: str = "https://api.binance.com/api/v3/klines"
    timeout: int = 10
    max_retries: int = 3
    rate_limit_delay: float = 0.1


@dataclass
class IntervalConfig:
    """Configuration for each interval"""
    limit: int
    update_every_seconds: int


@dataclass
class CrawlerConfig:
    """Crawler configuration"""
    intervals: Dict[str, IntervalConfig] = field(default_factory=lambda: {
        '1m': IntervalConfig(limit=120, update_every_seconds=3600),
        '3m': IntervalConfig(limit=60, update_every_seconds=7200),
        '5m': IntervalConfig(limit=60, update_every_seconds=10800),
        '15m': IntervalConfig(limit=32, update_every_seconds=21600),
        '30m': IntervalConfig(limit=24, update_every_seconds=21600),
        '1h': IntervalConfig(limit=48, update_every_seconds=21600),
        '2h': IntervalConfig(limit=24, update_every_seconds=21600),
        '4h': IntervalConfig(limit=18, update_every_seconds=43200),
        '6h': IntervalConfig(limit=28, update_every_seconds=43200),
        '12h': IntervalConfig(limit=14, update_every_seconds=86400),
        '1d': IntervalConfig(limit=7, update_every_seconds=86400),
        '1w': IntervalConfig(limit=4, update_every_seconds=604800),
    })
    symbols_file: str = "symbols_top20.txt"


@dataclass
class KafkaConfig:
    """Kafka configuration"""
    bootstrap_servers: str = field(
        default_factory=lambda: os.getenv('KAFKA_BOOTSTRAP_SERVERS')
    )
    topic_prefix: str = field(
        default_factory=lambda: os.getenv('KAFKA_TOPIC_PREFIX', 'binance.klines')
    )
    group_id: str = field(
        default_factory=lambda: os.getenv('KAFKA_GROUP_ID', 'binance-consumer-group')
    )
    compression_type: str = 'gzip'
    acks: int = 1
    retries: int = 3
    batch_size: int = 16384
    linger_ms: int = 10
    max_poll_records: int = 500
    api_version_auto_timeout_ms: int = 10000
    request_timeout_ms: int = 30000
    max_block_ms: int = 30000


@dataclass
class MongoDBConfig:
    """MongoDB configuration"""
    uri: str = field(
        default_factory=lambda: os.getenv('MONGODB_URI')
    )
    database: str = field(
        default_factory=lambda: os.getenv('MONGODB_DATABASE', 'binance')
    )
    collection: str = 'klines'
    batch_size: int = 100
    batch_timeout: int = 5


@dataclass
class Settings:
    """Application settings"""
    binance: BinanceConfig = field(default_factory=BinanceConfig)
    crawler: CrawlerConfig = field(default_factory=CrawlerConfig)
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    mongodb: MongoDBConfig = field(default_factory=MongoDBConfig)

    # Environment
    timezone: str = field(
        default_factory=lambda: os.getenv('TZ', 'Asia/Ho_Chi_Minh')
    )
    log_level: str = field(
        default_factory=lambda: os.getenv('LOG_LEVEL', 'INFO')
    )

    @property
    def active_intervals(self) -> List[str]:
        """Get active intervals from environment or default"""
        env_intervals = os.getenv('ACTIVE_INTERVALS')
        if env_intervals:
            return env_intervals.split(',')
        return ['1m', '5m', '15m', '1h', '4h', '1d']
