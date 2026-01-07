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
        '1m': IntervalConfig(limit=120, update_every_seconds=60),      # 1 minute
        '3m': IntervalConfig(limit=60, update_every_seconds=180),      # 3 minutes
        '5m': IntervalConfig(limit=60, update_every_seconds=300),      # 5 minutes
        '15m': IntervalConfig(limit=32, update_every_seconds=900),     # 15 minutes
        '30m': IntervalConfig(limit=24, update_every_seconds=1800),    # 30 minutes
        '1h': IntervalConfig(limit=48, update_every_seconds=3600),     # 1 hour
        '2h': IntervalConfig(limit=24, update_every_seconds=7200),     # 2 hours
        '4h': IntervalConfig(limit=18, update_every_seconds=14400),    # 4 hours
        '6h': IntervalConfig(limit=28, update_every_seconds=21600),    # 6 hours
        '12h': IntervalConfig(limit=14, update_every_seconds=43200),   # 12 hours
        '1d': IntervalConfig(limit=7, update_every_seconds=86400),     # 1 day
        '1w': IntervalConfig(limit=4, update_every_seconds=604800),    # 1 week
    })
    symbols_file: str = "symbols_top20.txt"
    max_workers: int = 4


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
    compression_type: str = field(
        default_factory=lambda: os.getenv('KAFKA_COMPRESSION', 'none')  
    )
    acks: int = field(
        default_factory=lambda: int(os.getenv('KAFKA_ACKS', '1')) 
    )
    retries: int = field(
        default_factory=lambda: int(os.getenv('KAFKA_RETRIES', '3'))
    )
    batch_size: int = field(
        default_factory=lambda: int(os.getenv('KAFKA_BATCH_SIZE', '16384'))
    )
    linger_ms: int = field(
        default_factory=lambda: int(os.getenv('KAFKA_LINGER_MS', '2')) 
    )
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
    batch_timeout: int = 2


@dataclass
class NatsConfig:
    """NATS configuration"""
    url: str = field(
        default_factory=lambda: os.getenv('NATS_URL', 'nats://localhost:4222')
    )
    max_reconnect_attempts: int = 60
    reconnect_time_wait: int = 2


@dataclass
class Settings:
    """Application settings"""
    binance: BinanceConfig = field(default_factory=BinanceConfig)
    crawler: CrawlerConfig = field(default_factory=CrawlerConfig)
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    mongodb: MongoDBConfig = field(default_factory=MongoDBConfig)
    nats: NatsConfig = field(default_factory=NatsConfig)

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
