"""
Core interfaces (dependency inversion)
"""
from abc import ABC, abstractmethod
from typing import List, Optional
from .models import Kline, KlineMessage


class IDataSource(ABC):
    """Interface for data source (e.g., Binance API)"""

    @abstractmethod
    def fetch_klines(self, symbol: str, interval: str, limit: int) -> Optional[List[Kline]]:
        """Fetch klines from data source"""
        pass


class IMessageBroker(ABC):
    """Interface for message broker (e.g., Kafka)"""

    @abstractmethod
    def publish(self, topic: str, key: str, message: KlineMessage) -> bool:
        """Publish message to broker"""
        pass

    @abstractmethod
    def consume(self, topics: List[str], callback) -> None:
        """Consume messages from broker"""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close connection"""
        pass


class IStorage(ABC):
    """Interface for storage (e.g., MongoDB)"""

    @abstractmethod
    def save_batch(self, klines: List[Kline]) -> int:
        """Save batch of klines, return count saved"""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close connection"""
        pass
