"""
NATS Publisher 
Publishes candle data to NATS subjects following pattern: candles.{symbol}.{interval}
"""
import json
from typing import Optional
from nats.aio.client import Client as NATS
from loguru import logger

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.interfaces import INatsPublisher
from core.models import Kline


class NatsPublisher(INatsPublisher):
    """NATS publisher for candle data"""

    def __init__(self, nats_url: str = "nats://localhost:4222"):
        """
        Initialize NATS publisher
        
        Args:
            nats_url: NATS server URL (e.g., nats://localhost:4222)
        """
        self.nats_url = nats_url
        self.nc: Optional[NATS] = None
        self._connected = False

    async def connect(self) -> None:
        """Establish connection to NATS server"""
        try:
            self.nc = NATS()
            await self.nc.connect(self.nats_url)
            self._connected = True
            logger.info(f"Connected to NATS: {self.nats_url}")
        except Exception as e:
            logger.error(f"Failed to connect to NATS: {e}")
            raise

    async def publish(self, subject: str, data: bytes) -> bool:
        """
        Publish message to NATS subject

        Args:
            subject: NATS subject (e.g., candles.BTCUSDT.1m)
            data: Message payload as bytes

        Returns:
            True if published successfully, False otherwise
        """
        if not self.is_connected():
            logger.info("NATS reconnecting (new event loop)...")
            await self.connect()

        try:
            await self.nc.publish(subject, data)
            logger.debug(f"Published to NATS subject: {subject} ({len(data)} bytes)")
            return True
        except Exception as e:
            logger.error(f"Failed to publish to NATS subject {subject}: {e}")
            return False

    async def publish_kline(self, kline: Kline) -> bool:
        """
        Publish kline to NATS following architecture pattern: candles.{symbol}.{interval}

        Args:
            kline: Kline data model

        Returns:
            True if published successfully, False otherwise
        """
        subject = f"candles.{kline.symbol}.{kline.interval}"

        # Convert kline to JSON bytes
        kline_dict = kline.to_dict()
        payload = json.dumps(kline_dict).encode('utf-8')

        # Log the actual payload for debugging
        logger.debug(f"NATS Publishing to {subject}: {json.dumps(kline_dict)}")

        return await self.publish(subject, payload)

    async def close(self) -> None:
        """Close NATS connection"""
        if self.nc and self._connected:
            try:
                await self.nc.drain()
                await self.nc.close()
                self._connected = False
                logger.info("NATS connection closed")
            except Exception as e:
                logger.error(f"Error closing NATS connection: {e}")

    def is_connected(self) -> bool:
        """Check if NATS is connected"""
        return self._connected and self.nc is not None and self.nc.is_connected
