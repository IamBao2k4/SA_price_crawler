"""
Binance API data source implementation
"""
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import List, Optional
import logging

from ..core.interfaces import IDataSource
from ..core.models import Kline, KlineMessage
from ..config.settings import BinanceConfig

logger = logging.getLogger(__name__)


class BinanceDataSource(IDataSource):
    """Binance API data source"""

    def __init__(self, config: BinanceConfig):
        self.config = config
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create HTTP session with retry logic"""
        session = requests.Session()
        retry = Retry(
            total=self.config.max_retries,
            backoff_factor=0.5,
            status_forcelist=(500, 502, 503, 504)
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    def fetch_klines(
        self,
        symbol: str,
        interval: str,
        limit: int
    ) -> Optional[List[KlineMessage]]:
        """Fetch klines from Binance API"""
        params = {
            'symbol': symbol,
            'interval': interval,
            'limit': min(limit, 1000)  # Binance max
        }

        try:
            response = self.session.get(
                self.config.base_url,
                params=params,
                timeout=self.config.timeout
            )
            response.raise_for_status()
            data = response.json()

            if not data:
                return None

            # Convert raw data to Kline objects
            klines = [
                KlineMessage.from_binance_raw(symbol, interval, raw)
                for raw in data
            ]

            logger.debug(f"Fetched {len(klines)} klines for {symbol} {interval}")
            return klines

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching {symbol} {interval}: {e}")
            return None

    def close(self):
        """Close session"""
        self.session.close()
