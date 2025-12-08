"""
Core domain models
"""
from dataclasses import dataclass, asdict
from typing import Optional
import time


@dataclass
class Kline:
    """Candlestick data model"""
    symbol: str
    interval: str
    open_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time: int
    quote_asset_volume: float
    number_of_trades: int
    taker_buy_base_asset_volume: float
    taker_buy_quote_asset_volume: float

    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return asdict(self)


@dataclass
class KlineMessage(Kline):
    """Kline with metadata for messaging"""
    timestamp: Optional[int] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = int(time.time() * 1000)

    @classmethod
    def from_binance_raw(cls, symbol: str, interval: str, raw_data: list) -> 'KlineMessage':
        """Create from Binance API raw response"""
        return cls(
            symbol=symbol,
            interval=interval,
            open_time=raw_data[0],
            open=float(raw_data[1]),
            high=float(raw_data[2]),
            low=float(raw_data[3]),
            close=float(raw_data[4]),
            volume=float(raw_data[5]),
            close_time=raw_data[6],
            quote_asset_volume=float(raw_data[7]),
            number_of_trades=int(raw_data[8]),
            taker_buy_base_asset_volume=float(raw_data[9]),
            taker_buy_quote_asset_volume=float(raw_data[10])
        )
