"""
Binance USD-M Futures Market Data Response DTOs.

Binance Docs:
  - GET /fapi/v1/ticker/price  (Symbol Price Ticker)
  - GET /fapi/v1/time          (Server Time)
"""

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass(frozen=True)
class SymbolPriceTickerRespDto:
    """
    현재가 조회 응답 DTO.

    GET /fapi/v1/ticker/price

    Response Example:
    {
        "symbol": "BTCUSDT",
        "price": "6000.01",
        "time": 1589437530011
    }
    """
    symbol: Optional[str]
    price: Optional[str]
    time: Optional[int]
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_response(cls, row: dict[str, Any]) -> "SymbolPriceTickerRespDto":
        return cls(
            symbol=row.get("symbol"),
            price=row.get("price"),
            time=row.get("time"),
            raw=row,
        )


@dataclass(frozen=True)
class ServerTimeRespDto:
    """
    서버 시간 조회 응답 DTO.

    GET /fapi/v1/time

    Response Example:
    {
        "serverTime": 1499827319559
    }
    """
    serverTime: Optional[int]
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_response(cls, row: dict[str, Any]) -> "ServerTimeRespDto":
        return cls(
            serverTime=row.get("serverTime"),
            raw=row,
        )
