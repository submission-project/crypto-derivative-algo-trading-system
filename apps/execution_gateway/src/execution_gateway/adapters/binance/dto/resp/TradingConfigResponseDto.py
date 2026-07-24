"""
Binance USD-M Futures Leverage & Listen Key Response DTOs.

Binance Docs:
  - POST   /fapi/v1/leverage    (Change Initial Leverage)
  - POST   /fapi/v1/listenKey   (Start User Data Stream)
  - PUT    /fapi/v1/listenKey   (Keepalive User Data Stream)
  - DELETE /fapi/v1/listenKey   (Close User Data Stream)
"""

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass(frozen=True)
class ChangeLeverageRespDto:
    """
    레버리지 변경 응답 DTO.

    POST /fapi/v1/leverage

    Response Example:
    {
        "leverage": 21,
        "maxNotionalValue": "1000000",
        "symbol": "BTCUSDT"
    }
    """
    leverage: Optional[int]
    maxNotionalValue: Optional[str]
    symbol: Optional[str]
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_response(cls, row: dict[str, Any]) -> "ChangeLeverageRespDto":
        return cls(
            leverage=row.get("leverage"),
            maxNotionalValue=row.get("maxNotionalValue"),
            symbol=row.get("symbol"),
            raw=row,
        )


@dataclass(frozen=True)
class ListenKeyRespDto:
    """
    listenKey 발급 응답 DTO.

    POST /fapi/v1/listenKey

    Response Example:
    {
        "listenKey": "pqia91ma19a5s61cv6a81va65sdf19v8a65a1a5s61cv6a81va65sdf19v8a65a1"
    }
    """
    listenKey: Optional[str]
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_response(cls, row: dict[str, Any]) -> "ListenKeyRespDto":
        return cls(
            listenKey=row.get("listenKey"),
            raw=row,
        )
