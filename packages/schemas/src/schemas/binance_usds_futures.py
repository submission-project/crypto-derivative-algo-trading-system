"""
USDⓈ-M Futures User Data Stream — ORDER_TRADE_UPDATE 필드 정의.

Execution Type (`o.x`) 등은 아래 문서와 동일한 문자열 값을 사용한다.

Execution Type
- NEW
- CANCELED
- CALCULATED - Liquidation Execution
- EXPIRED
- TRADE
- AMENDMENT - Order Modified

https://developers.binance.com/docs/derivatives/usds-margined-futures/user-data-streams/Event-Order-Update
"""

from __future__ import annotations

from enum import Enum


class BinanceUsdsFuturesExecutionType(str, Enum):
    """ORDER_TRADE_UPDATE의 주문 객체 `o` 필드 중 `x` (Execution Type)."""

    NEW = "NEW"
    CANCELED = "CANCELED"
    CALCULATED = "CALCULATED"
    EXPIRED = "EXPIRED"
    TRADE = "TRADE"
    AMENDMENT = "AMENDMENT"


def parse_binance_usds_futures_execution_type(
    raw: object,
) -> BinanceUsdsFuturesExecutionType | None:
    """
    `o.x` 원시값을 파싱한다.

    미지정·공백·알 수 없는 값은 None.

    WebSocket/JSON에서는 보통 문자열이 들어오지만, 내부 파이프라인에서는
    이미 `BinanceUsdsFuturesExecutionType` 멤버가 들어올 수 있다.
    """
    if raw is None:
        return None
    if isinstance(raw, BinanceUsdsFuturesExecutionType):
        return raw

    if isinstance(raw, str):
        s = raw.strip()
    else:
        # Enum(str) 멤버 등: str(x)는 Enum 표시명이 될 수 있으므로 value 우선
        value = getattr(raw, "value", None)
        if isinstance(value, str) and value:
            s = value.strip()
        else:
            s = str(raw).strip()

    if not s:
        return None
    try:
        return BinanceUsdsFuturesExecutionType(s)
    except ValueError:
        return None
