from __future__ import annotations

from typing import Any, Mapping

from common.market_naming import build_market_topic

from .models import now_ms
from .operational_models import MarketEvent
from .utils import first_float, first_str, notional_str, to_str


def topic(exchange: str, data_type: str, *, market_type: str | None = None) -> str:
    return build_market_topic(exchange=exchange, data_type=data_type, market_type=market_type)


def trade_event(
    *,
    exchange: str,
    symbol: str,
    price: Any,
    size: Any,
    exchange_ts: Any,
    trade_id: Any = None,
    side: Any = None,
    raw: Any,
) -> MarketEvent:
    return {
        "exchange": exchange,
        "market_type": "perp",
        "data_type": "trade",
        "symbol": symbol,
        "trade_id": to_str(trade_id),
        "price": to_str(price),
        "size": to_str(size),
        "side": to_str(side),
        "exchange_ts": int(first_float({"ts": exchange_ts}, "ts") or now_ms()),
        "local_ts": now_ms(),
        "raw": raw,
    }


def orderbook_event(
    *,
    exchange: str,
    symbol: str,
    bids: Any,
    asks: Any,
    exchange_ts: Any,
    sequence: Any = None,
    raw: Any,
) -> MarketEvent:
    return {
        "exchange": exchange,
        "market_type": "perp",
        "data_type": "orderbook",
        "symbol": symbol,
        "bids": levels(bids),
        "asks": levels(asks),
        "exchange_ts": int(first_float({"ts": exchange_ts}, "ts") or now_ms()),
        "local_ts": now_ms(),
        "sequence": to_str(sequence),
        "raw": raw,
    }


def open_interest_event(
    *,
    exchange: str,
    symbol: str,
    open_interest: Any,
    exchange_ts: Any,
    unit: str | None = None,
    open_interest_value_usd: Any = None,
    mark_price: Any = None,
    raw: Any,
    note: str | None = None,
) -> MarketEvent:
    amount = to_str(open_interest)
    value = to_str(open_interest_value_usd) or notional_str(amount, to_str(mark_price))
    return {
        "exchange": exchange,
        "market_type": "perp",
        "data_type": "open_interest",
        "symbol": symbol,
        "open_interest": amount,
        "open_interest_unit": unit,
        "open_interest_value_usd": value,
        "exchange_ts": int(first_float({"ts": exchange_ts}, "ts") or now_ms()),
        "local_ts": now_ms(),
        "raw": raw,
        "note": note,
    }


def levels(rows: Any) -> list[dict[str, str]]:
    if not isinstance(rows, list):
        return []
    normalized = []
    for row in rows:
        if isinstance(row, (list, tuple)) and len(row) >= 2:
            price = to_str(row[0])
            size = to_str(row[1])
        elif isinstance(row, Mapping):
            price = first_str(row, "p", "price")
            size = first_str(row, "q", "s", "size", "amount", "qty")
        else:
            continue
        if price is not None and size is not None:
            normalized.append({"price": price, "size": size})
    return normalized
