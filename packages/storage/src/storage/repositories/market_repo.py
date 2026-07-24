"""
Market data repositories for generic collector events.

These repositories handle the operational market topics produced by
``cex_market_data_collector``:

- ``trade`` events from WebSocket streams
- ``orderbook``/``depth`` snapshots from WebSocket or REST polling
- ``open_interest`` snapshots from REST polling

They intentionally do not replace ``TradeQuestDBRepository``.  That repository
is for the stricter canonical trade topic, while this module stores broader
research/monitoring market events.
"""

from __future__ import annotations

import math
from typing import Any

import orjson

from common.logging import setup_logger
from common.market_naming import build_market_redis_stream_key
from storage.questdb_client import QuestDBClient
from storage.redis_client import RedisStreamClient
from .base_questdb import BaseQuestDBRepository
from .redis.base_redis import BaseHotBufferRepository

logger = setup_logger(__name__)


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(result):
        return None
    return result


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _json_string(value: Any) -> str | None:
    if value is None:
        return None
    try:
        return orjson.dumps(value).decode("utf-8")
    except TypeError:
        return orjson.dumps(str(value)).decode("utf-8")


def _timestamp_nanos(data: dict) -> int:
    ts_ms = _to_int(data.get("exchange_ts") or data.get("timestamp") or data.get("ts"))
    return (ts_ms or 0) * 1_000_000


def _first_level(levels: Any) -> dict[str, Any] | None:
    if not isinstance(levels, list) or not levels:
        return None
    first = levels[0]
    if isinstance(first, dict):
        return first
    if isinstance(first, (list, tuple)) and len(first) >= 2:
        return {"price": first[0], "size": first[1]}
    return None


def _level_price(level: dict[str, Any] | None) -> float | None:
    if not level:
        return None
    return _to_float(level.get("price") or level.get("p"))


def _level_size(level: dict[str, Any] | None) -> float | None:
    if not level:
        return None
    return _to_float(level.get("size") or level.get("q") or level.get("amount"))


class MarketTradeQuestDBRepository(BaseQuestDBRepository):
    """Store generic trade events from operational market topics."""

    def __init__(self, questdb: QuestDBClient, table_name: str):
        super().__init__(questdb, table_name)

    def encode(self, data: dict) -> dict[str, Any]:
        columns: dict[str, Any] = {}
        for key in ("trade_id", "exchange_ts", "local_ts", "repair_from_trade_id", "repair_to_trade_id", "repair_detected_at_ms"):
            value = _to_int(data.get(key))
            if value is not None:
                columns[key] = value
        if data.get("verified_by_rest") is not None:
            columns["verified_by_rest"] = str(data.get("verified_by_rest")).lower() in {"1", "true", "yes"}
        if data.get("repair_reason") is not None:
            columns["repair_reason"] = str(data.get("repair_reason"))
        for key in ("price", "size"):
            value = _to_float(data.get(key))
            if value is not None:
                columns[key] = value
            elif data.get(key) is not None:
                logger.warning("MarketTradeQuestDBRepository: invalid numeric value for '%s': %r", key, data.get(key))

        raw_json = _json_string(data.get("raw"))
        if raw_json is not None:
            columns["raw_json"] = raw_json

        return {
            "symbols": {
                "exchange": str(data.get("exchange", "unknown")).lower(),
                "market_type": str(data.get("market_type", "unknown")).lower(),
                "symbol": str(data.get("symbol", "unknown")).upper(),
                "data_type": "trade",
                "side": str(data.get("side", "unknown")).lower(),
                "source": str(data.get("source", "websocket")).lower(),
            },
            "columns": columns,
            "at": _timestamp_nanos(data),
        }


class OrderBookQuestDBRepository(BaseQuestDBRepository):
    """Store orderbook/depth snapshots with top-of-book features."""

    def __init__(self, questdb: QuestDBClient, table_name: str):
        super().__init__(questdb, table_name)

    def encode(self, data: dict) -> dict[str, Any]:
        bids = data.get("bids") or []
        asks = data.get("asks") or []
        best_bid = _first_level(bids)
        best_ask = _first_level(asks)
        best_bid_price = _level_price(best_bid)
        best_bid_size = _level_size(best_bid)
        best_ask_price = _level_price(best_ask)
        best_ask_size = _level_size(best_ask)

        columns: dict[str, Any] = {}
        numeric_values = {
            "best_bid_price": best_bid_price,
            "best_bid_size": best_bid_size,
            "best_ask_price": best_ask_price,
            "best_ask_size": best_ask_size,
        }
        if best_bid_price is not None and best_ask_price is not None:
            numeric_values["mid_price"] = (best_bid_price + best_ask_price) / 2.0
            numeric_values["spread"] = best_ask_price - best_bid_price
        for key, value in numeric_values.items():
            if value is not None:
                columns[key] = value

        columns["bid_depth"] = len(bids) if isinstance(bids, list) else 0
        columns["ask_depth"] = len(asks) if isinstance(asks, list) else 0
        for key in ("exchange_ts", "local_ts"):
            value = _to_int(data.get(key))
            if value is not None:
                columns[key] = value
        if data.get("sequence") is not None:
            columns["sequence"] = str(data.get("sequence"))
        columns["bids_json"] = _json_string(bids) or "[]"
        columns["asks_json"] = _json_string(asks) or "[]"
        raw_json = _json_string(data.get("raw"))
        if raw_json is not None:
            columns["raw_json"] = raw_json

        return {
            "symbols": {
                "exchange": str(data.get("exchange", "unknown")).lower(),
                "market_type": str(data.get("market_type", "unknown")).lower(),
                "symbol": str(data.get("symbol", "unknown")).upper(),
                "data_type": "orderbook",
            },
            "columns": columns,
            "at": _timestamp_nanos(data),
        }


class OpenInterestQuestDBRepository(BaseQuestDBRepository):
    """Store open interest snapshots from exchange REST pollers."""

    def __init__(self, questdb: QuestDBClient, table_name: str):
        super().__init__(questdb, table_name)

    def encode(self, data: dict) -> dict[str, Any]:
        columns: dict[str, Any] = {}
        numeric_fields = ("open_interest", "open_interest_value_usd")
        for key in numeric_fields:
            value = _to_float(data.get(key))
            if value is not None:
                columns[key] = value
            elif data.get(key) is not None:
                logger.warning("OpenInterestQuestDBRepository: invalid numeric value for '%s': %r", key, data.get(key))
        for key in ("exchange_ts", "local_ts"):
            value = _to_int(data.get(key))
            if value is not None:
                columns[key] = value
        for key in ("note", "error"):
            if data.get(key) is not None:
                columns[key] = str(data.get(key))
        raw_json = _json_string(data.get("raw"))
        if raw_json is not None:
            columns["raw_json"] = raw_json

        return {
            "symbols": {
                "exchange": str(data.get("exchange", "unknown")).lower(),
                "market_type": str(data.get("market_type", "unknown")).lower(),
                "symbol": str(data.get("symbol", "unknown")).upper(),
                "data_type": "open_interest",
                "open_interest_unit": str(data.get("open_interest_unit", "unknown")),
            },
            "columns": columns,
            "at": _timestamp_nanos(data),
        }


class MarketEventRedisBufferRepository(BaseHotBufferRepository):
    """Store latest generic market events in Redis Streams."""

    def __init__(self, redis: RedisStreamClient, maxlen: int):
        super().__init__(redis, maxlen)

    def get_stream_key(self, data: dict) -> str:
        data_type = str(data.get("data_type", "unknown")).lower()
        if data_type == "depth":
            data_type = "orderbook"
        return self._build_key(
            data_type=data_type,
            exchange=str(data.get("exchange", "unknown")).lower(),
            market_type=str(data.get("market_type", "unknown")).lower(),
            symbol=str(data.get("symbol", "unknown")).upper(),
        )

    def _build_key(self, *, data_type: str, exchange: str, market_type: str, symbol: str) -> str:
        return build_market_redis_stream_key(
            data_type=data_type,
            exchange=exchange,
            market_type=market_type,
            symbol=symbol,
        )

    def encode(self, data: dict) -> dict:
        encoded: dict[str, str] = {}
        for key, value in data.items():
            if value is None:
                continue
            if isinstance(value, (dict, list, tuple)):
                encoded[key] = _json_string(value) or ""
            else:
                encoded[key] = str(value)
        return encoded
