from __future__ import annotations

import os
from collections.abc import Iterable

from .config import settings

DEFAULT_MARKET_TOPIC_PREFIX = "market"
DEFAULT_MARKET_TYPE = "perp"
# DEFAULT_MARKET_PIPELINE_EXCHANGES: _, ...] = ("binance", "bybit", "okx", "bitget")
DEFAULT_MARKET_PIPELINE_DATA_TYPES: tuple[str, ...] = ("mixed", "open_interest")
DEFAULT_MARKET_REDIS_STREAM_PREFIX = "market"


def csv_values(raw: str | Iterable[str] | None) -> tuple[str, ...]:
    """Parse comma-separated config values while preserving order."""
    if raw is None:
        return ()
    values = raw.split(",") if isinstance(raw, str) else list(raw)

    seen: set[str] = set()
    parsed: list[str] = []
    for value in values:
        item = str(value).strip()
        if item and item not in seen:
            seen.add(item)
            parsed.append(item)
    return tuple(parsed)


def market_topic_prefix() -> str:
    return _topic_part(os.getenv("MARKET_TOPIC_PREFIX"), default=DEFAULT_MARKET_TOPIC_PREFIX)


def configured_market_type(default: str = DEFAULT_MARKET_TYPE) -> str:
    return _topic_part(os.getenv("MARKET_TOPIC_MARKET_TYPE"), default=default)


def build_market_topic(
    *,
    exchange: str,
    data_type: str,
    market_type: str | None = None,
    prefix: str | None = None,
) -> str:
    """Build operational market topic names.

    Default format:
        market.{data_type}.{exchange}.perp
    """
    topic_prefix = _topic_part(prefix, default=market_topic_prefix())
    market = _topic_part(market_type, default=configured_market_type())
    return ".".join(
        (
            topic_prefix,
            _topic_part(data_type, default="unknown"),
            _topic_part(exchange, default="unknown"),
            market,
        )
    )


def default_market_topics(
    *,
    exchanges: Iterable[str] | None = None,
    data_types: Iterable[str] | None = None,
) -> tuple[str, ...]:
    configured_exchanges = csv_values(settings.market_pipeline_exchanges)
    topic_exchanges = tuple(exchanges or configured_exchanges)
    topic_data_types = tuple(data_types or DEFAULT_MARKET_PIPELINE_DATA_TYPES)
    return tuple(
        build_market_topic(exchange=exchange, data_type=data_type)
        for data_type in topic_data_types
        for exchange in topic_exchanges
    )


def build_market_redis_stream_key(
    *,
    data_type: str,
    exchange: str,
    market_type: str,
    symbol: str,
    prefix: str | None = None,
) -> str:
    stream_prefix = _redis_part(
        prefix or os.getenv("MARKET_REDIS_STREAM_PREFIX"),
        default=DEFAULT_MARKET_REDIS_STREAM_PREFIX,
    )
    parts = (
        stream_prefix,
        _redis_part(data_type, default="unknown"),
        _redis_part(exchange, default="unknown"),
        _redis_part(market_type, default="unknown"),
        _redis_part(symbol, default="unknown").upper(),
    )
    return ":".join(parts)


def _topic_part(value: str | None, *, default: str) -> str:
    cleaned = str(value or default).strip().strip(".")
    return cleaned.lower() or default


def _redis_part(value: str | None, *, default: str) -> str:
    cleaned = str(value or default).strip().strip(":")
    return cleaned or default
