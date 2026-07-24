import warnings
from typing import Dict, Any
from schemas.market import TradeSource
from common.time import current_time_ms
from binance_perp_collector.core.events import WsTradeEvent, WsAggTradeEvent


def normalize_ws_trade(event: WsTradeEvent, source: TradeSource) -> Dict[str, Any]:
    now_ms = current_time_ms()
    exchange_ts = event.trade_time_ms

    return {
        "exchange": "binance",
        "market_type": "perp",
        "symbol": event.symbol,
        "trade_id": event.trade_id,
        "price": event.price,
        "size": event.quantity,
        "is_buyer_maker": event.is_buyer_maker,
        "exchange_ts": exchange_ts,
        "event_ts": (
            event.event_time_ms if event.event_time_ms is not None else exchange_ts
        ),
        "local_ts": now_ms,
        "source": source.value,
        "verified_by_rest": False,
        "reconstructed_from_agg": False,
        "source_agg_trade_id": None,
        "lag_ms": max(0, now_ms - exchange_ts),
    }


def normalize_rest_trade(
    raw: dict,
    symbol: str,
    source: TradeSource,
    source_agg_trade_id: int | None = None,
) -> Dict[str, Any]:
    """REST API에서 수신한 원시 Trade 데이터를 정규화합니다."""
    now_ms = current_time_ms()
    exchange_ts = raw.get("time", 0)

    return {
        "exchange": "binance",
        "market_type": "perp",
        "symbol": symbol,
        "trade_id": raw.get("id", 0),
        "price": raw.get("price", "0"),
        "size": raw.get("qty", "0"),
        "is_buyer_maker": raw.get("isBuyerMaker", False),
        "exchange_ts": exchange_ts,
        "event_ts": None,
        "local_ts": now_ms,
        "source": source.value,
        "verified_by_rest": True,
        "reconstructed_from_agg": source_agg_trade_id is not None,
        "source_agg_trade_id": source_agg_trade_id,
        "lag_ms": None,
    }


def normalize_agg_trade_event(
    event: WsAggTradeEvent, source: TradeSource
) -> Dict[str, Any]:
    now_ms = current_time_ms()
    exchange_ts = event.trade_time_ms

    return {
        "exchange": "binance",
        "market_type": "perp",
        "symbol": event.symbol,
        "agg_trade_id": event.agg_trade_id,
        "first_trade_id": event.first_trade_id,
        "last_trade_id": event.last_trade_id,
        "trade_count_est": event.last_trade_id - event.first_trade_id + 1,
        "price": event.price,
        "total_size": event.quantity,
        "is_buyer_maker": event.is_buyer_maker,
        "exchange_ts": exchange_ts,
        "event_ts": (
            event.event_time_ms if event.event_time_ms is not None else exchange_ts
        ),
        "local_ts": now_ms,
        "source": source.value,
        "expanded": False,
        "lag_ms": max(0, now_ms - exchange_ts),
    }


def normalize_trade(raw: dict, symbol: str, source: TradeSource) -> Dict[str, Any]:
    """
    .. deprecated::
        이 함수는 더 이상 사용하지 마세요.

        - WebSocket @trade  → :func:`normalize_ws_trade`
        - WebSocket @aggTrade → :func:`normalize_agg_trade_event`
        - REST historicalTrades / trades → :func:`normalize_rest_trade`

        세 신규 함수는 정밀도 보존을 위해 price/size를 십진 문자열로 운반하는
        string-first 정책을 따릅니다 (참고: schemas.market.DecimalString).
        반면 이 레거시 함수는 ``float()`` 으로 변환하여 정밀도가 손실됩니다.
    """
    warnings.warn(
        "normalize_trade is deprecated and will be removed. "
        "Use normalize_ws_trade / normalize_agg_trade_event / normalize_rest_trade — "
        "they preserve decimal precision via string-first policy.",
        DeprecationWarning,
        stacklevel=2,
    )

    now_ms = current_time_ms()
    exchange_ts = raw.get("T", 0)
    lag_ms = max(0, now_ms - exchange_ts)

    trade_id = raw.get("t", raw.get("f", raw.get("l", 0)))

    return {
        "exchange": "binance",
        "market_type": "perp",
        "symbol": symbol,
        "trade_id": str(trade_id),
        "price": float(raw.get("p", 0.0)),
        "size": float(raw.get("q", 0.0)),
        "is_buyer_maker": bool(raw.get("m", False)),
        "exchange_ts": exchange_ts,
        "local_ts": now_ms,
        "source": source.value,
        "verified_by_rest": False,
        "lag_ms": lag_ms,
    }
