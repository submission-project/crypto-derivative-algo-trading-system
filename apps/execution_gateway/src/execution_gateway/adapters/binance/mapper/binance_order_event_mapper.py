from __future__ import annotations

from typing import Any

from schemas.market import Exchange, MarketType
from schemas.order import OrderStatus
from schemas.order_update_event import NormalizedOrderUpdateEvent

from ..constant.binance_constant import BinanceOrderState

_MAPPER_INTERNAL_TO_BINANCE_ORDER_STATUS: dict[OrderStatus, str] = {
    OrderStatus.ACKNOWLEDGED : BinanceOrderState.new,
    OrderStatus.PARTIALLY_FILLED: BinanceOrderState.partially_filled,
    OrderStatus.FILLED: BinanceOrderState.filled,
    # OrderStatus.CANCELLED: BinanceOrderState.canceled,
    OrderStatus.CANCELLED: BinanceOrderState.cancelled,
    OrderStatus.EXPIRED: BinanceOrderState.expired,
    OrderStatus.REJECTED: BinanceOrderState.rejected,
}

_BINANCE_ORDER_STATUS_MAP: dict[str, OrderStatus] = {
    BinanceOrderState.new: OrderStatus.ACKNOWLEDGED,
    BinanceOrderState.partially_filled: OrderStatus.PARTIALLY_FILLED,
    BinanceOrderState.filled: OrderStatus.FILLED,
    BinanceOrderState.canceled: OrderStatus.CANCELLED,
    BinanceOrderState.cancelled: OrderStatus.CANCELLED,
    BinanceOrderState.expired: OrderStatus.EXPIRED,
    BinanceOrderState.expired_in_match: OrderStatus.EXPIRED,
    BinanceOrderState.rejected: OrderStatus.REJECTED,
}

def map_binance_order_status(raw_status: str | None) -> OrderStatus | None:
    if not raw_status:
        return None

    return _BINANCE_ORDER_STATUS_MAP.get(str(raw_status).upper())


def _optional_str(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def normalize_binance_order_update(
    raw_event: dict[str, Any],
    *,
    market_type: MarketType,
) -> NormalizedOrderUpdateEvent:
    order = raw_event.get("o", {})
    if not isinstance(order, dict):
        raise ValueError(f"invalid ORDER_TRADE_UPDATE payload: {raw_event}")

    client_order_id = order.get("c")
    if not client_order_id:
        raise ValueError(f"ORDER_TRADE_UPDATE missing client order id: {raw_event}")

    symbol = str(order.get("s") or "").upper()
    if not symbol:
        raise ValueError(f"ORDER_TRADE_UPDATE missing symbol: {raw_event}")

    raw_status = _optional_str(order.get("X"))

    trade_id = order.get("t")
    if trade_id in (None, "", 0, "0", -1, "-1"):
        trade_id = None

    reject_reason = (
        order.get("r")
        or order.get("rejectReason")
        or order.get("reject_reason")
    )

    if reject_reason in (None, "", "NONE"):
        reject_reason = None

    return NormalizedOrderUpdateEvent(
        exchange=Exchange.BINANCE,
        market_type=market_type,
        symbol=symbol,
        client_order_id=str(client_order_id),
        exchange_order_id=_optional_str(order.get("i")),
        target_status=map_binance_order_status(raw_status),
        exchange_status=raw_status,
        execution_type=_optional_str(order.get("x")),
        filled_quantity=_optional_str(order.get("z")),
        avg_fill_price=_optional_str(order.get("ap")),
        last_fill_quantity=_optional_str(order.get("l")),
        last_fill_price=_optional_str(order.get("L")),
        trade_id=str(trade_id) if trade_id is not None else None,
        commission=_optional_str(order.get("n")),
        commission_asset=_optional_str(order.get("N")),
        is_maker=order.get("m") if isinstance(order.get("m"), bool) else None,
        reject_reason_text=str(reject_reason) if reject_reason is not None else None,
        event_time=raw_event.get("E"),
        transaction_time=raw_event.get("T"),
        raw=raw_event,
    )