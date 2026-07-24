from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from schemas.market import Exchange, MarketType
from schemas.order import ConditionalStatus, OrderStatus, RejectReason
from schemas.position import PositionSide


@dataclass(frozen=True, slots=True)
class ExchangeOrderAck:
    """Normalized successful order placement response."""

    exchange: Exchange
    market_type: MarketType
    symbol: str
    client_order_id: str
    exchange_order_id: str | None = None
    status: OrderStatus = OrderStatus.ACKNOWLEDGED
    raw_status: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ExchangeOrderReject:
    """Normalized per-order rejection response, especially for batch routes."""

    exchange: Exchange
    market_type: MarketType
    symbol: str
    client_order_id: str
    reject_reason: RejectReason
    message: str | None = None
    code: int | str | None = None
    raw: dict[str, Any] = field(default_factory=dict)


ExchangeBatchOrderResult = ExchangeOrderAck | ExchangeOrderReject


@dataclass(frozen=True, slots=True)
class ExchangeCancelResult:
    """Normalized order cancel response."""

    exchange: Exchange
    market_type: MarketType
    symbol: str
    client_order_id: str | None = None
    exchange_order_id: str | None = None
    client_conditional_id: str | None = None
    exchange_conditional_id: str | None = None
    status: OrderStatus | None = None
    conditional_status: ConditionalStatus | None = None
    raw_status: str | None = None
    unknown_execution: bool = False
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ExchangeOrderSnapshot:
    """거래소에서 조회한 일반 주문 상태를 내부 공통 포맷으로 담는 객체"""

    exchange: Exchange
    market_type: MarketType
    symbol: str
    status: OrderStatus
    client_order_id: str | None = None
    exchange_order_id: str | None = None
    filled_quantity: str = "0"
    avg_fill_price: str = "0"
    raw_status: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ExchangeConditionalAck:
    """거래소에서 조회한 조건부 주문 상태를 내부 공통 포맷으로 담는 객체"""

    exchange: Exchange
    market_type: MarketType
    symbol: str
    client_conditional_id: str
    exchange_conditional_id: str | None = None
    conditional_status: ConditionalStatus = ConditionalStatus.NEW
    raw_status: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ExchangeConditionalSnapshot:
    """Normalized conditional order state loaded from exchange."""

    exchange: Exchange
    market_type: MarketType
    symbol: str
    conditional_status: ConditionalStatus
    client_conditional_id: str | None = None
    exchange_conditional_id: str | None = None
    triggered_order_id: str | None = None
    triggered_client_order_id: str | None = None
    filled_quantity: str | None = None
    avg_fill_price: str | None = None
    raw_status: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ExchangeLeverageResult:
    """Normalized leverage change response."""

    exchange: Exchange
    market_type: MarketType
    symbol: str
    leverage: int
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ExchangePositionSnapshot:
    exchange: Exchange
    market_type: MarketType
    symbol: str
    position_side: PositionSide = PositionSide.BOTH

    position_amt: str = "0"
    entry_price: str | None = None
    break_even_price: str | None = None
    mark_price: str | None = None
    unrealized_pnl: str | None = None

    isolated_margin: str | None = None
    isolated_wallet: str | None = None
    margin_type: str | None = None
    leverage: int | None = None
    liquidation_price: str | None = None
    notional: str | None = None
    updated_ts: int | None = None

    raw_status: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)