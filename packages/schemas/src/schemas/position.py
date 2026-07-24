from __future__ import annotations

from decimal import Decimal
from enum import Enum
from typing import Optional

from pydantic import BaseModel, model_validator

from schemas.market import Exchange, MarketType

class PositionCloseOrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP_MARKET = "STOP_MARKET"
    STOP_LIMIT = "STOP_LIMIT"


class PositionSide(str, Enum):
    BOTH = "BOTH"
    LONG = "LONG"
    SHORT = "SHORT"


class PositionStatus(str, Enum):
    FLAT = "FLAT" # 포지션이 없는 상태, position_amt == 0, 포지션을 안 들고 있거나, 기존 포지션을 전부 청산해서 수량이 0이 된 상태
    OPEN = "OPEN" # 포지션이 열려 있는 상태, position_amt != 0, 양수/음수 상관없이 수량이 0이 아니면 OPEN


def make_position_id(
    *,
    exchange: Exchange | str,
    market_type: MarketType | str,
    symbol: str,
    position_side: PositionSide | str,
) -> str:
    exchange_value = exchange.value if hasattr(exchange, "value") else str(exchange)
    market_type_value = (
        market_type.value if hasattr(market_type, "value") else str(market_type)
    )
    position_side_value = (
        position_side.value if hasattr(position_side, "value") else str(position_side)
    )

    return (
        f"{exchange_value.upper()}:"
        f"{market_type_value.upper()}:"
        f"{symbol.upper()}:"
        f"{position_side_value.upper()}"
    )


def infer_position_status(position_amt: str | Decimal | int | float) -> PositionStatus:
    try:
        amt = Decimal(str(position_amt))
    except Exception:
        amt = Decimal("0")

    return PositionStatus.FLAT if amt == 0 else PositionStatus.OPEN


class Position(BaseModel):
    position_id: Optional[str] = None

    exchange: Exchange
    market_type: MarketType
    symbol: str
    position_side: PositionSide = PositionSide.BOTH

    status: PositionStatus = PositionStatus.FLAT

    position_amt: str = "0"
    entry_price: Optional[str] = None
    break_even_price: Optional[str] = None
    mark_price: Optional[str] = None

    unrealized_pnl: Optional[str] = None
    isolated_margin: Optional[str] = None
    isolated_wallet: Optional[str] = None
    margin_type: Optional[str] = None
    leverage: Optional[int] = None
    liquidation_price: Optional[str] = None
    notional: Optional[str] = None

    update_reason: Optional[str] = None
    last_event_time: Optional[int] = None
    last_transaction_time: Optional[int] = None

    opened_ts: Optional[int] = None
    closed_ts: Optional[int] = None
    updated_ts: int

    version: int = 1

    @model_validator(mode="after")
    def fill_derived_fields(self) -> "Position":
        self.symbol = self.symbol.upper()

        expected_id = make_position_id(
            exchange=self.exchange,
            market_type=self.market_type,
            symbol=self.symbol,
            position_side=self.position_side,
        )

        if not self.position_id:
            self.position_id = expected_id

        elif self.position_id != expected_id:
            raise ValueError(
                f"position_id mismatch: position_id={self.position_id}, expected={expected_id}"
            )

        self.status = infer_position_status(self.position_amt)

        return self