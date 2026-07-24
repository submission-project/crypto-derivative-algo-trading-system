from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel

from schemas.market import Exchange, MarketType
from schemas.position import (
    Position,
    PositionSide,
    PositionStatus,
    make_position_id,
)


class NormalizedPositionSnapshot(BaseModel):
    """
    거래소별 position update event를 Takora 내부 표준 snapshot으로 정규화한 모델.

    Binance:
      ACCOUNT_UPDATE.a.P[]

    OKX / Bitget / Bybit:
      position update event를 이 모델로 정규화해서
      PositionStateService에 넘기면 된다.
    """

    exchange: Exchange
    market_type: MarketType
    symbol: str
    position_side: PositionSide

    status: PositionStatus
    position_amt: str

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
    event_time: int
    transaction_time: Optional[int] = None

    raw: dict[str, Any]

    def to_position(self) -> Position:
        """
        NormalizedPositionSnapshot을 Position model로 변환.

        opened_ts / closed_ts는 여기서 직접 계산하지 않는다.
        PositionPostgresRepository.upsert()가
        기존 position_amt와 신규 position_amt를 비교해서 계산한다.
        """
        updated_ts = self.transaction_time or self.event_time

        return Position(
            position_id=make_position_id(
                exchange=self.exchange,
                market_type=self.market_type,
                symbol=self.symbol,
                position_side=self.position_side,
            ),
            exchange=self.exchange,
            market_type=self.market_type,
            symbol=self.symbol.upper(),
            position_side=self.position_side,
            status=self.status,
            position_amt=self.position_amt,
            entry_price=self.entry_price,
            break_even_price=self.break_even_price,
            mark_price=self.mark_price,
            unrealized_pnl=self.unrealized_pnl,
            isolated_margin=self.isolated_margin,
            isolated_wallet=self.isolated_wallet,
            margin_type=self.margin_type,
            leverage=self.leverage,
            liquidation_price=self.liquidation_price,
            notional=self.notional,
            update_reason=self.update_reason,
            last_event_time=self.event_time,
            last_transaction_time=self.transaction_time,
            opened_ts=None,
            closed_ts=None,
            updated_ts=updated_ts,
            version=1,
        )