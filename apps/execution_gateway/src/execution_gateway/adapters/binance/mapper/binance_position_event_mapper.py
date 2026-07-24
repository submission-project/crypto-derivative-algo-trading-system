from __future__ import annotations

from decimal import Decimal
from typing import Any

from schemas.market import Exchange, MarketType
from schemas.position import PositionSide, PositionStatus
from schemas.position_update_event import NormalizedPositionSnapshot


def _as_optional_str(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _position_side(raw: Any) -> PositionSide:
    if raw in (None, ""):
        return PositionSide.BOTH

    value = str(raw).upper()

    try:
        return PositionSide(value)
    except ValueError:
        return PositionSide.BOTH


def _status_from_position_amt(position_amt: str) -> PositionStatus:
    amt = Decimal(str(position_amt))

    if amt == 0:
        return PositionStatus.FLAT

    return PositionStatus.OPEN


def normalize_binance_account_update_positions(
    raw_event: dict[str, Any],
    *,
    market_type: MarketType = MarketType.PERP,
) -> list[NormalizedPositionSnapshot]:
    """
    Binance USD-M Futures ACCOUNT_UPDATE raw event를
    Takora 내부 Position snapshot list로 변환.

    Binance ACCOUNT_UPDATE 구조 예:

    {
      "e": "ACCOUNT_UPDATE",
      "E": 1564745798939,
      "T": 1564745798938,
      "a": {
        "m": "ORDER",
        "P": [
          {
            "s": "BTCUSDT",
            "pa": "0.001",
            "ep": "50000",
            "bep": "50000",
            "cr": "0",
            "up": "1.23",
            "mt": "cross",
            "iw": "0",
            "ps": "BOTH"
          }
        ]
      }
    }
    """
    event_type = raw_event.get("e")
    if event_type != "ACCOUNT_UPDATE":
        raise ValueError(f"not ACCOUNT_UPDATE event: {raw_event}")

    account = raw_event.get("a")
    if not isinstance(account, dict):
        raise ValueError(f"ACCOUNT_UPDATE missing account payload: {raw_event}")

    positions = account.get("P", [])
    if positions is None:
        positions = []

    if not isinstance(positions, list):
        raise ValueError(f"ACCOUNT_UPDATE positions payload is not list: {raw_event}")

    event_time = int(raw_event.get("E") or 0)
    transaction_time = (
        int(raw_event["T"])
        if raw_event.get("T") is not None
        else None
    )

    reason = _as_optional_str(account.get("m"))

    snapshots: list[NormalizedPositionSnapshot] = []

    for item in positions:
        if not isinstance(item, dict):
            continue

        symbol = str(item.get("s") or "").upper()
        if not symbol:
            continue

        position_amt = str(item.get("pa") or "0")
        position_side = _position_side(item.get("ps"))
        status = _status_from_position_amt(position_amt)

        snapshots.append(
            NormalizedPositionSnapshot(
                exchange=Exchange.BINANCE,
                market_type=market_type,
                symbol=symbol,
                position_side=position_side,
                status=status,
                position_amt=position_amt,
                entry_price=_as_optional_str(item.get("ep")),
                break_even_price=_as_optional_str(item.get("bep")),
                mark_price=None,
                unrealized_pnl=_as_optional_str(item.get("up")),
                isolated_margin=None,
                isolated_wallet=_as_optional_str(item.get("iw")),
                margin_type=_as_optional_str(item.get("mt")),
                leverage=None,
                liquidation_price=None,
                notional=None,
                update_reason=reason,
                event_time=event_time,
                transaction_time=transaction_time,
                raw=item,
            )
        )

    return snapshots