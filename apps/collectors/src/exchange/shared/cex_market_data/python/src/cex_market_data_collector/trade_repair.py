from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Protocol

from .adapter_base import JsonGetter
from .models import now_ms


class TradeRepairAdapter(Protocol):
    exchange: str

    async def fetch_repair_trades(
        self,
        client: JsonGetter,
        gap: TradeGap,
    ) -> list[dict[str, Any]]: ...


@dataclass(frozen=True, slots=True)
class TradeGap:
    exchange: str
    symbol: str
    from_trade_id: int | None
    to_trade_id: int | None
    last_trade_id: str | None
    current_trade_id: str
    last_exchange_ts: int | None
    current_exchange_ts: int | None
    reason: str


class TradeRepairState:
    """Detect numeric trade-id gaps per symbol and keep timestamp context."""

    def __init__(self) -> None:
        self._last_ids: dict[str, str] = {}
        self._last_numeric_ids: dict[str, int] = {}
        self._last_ts: dict[str, int] = {}
        self._stream_interrupted = False

    def mark_stream_interrupted(self) -> None:
        self._stream_interrupted = True

    def observe(self, event: Mapping[str, Any]) -> TradeGap | None:
        if event.get("data_type") != "trade":
            return None

        symbol = str(event.get("symbol", "unknown")).upper()
        exchange = str(event.get("exchange", "unknown")).lower()
        trade_id = event.get("trade_id")
        if trade_id is None:
            return None

        trade_id_str = str(trade_id)
        current_numeric = _to_int(trade_id)
        current_ts = _to_int(event.get("exchange_ts"))
        last_id = self._last_ids.get(symbol)
        last_numeric = self._last_numeric_ids.get(symbol)
        last_ts = self._last_ts.get(symbol)
        stream_interrupted = self._stream_interrupted
        self._stream_interrupted = False

        self._last_ids[symbol] = trade_id_str
        if current_ts is not None:
            self._last_ts[symbol] = current_ts
        if current_numeric is None:
            if stream_interrupted and last_ts is not None and current_ts is not None and current_ts > last_ts:
                return TradeGap(
                    exchange=exchange,
                    symbol=symbol,
                    from_trade_id=None,
                    to_trade_id=None,
                    last_trade_id=last_id,
                    current_trade_id=trade_id_str,
                    last_exchange_ts=last_ts,
                    current_exchange_ts=current_ts,
                    reason="stream_resume_time_gap",
                )
            return None

        self._last_numeric_ids[symbol] = current_numeric
        if stream_interrupted and last_ts is not None and current_ts is not None and current_ts > last_ts:
            return TradeGap(
                exchange=exchange,
                symbol=symbol,
                from_trade_id=None,
                to_trade_id=None,
                last_trade_id=last_id,
                current_trade_id=trade_id_str,
                last_exchange_ts=last_ts,
                current_exchange_ts=current_ts,
                reason="stream_resume_time_gap",
            )
        if last_numeric is None or current_numeric <= last_numeric + 1:
            return None

        return TradeGap(
            exchange=exchange,
            symbol=symbol,
            from_trade_id=last_numeric + 1,
            to_trade_id=current_numeric - 1,
            last_trade_id=last_id,
            current_trade_id=trade_id_str,
            last_exchange_ts=last_ts,
            current_exchange_ts=current_ts,
            reason="numeric_trade_id_gap",
        )


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def filter_trade_rows_by_gap(
    rows: list[dict[str, Any]],
    *,
    id_getter,
    ts_getter=None,
    gap: TradeGap,
) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for row in rows:
        trade_id = _to_int(id_getter(row))
        if gap.from_trade_id is not None or gap.to_trade_id is not None:
            if trade_id is None:
                continue
            if gap.from_trade_id is not None and trade_id < gap.from_trade_id:
                continue
            if gap.to_trade_id is not None and trade_id > gap.to_trade_id:
                continue
        elif ts_getter is not None and gap.last_exchange_ts is not None and gap.current_exchange_ts is not None:
            trade_ts = _to_int(ts_getter(row))
            if trade_ts is None:
                continue
            if not (gap.last_exchange_ts < trade_ts < gap.current_exchange_ts):
                continue
        filtered.append(row)
    return sorted(filtered, key=lambda row: _sort_key(row, id_getter=id_getter, ts_getter=ts_getter))


def _sort_key(row: dict[str, Any], *, id_getter, ts_getter=None) -> int:
    trade_id = _to_int(id_getter(row))
    if trade_id is not None:
        return trade_id
    if ts_getter is not None:
        trade_ts = _to_int(ts_getter(row))
        if trade_ts is not None:
            return trade_ts
    return 0


def mark_repaired_trade(event: dict[str, Any], gap: TradeGap) -> dict[str, Any]:
    event["source"] = "rest_gap_fill"
    event["verified_by_rest"] = True
    event["repair_reason"] = gap.reason
    event["repair_from_trade_id"] = gap.from_trade_id
    event["repair_to_trade_id"] = gap.to_trade_id
    event["repair_detected_at_ms"] = now_ms()
    return event
