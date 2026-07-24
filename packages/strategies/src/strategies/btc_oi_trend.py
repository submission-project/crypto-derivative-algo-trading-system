from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal

from common.time import current_time_ms
from schemas.market import Exchange, MarketType
from schemas.signal import Signal, SignalDirection, SignalStatus

from ._common import decimal_or_none, normalize_symbol


@dataclass(slots=True)
class BtcOiTrendStrategy:
    """
    단순 가격/OI 추세추종 전략.
    """

    name: str = "btc_oi_trend_v1"
    target_exchange: str = "binance"
    target_symbol: str = "BTCUSDT"
    window_size: int = 12
    min_price_move_bps: Decimal = Decimal("8")
    min_oi_move_bps: Decimal = Decimal("3")
    quantity: Decimal = Decimal("0.001")
    cooldown_ms: int = 60_000
    confidence_floor: float = 0.55
    ttl_ms: int = 5 * 60_000
    _prices: deque[Decimal] = field(default_factory=deque)
    _open_interests: deque[Decimal] = field(default_factory=deque)
    _last_signal_ts: int = 0

    def on_market_event(self, event: dict) -> list[Signal]:
        if not self._accepts_event(event):
            return []

        data_type = str(event.get("data_type") or "").lower()
        if data_type == "trade":
            price = decimal_or_none(event.get("price"))
            if price is not None and price > 0:
                self._append(self._prices, price)
        elif data_type == "open_interest":
            oi = decimal_or_none(event.get("open_interest_value_usd") or event.get("open_interest"))
            if oi is not None and oi > 0:
                self._append(self._open_interests, oi)

        if len(self._prices) < self.window_size or len(self._open_interests) < self.window_size:
            return []

        now_ms = int(event.get("local_ts") or event.get("exchange_ts") or current_time_ms())
        if now_ms - self._last_signal_ts < self.cooldown_ms:
            return []

        price_bps = self._change_bps(self._prices)
        oi_bps = self._change_bps(self._open_interests)
        if price_bps is None or oi_bps is None:
            return []

        direction: SignalDirection | None = None
        if price_bps >= self.min_price_move_bps and oi_bps >= self.min_oi_move_bps:
            direction = SignalDirection.LONG
        elif price_bps <= -self.min_price_move_bps and oi_bps >= self.min_oi_move_bps:
            direction = SignalDirection.SHORT
        if direction is None:
            return []

        self._last_signal_ts = now_ms
        side = "BUY" if direction == SignalDirection.LONG else "SELL"
        strength = min(float((abs(price_bps) / self.min_price_move_bps) + (oi_bps / self.min_oi_move_bps)) / 4, 0.99)
        confidence = max(self.confidence_floor, strength)
        signal = Signal(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol=self.target_symbol,
            strategy_name=self.name,
            direction=direction,
            confidence=confidence,
            generated_ts=now_ms,
            status=SignalStatus.PENDING,
            suggested_side=side,
            suggested_quantity=str(self.quantity),
            suggested_order_type="MARKET",
            expires_ts=now_ms + self.ttl_ms,
        )
        return [signal]

    def _accepts_event(self, event: dict) -> bool:
        exchange = str(event.get("exchange") or "").lower()
        symbol = normalize_symbol(event.get("symbol"))
        data_type = str(event.get("data_type") or "").lower()
        return (
            exchange == self.target_exchange
            and symbol.startswith(self.target_symbol)
            and data_type in {"trade", "open_interest"}
        )

    def _append(self, values: deque[Decimal], value: Decimal) -> None:
        values.append(value)
        while len(values) > self.window_size:
            values.popleft()

    @staticmethod
    def _change_bps(values: deque[Decimal]) -> Decimal | None:
        first = values[0]
        last = values[-1]
        if first <= 0:
            return None
        return (last - first) / first * Decimal("10000")
