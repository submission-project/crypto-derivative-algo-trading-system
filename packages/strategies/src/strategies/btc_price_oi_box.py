from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal

from common.time import current_time_ms
from schemas.market import Exchange, MarketType
from schemas.signal import Signal, SignalDirection, SignalStatus

from ._common import decimal_or_none, normalize_symbol


@dataclass(frozen=True, slots=True)
class LiveBoxState:
    low: Decimal
    high: Decimal
    mid: Decimal
    width: Decimal
    coverage: float


@dataclass(slots=True)
class BtcPriceOiBoxStrategy:
    """
    OI/Price 박스권 추세추종 전략.
    """

    name: str = "btc_price_oi_box_v1"
    target_exchange: str = "binance"
    target_symbol: str = "BTCUSDT"
    window_size: int = 72
    min_box_points: int = 36
    lower_quantile: float = 0.10
    upper_quantile: float = 0.90
    margin_ratio: Decimal = Decimal("0.08")
    min_box_coverage: float = 0.70
    max_oi_box_width_ratio: Decimal = Decimal("0.035")
    entry_edge_ratio: Decimal = Decimal("0.12")
    bounce_bars: int = 3
    bounce_confirm_bps: Decimal = Decimal("2")
    breakout_buffer_bps: Decimal = Decimal("20")
    min_trend_momentum_bps: Decimal = Decimal("12")
    oi_breakout_buffer_bps: Decimal = Decimal("8")
    quantity: Decimal = Decimal("0.001")
    cooldown_ms: int = 5 * 60_000
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

        price_box = self._box_state(self._prices)
        oi_box = self._box_state(self._open_interests)
        if price_box is None or oi_box is None:
            return []
        if oi_box.width / max(oi_box.mid, Decimal("1e-12")) > self.max_oi_box_width_ratio:
            return []

        now_ms = int(event.get("local_ts") or event.get("exchange_ts") or current_time_ms())
        if now_ms - self._last_signal_ts < self.cooldown_ms:
            return []

        price = self._prices[-1]
        oi = self._open_interests[-1]
        price_momentum_bps = self._change_bps(self._prices)
        bounce_bps = self._bounce_bps()
        if price_momentum_bps is None or bounce_bps is None:
            return []

        direction: SignalDirection | None = None
        reason: str | None = None
        stop_loss: Decimal | None = None
        take_profit: Decimal | None = None
        width = price_box.width
        lower_edge = price_box.low + width * self.entry_edge_ratio
        upper_edge = price_box.high - width * self.entry_edge_ratio
        breakout_up = price >= self._apply_bps(price_box.high, self.breakout_buffer_bps)
        breakout_down = price <= self._apply_bps(price_box.low, -self.breakout_buffer_bps)
        oi_breakout = oi >= self._apply_bps(oi_box.high, self.oi_breakout_buffer_bps)

        if breakout_up and oi_breakout and price_momentum_bps >= self.min_trend_momentum_bps:
            direction = SignalDirection.LONG
            reason = "price_oi_box_breakout_up"
            stop_loss = price_box.high
            take_profit = price + max(width, price - price_box.high)
        elif breakout_down and oi_breakout and price_momentum_bps <= -self.min_trend_momentum_bps:
            direction = SignalDirection.SHORT
            reason = "price_oi_box_breakout_down"
            stop_loss = price_box.low
            take_profit = price - max(width, price_box.low - price)
        elif price <= lower_edge and bounce_bps >= self.bounce_confirm_bps:
            direction = SignalDirection.LONG
            reason = "price_box_lower_edge_bounce_with_stable_oi"
            stop_loss = price_box.low
            take_profit = price_box.mid
        elif price >= upper_edge and bounce_bps <= -self.bounce_confirm_bps:
            direction = SignalDirection.SHORT
            reason = "price_box_upper_edge_reject_with_stable_oi"
            stop_loss = price_box.high
            take_profit = price_box.mid

        if direction is None or reason is None or stop_loss is None or take_profit is None:
            return []
        if direction == SignalDirection.LONG and not (stop_loss < price < take_profit):
            return []
        if direction == SignalDirection.SHORT and not (take_profit < price < stop_loss):
            return []

        self._last_signal_ts = now_ms
        side = "BUY" if direction == SignalDirection.LONG else "SELL"
        confidence = self._confidence(price_box=price_box, oi_box=oi_box, reason=reason)
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
            suggested_entry_price=str(price),
            suggested_stop_loss=str(stop_loss),
            suggested_take_profit=str(take_profit),
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

    def _box_state(self, values: deque[Decimal]) -> LiveBoxState | None:
        if len(values) < self.min_box_points:
            return None
        ordered = sorted(values)
        low = self._quantile(ordered, self.lower_quantile)
        high = self._quantile(ordered, self.upper_quantile)
        width = max(high - low, Decimal("1e-12"))
        low = low - width * self.margin_ratio
        high = high + width * self.margin_ratio
        width = high - low
        mid = (low + high) / Decimal("2")
        inside = sum(1 for value in values if low <= value <= high)
        coverage = inside / len(values)
        if coverage < self.min_box_coverage:
            return None
        return LiveBoxState(low=low, high=high, mid=mid, width=width, coverage=coverage)

    @staticmethod
    def _quantile(ordered: list[Decimal], q: float) -> Decimal:
        if not ordered:
            return Decimal("0")
        idx = min(max(int(round((len(ordered) - 1) * q)), 0), len(ordered) - 1)
        return ordered[idx]

    @staticmethod
    def _apply_bps(value: Decimal, bps: Decimal) -> Decimal:
        return value * (Decimal("1") + bps / Decimal("10000"))

    @staticmethod
    def _change_bps(values: deque[Decimal]) -> Decimal | None:
        first = values[0]
        last = values[-1]
        if first <= 0:
            return None
        return (last - first) / first * Decimal("10000")

    def _bounce_bps(self) -> Decimal | None:
        if len(self._prices) <= self.bounce_bars:
            return None
        before = self._prices[-self.bounce_bars - 1]
        now = self._prices[-1]
        if before <= 0:
            return None
        return (now - before) / before * Decimal("10000")

    @staticmethod
    def _confidence(*, price_box: LiveBoxState, oi_box: LiveBoxState, reason: str) -> float:
        base = 0.62 if "breakout" in reason else 0.58
        price_quality = min(float(price_box.coverage), 1.0)
        oi_quality = min(float(oi_box.coverage), 1.0)
        return min(base + 0.15 * price_quality + 0.15 * oi_quality, 0.95)
