from __future__ import annotations

import time
from dataclasses import dataclass, field
from math import log
from typing import Any, Sequence

from .features import (
    BookTop,
    TradeBucket,
    forward_returns,
    hurst_exponent,
    microprice,
    normalized_trade_imbalance,
    orderbook_imbalance,
    rolling_realized_volatility,
    rolling_period_volatility_bps,
    signal_from_imbalance,
    vpin,
    rolling_min_max_channel,
    buyer_taker_density,
    market_quantum_density_matrix,
)


# ── Strategy Configuration ──


@dataclass(frozen=True, slots=True)
class StrategyConfig:
    """Strategy parameters, loadable from configs/strategies/*.yaml."""

    symbol: str = "BTCUSDT"
    market_type: str = "perp"

    # Feature parameters
    trade_window_ms: int = 1_000
    orderbook_depth_levels: int = 5
    forward_return_horizons_ms: tuple[int, ...] = (1_000, 5_000, 30_000)

    # Signal thresholds
    imbalance_entry_threshold: float = 0.35
    max_spread_bps: float = 3.0
    max_realized_volatility_bps: float = 20.0

    # Regime detection
    hurst_window: int = 100
    hurst_momentum_threshold: float = 0.55
    hurst_reversion_threshold: float = 0.45

    # VPIN
    vpin_window: int = 50
    vpin_alert_threshold: float = 0.7

    # OI Box & Price Box parameters (newly added)
    oi_window: int = 50
    oi_noise_percent: float = 0.005
    price_window: int = 50
    price_noise_percent: float = 0.005
    volatility_threshold_bps: float = 5.0
    buyer_density_window: int = 50

    # Risk integration
    max_holding_periods: int = 30
    max_position_notional: float = 2_000.0


@dataclass(frozen=True, slots=True)
class MarketSnapshot:
    """A single point-in-time market observation."""

    timestamp_ms: int
    mid_price: float
    spread_bps: float
    trade_imbalance: float
    ob_imbalance: float
    microprice_val: float
    buy_volume: float
    sell_volume: float


@dataclass(frozen=True, slots=True)
class StrategySignal:
    """Output signal from the strategy engine."""

    timestamp_ms: int
    direction: int  # 1=LONG, -1=SHORT, 0=FLAT
    confidence: float  # 0.0–1.0
    regime: str  # "momentum", "mean_reversion", "neutral", "quantum_box", "quantum_impulse"
    features: dict[str, float] = field(default_factory=dict)


# ── Data Fetching (Binance Public REST API) ──


def fetch_klines(
    symbol: str = "BTCUSDT",
    interval: str = "1m",
    limit: int = 1000,
) -> list[dict[str, Any]]:
    """
    Fetch klines from Binance Futures public API (no API key required).

    Returns list of dicts with keys:
        open_time, open, high, low, close, volume, close_time,
        quote_volume, trades, taker_buy_volume, taker_sell_volume
    """
    import urllib.request
    import json

    url = (
        f"https://fapi.binance.com/fapi/v1/klines"
        f"?symbol={symbol}&interval={interval}&limit={limit}"
    )
    with urllib.request.urlopen(url, timeout=10) as resp:
        raw = json.loads(resp.read().decode())

    result: list[dict[str, Any]] = []
    for row in raw:
        total_vol = float(row[5])
        taker_buy_vol = float(row[9])
        result.append(
            {
                "open_time": int(row[0]),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": total_vol,
                "close_time": int(row[6]),
                "quote_volume": float(row[7]),
                "trades": int(row[8]),
                "taker_buy_volume": taker_buy_vol,
                "taker_sell_volume": total_vol - taker_buy_vol,
            }
        )
    return result


def fetch_open_interest(
    symbol: str = "BTCUSDT",
    period: str = "5m",
    limit: int = 1000,
) -> list[dict[str, Any]]:
    """
    Fetch historical open interest statistics from Binance Futures public API.

    Returns list of dicts with keys:
        timestamp, oi, oi_value
    """
    import urllib.request
    import json

    url = (
        f"https://fapi.binance.com/futures/data/openInterestHist"
        f"?symbol={symbol}&period={period}&limit={limit}"
    )
    with urllib.request.urlopen(url, timeout=10) as resp:
        raw = json.loads(resp.read().decode())

    result: list[dict[str, Any]] = []
    for row in raw:
        result.append(
            {
                "timestamp": int(row["timestamp"]),
                "oi": float(row["sumOpenInterest"]),
                "oi_value": float(row["sumOpenInterestValue"]),
            }
        )
    return result


def fetch_agg_trades(
    symbol: str = "BTCUSDT",
    limit: int = 1000,
) -> list[dict[str, Any]]:
    """
    Fetch recent aggregate trades from Binance Futures public API.

    Returns list of dicts with keys:
        agg_trade_id, price, quantity, first_trade_id,
        last_trade_id, timestamp, is_buyer_maker
    """
    import urllib.request
    import json

    url = (
        f"https://fapi.binance.com/fapi/v1/aggTrades"
        f"?symbol={symbol}&limit={limit}"
    )
    with urllib.request.urlopen(url, timeout=10) as resp:
        raw = json.loads(resp.read().decode())

    result: list[dict[str, Any]] = []
    for row in raw:
        result.append(
            {
                "agg_trade_id": int(row["a"]),
                "price": float(row["p"]),
                "quantity": float(row["q"]),
                "first_trade_id": int(row["f"]),
                "last_trade_id": int(row["l"]),
                "timestamp": int(row["T"]),
                "is_buyer_maker": bool(row["m"]),
            }
        )
    return result


# ── Alignment Utility ──


def align_klines_and_oi(
    klines: Sequence[dict[str, Any]],
    oi_hist: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Align klines and open interest history by timestamp."""
    aligned: list[dict[str, Any]] = []
    # Map timestamp -> OI
    oi_map = {row["timestamp"]: row for row in oi_hist}

    for k in klines:
        ts = k["open_time"]
        oi_row = oi_map.get(ts)
        if not oi_row:
            # Fallback: find closest within 5 minutes
            closest_oi = None
            min_diff = float("inf")
            for o_row in oi_hist:
                diff = abs(o_row["timestamp"] - ts)
                if diff < min_diff:
                    min_diff = diff
                    closest_oi = o_row
            if min_diff < 5 * 60 * 1000:
                oi_row = closest_oi

        if oi_row:
            aligned.append({
                **k,
                "oi": oi_row["oi"],
                "oi_value": oi_row["oi_value"]
            })
    return aligned


# ── Strategy Engine ──


class MicrostructureAlphaStrategy:
    """
    Stateless signal generator combining:
    - Normalized trade imbalance (buy/sell taker flow)
    - VPIN (informed trading probability)
    - Hurst exponent (regime detection: trending vs mean-reverting)
    - Spread and volatility filters
    - Open Interest & Price Box range-bound accumulation/distribution models
    - Quantum Density Matrix state (Wave/Box vs Particle/Impulse)
    """

    def __init__(self, config: StrategyConfig | None = None) -> None:
        self.config = config or StrategyConfig()

    @staticmethod
    def load_config_from_yaml(path: str) -> StrategyConfig:
        """Load strategy config from a YAML file."""
        import yaml  # type: ignore[import-untyped]

        with open(path, "r") as f:
            data = yaml.safe_load(f)

        return StrategyConfig(
            symbol=data.get("symbol", "BTCUSDT"),
            market_type=data.get("market_type", "perp"),
            trade_window_ms=data.get("features", {}).get("trade_window_ms", 1_000),
            orderbook_depth_levels=data.get("features", {}).get("orderbook_depth_levels", 5),
            forward_return_horizons_ms=tuple(
                data.get("features", {}).get("forward_return_horizons_ms", [1_000, 5_000, 30_000])
            ),
            imbalance_entry_threshold=data.get("signal", {}).get("imbalance_entry_threshold", 0.35),
            max_spread_bps=data.get("signal", {}).get("max_spread_bps", 3.0),
            max_realized_volatility_bps=data.get("signal", {}).get("max_realized_volatility_bps", 20.0),
            max_position_notional=data.get("risk", {}).get("max_position_notional", 2_000.0),
        )

    def generate_signals_from_klines(
        self,
        klines: Sequence[dict[str, Any]],
    ) -> list[StrategySignal]:
        """
        Fallback generator using pure klines when OI data is not present.
        """
        if len(klines) < self.config.hurst_window + 10:
            return []

        prices = [k["close"] for k in klines]
        buy_vols = [k["taker_buy_volume"] for k in klines]
        sell_vols = [k["taker_sell_volume"] for k in klines]
        timestamps = [k["open_time"] for k in klines]

        trade_buckets = [
            TradeBucket(buy_taker_qty=bv, sell_taker_qty=sv)
            for bv, sv in zip(buy_vols, sell_vols)
        ]
        imbalances = [normalized_trade_imbalance(tb) for tb in trade_buckets]
        vpin_values = vpin(buy_vols, sell_vols, window=min(self.config.vpin_window, len(klines) // 2))
        vol_values = rolling_realized_volatility(prices, window=20)

        regime_labels: list[str] = []
        for i in range(len(prices)):
            if i < self.config.hurst_window:
                regime_labels.append("neutral")
                continue
            h = hurst_exponent(prices[i - self.config.hurst_window: i + 1], max_lag=20)
            if h > self.config.hurst_momentum_threshold:
                regime_labels.append("momentum")
            elif h < self.config.hurst_reversion_threshold:
                regime_labels.append("mean_reversion")
            else:
                regime_labels.append("neutral")

        signals: list[StrategySignal] = []
        for i in range(len(klines)):
            imb = imbalances[i]
            vpin_val = vpin_values[i]
            vol_val = vol_values[i]
            regime = regime_labels[i]

            if vol_val is not None and vol_val * 10_000 > self.config.max_realized_volatility_bps:
                signals.append(StrategySignal(
                    timestamp_ms=timestamps[i],
                    direction=0,
                    confidence=0.0,
                    regime=regime,
                    features={"imbalance": imb, "vpin": vpin_val or 0.0, "vol": vol_val},
                ))
                continue

            threshold = self.config.imbalance_entry_threshold
            if regime == "momentum":
                threshold *= 0.8
            elif regime == "mean_reversion":
                threshold *= 1.2

            vpin_boost = 1.0
            if vpin_val is not None and vpin_val > self.config.vpin_alert_threshold:
                vpin_boost = 1.3

            effective_imbalance = imb * vpin_boost

            direction = signal_from_imbalance(
                effective_imbalance,
                entry_threshold=threshold,
            )

            confidence = min(abs(effective_imbalance) / threshold, 1.0) if threshold > 0 else 0.0

            signals.append(StrategySignal(
                timestamp_ms=timestamps[i],
                direction=direction,
                confidence=confidence,
                regime=regime,
                features={
                    "imbalance": round(imb, 6),
                    "vpin": round(vpin_val, 6) if vpin_val is not None else 0.0,
                    "vol": round(vol_val, 6) if vol_val is not None else 0.0,
                    "hurst_regime": regime,
                    "effective_imbalance": round(effective_imbalance, 6),
                    "threshold": round(threshold, 6),
                },
            ))

        return signals

    def generate_signals_with_oi_box(
        self,
        klines: Sequence[dict[str, Any]],
        oi_hist: Sequence[dict[str, Any]],
    ) -> list[StrategySignal]:
        """
        Advanced signal generator incorporating Price & OI Box Ranges,
        Taker Trade Density, and Quantum Density Matrix states.
        """
        aligned = align_klines_and_oi(klines, oi_hist)
        min_required = max(self.config.oi_window, self.config.price_window, self.config.buyer_density_window)
        if len(aligned) < min_required + 10:
            return []

        prices = [k["close"] for k in aligned]
        oi_values = [k["oi"] for k in aligned]
        timestamps = [k["open_time"] for k in aligned]

        # 1. Box Channel calculations
        price_bounds = rolling_min_max_channel(prices, self.config.price_window, self.config.price_noise_percent)
        oi_bounds = rolling_min_max_channel(oi_values, self.config.oi_window, self.config.oi_noise_percent)

        # 2. Volatility & Imbalances for Quantum Matrix
        vol_values = rolling_period_volatility_bps(prices, window=20)
        ob_imbalances = [
            normalized_trade_imbalance(
                TradeBucket(
                    buy_taker_qty=k["taker_buy_volume"],
                    sell_taker_qty=k["taker_sell_volume"],
                )
            )
            for k in aligned
        ]
        vpin_values = vpin(
            [k["taker_buy_volume"] for k in aligned],
            [k["taker_sell_volume"] for k in aligned],
            window=self.config.vpin_window,
        )

        quantum_states = market_quantum_density_matrix(vol_values, ob_imbalances, self.config.volatility_threshold_bps)

        # 3. Buyer-initiated trade density
        # Estimated from kline taker volumes: buy density = taker_buy / total
        buyer_densities = [
            k["taker_buy_volume"] / k["volume"] if k["volume"] > 0 else 0.5
            for k in aligned
        ]

        signals: list[StrategySignal] = []

        for i in range(len(aligned)):
            imb = ob_imbalances[i]
            vol_val = vol_values[i]
            q_state = quantum_states[i]
            p_bound = price_bounds[i]
            o_bound = oi_bounds[i]
            vpin_val = vpin_values[i]
            density = buyer_densities[i]

            if q_state is None or p_bound is None or o_bound is None or vol_val is None:
                # Fallback to pure imbalance or 0 during setup window
                signals.append(StrategySignal(
                    timestamp_ms=timestamps[i],
                    direction=0,
                    confidence=0.0,
                    regime="setup",
                    features={"imbalance": imb},
                ))
                continue

            p_box, p_impulse, coherence = q_state
            price_min, price_max = p_bound
            oi_min, oi_max = o_bound

            # Calculate relative positions in channels (0.0 to 1.0)
            price_range = price_max - price_min
            price_pos = (prices[i] - price_min) / price_range if price_range > 0 else 0.5

            oi_range = oi_max - oi_min
            oi_pos = (oi_values[i] - oi_min) / oi_range if oi_range > 0 else 0.5

            direction = 0
            regime = "neutral"
            confidence = 0.0

            # Volatility filter (vol_val is already in basis points)
            if vol_val > self.config.max_realized_volatility_bps:
                direction = 0
                regime = "high_vol_filter"
            
            # Scenario A: Range-bound / Box Regime (Wave State)
            elif p_box > 0.5:
                regime = "quantum_box"
                # Accumulation near box bottom
                if price_pos < 0.20 and oi_pos < 0.20 and density > 0.52 and imb > 0.1:
                    direction = 1
                    confidence = p_box * density
                # Distribution near box top
                elif price_pos > 0.80 and oi_pos > 0.80 and density < 0.48 and imb < -0.1:
                    direction = -1
                    confidence = p_box * (1.0 - density)

            # Scenario B: Impulse / Breakout Regime (Particle State)
            elif p_impulse > 0.5:
                regime = "quantum_impulse"
                # Price & OI breaking upper bounds with positive flow
                if prices[i] >= price_max and oi_values[i] >= oi_max and imb > 0.2:
                    direction = 1
                    confidence = p_impulse * imb
                # Price breaking lower, OI rising (short build) or falling (long squeeze)
                elif prices[i] <= price_min and imb < -0.2:
                    direction = -1
                    confidence = p_impulse * abs(imb)

            # Scenario C: Default fall-through to simple microstructure logic
            if direction == 0 and regime not in {"high_vol_filter"}:
                vpin_boost = 1.3 if (vpin_val is not None and vpin_val > self.config.vpin_alert_threshold) else 1.0
                effective_imb = imb * vpin_boost
                direction = signal_from_imbalance(effective_imb, entry_threshold=self.config.imbalance_entry_threshold)
                confidence = min(abs(effective_imb) / self.config.imbalance_entry_threshold, 1.0) if self.config.imbalance_entry_threshold > 0 else 0.0

            signals.append(StrategySignal(
                timestamp_ms=timestamps[i],
                direction=direction,
                confidence=confidence,
                regime=regime,
                features={
                    "price_pos": round(price_pos, 4),
                    "oi_pos": round(oi_pos, 4),
                    "buyer_density": round(density, 4),
                    "p_box": round(p_box, 4),
                    "p_impulse": round(p_impulse, 4),
                    "coherence": round(coherence, 4),
                    "imbalance": round(imb, 4),
                    "vpin": round(vpin_val, 4) if vpin_val is not None else 0.0,
                },
            ))

        return signals

    def run_live_snapshot(self) -> list[StrategySignal]:
        """
        Fetch live data and Open Interest, and generate current signals.
        """
        # Fetch 5m klines and 5m OI to ensure alignment
        klines = fetch_klines(
            symbol=self.config.symbol,
            interval="5m",
            limit=max(200, self.config.oi_window + 50),
        )
        oi_hist = fetch_open_interest(
            symbol=self.config.symbol,
            period="5m",
            limit=max(200, self.config.oi_window + 50),
        )
        return self.generate_signals_with_oi_box(klines, oi_hist)
