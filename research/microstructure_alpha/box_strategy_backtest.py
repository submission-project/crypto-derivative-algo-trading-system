from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from itertools import product
from typing import Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from research.backtests.microstructure.metrics import compute_all_metrics
from research.microstructure_alpha.oi_box import OIBox


@dataclass(frozen=True, slots=True)
class BoxStrategyConfig:
    entry_edge_ratio: float = 0.18
    exit_mid_ratio: float = 0.50
    breakout_buffer_bps: float = 20.0
    momentum_bars: int = 12
    min_trend_momentum_bps: float = 12.0
    bounce_bars: int = 3
    bounce_confirm_bps: float = 2.0
    trend_activation_bars: int = 144
    trend_exit_bps: float = 4.0
    trailing_stop_bps: float = 180.0
    range_stop_buffer_bps: float = 80.0
    max_holding_bars: int = 288
    min_stop_bps: float = 60.0
    max_stop_bps: float = 320.0
    risk_per_trade: float = 0.004
    max_leverage: float = 1.0
    max_notional: float = 0.0
    require_oi_box_for_range: bool = False
    max_range_momentum_bps: float = 0.0
    fee_bps: float = 4.0
    slippage_bps: float = 2.0
    spread_bps: float = 1.0
    allow_short: bool = True


@dataclass(frozen=True, slots=True)
class BoxStrategyTrade:
    entry_time: pd.Timestamp
    exit_time: pd.Timestamp
    direction: int
    mode: str
    entry_reason: str
    exit_reason: str
    entry_price: float
    exit_price: float
    stop_loss: float
    take_profit: float
    notional: float
    gross_pnl: float
    cost: float
    pnl: float
    bars_held: int

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class BoxStrategyBacktestResult:
    signals: pd.DataFrame
    trades: pd.DataFrame
    equity_curve: pd.DataFrame
    metrics: dict[str, float]
    config: BoxStrategyConfig
    passes_constraints: bool = False


def _to_datetime_ns(values: pd.Series) -> pd.Series:
    return pd.Series(
        pd.to_datetime(values).to_numpy(dtype="datetime64[ns]"),
        index=values.index,
        name=values.name,
    )


def _prepare_price_frame(
    price_frame: pd.DataFrame,
    *,
    timestamp_col: str,
    price_col: str,
    high_col: str,
    low_col: str,
) -> pd.DataFrame:
    missing = [col for col in [timestamp_col, price_col, high_col, low_col] if col not in price_frame.columns]
    if missing:
        raise ValueError(f"missing price columns: {missing}")
    data = price_frame[[timestamp_col, price_col, high_col, low_col]].copy()
    data[timestamp_col] = _to_datetime_ns(data[timestamp_col])
    for col in [price_col, high_col, low_col]:
        data[col] = pd.to_numeric(data[col], errors="coerce")
    data = data.dropna().sort_values(timestamp_col).drop_duplicates(timestamp_col)
    return data.reset_index(drop=True)


def _overlay_boxes(
    data: pd.DataFrame,
    boxes: Sequence[OIBox] | None,
    *,
    timestamp_col: str,
    prefix: str,
) -> pd.DataFrame:
    out = data.copy()
    out[f"{prefix}_box_id"] = pd.NA
    out[f"{prefix}_box_low"] = np.nan
    out[f"{prefix}_box_high"] = np.nan
    out[f"{prefix}_box_mid"] = np.nan
    if not boxes:
        return out
    for box in boxes:
        mask = (out[timestamp_col] >= box.start) & (out[timestamp_col] <= box.end)
        out.loc[mask, f"{prefix}_box_id"] = box.box_id
        out.loc[mask, f"{prefix}_box_low"] = box.low
        out.loc[mask, f"{prefix}_box_high"] = box.high
        out.loc[mask, f"{prefix}_box_mid"] = box.mid
    return out


def _idx_at_or_after(timestamps: pd.Series, timestamp: pd.Timestamp) -> int:
    idx = int(np.searchsorted(timestamps.to_numpy(dtype="datetime64[ns]"), np.datetime64(timestamp), side="left"))
    return min(max(idx, 0), len(timestamps) - 1)


def build_box_strategy_frame(
    *,
    price_frame: pd.DataFrame,
    oi_frame: pd.DataFrame | None = None,
    price_boxes: Sequence[OIBox] | None = None,
    oi_boxes: Sequence[OIBox] | None = None,
    terminal_oi_volatility: pd.DataFrame | None = None,
    transient_oi_shocks: pd.DataFrame | None = None,
    deadcat_bounce_ranges: pd.DataFrame | None = None,
    timestamp_col: str = "timestamp",
    price_col: str = "close",
    high_col: str = "high",
    low_col: str = "low",
    oi_col: str = "oi_total",
    config: BoxStrategyConfig | None = None,
) -> pd.DataFrame:
    """
    Build a tradable signal frame from price/OI boxes and box-event signals.

    The frame intentionally keeps risk filters visible: terminal OI expansion,
    transient shocks, persistent shocks, active price/dead-cat range, and
    momentum all become explicit columns used by the simulator.
    """
    config = config or BoxStrategyConfig()
    data = _prepare_price_frame(
        price_frame,
        timestamp_col=timestamp_col,
        price_col=price_col,
        high_col=high_col,
        low_col=low_col,
    )
    if oi_frame is not None and not oi_frame.empty and oi_col in oi_frame.columns:
        oi = oi_frame[[timestamp_col, oi_col]].copy()
        oi[timestamp_col] = _to_datetime_ns(oi[timestamp_col])
        oi[oi_col] = pd.to_numeric(oi[oi_col], errors="coerce")
        oi = oi.dropna().sort_values(timestamp_col).drop_duplicates(timestamp_col)
        data = pd.merge_asof(data, oi, on=timestamp_col, direction="backward")

    data = _overlay_boxes(data, price_boxes, timestamp_col=timestamp_col, prefix="price")
    data = _overlay_boxes(data, oi_boxes, timestamp_col=timestamp_col, prefix="oi")

    data["deadcat_upper"] = np.nan
    data["deadcat_lower"] = np.nan
    data["deadcat_event_time"] = pd.NaT
    if deadcat_bounce_ranges is not None and not deadcat_bounce_ranges.empty:
        for row in deadcat_bounce_ranges.itertuples(index=False):
            if hasattr(row, "is_deadcat_range") and not row.is_deadcat_range:
                continue
            range_end = getattr(row, "range_end", None)
            if pd.isna(range_end):
                continue
            mask = (data[timestamp_col] >= row.event_time) & (data[timestamp_col] <= range_end)
            data.loc[mask, "deadcat_upper"] = float(row.deadcat_upper)
            data.loc[mask, "deadcat_lower"] = float(row.deadcat_lower)
            data.loc[mask, "deadcat_event_time"] = row.event_time

    data["active_range_low"] = data["deadcat_lower"].combine_first(data["price_box_low"])
    data["active_range_high"] = data["deadcat_upper"].combine_first(data["price_box_high"])
    data["active_range_mid"] = (data["active_range_low"] + data["active_range_high"]) / 2.0
    data["range_width"] = data["active_range_high"] - data["active_range_low"]
    data["range_position"] = (
        (data[price_col] - data["active_range_low"]) / data["range_width"].replace(0, np.nan)
    )

    data["terminal_oi_risk"] = False
    if terminal_oi_volatility is not None and not terminal_oi_volatility.empty:
        for row in terminal_oi_volatility.itertuples(index=False):
            if hasattr(row, "is_terminal_oi_expansion") and not row.is_terminal_oi_expansion:
                continue
            mask = (data[timestamp_col] >= row.terminal_start) & (data[timestamp_col] <= row.box_end)
            data.loc[mask, "terminal_oi_risk"] = True

    data["transient_oi_risk"] = False
    data["persistent_oi_direction"] = 0
    if transient_oi_shocks is not None and not transient_oi_shocks.empty:
        timestamps = data[timestamp_col].reset_index(drop=True)
        for row in transient_oi_shocks.itertuples(index=False):
            direction = 1 if row.direction == "up" else -1
            start_idx = _idx_at_or_after(timestamps, pd.Timestamp(row.event_time))
            if row.is_transient:
                end_time = row.reversion_time if pd.notna(row.reversion_time) else row.event_time
                mask = (data[timestamp_col] >= row.event_time) & (data[timestamp_col] <= end_time)
                data.loc[mask, "transient_oi_risk"] = True
            else:
                end_idx = min(start_idx + config.trend_activation_bars, len(data))
                data.loc[start_idx:end_idx, "persistent_oi_direction"] = direction

    log_price = np.log(pd.to_numeric(data[price_col], errors="coerce").replace(0, np.nan))
    data["momentum_bps"] = (log_price - log_price.shift(config.momentum_bars)).fillna(0.0) * 10_000.0
    data["bounce_bps"] = (log_price - log_price.shift(config.bounce_bars)).fillna(0.0) * 10_000.0
    data["risk_off"] = data["terminal_oi_risk"] | data["transient_oi_risk"]
    return data


def _execution_cost_bps(config: BoxStrategyConfig) -> float:
    return config.fee_bps + config.slippage_bps + config.spread_bps / 2.0


def _entry_notional(equity: float, stop_bps: float, config: BoxStrategyConfig) -> float:
    stop_bps = min(max(abs(stop_bps), config.min_stop_bps), config.max_stop_bps)
    risk_budget = equity * config.risk_per_trade
    notional = risk_budget / (stop_bps / 10_000.0)
    notional = min(notional, equity * config.max_leverage)
    if config.max_notional > 0:
        notional = min(notional, config.max_notional)
    return max(0.0, float(notional))


def _close_trade(
    *,
    trade: dict[str, object],
    exit_time: pd.Timestamp,
    exit_price: float,
    exit_reason: str,
    config: BoxStrategyConfig,
) -> tuple[BoxStrategyTrade, float]:
    direction = int(trade["direction"])
    entry_price = float(trade["entry_price"])
    notional = float(trade["notional"])
    entry_cost = float(trade["entry_cost"])
    exit_cost = notional * _execution_cost_bps(config) / 10_000.0
    gross = direction * notional * (exit_price / entry_price - 1.0)
    pnl = gross - entry_cost - exit_cost
    closed = BoxStrategyTrade(
        entry_time=pd.Timestamp(trade["entry_time"]),
        exit_time=exit_time,
        direction=direction,
        mode=str(trade["mode"]),
        entry_reason=str(trade["entry_reason"]),
        exit_reason=exit_reason,
        entry_price=entry_price,
        exit_price=float(exit_price),
        stop_loss=float(trade["stop_loss"]),
        take_profit=float(trade["take_profit"]),
        notional=notional,
        gross_pnl=float(gross),
        cost=float(entry_cost + exit_cost),
        pnl=float(pnl),
        bars_held=int(trade["bars_held"]),
    )
    equity_delta = gross - exit_cost
    return closed, equity_delta


def _mark_to_market(equity: float, trade: dict[str, object] | None, price: float) -> float:
    if trade is None:
        return equity
    direction = int(trade["direction"])
    notional = float(trade["notional"])
    entry_price = float(trade["entry_price"])
    return equity + direction * notional * (price / entry_price - 1.0)


def run_box_strategy_backtest(
    frame: pd.DataFrame,
    *,
    timestamp_col: str = "timestamp",
    price_col: str = "close",
    high_col: str = "high",
    low_col: str = "low",
    initial_equity: float = 10_000.0,
    config: BoxStrategyConfig | None = None,
    min_win_rate: float = 0.50,
    min_total_return: float = 0.0,
) -> BoxStrategyBacktestResult:
    """
    명시적인 리스크 관리를 적용한 박스형 전략 시뮬레이션.

    설계:
    - 안정 구간/데드캣 반등 구간 내에서만 범위 평균 회귀 전략을 적용하며, 리스크 오프 전략은 배제;
    - 지속적인 미결제 약정(OI) 충격과 가격 돌파가 발생한 후에만 추세 추종 전략을 적용;
    - 손절매, 추적 손절매, 최대 보유량, 변동성/리스크 필터는 항상
      새로운 진입보다 우선시됨.
    """
    config = config or BoxStrategyConfig()
    if initial_equity <= 0:
        raise ValueError("initial_equity must be positive")
    if not 0.0 <= config.entry_edge_ratio <= 0.5:
        raise ValueError("entry_edge_ratio must be in [0, 0.5]")

    data = frame.copy().sort_values(timestamp_col).reset_index(drop=True)
    equity = float(initial_equity)
    current: dict[str, object] | None = None
    trades: list[BoxStrategyTrade] = []
    equity_rows: list[dict[str, object]] = []
    signal_rows: list[dict[str, object]] = []

    for idx, row in data.iterrows():
        timestamp = pd.Timestamp(row[timestamp_col])
        price = float(row[price_col])
        high = float(row[high_col])
        low = float(row[low_col])
        range_low = row.get("active_range_low", np.nan)
        range_high = row.get("active_range_high", np.nan)
        range_mid = row.get("active_range_mid", np.nan)
        range_width = row.get("range_width", np.nan)
        range_position = row.get("range_position", np.nan)
        momentum_bps = float(row.get("momentum_bps", 0.0))
        bounce_bps = float(row.get("bounce_bps", 0.0))
        risk_off = bool(row.get("risk_off", False))
        persistent_direction = int(row.get("persistent_oi_direction", 0))
        has_oi_box = pd.notna(row.get("oi_box_id", pd.NA))
        range_allowed = not risk_off
        if config.require_oi_box_for_range and not has_oi_box:
            range_allowed = False
        if config.max_range_momentum_bps > 0 and abs(momentum_bps) > config.max_range_momentum_bps:
            range_allowed = False

        signal = 0
        signal_reason = ""

        if current is not None:
            current["bars_held"] = int(current["bars_held"]) + 1
            direction = int(current["direction"])
            mode = str(current["mode"])

            if direction > 0:
                current["peak_price"] = max(float(current["peak_price"]), high)
                trail = float(current["peak_price"]) * (1.0 - config.trailing_stop_bps / 10_000.0)
                current["stop_loss"] = max(float(current["stop_loss"]), trail if mode == "trend" else float(current["stop_loss"]))
            else:
                current["trough_price"] = min(float(current["trough_price"]), low)
                trail = float(current["trough_price"]) * (1.0 + config.trailing_stop_bps / 10_000.0)
                current["stop_loss"] = min(float(current["stop_loss"]), trail if mode == "trend" else float(current["stop_loss"]))

            exit_price: float | None = None
            exit_reason: str | None = None
            stop_loss = float(current["stop_loss"])
            take_profit = float(current["take_profit"])

            if direction > 0 and low <= stop_loss:
                exit_price = stop_loss
                exit_reason = "stop_loss"
            elif direction < 0 and high >= stop_loss:
                exit_price = stop_loss
                exit_reason = "stop_loss"
            elif mode == "range" and direction > 0 and high >= take_profit:
                exit_price = take_profit
                exit_reason = "range_take_profit"
            elif mode == "range" and direction < 0 and low <= take_profit:
                exit_price = take_profit
                exit_reason = "range_take_profit"
            elif mode == "trend" and direction * momentum_bps < config.trend_exit_bps:
                exit_price = price
                exit_reason = "trend_decay"
            elif risk_off and mode == "range":
                exit_price = price
                exit_reason = "risk_off_exit"
            elif int(current["bars_held"]) >= config.max_holding_bars:
                exit_price = price
                exit_reason = "max_holding"

            if exit_price is not None and exit_reason is not None:
                closed, equity_delta = _close_trade(
                    trade=current,
                    exit_time=timestamp,
                    exit_price=exit_price,
                    exit_reason=exit_reason,
                    config=config,
                )
                equity += equity_delta
                trades.append(closed)
                current = None

        if current is None and np.isfinite(range_low) and np.isfinite(range_high) and range_width > 0:
            long_edge = price <= float(range_low) + float(range_width) * config.entry_edge_ratio
            short_edge = price >= float(range_high) - float(range_width) * config.entry_edge_ratio
            breakout_up = price >= float(range_high) * (1.0 + config.breakout_buffer_bps / 10_000.0)
            breakout_down = price <= float(range_low) * (1.0 - config.breakout_buffer_bps / 10_000.0)

            if (
                persistent_direction > 0
                and breakout_up
                and momentum_bps >= config.min_trend_momentum_bps
            ):
                signal = 1
                signal_reason = "persistent_oi_breakout_up"
            elif (
                config.allow_short
                and persistent_direction < 0
                and breakout_down
                and momentum_bps <= -config.min_trend_momentum_bps
            ):
                signal = -1
                signal_reason = "persistent_oi_breakout_down"
            elif range_allowed and long_edge and bounce_bps >= config.bounce_confirm_bps:
                signal = 1
                signal_reason = "range_lower_edge_bounce"
            elif (
                config.allow_short
                and range_allowed
                and short_edge
                and bounce_bps <= -config.bounce_confirm_bps
            ):
                signal = -1
                signal_reason = "range_upper_edge_reject"

            if signal != 0:
                mode = "trend" if signal_reason.startswith("persistent") else "range"
                if signal > 0:
                    stop_loss = price * (1.0 - config.trailing_stop_bps / 10_000.0) if mode == "trend" else float(range_low) * (1.0 - config.range_stop_buffer_bps / 10_000.0)
                    take_profit = price * (1.0 + config.trailing_stop_bps / 10_000.0 * 1.5) if mode == "trend" else float(range_low) + float(range_width) * config.exit_mid_ratio
                    stop_bps = (price / max(stop_loss, 1e-12) - 1.0) * 10_000.0
                else:
                    stop_loss = price * (1.0 + config.trailing_stop_bps / 10_000.0) if mode == "trend" else float(range_high) * (1.0 + config.range_stop_buffer_bps / 10_000.0)
                    take_profit = price * (1.0 - config.trailing_stop_bps / 10_000.0 * 1.5) if mode == "trend" else float(range_high) - float(range_width) * (1.0 - config.exit_mid_ratio)
                    stop_bps = (stop_loss / max(price, 1e-12) - 1.0) * 10_000.0

                valid_geometry = (
                    stop_loss < price < take_profit
                    if signal > 0
                    else take_profit < price < stop_loss
                )
                notional = _entry_notional(equity, stop_bps, config) if valid_geometry else 0.0
                if notional > 0:
                    entry_cost = notional * _execution_cost_bps(config) / 10_000.0
                    equity -= entry_cost
                    current = {
                        "entry_time": timestamp,
                        "direction": signal,
                        "mode": mode,
                        "entry_reason": signal_reason,
                        "entry_price": price,
                        "stop_loss": float(stop_loss),
                        "take_profit": float(take_profit),
                        "notional": float(notional),
                        "entry_cost": float(entry_cost),
                        "bars_held": 0,
                        "peak_price": high,
                        "trough_price": low,
                    }

        signal_rows.append(
            {
                "timestamp": timestamp,
                "price": price,
                "signal": signal,
                "signal_reason": signal_reason,
                "position": int(current["direction"]) if current is not None else 0,
                "range_low": range_low,
                "range_high": range_high,
                "range_position": range_position,
                "risk_off": risk_off,
                "persistent_oi_direction": persistent_direction,
                "momentum_bps": momentum_bps,
                "bounce_bps": bounce_bps,
            }
        )
        equity_rows.append(
            {
                "timestamp": timestamp,
                "equity": _mark_to_market(equity, current, price),
                "cash_equity": equity,
                "position": int(current["direction"]) if current is not None else 0,
            }
        )

    if current is not None and not data.empty:
        last = data.iloc[-1]
        closed, equity_delta = _close_trade(
            trade=current,
            exit_time=pd.Timestamp(last[timestamp_col]),
            exit_price=float(last[price_col]),
            exit_reason="final_flatten",
            config=config,
        )
        equity += equity_delta
        trades.append(closed)
        equity_rows.append(
            {
                "timestamp": pd.Timestamp(last[timestamp_col]),
                "equity": equity,
                "cash_equity": equity,
                "position": 0,
            }
        )

    trades_df = pd.DataFrame([trade.to_dict() for trade in trades])
    equity_df = pd.DataFrame(equity_rows)
    signal_df = pd.DataFrame(signal_rows)
    pnl_values = trades_df["pnl"].tolist() if not trades_df.empty else []
    metrics = compute_all_metrics(equity_df["equity"].tolist(), pnl_values, periods_per_year=365.0 * 24.0 * 12.0)
    passes = (
        metrics.get("total_trades", 0.0) > 0
        and metrics.get("win_rate", 0.0) > min_win_rate
        and metrics.get("total_return", 0.0) >= min_total_return
    )
    return BoxStrategyBacktestResult(
        signals=signal_df,
        trades=trades_df,
        equity_curve=equity_df,
        metrics=metrics,
        config=config,
        passes_constraints=bool(passes),
    )


def candidate_box_strategy_configs(
    base: BoxStrategyConfig | None = None,
    *,
    entry_edge_ratios: Iterable[float] = (0.12, 0.18, 0.24),
    bounce_confirm_bps_values: Iterable[float] = (0.0, 2.0, 5.0),
    trailing_stop_bps_values: Iterable[float] = (120.0, 180.0, 240.0),
    range_stop_buffer_bps_values: Iterable[float] = (60.0, 90.0, 120.0),
    risk_per_trade_values: Iterable[float] = (0.0025, 0.004, 0.006),
) -> list[BoxStrategyConfig]:
    base = base or BoxStrategyConfig()
    configs: list[BoxStrategyConfig] = []
    for edge, bounce, trailing, buffer, risk in product(
        entry_edge_ratios,
        bounce_confirm_bps_values,
        trailing_stop_bps_values,
        range_stop_buffer_bps_values,
        risk_per_trade_values,
    ):
        configs.append(
            replace(
                base,
                entry_edge_ratio=edge,
                bounce_confirm_bps=bounce,
                trailing_stop_bps=trailing,
                range_stop_buffer_bps=buffer,
                risk_per_trade=risk,
            )
        )
    return configs


def optimize_box_strategy(
    frame: pd.DataFrame,
    *,
    candidate_configs: Sequence[BoxStrategyConfig] | None = None,
    timestamp_col: str = "timestamp",
    price_col: str = "close",
    high_col: str = "high",
    low_col: str = "low",
    initial_equity: float = 10_000.0,
    min_win_rate: float = 0.50,
    min_total_return: float = 0.0,
    min_trades: int = 3,
) -> tuple[BoxStrategyBacktestResult, pd.DataFrame]:
    """
    그리드 검색 전략의 매개변수 및 다음 조건을 충족하는 구성을 우선적으로 선택: 
    win_rate > `min_win_rate`, total_return >= `min_total_return`, trades >= `min_trades`.

     해당 조건을 충족하는 구성이 없는 경우에도, 관찰된 최상의 구성이
    `passes_constraints=False`와 함께 반환
    """
    candidate_configs = list(candidate_configs or candidate_box_strategy_configs())
    if not candidate_configs:
        raise ValueError("candidate_configs must not be empty")

    rows: list[dict[str, object]] = []
    results: list[BoxStrategyBacktestResult] = []
    for idx, config in enumerate(candidate_configs):
        result = run_box_strategy_backtest(
            frame,
            timestamp_col=timestamp_col,
            price_col=price_col,
            high_col=high_col,
            low_col=low_col,
            initial_equity=initial_equity,
            config=config,
            min_win_rate=min_win_rate,
            min_total_return=min_total_return,
        )
        metrics = result.metrics
        passes = (
            metrics.get("total_trades", 0.0) >= min_trades
            and metrics.get("win_rate", 0.0) > min_win_rate
            and metrics.get("total_return", 0.0) >= min_total_return
        )
        result = BoxStrategyBacktestResult(
            signals=result.signals,
            trades=result.trades,
            equity_curve=result.equity_curve,
            metrics=result.metrics,
            config=result.config,
            passes_constraints=bool(passes),
        )
        results.append(result)
        rows.append(
            {
                "config_id": idx,
                "passes_constraints": passes,
                **asdict(config),
                **metrics,
            }
        )

    report = pd.DataFrame(rows)
    report = report.sort_values(
        ["passes_constraints", "total_return", "profit_factor", "win_rate", "max_drawdown"],
        ascending=[False, False, False, False, False],
    ).reset_index(drop=True)
    best_config_id = int(report.loc[0, "config_id"])
    return results[best_config_id], report
