import numpy as np
import pandas as pd

from research.microstructure_alpha.box_strategy_backtest import (
    BoxStrategyConfig,
    build_box_strategy_frame,
    candidate_box_strategy_configs,
    optimize_box_strategy,
    run_box_strategy_backtest,
)
from research.microstructure_alpha.oi_box import OIBox


def _box(box_id: int, start: pd.Timestamp, end: pd.Timestamp, low: float, high: float) -> OIBox:
    return OIBox(
        box_id=box_id,
        start=start,
        end=end,
        low=low,
        high=high,
        mid=(low + high) / 2.0,
        width=high - low,
        bars=1,
        coverage=1.0,
        low_touches=1,
        high_touches=1,
        break_direction="end",
        score=1.0,
    )


def _range_price_frame(cycles: int = 20) -> pd.DataFrame:
    pattern = [100.0, 92.0, 94.0, 100.0, 108.0, 106.0, 100.0]
    close = np.array(pattern * cycles, dtype=float)
    timestamps = pd.date_range("2026-01-01", periods=len(close), freq="5min")
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "close": close,
            "high": close + 0.4,
            "low": close - 0.4,
        }
    )


def test_box_strategy_range_reversion_can_pass_profit_constraints() -> None:
    price = _range_price_frame(cycles=18)
    price_boxes = [_box(0, price["timestamp"].iloc[0], price["timestamp"].iloc[-1], 90.0, 110.0)]
    frame = build_box_strategy_frame(
        price_frame=price,
        price_boxes=price_boxes,
        config=BoxStrategyConfig(
            entry_edge_ratio=0.25,
            bounce_bars=1,
            bounce_confirm_bps=0.0,
            range_stop_buffer_bps=80.0,
            fee_bps=0.0,
            slippage_bps=0.0,
            spread_bps=0.0,
        ),
    )

    result = run_box_strategy_backtest(
        frame,
        config=BoxStrategyConfig(
            entry_edge_ratio=0.25,
            bounce_bars=1,
            bounce_confirm_bps=0.0,
            range_stop_buffer_bps=80.0,
            fee_bps=0.0,
            slippage_bps=0.0,
            spread_bps=0.0,
            risk_per_trade=0.003,
        ),
    )

    assert result.metrics["total_trades"] >= 5
    assert result.metrics["win_rate"] > 0.50
    assert result.metrics["total_return"] > 0.0
    assert result.passes_constraints


def test_box_strategy_stop_loss_limits_wrong_entry() -> None:
    close = np.r_[np.full(20, 100.0), np.linspace(92.0, 80.0, 20)]
    timestamps = pd.date_range("2026-01-01", periods=len(close), freq="5min")
    price = pd.DataFrame({"timestamp": timestamps, "close": close, "high": close + 0.2, "low": close - 0.2})
    price_boxes = [_box(0, timestamps[0], timestamps[-1], 90.0, 110.0)]
    config = BoxStrategyConfig(
        entry_edge_ratio=0.30,
        bounce_confirm_bps=-10_000.0,
        range_stop_buffer_bps=60.0,
        fee_bps=0.0,
        slippage_bps=0.0,
        spread_bps=0.0,
        risk_per_trade=0.003,
        max_leverage=1.0,
    )
    frame = build_box_strategy_frame(price_frame=price, price_boxes=price_boxes, config=config)

    result = run_box_strategy_backtest(frame, config=config)

    assert not result.trades.empty
    assert (result.trades["exit_reason"] == "stop_loss").any()
    assert result.metrics["max_drawdown"] > -0.02


def test_box_strategy_rejects_invalid_short_risk_geometry() -> None:
    timestamps = pd.date_range("2026-01-01", periods=20, freq="5min")
    close = np.full(len(timestamps), 105.0)
    price = pd.DataFrame({"timestamp": timestamps, "close": close, "high": close + 0.2, "low": close - 0.2})
    price_boxes = [_box(0, timestamps[0], timestamps[-1], 90.0, 100.0)]
    config = BoxStrategyConfig(
        entry_edge_ratio=0.30,
        bounce_confirm_bps=0.0,
        range_stop_buffer_bps=60.0,
        fee_bps=0.0,
        slippage_bps=0.0,
        spread_bps=0.0,
    )
    frame = build_box_strategy_frame(price_frame=price, price_boxes=price_boxes, config=config)

    result = run_box_strategy_backtest(frame, config=config)

    assert result.trades.empty


def test_box_strategy_can_require_oi_box_for_range_entries() -> None:
    price = _range_price_frame(cycles=4)
    price_boxes = [_box(0, price["timestamp"].iloc[0], price["timestamp"].iloc[-1], 90.0, 110.0)]
    config = BoxStrategyConfig(
        entry_edge_ratio=0.25,
        bounce_bars=1,
        bounce_confirm_bps=0.0,
        require_oi_box_for_range=True,
        fee_bps=0.0,
        slippage_bps=0.0,
        spread_bps=0.0,
    )
    frame = build_box_strategy_frame(price_frame=price, price_boxes=price_boxes, config=config)

    result = run_box_strategy_backtest(frame, config=config)

    assert result.trades.empty


def test_box_strategy_normalizes_datetime_units_before_merge() -> None:
    timestamps_ns = pd.date_range("2026-01-01", periods=10, freq="5min")
    timestamps_us = pd.Series(timestamps_ns.to_numpy(dtype="datetime64[us]"))
    price = pd.DataFrame(
        {
            "timestamp": timestamps_us,
            "close": np.linspace(100.0, 101.0, len(timestamps_us)),
            "high": np.linspace(100.2, 101.2, len(timestamps_us)),
            "low": np.linspace(99.8, 100.8, len(timestamps_us)),
        }
    )
    oi = pd.DataFrame(
        {
            "timestamp": pd.Series(timestamps_ns.to_numpy(dtype="datetime64[ns]")),
            "oi_total": np.linspace(1_000.0, 1_010.0, len(timestamps_ns)),
        }
    )

    frame = build_box_strategy_frame(price_frame=price, oi_frame=oi, oi_col="oi_total")

    assert frame["timestamp"].dtype == "datetime64[ns]"
    assert frame["oi_total"].notna().all()


def test_box_strategy_trend_follows_persistent_oi_breakout() -> None:
    timestamps = pd.date_range("2026-01-01", periods=120, freq="5min")
    close = np.r_[np.full(30, 100.0), np.linspace(111.0, 125.0, 90)]
    price = pd.DataFrame({"timestamp": timestamps, "close": close, "high": close + 0.4, "low": close - 0.4})
    price_boxes = [_box(0, timestamps[0], timestamps[-1], 90.0, 110.0)]
    persistent = pd.DataFrame(
        {
            "event_time": [timestamps[30]],
            "direction": ["up"],
            "is_transient": [False],
            "reversion_time": [pd.NaT],
        }
    )
    config = BoxStrategyConfig(
        min_trend_momentum_bps=1.0,
        breakout_buffer_bps=5.0,
        trend_exit_bps=-10_000.0,
        max_holding_bars=80,
        fee_bps=0.0,
        slippage_bps=0.0,
        spread_bps=0.0,
    )
    frame = build_box_strategy_frame(
        price_frame=price,
        price_boxes=price_boxes,
        transient_oi_shocks=persistent,
        config=config,
    )

    result = run_box_strategy_backtest(frame, config=config)

    assert not result.trades.empty
    assert (result.trades["mode"] == "trend").any()
    assert result.trades["pnl"].sum() > 0.0


def test_optimize_box_strategy_selects_passing_config() -> None:
    price = _range_price_frame(cycles=15)
    price_boxes = [_box(0, price["timestamp"].iloc[0], price["timestamp"].iloc[-1], 90.0, 110.0)]
    base = BoxStrategyConfig(fee_bps=0.0, slippage_bps=0.0, spread_bps=0.0, bounce_bars=1)
    frame = build_box_strategy_frame(price_frame=price, price_boxes=price_boxes, config=base)
    candidates = candidate_box_strategy_configs(
        base,
        entry_edge_ratios=(0.12, 0.25),
        bounce_confirm_bps_values=(0.0,),
        trailing_stop_bps_values=(120.0,),
        range_stop_buffer_bps_values=(80.0,),
        risk_per_trade_values=(0.003,),
    )

    best, report = optimize_box_strategy(frame, candidate_configs=candidates, min_trades=3)

    assert not report.empty
    assert best.passes_constraints
    assert best.metrics["win_rate"] > 0.50
    assert best.metrics["total_return"] > 0.0
