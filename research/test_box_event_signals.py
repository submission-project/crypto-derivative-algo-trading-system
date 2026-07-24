import numpy as np
import pandas as pd

from research.microstructure_alpha.box_event_signals import (
    detect_box_event_signals,
    detect_box_terminal_oi_volatility,
    detect_deadcat_bounce_ranges,
    detect_transient_oi_shocks,
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


def test_detect_box_terminal_oi_volatility_flags_box_end_expansion() -> None:
    timestamps = pd.date_range("2026-01-01", periods=220, freq="5min")
    values = np.full(220, 1000.0)
    values[:170] += np.sin(np.arange(170)) * 2.0
    values[170:200] = np.linspace(1000, 1120, 30)
    frame = pd.DataFrame({"timestamp": timestamps, "oi_total": values})
    boxes = [_box(0, timestamps[0], timestamps[199], 990.0, 1010.0)]

    result = detect_box_terminal_oi_volatility(
        frame,
        boxes,
        lookback_bars=80,
        terminal_bars=24,
        min_range_ratio=1.5,
        min_net_change_z=3.0,
    )

    assert not result.empty
    assert result["is_terminal_oi_expansion"].iloc[0]
    assert result["direction"].iloc[0] == "up"


def test_detect_transient_oi_shocks_marks_fast_reversion() -> None:
    timestamps = pd.date_range("2026-01-01", periods=260, freq="5min")
    values = np.full(260, 1000.0)
    values += np.sin(np.arange(260)) * 2.0
    values[150:154] = 1160.0
    values[154:180] = 1005.0
    frame = pd.DataFrame({"timestamp": timestamps, "oi_total": values})

    result = detect_transient_oi_shocks(
        frame,
        baseline_bars=80,
        shock_window_bars=6,
        reversion_bars=40,
        shock_z=8.0,
        min_change_ratio=0.05,
        max_reversion_fraction=0.10,
    )

    assert not result.empty
    assert result["is_transient"].iloc[0]
    assert result["direction"].iloc[0] == "up"


def test_detect_deadcat_bounce_ranges_uses_anchor_and_box_levels() -> None:
    timestamps = pd.date_range("2026-01-01", periods=360, freq="5min")
    close = np.full(360, 100.0)
    close[150:157] = np.linspace(100.0, 90.0, 7)
    close[157:220] = np.linspace(90.0, 96.0, 63)
    close[220:] = 94.0
    high = close + 0.5
    low = close - 0.5
    frame = pd.DataFrame({"timestamp": timestamps, "close": close, "high": high, "low": low})
    price_boxes = [
        _box(1, timestamps[157], timestamps[240], 89.5, 96.5),
        _box(2, timestamps[241], timestamps[320], 87.0, 91.0),
    ]

    result = detect_deadcat_bounce_ranges(
        frame,
        anchor_times=[timestamps[150]],
        price_boxes=price_boxes,
        lookback_bars=100,
        trough_search_bars=20,
        rebound_bars=100,
        min_rebound_bps=100.0,
    )

    assert not result.empty
    assert result["is_deadcat_range"].iloc[0]
    assert result["deadcat_upper"].iloc[0] == 96.5
    assert result["deadcat_lower"].iloc[0] == 91.0


def test_detect_box_event_signals_returns_all_signal_tables() -> None:
    timestamps = pd.date_range("2026-01-01", periods=360, freq="5min")
    oi_values = np.full(360, 1000.0)
    oi_values[150:154] = 1160.0
    oi_values[154:220] = 1005.0
    oi_values[250:288] = np.linspace(1000.0, 1120.0, 38)
    oi_frame = pd.DataFrame({"timestamp": timestamps, "oi_total": oi_values})
    close = np.full(360, 100.0)
    close[150:157] = np.linspace(100.0, 90.0, 7)
    close[157:220] = np.linspace(90.0, 96.0, 63)
    price_frame = pd.DataFrame(
        {"timestamp": timestamps, "close": close, "high": close + 0.5, "low": close - 0.5}
    )
    oi_boxes = [_box(0, timestamps[0], timestamps[287], 990.0, 1010.0)]

    result = detect_box_event_signals(
        oi_frame=oi_frame,
        price_frame=price_frame,
        oi_boxes=oi_boxes,
        drop_anchor_times=[timestamps[150]],
    )

    assert {"terminal_oi_volatility", "transient_oi_shocks", "deadcat_bounce_ranges"} == set(result)
