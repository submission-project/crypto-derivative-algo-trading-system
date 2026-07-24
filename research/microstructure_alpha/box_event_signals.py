from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np
import pandas as pd

from research.microstructure_alpha.oi_box import OIBox


@dataclass(frozen=True, slots=True)
class BoxEventSignalConfig:
    terminal_lookback_bars: int = 144
    terminal_bars: int = 36
    terminal_range_ratio: float = 1.8
    terminal_net_change_z: float = 3.0
    shock_baseline_bars: int = 288
    shock_window_bars: int = 6
    shock_reversion_bars: int = 72
    shock_z: float = 5.0
    shock_min_change_ratio: float = 0.008
    shock_max_reversion_fraction: float = 0.35
    drop_lookback_bars: int = 288
    trough_search_bars: int = 72
    rebound_bars: int = 864
    drop_threshold_bps: float = 350.0
    min_rebound_bps: float = 100.0


def _prepare_frame(
    frame: pd.DataFrame,
    *,
    timestamp_col: str,
    numeric_cols: Sequence[str],
) -> pd.DataFrame:
    missing = [col for col in [timestamp_col, *numeric_cols] if col not in frame.columns]
    if missing:
        raise ValueError(f"missing columns: {missing}")

    data = frame[[timestamp_col, *numeric_cols]].copy()
    data[timestamp_col] = pd.to_datetime(data[timestamp_col])
    for col in numeric_cols:
        data[col] = pd.to_numeric(data[col], errors="coerce")
    data = data.dropna(subset=[timestamp_col, *numeric_cols])
    data = data.sort_values(timestamp_col).drop_duplicates(timestamp_col)
    return data.reset_index(drop=True)


def _robust_scale(values: np.ndarray) -> float:
    values = np.asarray(values, dtype=float)
    values = values[np.isfinite(values)]
    if len(values) == 0:
        return 1e-12
    median = float(np.nanmedian(values))
    mad = float(np.nanmedian(np.abs(values - median)))
    return max(mad * 1.4826, 1e-12)


def _robust_range(values: np.ndarray, lower_quantile: float = 0.10, upper_quantile: float = 0.90) -> float:
    values = np.asarray(values, dtype=float)
    values = values[np.isfinite(values)]
    if len(values) == 0:
        return 0.0
    low, high = np.nanquantile(values, [lower_quantile, upper_quantile])
    return float(high - low)


def _nearest_index_at_or_after(timestamps: pd.Series, timestamp: pd.Timestamp) -> int:
    idx = int(np.searchsorted(timestamps.to_numpy(dtype="datetime64[ns]"), np.datetime64(timestamp), side="left"))
    return min(max(idx, 0), len(timestamps) - 1)


def detect_box_terminal_oi_volatility(
    oi_frame: pd.DataFrame,
    boxes: Sequence[OIBox],
    *,
    value_col: str = "oi_total",
    timestamp_col: str = "timestamp",
    lookback_bars: int = 144,
    terminal_bars: int = 36,
    min_range_ratio: float = 1.8,
    min_net_change_z: float = 3.0,
) -> pd.DataFrame:
    """
    각 박스의 끝부분 근처에서 미결제약정(OI)이 비정상적으로 불안정해지는지 감지

    가격/미결제약정(OI) 박스의 끝부분 근처에서,
    다음 국면이 나타나기 전에 미결제약정(OI)이 늘어나기 시작하거나 변동 폭이 더 커짐
    """
    if lookback_bars < 3:
        raise ValueError("lookback_bars must be at least 3")
    if terminal_bars < 2:
        raise ValueError("terminal_bars must be at least 2")

    data = _prepare_frame(oi_frame, timestamp_col=timestamp_col, numeric_cols=[value_col])
    rows: list[dict[str, object]] = []

    for box in boxes:
        segment = data[(data[timestamp_col] >= box.start) & (data[timestamp_col] <= box.end)]
        if len(segment) < lookback_bars + terminal_bars:
            continue

        baseline = segment.iloc[-(lookback_bars + terminal_bars) : -terminal_bars]
        terminal = segment.iloc[-terminal_bars:]
        baseline_values = baseline[value_col].to_numpy(dtype=float)
        terminal_values = terminal[value_col].to_numpy(dtype=float)

        baseline_range = _robust_range(baseline_values)
        terminal_range = _robust_range(terminal_values)
        range_ratio = terminal_range / max(baseline_range, _robust_scale(baseline_values), 1e-12)

        baseline_diff_scale = _robust_scale(np.diff(baseline_values))
        terminal_net_change = float(terminal_values[-1] - terminal_values[0])
        net_change_z = abs(terminal_net_change) / baseline_diff_scale
        terminal_abs_path = float(np.sum(np.abs(np.diff(terminal_values))))
        baseline_abs_path = float(np.sum(np.abs(np.diff(baseline_values[-terminal_bars:]))))
        path_ratio = terminal_abs_path / max(baseline_abs_path, baseline_diff_scale, 1e-12)
        is_signal = range_ratio >= min_range_ratio or net_change_z >= min_net_change_z

        rows.append(
            {
                "box_id": box.box_id,
                "box_start": box.start,
                "box_end": box.end,
                "terminal_start": pd.Timestamp(terminal[timestamp_col].iloc[0]),
                "terminal_end": pd.Timestamp(terminal[timestamp_col].iloc[-1]),
                "box_low": box.low,
                "box_high": box.high,
                "baseline_range": baseline_range,
                "terminal_range": terminal_range,
                "range_ratio": float(range_ratio),
                "terminal_net_change": terminal_net_change,
                "net_change_z": float(net_change_z),
                "terminal_abs_path": terminal_abs_path,
                "path_ratio": float(path_ratio),
                "direction": "up" if terminal_net_change > 0 else "down" if terminal_net_change < 0 else "flat",
                "is_terminal_oi_expansion": bool(is_signal),
            }
        )

    return pd.DataFrame(rows)


def detect_transient_oi_shocks(
    oi_frame: pd.DataFrame,
    *,
    value_col: str = "oi_total",
    timestamp_col: str = "timestamp",
    baseline_bars: int = 288,
    shock_window_bars: int = 6,
    reversion_bars: int = 72,
    shock_z: float = 5.0,
    min_change_ratio: float = 0.008,
    max_reversion_fraction: float = 0.35,
    cooldown_bars: int | None = None,
) -> pd.DataFrame:
    """
    Detect large OI jumps that quickly return toward their prior state.

    A transient shock is a large deviation from the prior baseline whose future
    path comes back close to that baseline. A non-transient shock is more likely
    to be a real regime transition.
    """
    if baseline_bars < 3:
        raise ValueError("baseline_bars must be at least 3")
    if shock_window_bars < 1:
        raise ValueError("shock_window_bars must be positive")
    if reversion_bars < 1:
        raise ValueError("reversion_bars must be positive")

    data = _prepare_frame(oi_frame, timestamp_col=timestamp_col, numeric_cols=[value_col])
    values = data[value_col].to_numpy(dtype=float)
    timestamps = data[timestamp_col].reset_index(drop=True)
    rows: list[dict[str, object]] = []
    cooldown = cooldown_bars if cooldown_bars is not None else reversion_bars

    idx = baseline_bars
    limit = len(data) - shock_window_bars - 1
    while idx < limit:
        baseline_values = values[idx - baseline_bars : idx]
        baseline_median = float(np.nanmedian(baseline_values))
        baseline_scale = _robust_scale(baseline_values)

        shock_window = values[idx : min(idx + shock_window_bars, len(values))]
        deviations = shock_window - baseline_median
        peak_rel_idx = int(np.nanargmax(np.abs(deviations)))
        peak_idx = idx + peak_rel_idx
        shock_size = float(values[peak_idx] - baseline_median)
        shock_abs = abs(shock_size)
        current_z = shock_abs / baseline_scale
        change_ratio = shock_abs / max(abs(baseline_median), 1e-12)

        if current_z < shock_z or change_ratio < min_change_ratio:
            idx += 1
            continue

        future_start = peak_idx + 1
        future_end = min(peak_idx + 1 + reversion_bars, len(values))
        future_values = values[future_start:future_end]
        future_timestamps = timestamps.iloc[future_start:future_end]
        if len(future_values) == 0:
            break

        remaining_gap = np.abs(future_values - baseline_median)
        best_rel_idx = int(np.nanargmin(remaining_gap))
        best_gap = float(remaining_gap[best_rel_idx])
        reversion_fraction = best_gap / max(shock_abs, 1e-12)
        is_transient = reversion_fraction <= max_reversion_fraction
        reversion_time = pd.Timestamp(future_timestamps.iloc[best_rel_idx]) if is_transient else pd.NaT

        rows.append(
            {
                "event_time": pd.Timestamp(timestamps.iloc[peak_idx]),
                "baseline_start": pd.Timestamp(timestamps.iloc[idx - baseline_bars]),
                "baseline_end": pd.Timestamp(timestamps.iloc[idx - 1]),
                "baseline_median": baseline_median,
                "shock_value": float(values[peak_idx]),
                "shock_size": shock_size,
                "shock_abs": shock_abs,
                "shock_z": float(current_z),
                "change_ratio": float(change_ratio),
                "direction": "up" if shock_size > 0 else "down",
                "reversion_time": reversion_time,
                "reversion_fraction": float(reversion_fraction),
                "is_transient": bool(is_transient),
                "is_persistent": bool(not is_transient),
            }
        )
        idx = peak_idx + max(cooldown, 1)

    return pd.DataFrame(rows)


def _match_price_box(
    boxes: Sequence[OIBox] | None,
    event_time: pd.Timestamp,
    *,
    tolerance: pd.Timedelta,
) -> tuple[OIBox | None, OIBox | None]:
    if not boxes:
        return None, None
    sorted_boxes = sorted(boxes, key=lambda box: box.start)
    matched_idx: int | None = None
    for idx, box in enumerate(sorted_boxes):
        if box.start <= event_time <= box.end:
            matched_idx = idx
            break
        if event_time - tolerance <= box.start <= event_time + tolerance:
            matched_idx = idx
            break
    if matched_idx is None:
        after = [idx for idx, box in enumerate(sorted_boxes) if box.start > event_time]
        matched_idx = after[0] if after else None
    if matched_idx is None:
        return None, None
    matched = sorted_boxes[matched_idx]
    next_box = sorted_boxes[matched_idx + 1] if matched_idx + 1 < len(sorted_boxes) else None
    return matched, next_box


def _summarize_deadcat_event(
    data: pd.DataFrame,
    *,
    anchor_idx: int,
    timestamp_col: str,
    price_col: str,
    high_col: str,
    low_col: str,
    lookback_bars: int,
    trough_search_bars: int,
    rebound_bars: int,
    min_rebound_bps: float,
    price_boxes: Sequence[OIBox] | None,
    box_match_tolerance: pd.Timedelta,
) -> dict[str, object] | None:
    values_high = data[high_col].to_numpy(dtype=float)
    values_low = data[low_col].to_numpy(dtype=float)
    timestamps = data[timestamp_col].reset_index(drop=True)

    lookback_start = max(0, anchor_idx - lookback_bars)
    peak_slice = values_high[lookback_start : anchor_idx + 1]
    if len(peak_slice) == 0:
        return None
    peak_idx = lookback_start + int(np.nanargmax(peak_slice))

    trough_end = min(anchor_idx + trough_search_bars, len(data))
    trough_slice = values_low[anchor_idx:trough_end]
    if len(trough_slice) == 0:
        return None
    trough_idx = anchor_idx + int(np.nanargmin(trough_slice))

    rebound_end = min(trough_idx + rebound_bars, len(data))
    rebound_slice = values_high[trough_idx:rebound_end]
    if len(rebound_slice) == 0:
        return None
    rebound_idx = trough_idx + int(np.nanargmax(rebound_slice))

    peak_price = float(values_high[peak_idx])
    trough_price = float(values_low[trough_idx])
    rebound_high = float(values_high[rebound_idx])
    drop_bps = (trough_price / max(peak_price, 1e-12) - 1.0) * 10_000.0
    rebound_bps = (rebound_high / max(trough_price, 1e-12) - 1.0) * 10_000.0

    event_time = pd.Timestamp(timestamps.iloc[anchor_idx])
    matched_box, next_box = _match_price_box(price_boxes, event_time, tolerance=box_match_tolerance)
    box_upper = float(matched_box.high) if matched_box is not None else np.nan
    box_lower = float(matched_box.low) if matched_box is not None else np.nan
    next_lower_box_high = np.nan
    if next_box is not None and matched_box is not None and next_box.mid < matched_box.mid:
        next_lower_box_high = float(next_box.high)

    deadcat_upper = box_upper if np.isfinite(box_upper) else rebound_high
    if np.isfinite(next_lower_box_high) and next_lower_box_high < deadcat_upper:
        deadcat_lower = next_lower_box_high
    elif np.isfinite(box_lower):
        deadcat_lower = box_lower
    else:
        deadcat_lower = trough_price

    return {
        "event_time": event_time,
        "peak_time": pd.Timestamp(timestamps.iloc[peak_idx]),
        "trough_time": pd.Timestamp(timestamps.iloc[trough_idx]),
        "rebound_high_time": pd.Timestamp(timestamps.iloc[rebound_idx]),
        "range_end": pd.Timestamp(timestamps.iloc[rebound_end - 1]),
        "peak_price": peak_price,
        "trough_price": trough_price,
        "rebound_high": rebound_high,
        "drop_bps": float(drop_bps),
        "rebound_bps": float(rebound_bps),
        "box_id": matched_box.box_id if matched_box is not None else pd.NA,
        "box_upper": box_upper,
        "box_lower": box_lower,
        "next_box_id": next_box.box_id if next_box is not None else pd.NA,
        "next_lower_box_high": next_lower_box_high,
        "deadcat_upper": float(deadcat_upper),
        "deadcat_lower": float(deadcat_lower),
        "is_deadcat_range": bool(drop_bps < 0 and rebound_bps >= min_rebound_bps),
    }


def detect_deadcat_bounce_ranges(
    price_frame: pd.DataFrame,
    *,
    timestamp_col: str = "timestamp",
    price_col: str = "close",
    high_col: str = "high",
    low_col: str = "low",
    anchor_times: Sequence[str | pd.Timestamp] | None = None,
    price_boxes: Sequence[OIBox] | None = None,
    lookback_bars: int = 288,
    trough_search_bars: int = 72,
    rebound_bars: int = 864,
    drop_threshold_bps: float = 350.0,
    min_rebound_bps: float = 100.0,
    cooldown_bars: int = 288,
    box_match_tolerance: str | pd.Timedelta = "3h",
) -> pd.DataFrame:
    """
    Detect post-drop rebound ranges that can become resistance/support lines.

    If `anchor_times` are supplied, they are treated as manually observed drop
    times. Without anchors, the function searches for large drawdowns from a
    recent high and de-duplicates them with `cooldown_bars`.
    """
    if lookback_bars < 2:
        raise ValueError("lookback_bars must be at least 2")
    if trough_search_bars < 1:
        raise ValueError("trough_search_bars must be positive")
    if rebound_bars < 1:
        raise ValueError("rebound_bars must be positive")

    data = _prepare_frame(
        price_frame,
        timestamp_col=timestamp_col,
        numeric_cols=[price_col, high_col, low_col],
    )
    if data.empty:
        return pd.DataFrame()

    tolerance = pd.Timedelta(box_match_tolerance)
    timestamps = data[timestamp_col].reset_index(drop=True)
    anchor_indices: list[int] = []
    if anchor_times is not None:
        for anchor in anchor_times:
            anchor_indices.append(_nearest_index_at_or_after(timestamps, pd.Timestamp(anchor)))
    else:
        high = data[high_col].to_numpy(dtype=float)
        low = data[low_col].to_numpy(dtype=float)
        idx = lookback_bars
        while idx < len(data):
            recent_high = float(np.nanmax(high[idx - lookback_bars : idx + 1]))
            drawdown_bps = (float(low[idx]) / max(recent_high, 1e-12) - 1.0) * 10_000.0
            if drawdown_bps <= -abs(drop_threshold_bps):
                anchor_indices.append(idx)
                idx += max(cooldown_bars, 1)
            else:
                idx += 1

    rows: list[dict[str, object]] = []
    seen: set[int] = set()
    for anchor_idx in anchor_indices:
        if anchor_idx in seen:
            continue
        seen.add(anchor_idx)
        row = _summarize_deadcat_event(
            data,
            anchor_idx=anchor_idx,
            timestamp_col=timestamp_col,
            price_col=price_col,
            high_col=high_col,
            low_col=low_col,
            lookback_bars=lookback_bars,
            trough_search_bars=trough_search_bars,
            rebound_bars=rebound_bars,
            min_rebound_bps=min_rebound_bps,
            price_boxes=price_boxes,
            box_match_tolerance=tolerance,
        )
        if row is not None:
            rows.append(row)

    return pd.DataFrame(rows)


def detect_box_event_signals(
    *,
    oi_frame: pd.DataFrame,
    price_frame: pd.DataFrame,
    oi_boxes: Sequence[OIBox],
    price_boxes: Sequence[OIBox] | None = None,
    drop_anchor_times: Sequence[str | pd.Timestamp] | None = None,
    timestamp_col: str = "timestamp",
    oi_col: str = "oi_total",
    price_col: str = "close",
    high_col: str = "high",
    low_col: str = "low",
    config: BoxEventSignalConfig | None = None,
) -> dict[str, pd.DataFrame]:
    config = config or BoxEventSignalConfig()
    terminal = detect_box_terminal_oi_volatility(
        oi_frame,
        oi_boxes,
        value_col=oi_col,
        timestamp_col=timestamp_col,
        lookback_bars=config.terminal_lookback_bars,
        terminal_bars=config.terminal_bars,
        min_range_ratio=config.terminal_range_ratio,
        min_net_change_z=config.terminal_net_change_z,
    )
    transient = detect_transient_oi_shocks(
        oi_frame,
        value_col=oi_col,
        timestamp_col=timestamp_col,
        baseline_bars=config.shock_baseline_bars,
        shock_window_bars=config.shock_window_bars,
        reversion_bars=config.shock_reversion_bars,
        shock_z=config.shock_z,
        min_change_ratio=config.shock_min_change_ratio,
        max_reversion_fraction=config.shock_max_reversion_fraction,
    )
    deadcat = detect_deadcat_bounce_ranges(
        price_frame,
        timestamp_col=timestamp_col,
        price_col=price_col,
        high_col=high_col,
        low_col=low_col,
        anchor_times=drop_anchor_times,
        price_boxes=price_boxes,
        lookback_bars=config.drop_lookback_bars,
        trough_search_bars=config.trough_search_bars,
        rebound_bars=config.rebound_bars,
        drop_threshold_bps=config.drop_threshold_bps,
        min_rebound_bps=config.min_rebound_bps,
    )
    return {
        "terminal_oi_volatility": terminal,
        "transient_oi_shocks": transient,
        "deadcat_bounce_ranges": deadcat,
    }


def plot_box_event_signals(
    *,
    oi_frame: pd.DataFrame,
    price_frame: pd.DataFrame,
    terminal_oi_volatility: pd.DataFrame,
    transient_oi_shocks: pd.DataFrame,
    deadcat_bounce_ranges: pd.DataFrame,
    oi_boxes: Sequence[OIBox] | None = None,
    price_boxes: Sequence[OIBox] | None = None,
    timestamp_col: str = "timestamp",
    oi_col: str = "oi_total",
    price_col: str = "close",
    axes=None,
):
    """
    Plot the three event layers:
    1. OI terminal volatility near box ends;
    2. transient vs persistent OI shocks;
    3. post-drop dead-cat rebound ranges on price.
    """
    import matplotlib.pyplot as plt

    from research.microstructure_alpha.oi_box import plot_oi_boxes

    oi_data = _prepare_frame(oi_frame, timestamp_col=timestamp_col, numeric_cols=[oi_col])
    price_data = _prepare_frame(price_frame, timestamp_col=timestamp_col, numeric_cols=[price_col])

    if axes is None:
        _, axes = plt.subplots(3, 1, figsize=(20, 14), sharex=True)
    ax_oi, ax_shock, ax_price = axes

    if oi_boxes:
        plot_oi_boxes(oi_data, list(oi_boxes), value_col=oi_col, timestamp_col=timestamp_col, ax=ax_oi, alpha=0.25)
    else:
        ax_oi.plot(oi_data[timestamp_col], oi_data[oi_col], color="black", linewidth=1.0)
    terminal_signals = terminal_oi_volatility
    if not terminal_signals.empty and "is_terminal_oi_expansion" in terminal_signals.columns:
        terminal_signals = terminal_signals[terminal_signals["is_terminal_oi_expansion"]]
    for row in terminal_signals.itertuples(index=False):
        ax_oi.axvspan(row.terminal_start, row.terminal_end, color="orange", alpha=0.22)
        ax_oi.axvline(row.box_end, color="red", linestyle="--", linewidth=0.9)
    ax_oi.set_title("OI terminal volatility near box end")
    ax_oi.grid(True, alpha=0.2)

    ax_shock.plot(oi_data[timestamp_col], oi_data[oi_col], color="tab:purple", linewidth=1.0)
    if not transient_oi_shocks.empty:
        for row in transient_oi_shocks.itertuples(index=False):
            color = "tab:blue" if row.is_transient else "tab:red"
            end_time = row.reversion_time if pd.notna(row.reversion_time) else row.event_time
            ax_shock.axvspan(row.event_time, end_time, color=color, alpha=0.18)
            ax_shock.axvline(row.event_time, color=color, linestyle="--", linewidth=0.9)
    ax_shock.set_title("Transient OI shock vs persistent OI shock")
    ax_shock.grid(True, alpha=0.2)

    if price_boxes:
        plot_oi_boxes(
            price_data.rename(columns={price_col: "_plot_price"}),
            list(price_boxes),
            value_col="_plot_price",
            timestamp_col=timestamp_col,
            ax=ax_price,
            box_color="tab:cyan",
            alpha=0.13,
        )
    else:
        ax_price.plot(price_data[timestamp_col], price_data[price_col], color="black", linewidth=1.0)
    if not deadcat_bounce_ranges.empty:
        for row in deadcat_bounce_ranges.itertuples(index=False):
            if not row.is_deadcat_range:
                continue
            ax_price.axvline(row.event_time, color="red", linestyle="--", linewidth=1.0)
            ax_price.axvline(row.trough_time, color="pink", linestyle=":", linewidth=1.0)
            ax_price.hlines(row.deadcat_upper, xmin=row.event_time, xmax=row.range_end, color="lime", linewidth=1.3)
            ax_price.hlines(row.deadcat_lower, xmin=row.event_time, xmax=row.range_end, color="lime", linewidth=1.3)
            ax_price.axvspan(row.event_time, row.range_end, color="lime", alpha=0.06)
    ax_price.set_title("Dead-cat bounce range levels after large price drops")
    ax_price.grid(True, alpha=0.2)
    return axes
