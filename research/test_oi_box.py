import numpy as np
import pandas as pd

from research.microstructure_alpha.oi_box import (
    assign_oi_box_features,
    detect_oi_box_ranges,
    detect_stable_oi_box_ranges,
    diagnose_stable_oi_box_ranges,
    oi_box_lines_to_frame,
    oi_boxes_to_frame,
)


def _synthetic_oi_frame() -> pd.DataFrame:
    rng = np.random.default_rng(7)
    values = np.concatenate(
        [
            100_000 + rng.normal(0, 250, 72),
            116_000 + rng.normal(0, 300, 72),
            94_000 + rng.normal(0, 220, 72),
        ]
    )
    return pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=len(values), freq="5min"),
            "exchange_oi_sum": values,
        }
    )


def test_detect_oi_box_ranges_finds_center_shifts() -> None:
    frame = _synthetic_oi_frame()

    boxes = detect_oi_box_ranges(
        frame,
        min_bars=24,
        center_shift_bars=12,
        center_shift_ratio=0.50,
        breakout_confirm_bars=2,
    )
    summary = oi_boxes_to_frame(boxes)

    assert len(summary) >= 3
    assert summary["mid"].iloc[0] < 102_000
    assert (summary["mid"] > 112_000).any()
    assert (summary["mid"] < 98_000).any()


def test_assign_oi_box_features_marks_edges_and_positions() -> None:
    frame = _synthetic_oi_frame()
    boxes = detect_oi_box_ranges(
        frame,
        min_bars=24,
        center_shift_bars=12,
        center_shift_ratio=0.50,
        breakout_confirm_bars=2,
    )

    with_features = assign_oi_box_features(frame, boxes)

    assert with_features["oi_box_id"].notna().any()
    assert with_features["oi_box_low"].notna().any()
    assert with_features["oi_box_position"].notna().any()
    assert set(with_features["oi_box_zone"].unique()) & {"lower_edge", "inside", "upper_edge"}


def test_oi_box_lines_to_frame_exposes_vertical_and_horizontal_lines() -> None:
    frame = _synthetic_oi_frame()
    boxes = detect_oi_box_ranges(
        frame,
        min_bars=24,
        center_shift_bars=12,
        center_shift_ratio=0.50,
        breakout_confirm_bars=2,
    )

    lines = oi_box_lines_to_frame(boxes)

    assert {"vertical", "horizontal"} <= set(lines["line_type"])
    assert {"start", "end", "low", "high", "mid"} <= set(lines["line_name"])
    assert lines.loc[lines["line_name"].eq("start"), "timestamp"].notna().all()
    assert lines.loc[lines["line_name"].eq("low"), "level"].notna().all()


def test_detect_stable_oi_box_ranges_filters_overwide_boxes() -> None:
    frame = _synthetic_oi_frame()

    boxes = detect_stable_oi_box_ranges(
        frame,
        min_bars=24,
        center_shift_bars=12,
        center_shift_ratio=0.50,
        breakout_confirm_bars=2,
        start_step_bars=6,
        max_start_offset_bars=24,
        warmup_bars=12,
        max_box_width_ratio=0.035,
        min_consensus_starts=2,
        min_consensus_ratio=0.25,
        level_tolerance_ratio=0.012,
    )
    summary = oi_boxes_to_frame(boxes)

    assert len(summary) >= 2
    assert (summary["width"] / summary["mid"]).max() <= 0.035
    assert (summary["mid"] > 112_000).any()
    assert (summary["mid"] < 98_000).any()


def test_detect_stable_oi_box_ranges_is_less_start_sensitive() -> None:
    frame = _synthetic_oi_frame()
    shifted_frame = frame.iloc[6:].reset_index(drop=True)
    kwargs = {
        "min_bars": 24,
        "center_shift_bars": 12,
        "center_shift_ratio": 0.50,
        "breakout_confirm_bars": 2,
        "start_step_bars": 6,
        "max_start_offset_bars": 24,
        "warmup_bars": 12,
        "max_box_width_ratio": 0.035,
        "min_consensus_starts": 2,
        "min_consensus_ratio": 0.25,
        "level_tolerance_ratio": 0.012,
    }

    boxes = detect_stable_oi_box_ranges(frame, **kwargs)
    shifted_boxes = detect_stable_oi_box_ranges(shifted_frame, **kwargs)
    mids = np.array([box.mid for box in boxes])
    shifted_mids = np.array([box.mid for box in shifted_boxes])

    assert len(mids) >= 2
    assert len(shifted_mids) >= 2
    assert any(np.min(np.abs(shifted_mids - mid)) / mid < 0.012 for mid in mids)


def test_diagnose_stable_oi_box_ranges_explains_filtering() -> None:
    frame = _synthetic_oi_frame()
    kwargs = {
        "min_bars": 24,
        "center_shift_bars": 12,
        "center_shift_ratio": 0.50,
        "breakout_confirm_bars": 2,
        "start_step_bars": 6,
        "max_start_offset_bars": 24,
        "warmup_bars": 12,
        "max_box_width_ratio": 0.035,
        "min_consensus_starts": 2,
        "min_consensus_ratio": 0.25,
        "level_tolerance_ratio": 0.012,
    }

    boxes = detect_stable_oi_box_ranges(frame, **kwargs)
    diagnostics = diagnose_stable_oi_box_ranges(frame, **kwargs)

    assert diagnostics.raw_boxes >= diagnostics.after_warmup_boxes
    assert diagnostics.after_warmup_boxes >= diagnostics.after_width_boxes
    assert diagnostics.stable_boxes == len(boxes)
    assert diagnostics.required_support >= 1
    assert diagnostics.cluster_supports
