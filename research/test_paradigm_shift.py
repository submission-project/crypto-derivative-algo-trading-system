import numpy as np
import pandas as pd

from research.microstructure_alpha.paradigm_shift import (
    OnlineEWMParadigmShiftDetector,
    add_orderbook_update_intensity_features,
    build_microstructure_shift_frame,
    compare_window_before_after,
    detect_orderbook_update_intensity_shifts,
    detect_orderbook_update_bursts,
    detect_paradigm_shifts,
    label_shift_price_lead,
)


def test_detect_paradigm_shifts_accepts_ns_timestamps() -> None:
    rng = np.random.default_rng(11)
    timestamps = pd.date_range("2026-01-01", periods=700, freq="10ms")
    base = rng.normal(0, 0.2, 700)
    shifted = base.copy()
    shifted[520:] += 3.5
    frame = pd.DataFrame(
        {
            "timestamp": timestamps.astype("int64"),
            "update_intensity": shifted,
            "oi_delta": np.r_[np.zeros(520), np.full(180, -2.0)],
        }
    )

    result = detect_paradigm_shifts(
        frame,
        feature_cols=["update_intensity", "oi_delta"],
        baseline_bars=300,
        recent_bars=30,
        threshold=3.0,
        cooldown_bars=50,
    )

    signals = result[result["is_shift"]]
    assert not signals.empty
    assert signals["timestamp"].iloc[0] >= timestamps[500]


def test_build_microstructure_shift_frame_and_compare_window() -> None:
    timestamps = pd.date_range("2026-01-01", periods=20, freq="10ms")
    price_df = pd.DataFrame(
        {
            "timestamp": timestamps[::2],
            "close": np.linspace(100, 101, 10),
            "high": np.linspace(100.1, 101.1, 10),
            "low": np.linspace(99.9, 100.9, 10),
            "volume": np.ones(10),
            "trade_count": np.ones(10),
        }
    )
    orderbook_df = pd.DataFrame(
        {
            "timestamp": timestamps,
            "binance_bid_update_count": np.r_[np.ones(10), np.ones(10) * 5],
            "binance_ask_update_count": np.ones(20),
            "binance_bid_size": np.ones(20),
            "binance_ask_size": np.ones(20) * 2,
        }
    )
    oi_df = pd.DataFrame({"timestamp": timestamps, "oi_total": np.linspace(1000, 995, 20)})

    frame = build_microstructure_shift_frame(
        price_df=price_df,
        orderbook_df=orderbook_df,
        oi_df=oi_df,
        freq="10ms",
    )
    report = compare_window_before_after(
        frame,
        start=timestamps[10],
        end=timestamps[-1],
        feature_cols=["total_bid_update_count"],
        lookback="100ms",
    )

    assert {"price_ret_bps", "oi_delta", "total_bid_update_count", "size_imbalance"}.issubset(frame.columns)
    assert report.loc[0, "during_mean"] > report.loc[0, "before_mean"]


def test_online_ewm_detector_flags_large_shift_after_warmup() -> None:
    detector = OnlineEWMParadigmShiftDetector(
        ["price_ret_bps", "update_intensity"],
        half_life_bars=20,
        threshold=3.0,
        warmup_bars=50,
        cooldown_bars=10,
    )
    ts = pd.Timestamp("2026-01-01")
    flagged = False
    for idx in range(80):
        result = detector.update(ts + pd.Timedelta(milliseconds=idx), [0.0, 1.0])
        flagged = flagged or result.is_shift
    for idx in range(80, 90):
        result = detector.update(ts + pd.Timedelta(milliseconds=idx), [10.0, 20.0])
        flagged = flagged or result.is_shift

    assert flagged


def test_orderbook_update_shift_can_lead_price_move() -> None:
    timestamps = pd.date_range("2026-01-01", periods=700, freq="10ms")
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "close": np.r_[np.full(540, 100.0), np.linspace(100.0, 99.8, 160)],
            "total_bid_update_count": np.r_[np.ones(520), np.ones(180) * 40],
            "total_ask_update_count": np.r_[np.ones(520), np.ones(180) * 35],
        }
    )

    enriched, result = detect_orderbook_update_intensity_shifts(
        frame,
        baseline_bars=300,
        recent_bars=10,
        threshold=4.0,
        cooldown_bars=100,
    )
    lead_report = label_shift_price_lead(
        enriched,
        result,
        price_move_bps=5.0,
        lookahead="3s",
    )

    assert "ob_update_intensity_log" in add_orderbook_update_intensity_features(frame).columns
    assert result["is_shift"].any()
    assert not lead_report.empty
    assert lead_report["lead_time_ms"].dropna().iloc[0] > 0


def test_orderbook_update_burst_detects_sustained_exceedance() -> None:
    timestamps = pd.date_range("2026-01-01", periods=700, freq="10ms")
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "total_bid_update_count": np.r_[np.ones(520) * 2, np.ones(180) * 30],
            "total_ask_update_count": np.r_[np.ones(520) * 2, np.ones(180) * 28],
        }
    )

    result = detect_orderbook_update_bursts(
        frame,
        baseline_bars=300,
        recent_bars=10,
        baseline_quantile=0.90,
        min_exceedance_ratio=0.50,
        min_intensity_ratio=2.0,
        cooldown_bars=100,
    )

    bursts = result[result["is_burst"]]
    assert not bursts.empty
    assert bursts["timestamp"].iloc[0] >= timestamps[520]
    assert bursts["intensity_ratio"].iloc[0] > 2.0

    lead = label_shift_price_lead(
        pd.concat([frame, pd.Series(np.r_[np.full(540, 100.0), np.linspace(100.0, 99.8, 160)], name="close")], axis=1),
        result,
        signal_col="is_burst",
        price_move_bps=5.0,
        lookahead="3s",
    )
    assert not lead.empty
