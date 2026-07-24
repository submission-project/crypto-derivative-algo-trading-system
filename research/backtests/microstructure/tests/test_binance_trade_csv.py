import ast
import os
from pathlib import Path

import pytest

from research.backtests.microstructure.binance_trade_csv import (
    aggregate_trade_csv_to_bars,
    aggregate_trade_csv_files_to_bars,
    bars_to_backtest_points,
    bars_to_dataframe,
    TradeBar,
)
from research.backtests.microstructure.cost_model import CostModel
from research.backtests.microstructure.metrics import compute_all_metrics
from research.backtests.microstructure.simulator import run_directional_backtest
from research.backtests.microstructure.strategy_evaluation import (
    evaluate_market_memory_reversion_csv,
    evaluate_moving_average_crossover_csv,
)
from research.backtests.microstructure.strategies.box_reversion import generate_box_reversion_signals
from research.backtests.microstructure.strategies.taker_imbalance import generate_taker_imbalance_signals


DEFAULT_DATA_PATHS = [
    "research/datasets/exchange/binance/assets/btcusdt/future/trade/daily/"
    "BTCUSDT-trades-2026-02-15.csv",
]


def _parse_data_paths() -> list[Path]:
    raw = os.environ.get("DATA_PATHS", "").strip()
    if not raw:
        return [Path(item) for item in DEFAULT_DATA_PATHS]

    if raw.startswith("["):
        values = ast.literal_eval(raw)
        if not isinstance(values, list):
            raise ValueError("DATA_PATHS list syntax must evaluate to a list")
    else:
        values = [item.strip() for item in raw.split(",")]

    paths = [Path(str(item)) for item in values if str(item).strip()]
    if not paths:
        raise ValueError("DATA_PATHS must contain at least one CSV path")
    return paths


DATA_PATHS = _parse_data_paths()

print(f"\nDATA_PATHS: {DATA_PATHS}")


def _parse_memory_lines() -> list[float]:
    raw = os.environ.get("MEMORY_LINES", "").strip()
    if not raw:
        return []

    if raw.startswith("["):
        values = ast.literal_eval(raw)
        if not isinstance(values, list):
            raise ValueError("MEMORY_LINES list syntax must evaluate to a list")
    else:
        values = [item.strip() for item in raw.split(",")]

    lines = [float(item) for item in values if str(item).strip()]
    if lines and len(lines) < 2:
        raise ValueError("MEMORY_LINES must contain at least two price lines")
    return lines


MEMORY_LINES = _parse_memory_lines()

print(f"MEMORY_LINES: {MEMORY_LINES}")


def _data_paths_available() -> bool:
    return all(path.exists() for path in DATA_PATHS)


def _memory_lines_available() -> bool:
    return len(MEMORY_LINES) >= 2


def _assert_cost_aware_backtest_runs(points) -> None:
    assert len(points) >= 10
    assert any(point.signal != 0 for point in points)

    result = run_directional_backtest(
        points,
        initial_equity=10_000.0,
        notional_per_trade=1_000.0,
        cost_model=CostModel(taker_fee_bps=4.0, slippage_bps=1.0, sqrt_impact_coeff=0.001),
        latency_periods=1,
        max_holding_periods=10,
        max_drawdown_stop=0.10,
        default_spread_bps=1.0,
    )
    metrics = compute_all_metrics(result.equity_curve, result.trade_pnls)

    print(metrics)

    assert len(result.equity_curve) == len(points) + 1
    assert "max_drawdown" in metrics


def _write_trade_csv(path: Path, rows: list[tuple[int, float, float, int, bool]]) -> None:
    lines = ["id,price,qty,quote_qty,time,is_buyer_maker"]
    for trade_id, price, qty, quote_qty, timestamp, is_buyer_maker in rows:
        lines.append(f"{trade_id},{price},{qty},{quote_qty},{timestamp},{is_buyer_maker}")
    path.write_text("\n".join(lines) + "\n")


def test_aggregate_multiple_trade_csv_files_to_bars(tmp_path: Path) -> None:
    first = tmp_path / "BTCUSDT-trades-2026-02-15.csv"
    second = tmp_path / "BTCUSDT-trades-2026-02-16.csv"
    _write_trade_csv(
        first,
        [
            (1, 100.0, 0.1, 10.0, 1_000, False),
            (2, 101.0, 0.1, 10.0, 2_000, True),
        ],
    )
    _write_trade_csv(
        second,
        [
            (3, 102.0, 0.1, 10.0, 60_000, False),
            (4, 103.0, 0.1, 10.0, 61_000, False),
        ],
    )

    for engine in ["python", "pandas", "polars"]:
        bars = aggregate_trade_csv_files_to_bars([first, second], bucket_ms=60_000, engine=engine)

        assert [bar.bucket_start_ms for bar in bars] == [0, 60_000]
        assert [bar.close_price for bar in bars] == [101.0, 103.0]
        assert [bar.trade_count for bar in bars] == [2, 2]
        assert [bar.volume for bar in bars] == [0.2, 0.2]
        assert [bar.quote_volume for bar in bars] == [20.0, 20.0]
        assert [bar.first_id for bar in bars] == [1, 3]
        assert [bar.last_id for bar in bars] == [2, 4]


def test_aggregate_multiple_trade_csv_files_respects_total_max_rows(tmp_path: Path) -> None:
    first = tmp_path / "BTCUSDT-trades-2026-02-15.csv"
    second = tmp_path / "BTCUSDT-trades-2026-02-16.csv"
    _write_trade_csv(first, [(1, 100.0, 0.1, 10.0, 1_000, False)])
    _write_trade_csv(
        second,
        [
            (2, 101.0, 0.1, 10.0, 60_000, False),
            (3, 102.0, 0.1, 10.0, 61_000, False),
        ],
    )

    for engine in ["python", "pandas", "polars"]:
        bars = aggregate_trade_csv_to_bars([first, second], bucket_ms=60_000, max_rows=2, engine=engine)

        assert [bar.bucket_start_ms for bar in bars] == [0, 60_000]
        assert sum(bar.trade_count for bar in bars) == 2
        assert bars[-1].close_price == 101.0
        assert bars[-1].first_id == 2
        assert bars[-1].last_id == 2


def test_aggregate_trade_csv_to_bars_requires_path_list(tmp_path: Path) -> None:
    csv_path = tmp_path / "BTCUSDT-trades-2026-02-15.csv"
    _write_trade_csv(csv_path, [(1, 100.0, 0.1, 10.0, 1_000, False)])

    with pytest.raises(TypeError):
        aggregate_trade_csv_to_bars(csv_path, bucket_ms=60_000, engine="python")


@pytest.mark.skipif(not _data_paths_available(), reason="local Binance trade CSV files are not available")
def test_real_binance_trade_csv_taker_imbalance_backtest_smoke() -> None:
    bars = aggregate_trade_csv_to_bars(
        DATA_PATHS,
        bucket_ms=60_000,
        max_rows=1_000_000,
        engine="polars",
    )
    signals = generate_taker_imbalance_signals(bars, threshold=0.10)
    points = bars_to_backtest_points(bars, signals=signals, default_spread_bps=1.0)

    _assert_cost_aware_backtest_runs(points)

@pytest.mark.skipif(not _data_paths_available(), reason="local Binance trade CSV files are not available")
def test_real_binance_trade_csv_box_reversion_backtest_smoke() -> None:
    bars = aggregate_trade_csv_to_bars(
        DATA_PATHS,
        bucket_ms=60_000,
        max_rows=1_000_000,
        engine="polars",
    )
    signals = generate_box_reversion_signals(bars, edge_threshold=0.10, flow_threshold=0.05)
    points = bars_to_backtest_points(bars, signals=signals, default_spread_bps=1.0)

    _assert_cost_aware_backtest_runs(points)


@pytest.mark.skipif(not _data_paths_available(), reason="local Binance trade CSV files are not available")
def test_real_binance_trade_csv_moving_average_crossover_evaluation() -> None:
    evaluation = evaluate_moving_average_crossover_csv(
        DATA_PATHS,
        bucket_ms=60_000,
        max_rows=10_000_000,
        engine="polars",
        fast_window=int(os.environ.get("MA_FAST_WINDOW", "20")),
        slow_window=int(os.environ.get("MA_SLOW_WINDOW", "80")),
        neutral_band_bps=float(os.environ.get("MA_NEUTRAL_BAND_BPS", "2.0")),
        exit_on_neutral=os.environ.get("MA_EXIT_ON_NEUTRAL", "false").lower() in {"1", "true", "yes"},
        cost_model=CostModel(taker_fee_bps=4.0, slippage_bps=1.0, sqrt_impact_coeff=0.001),
        latency_periods=1,
        max_holding_periods=int(os.environ.get("MA_MAX_HOLDING_PERIODS", "0")),
        max_drawdown_stop=0.10,
        default_spread_bps=1.0,
    )

    print("moving_average_crossover_metrics:", evaluation.metrics)
    print("moving_average_crossover_signal_counts:", evaluation.signal_counts)

    assert len(evaluation.bars) >= 100
    assert len(evaluation.points) == len(evaluation.bars)
    assert "total_return" in evaluation.metrics
    assert "max_drawdown" in evaluation.metrics
    assert evaluation.metrics["total_trades"] >= 1


@pytest.mark.skipif(not _data_paths_available(), reason="local Binance trade CSV files are not available")
@pytest.mark.skipif(not _memory_lines_available(), reason="MEMORY_LINES env var is not provided")
def test_real_binance_trade_csv_market_memory_reversion_evaluation() -> None:
    evaluation = evaluate_market_memory_reversion_csv(
        DATA_PATHS,
        lines=MEMORY_LINES,
        bucket_ms=60_000,
        max_rows=10_000_000,
        engine="polars",
        line_tolerance_bps=float(os.environ.get("LINE_TOLERANCE_BPS", "8.0")),
        flow_threshold=float(os.environ.get("FLOW_THRESHOLD", "0.05")),
        min_rejection_bps=float(os.environ.get("MIN_REJECTION_BPS", "1.0")),
        cooldown_bars=int(os.environ.get("COOLDOWN_BARS", "3")),
        cost_model=CostModel(taker_fee_bps=4.0, slippage_bps=1.0, sqrt_impact_coeff=0.001),
    )

    print("market_memory_metrics:", evaluation.metrics)
    print("market_memory_signal_counts:", evaluation.signal_counts)
    print("market_memory_signal_details:", [detail for detail in evaluation.details if detail.signal != 0][:10])

    assert len(evaluation.bars) >= 10
    assert len(evaluation.points) == len(evaluation.bars)
    assert "total_return" in evaluation.metrics
    assert "max_drawdown" in evaluation.metrics
    assert "total_cost_paid" in evaluation.metrics


def test_bars_to_backtest_points_forces_last_flat() -> None:
    bars = (
        aggregate_trade_csv_to_bars(DATA_PATHS, bucket_ms=60_000, max_rows=1_000, engine="python")
        if _data_paths_available()
        else []
    )
    if not bars:
        pytest.skip("local Binance trade CSV is not available")

    signals = [1] * len(bars)
    points = bars_to_backtest_points(bars, signals=signals, force_flat_last=True)
    assert points[-1].signal == 0


def test_bars_to_dataframe() -> None:
    bars = [
        TradeBar(
            bucket_start_ms=1000,
            start_price=100.0,
            close_price=100.0,
            price_base=100.0,
            price_gap_list=[0.0],
            volume=1.0,
            quote_volume=100.0,
            taker_buy_volume=0.6,
            taker_sell_volume=0.4,
            taker_buy_quote_volume=60.0,
            taker_sell_quote_volume=40.0,
            trade_count=5,
            first_id=10,
            last_id=14,
        )
    ]

    # Test polars (populated)
    df_pl = bars_to_dataframe(bars, engine="polars")
    assert df_pl.shape == (1, 14)
    assert list(df_pl.columns) == [
        "bucket_start_ms",
        "close_price",
        "start_price",
        "price_base",
        "price_gap_list",
        "volume",
        "quote_volume",
        "taker_buy_volume",
        "taker_sell_volume",
        "taker_buy_quote_volume",
        "taker_sell_quote_volume",
        "trade_count",
        "first_id",
        "last_id",
    ]
    assert df_pl["bucket_start_ms"][0] == 1000

    # Test pandas (populated)
    df_pd = bars_to_dataframe(bars, engine="pandas")
    assert df_pd.shape == (1, 14)
    assert list(df_pd.columns) == [
        "bucket_start_ms",
        "close_price",
        "start_price",
        "price_base",
        "price_gap_list",
        "volume",
        "quote_volume",
        "taker_buy_volume",
        "taker_sell_volume",
        "taker_buy_quote_volume",
        "taker_sell_quote_volume",
        "trade_count",
        "first_id",
        "last_id",
    ]
    assert df_pd["bucket_start_ms"].iloc[0] == 1000

    # Test polars (empty)
    df_pl_empty = bars_to_dataframe([], engine="polars")
    assert df_pl_empty.shape == (0, 14)

    # Test pandas (empty)
    df_pd_empty = bars_to_dataframe([], engine="pandas")
    assert df_pd_empty.shape == (0, 14)
