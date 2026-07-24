from __future__ import annotations

import sys
from dataclasses import asdict
from typing import Any

from .cost_model import CostModel
from .metrics import compute_all_metrics
from .parameter_sweep import run_cost_latency_sweep
from .simulator import BacktestPoint, run_directional_backtest
from .walk_forward import summarize_walk_forward, walk_forward_optimize


def _spread_bps_from_kline(kline: dict[str, Any], fallback_bps: float = 1.0) -> float:
    close = float(kline["close"])
    if close <= 0:
        return fallback_bps
    high_low_bps = (float(kline["high"]) - float(kline["low"])) / close * 10_000.0
    # Kline high-low is not the true bid/ask spread. Use it only as a bounded
    # liquidity stress proxy and keep a conservative default for normal bars.
    return max(fallback_bps, min(high_low_bps * 0.05, 10.0))


def _point_from_kline(kline: dict[str, Any], signal: int, *, timestamp_key: str = "open_time") -> BacktestPoint:
    close = float(kline["close"])
    spread_bps = _spread_bps_from_kline(kline)
    half_spread = close * spread_bps / 20_000.0
    return BacktestPoint(
        timestamp=int(kline[timestamp_key]),
        price=close,
        signal=signal,
        bid=close - half_spread,
        ask=close + half_spread,
        bar_volume_usd=float(kline.get("quote_volume", 0.0)),
    )


def run_demo_backtest() -> dict[str, float]:
    """Original demo backtest with hardcoded data points."""
    points = [
        BacktestPoint(timestamp=1, price=100.0, signal=1),
        BacktestPoint(timestamp=2, price=100.5, signal=1),
        BacktestPoint(timestamp=3, price=100.8, signal=0),
        BacktestPoint(timestamp=4, price=100.2, signal=-1),
        BacktestPoint(timestamp=5, price=99.8, signal=0),
    ]
    result = run_directional_backtest(
        points,
        cost_model=CostModel(taker_fee_bps=4.0, slippage_bps=1.0),
        latency_periods=1,
        default_spread_bps=1.0,
    )
    return compute_all_metrics(result.equity_curve, result.trade_pnls)


def run_live_data_backtest(
    *,
    symbol: str = "BTCUSDT",
    interval: str = "1m",
    limit: int = 500,
    initial_equity: float = 10_000.0,
    notional_per_trade: float = 1_000.0,
    imbalance_threshold: float = 0.35,
    max_holding_periods: int = 30,
    max_drawdown_stop: float = 0.10,
    latency_periods: int = 1,
) -> dict[str, Any]:
    """
    Fetch live kline data from Binance and run a full backtest.

    Uses the MicrostructureAlphaStrategy for signal generation
    and the enhanced simulator with risk controls.
    """
    from research.microstructure_alpha.strategy import (
        MicrostructureAlphaStrategy,
        StrategyConfig,
        fetch_klines,
    )

    # Fetch data
    klines = fetch_klines(symbol=symbol, interval=interval, limit=limit)
    if len(klines) < 120:
        return {"error": "Insufficient data", "klines_fetched": len(klines)}

    # Generate signals
    config = StrategyConfig(
        symbol=symbol,
        imbalance_entry_threshold=imbalance_threshold,
    )
    strategy = MicrostructureAlphaStrategy(config)
    signals = strategy.generate_signals_from_klines(klines)

    # Build backtest points
    points = [_point_from_kline(klines[i], sig.direction) for i, sig in enumerate(signals)]

    # Run backtest with risk controls
    cost_model = CostModel(
        taker_fee_bps=4.0,
        slippage_bps=1.0,
        sqrt_impact_coeff=0.001,
    )
    result = run_directional_backtest(
        points,
        initial_equity=initial_equity,
        notional_per_trade=notional_per_trade,
        cost_model=cost_model,
        max_holding_periods=max_holding_periods,
        max_drawdown_stop=max_drawdown_stop,
        latency_periods=latency_periods,
        default_spread_bps=1.0,
    )

    metrics = compute_all_metrics(result.equity_curve, result.trade_pnls)

    # Regime distribution
    regime_counts: dict[str, int] = {}
    for sig in signals:
        regime_counts[sig.regime] = regime_counts.get(sig.regime, 0) + 1

    return {
        "symbol": symbol,
        "interval": interval,
        "data_points": len(klines),
        "metrics": metrics,
        "trade_count": result.trade_count,
        "forced_exits": result.forced_exits,
        "total_cost_paid": result.total_cost_paid,
        "latency_periods": result.latency_periods,
        "regime_distribution": regime_counts,
        "final_equity": result.equity_curve[-1],
    }


def run_walk_forward_backtest(
    *,
    symbol: str = "BTCUSDT",
    interval: str = "1m",
    limit: int = 1000,
    n_folds: int = 5,
    initial_equity: float = 10_000.0,
    notional_per_trade: float = 1_000.0,
) -> dict[str, Any]:
    """
    Fetch data and run Walk-Forward optimization.

    Returns per-fold results and aggregated OOS metrics.
    """
    from research.microstructure_alpha.features import (
        TradeBucket,
        normalized_trade_imbalance,
    )
    from research.microstructure_alpha.strategy import fetch_klines

    klines = fetch_klines(symbol=symbol, interval=interval, limit=limit)
    if len(klines) < n_folds * 20:
        return {"error": "Insufficient data", "klines_fetched": len(klines)}

    prices = [k["close"] for k in klines]
    timestamps = [k["open_time"] for k in klines]
    imbalances = [
        normalized_trade_imbalance(
            TradeBucket(
                buy_taker_qty=k["taker_buy_volume"],
                sell_taker_qty=k["taker_sell_volume"],
            )
        )
        for k in klines
    ]

    cost_model = CostModel(
        taker_fee_bps=4.0,
        slippage_bps=1.0,
        sqrt_impact_coeff=0.001,
    )

    folds = walk_forward_optimize(
        prices,
        timestamps,
        imbalances,
        n_folds=n_folds,
        cost_model=cost_model,
        initial_equity=initial_equity,
        notional_per_trade=notional_per_trade,
        latency_periods=1,
        default_spread_bps=1.0,
    )

    summary = summarize_walk_forward(folds)

    fold_details = []
    for f in folds:
        fold_details.append({
            "fold": f.fold_index,
            "best_threshold": f.best_threshold,
            "train_sharpe": f.train_metrics.get("sharpe", 0.0),
            "test_sharpe": f.test_metrics.get("sharpe", 0.0),
            "train_return": f.train_metrics.get("total_return", 0.0),
            "test_return": f.test_metrics.get("total_return", 0.0),
            "test_win_rate": f.test_metrics.get("win_rate", 0.0),
            "test_mdd": f.test_metrics.get("max_drawdown", 0.0),
        })

    return {
        "symbol": symbol,
        "n_folds": n_folds,
        "data_points": len(klines),
        "summary": summary,
        "folds": fold_details,
    }


def run_oi_box_backtest(
    *,
    symbol: str = "BTCUSDT",
    period: str = "5m",
    limit: int = 500,
    initial_equity: float = 10_000.0,
    notional_per_trade: float = 1_000.0,
    max_holding_periods: int = 30,
    max_drawdown_stop: float = 0.10,
    latency_periods: int = 1,
) -> dict[str, Any]:
    """
    Fetch live 5m kline and 5m OI data, run the aligned OI Box + Microstructure strategy.
    """
    from research.microstructure_alpha.strategy import (
        MicrostructureAlphaStrategy,
        StrategyConfig,
        fetch_klines,
        fetch_open_interest,
        align_klines_and_oi,
    )

    # 1. Fetch data
    klines = fetch_klines(symbol=symbol, interval=period, limit=limit)
    oi_hist = fetch_open_interest(symbol=symbol, period=period, limit=limit)
    if len(klines) < 120 or len(oi_hist) < 120:
        return {
            "error": "Insufficient data",
            "klines": len(klines),
            "oi_hist": len(oi_hist),
        }

    # 2. Generate signals
    config = StrategyConfig(symbol=symbol)
    strategy = MicrostructureAlphaStrategy(config)
    signals = strategy.generate_signals_with_oi_box(klines, oi_hist)
    aligned = align_klines_and_oi(klines, oi_hist)

    if len(signals) != len(aligned):
        # Slice aligned to match signal length if there's any mismatch due to start windows
        aligned = aligned[-len(signals):]

    # 3. Build backtest points
    points = [_point_from_kline(aligned[i], sig.direction) for i, sig in enumerate(signals)]

    # 4. Run backtest with risk controls
    cost_model = CostModel(
        taker_fee_bps=4.0,
        slippage_bps=1.0,
        sqrt_impact_coeff=0.001,
    )
    result = run_directional_backtest(
        points,
        initial_equity=initial_equity,
        notional_per_trade=notional_per_trade,
        cost_model=cost_model,
        max_holding_periods=max_holding_periods,
        max_drawdown_stop=max_drawdown_stop,
        latency_periods=latency_periods,
        default_spread_bps=1.0,
    )

    metrics = compute_all_metrics(result.equity_curve, result.trade_pnls)

    # Regime distribution
    regime_counts: dict[str, int] = {}
    for sig in signals:
        regime_counts[sig.regime] = regime_counts.get(sig.regime, 0) + 1

    return {
        "symbol": symbol,
        "period": period,
        "aligned_points": len(aligned),
        "signals_generated": len(signals),
        "metrics": metrics,
        "trade_count": result.trade_count,
        "forced_exits": result.forced_exits,
        "total_cost_paid": result.total_cost_paid,
        "latency_periods": result.latency_periods,
        "regime_distribution": regime_counts,
        "final_equity": result.equity_curve[-1],
    }


def run_parameter_sweep_backtest(
    *,
    symbol: str = "BTCUSDT",
    interval: str = "1m",
    limit: int = 1000,
) -> dict[str, Any]:
    from research.microstructure_alpha.features import TradeBucket, normalized_trade_imbalance
    from research.microstructure_alpha.strategy import fetch_klines

    klines = fetch_klines(symbol=symbol, interval=interval, limit=limit)
    if len(klines) < 120:
        return {"error": "Insufficient data", "klines_fetched": len(klines)}

    prices = [k["close"] for k in klines]
    timestamps = [k["open_time"] for k in klines]
    imbalances = [
        normalized_trade_imbalance(
            TradeBucket(
                buy_taker_qty=k["taker_buy_volume"],
                sell_taker_qty=k["taker_sell_volume"],
            )
        )
        for k in klines
    ]
    rows = run_cost_latency_sweep(
        prices=prices,
        timestamps=timestamps,
        imbalances=imbalances,
    )
    top_rows = sorted(rows, key=lambda row: (row.sharpe, row.total_return), reverse=True)[:10]
    return {
        "symbol": symbol,
        "interval": interval,
        "data_points": len(klines),
        "rows": [asdict(row) for row in top_rows],
    }


def _format_metrics(metrics: dict[str, float]) -> str:
    """Format metrics dict for human-readable output."""
    lines = []
    for k, v in metrics.items():
        if "return" in k or "drawdown" in k:
            lines.append(f"  {k:30s}: {v:+.4%}")
        elif "ratio" in k or "sharpe" in k or "sortino" in k or "calmar" in k:
            lines.append(f"  {k:30s}: {v:+.4f}")
        else:
            lines.append(f"  {k:30s}: {v:.4f}")
    return "\n".join(lines)


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "demo"

    if mode == "demo":
        print("=== Demo Backtest ===")
        result = run_demo_backtest()
        print(_format_metrics(result))

    elif mode == "live":
        print("=== Live Data Backtest (Binance BTCUSDT 1m) ===")
        result = run_live_data_backtest()
        if "error" in result:
            print(f"Error: {result['error']}")
        else:
            print(f"Data points: {result['data_points']}")
            print(f"Trades: {result['trade_count']} (forced exits: {result['forced_exits']})")
            print(f"Total cost paid: ${result['total_cost_paid']:.2f}")
            print(f"Latency periods: {result['latency_periods']}")
            print(f"Regime distribution: {result['regime_distribution']}")
            print(f"Final equity: ${result['final_equity']:.2f}")
            print("\nMetrics:")
            print(_format_metrics(result["metrics"]))

    elif mode == "walkforward":
        print("=== Walk-Forward Backtest (Binance BTCUSDT 1m) ===")
        result = run_walk_forward_backtest()
        if "error" in result:
            print(f"Error: {result['error']}")
        else:
            print(f"Data points: {result['data_points']}")
            print(f"\nFold Details:")
            for f in result["folds"]:
                print(
                    f"  Fold {f['fold']}: threshold={f['best_threshold']:.2f}"
                    f"  train_sharpe={f['train_sharpe']:+.3f}"
                    f"  test_sharpe={f['test_sharpe']:+.3f}"
                    f"  test_return={f['test_return']:+.4%}"
                )
            print(f"\nOOS Summary:")
            print(_format_metrics(result["summary"]))

    elif mode == "oi":
        print("=== Aligned OI Box + Microstructure Backtest (Binance BTCUSDT 5m) ===")
        result = run_oi_box_backtest()
        if "error" in result:
            print(f"Error: {result['error']}")
        else:
            print(f"Aligned Data points: {result['aligned_points']}")
            print(f"Signals generated: {result['signals_generated']}")
            print(f"Trades: {result['trade_count']} (forced exits: {result['forced_exits']})")
            print(f"Total cost paid: ${result['total_cost_paid']:.2f}")
            print(f"Latency periods: {result['latency_periods']}")
            print(f"Regime distribution: {result['regime_distribution']}")
            print(f"Final equity: ${result['final_equity']:.2f}")
            print("\nMetrics:")
            print(_format_metrics(result["metrics"]))

    elif mode == "sweep":
        print("=== Cost/Latency Parameter Sweep (Binance BTCUSDT 1m) ===")
        result = run_parameter_sweep_backtest()
        if "error" in result:
            print(f"Error: {result['error']}")
        else:
            print(f"Data points: {result['data_points']}")
            for row in result["rows"]:
                print(
                    f"threshold={row['threshold']:.2f} "
                    f"slip={row['slippage_bps']:.1f}bps "
                    f"latency={row['latency_periods']} "
                    f"ret={row['total_return']:+.4%} "
                    f"sharpe={row['sharpe']:+.3f} "
                    f"mdd={row['max_drawdown']:+.4%} "
                    f"trades={row['total_trades']:.0f} "
                    f"cost=${row['total_cost_paid']:.2f}"
                )

    else:
        print(f"Unknown mode: {mode}. Use: demo, live, walkforward, oi, sweep")

