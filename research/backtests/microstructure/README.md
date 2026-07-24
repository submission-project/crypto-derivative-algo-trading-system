# Microstructure Backtests

This package contains a small public-review backtesting scaffold for
microstructure signals.

The first implementation is intentionally simple:

- discrete time steps
- target position from signal
- bid/ask crossing for market-order fills
- signal-to-fill latency in discrete periods
- taker fee
- fixed slippage
- optional market-impact approximation
- equity and drawdown metrics
- cost/latency parameter sweeps
- Markov regime transition simulation
- stress scenarios
- risk sizing comparison
- separable strategy signal modules
- Binance trade CSV aggregation for local dataset smoke tests

It is designed to demonstrate a cost-aware validation process rather than to
replace a full production event simulator.

## Execution Realism

The public-review simulator now separates signal time from fill time. A signal
can be filled `N` periods later to approximate network/exchange latency. Market
orders cross the synthetic or supplied bid/ask spread:

- long entry: ask
- long exit: bid
- short entry: bid
- short exit: ask

The cost model then applies fees, slippage, participation-rate adjustments, and
an optional square-root market-impact term.

## Commands

```bash
uv run python -m research.backtests.microstructure.backtest demo
uv run python -m research.backtests.microstructure.backtest walkforward
uv run python -m research.backtests.microstructure.backtest sweep
uv run python -m research.backtests.microstructure.backtest oi
```

Real CSV smoke tests can receive one or more local files through `DATA_PATHS`.
Use a comma-separated list for make:

```bash
make run-backtest \
  PYTHON_PATH='research/backtests/microstructure/tests/test_binance_trade_csv.py' \
  DATA_PATHS='research/datasets/exchange/binance/assets/btcusdt/future/trade/daily/BTCUSDT-trades-2026-02-15.csv,research/datasets/exchange/binance/assets/btcusdt/future/trade/daily/BTCUSDT-trades-2026-02-16.csv'
```

Market-memory reversion evaluation also accepts manually chosen price lines:

```bash
make run-backtest \
  PYTHON_PATH='research/backtests/microstructure/tests/test_binance_trade_csv.py' \
  DATA_PATHS='research/datasets/exchange/binance/assets/btcusdt/future/trade/daily/BTCUSDT-trades-2026-02-15.csv' \
  MEMORY_LINES='95000,96000,97000,98000,99000' \
  LINE_TOLERANCE_BPS='8.0' \
  FLOW_THRESHOLD='0.05' \
  MIN_REJECTION_BPS='1.0' \
  COOLDOWN_BARS='3'
```

A standard moving-average crossover baseline can be evaluated without manual
price lines:

```bash
make run-backtest \
  PYTHON_PATH='research/backtests/microstructure/tests/test_binance_trade_csv.py::test_real_binance_trade_csv_moving_average_crossover_evaluation' \
  DATA_PATHS='research/datasets/exchange/binance/assets/btcusdt/future/trade/daily/BTCUSDT-trades-2026-02-15.csv' \
  MA_FAST_WINDOW='20' \
  MA_SLOW_WINDOW='80' \
  MA_NEUTRAL_BAND_BPS='2.0'
```

## Simulation Modules

- `strategies/taker_imbalance.py`: creates a directional signal from taker
  buy/sell quote-volume imbalance.
- `strategies/box_reversion.py`: creates a box-regime mean-reversion signal
  from rolling price range position plus flow confirmation.
- `strategies/manual_memory_box.py`: creates a manually anchored market-memory
  box signal from user-provided horizontal price lines.
- `strategies/market_memory_reversion.py`: implements an explainable
  support/resistance mean-reversion strategy using manually anchored memory
  lines, rejection confirmation, taker-flow confirmation, and signal cooldown.
- `strategies/moving_average_crossover.py`: implements a standard
  time-series momentum baseline using fast/slow moving-average crossover.
- `regime_model.py`: labels market states such as normal box, accumulation,
  distribution, breakout, and stress.
- `markov_simulator.py`: simulates future regime paths and drawdown
  distributions from an estimated transition matrix.
- `stress_scenarios.py`: applies flash-crash, spread-widening,
  liquidity-drought, and latency-spike assumptions.
- `risk_sizing_comparison.py`: compares fixed notional, fixed fractional,
  capped Kelly, and volatility-target sizing.
- `paper_trading_validation.py`: structures Binance testnet readiness checks.
- `binance_trade_csv.py`: aggregates local Binance trade CSV files into
  `TradeBar` records and converts externally generated signals into
  `BacktestPoint` rows for real-data smoke tests. The loader supports
  `engine="auto"`, `engine="polars"`, `engine="pandas"`, and `engine="python"`.
  `auto` prefers Polars for large tick files, falls back to pandas when
  available, and finally uses the dependency-free Python CSV reader.

Manual market-memory boxes are defined by explicit user price lines:

```python
memory_lines = [99.5, 100.8, 102.0, 103.5, 105.0]
signals = generate_manual_memory_box_signals(
    bars,
    lines=memory_lines,
    line_tolerance_bps=8.0,
    flow_threshold=0.05,
)
points = bars_to_backtest_points(bars, signals=signals)
```

The resume-ready strategy variant adds rejection and cooldown filters:

```python
config = MarketMemoryReversionConfig.from_lines(
    [99.5, 100.8, 102.0, 103.5, 105.0],
    line_tolerance_bps=8.0,
    flow_threshold=0.05,
    min_rejection_bps=1.0,
    cooldown_bars=3,
)
signals = generate_market_memory_reversion_signals(bars, config=config)
details = generate_market_memory_reversion_details(bars, config=config)
points = bars_to_backtest_points(bars, signals=signals)
```

Use `strategy_evaluation.py` to run the full strategy-to-performance loop:

```python
from research.backtests.microstructure.cost_model import CostModel
from research.backtests.microstructure.strategy_evaluation import (
    evaluate_market_memory_reversion_csv,
)

evaluation = evaluate_market_memory_reversion_csv(
    DATA_PATHS,
    lines=[99.5, 100.8, 102.0, 103.5, 105.0],
    bucket_ms=60_000,
    max_rows=1_000_000,
    engine="polars",
    line_tolerance_bps=8.0,
    flow_threshold=0.05,
    min_rejection_bps=1.0,
    cooldown_bars=3,
    cost_model=CostModel(taker_fee_bps=4.0, slippage_bps=1.0, sqrt_impact_coeff=0.001),
)

evaluation.metrics
evaluation.signal_counts
```
