# Research Simulations

This directory is reserved for simulation studies that complement historical
backtests.

Planned studies:

- Markov regime transition simulation
- Stress scenarios such as flash crash, spread widening, liquidity drought, and
  latency spike
- Monte Carlo drawdown and ruin-risk simulation
- Risk sizing sensitivity analysis

Executable simulation helpers currently live with the microstructure backtest
engine under `research/backtests/microstructure/`:

- `regime_model.py`
- `markov_simulator.py`
- `stress_scenarios.py`
- `risk_sizing_comparison.py`
