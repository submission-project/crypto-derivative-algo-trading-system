import pytest
from research.microstructure_alpha.features import (
    density_matrix_determinant,
    density_matrix_eigenvalues,
    density_matrix_entropy,
    density_matrix_from_components,
    density_matrix_purity,
    density_matrix_trace,
    hurst_exponent,
    is_positive_semidefinite_density_matrix,
    vpin,
    transfer_entropy,
    rolling_min_max_channel,
    buyer_taker_density,
    market_quantum_density_matrix,
    market_quantum_density_matrix_states,
    rolling_period_volatility_bps,
)
from research.microstructure_alpha.strategy import (
    StrategyConfig,
    MicrostructureAlphaStrategy,
    StrategySignal,
)
from research.backtests.microstructure.simulator import (
    BacktestPoint,
    run_directional_backtest,
)
from research.backtests.microstructure.cost_model import CostModel
from research.backtests.microstructure.metrics import compute_all_metrics


def test_hurst_exponent():
    # Trending series: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10...
    prices = [10.0 + i * 0.5 for i in range(30)]
    h = hurst_exponent(prices, max_lag=10)
    assert 0.0 <= h <= 2.0


def test_vpin():
    buy_vols = [10.0] * 10
    sell_vols = [5.0] * 10
    result = vpin(buy_vols, sell_vols, window=5)
    assert len(result) == 10
    assert result[-1] == pytest.approx(1.0 / 3.0)


def test_transfer_entropy():
    source = [1.0, 2.0, 1.5, 3.0, 2.5, 4.0, 3.5, 5.0]
    target = [1.1, 2.1, 1.6, 3.1, 2.6, 4.1, 3.6, 5.1]
    te = transfer_entropy(source, target, delay=1, bins=3)
    assert te >= 0.0


def test_rolling_min_max_channel():
    values = [10.0, 12.0, 8.0, 11.0, 15.0]
    result = rolling_min_max_channel(values, window=3, noise_percent=0.1)
    assert len(result) == 5
    assert result[0] is None
    # Last window: [8.0, 11.0, 15.0] -> min=8.0, max=15.0, range=7.0
    # Expected Lower: 8.0 + 0.7 = 8.7
    # Expected Upper: 15.0 - 0.7 = 14.3
    assert result[-1] == pytest.approx((8.7, 14.3))


def test_buyer_taker_density():
    # is_buyer_maker=True (sell-initiated), False (buyer-initiated)
    events = [True, False, False, True, False]
    density = buyer_taker_density(events, window=3)
    assert len(density) == 5
    # Last window: [False, True, False] -> 2 buyer-initiated
    assert density[-1] == pytest.approx(2.0 / 3.0)


def test_market_quantum_density_matrix():
    vols = [1.0, 10.0, 2.0]  # in bps
    imbalances = [0.1, -0.5, 0.2]
    result = market_quantum_density_matrix(vols, imbalances, volatility_threshold_bps=5.0)
    assert len(result) == 3
    # Check stable/impulse probabilities sum to 1
    # pyrefly: ignore [not-iterable]
    p_box, p_impulse, coherence = result[1]
    assert p_box + p_impulse == pytest.approx(1.0)


def test_density_matrix_operations():
    matrix = density_matrix_from_components(0.6, 0.4, 0.2)

    assert density_matrix_trace(matrix) == pytest.approx(1.0)
    assert density_matrix_determinant(matrix) == pytest.approx(0.20)
    low, high = density_matrix_eigenvalues(matrix)
    assert low == pytest.approx(0.27639320225)
    assert high == pytest.approx(0.72360679775)
    assert density_matrix_purity(matrix) == pytest.approx(0.60)
    assert density_matrix_entropy(matrix) > 0.0
    assert is_positive_semidefinite_density_matrix(matrix)


def test_market_quantum_density_matrix_states():
    vols = [None, 1.0, 10.0]
    imbalances = [0.0, 0.1, -0.5]
    states = market_quantum_density_matrix_states(vols, imbalances, volatility_threshold_bps=5.0)

    assert len(states) == 3
    assert states[0] is None
    assert states[1] is not None
    assert states[2] is not None

    first_state = states[1]
    second_state = states[2]
    assert first_state.trace == pytest.approx(1.0)
    assert first_state.is_positive_semidefinite
    assert first_state.dominant_state == "box"
    assert second_state.dominant_state == "impulse"
    assert 0.0 <= first_state.purity <= 1.0
    assert first_state.entropy >= 0.0


def test_rolling_period_volatility_bps():
    prices = [100.0, 101.0, 100.5, 101.2, 100.8]
    vols = rolling_period_volatility_bps(prices, window=3)
    assert len(vols) == 5
    assert vols[-1] is not None
    assert vols[-1] >= 0.0


def test_strategy_signal_generation():
    config = StrategyConfig(hurst_window=5)
    strategy = MicrostructureAlphaStrategy(config)
    klines = [
        {
            "open_time": i * 60000,
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0 + (i % 3 - 1) * 0.5,
            "volume": 1000.0,
            "close_time": i * 60000 + 59999,
            "quote_volume": 100000.0,
            "trades": 50,
            "taker_buy_volume": 600.0 if i % 2 == 0 else 400.0,
            "taker_sell_volume": 400.0 if i % 2 == 0 else 600.0,
        }
        for i in range(20)
    ]
    signals = strategy.generate_signals_from_klines(klines)
    assert len(signals) == 20
    assert isinstance(signals[0], StrategySignal)


def test_directional_backtest():
    points = [
        BacktestPoint(timestamp=1, price=100.0, signal=1),
        BacktestPoint(timestamp=2, price=101.0, signal=1),
        BacktestPoint(timestamp=3, price=99.0, signal=-1),
        BacktestPoint(timestamp=4, price=98.0, signal=0),
    ]
    result = run_directional_backtest(
        points,
        initial_equity=10000.0,
        notional_per_trade=1000.0,
        cost_model=CostModel(taker_fee_bps=0.0, slippage_bps=0.0),
    )
    assert len(result.equity_curve) == 5
    metrics = compute_all_metrics(result.equity_curve, result.trade_pnls)
    assert "sharpe" in metrics
