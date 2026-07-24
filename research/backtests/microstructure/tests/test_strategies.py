from research.backtests.microstructure.binance_trade_csv import TradeBar, bars_to_backtest_points
from research.backtests.microstructure.strategies.box_reversion import generate_box_reversion_signals
from research.backtests.microstructure.strategies.manual_memory_box import (
    ManualMemoryBox,
    generate_manual_memory_box_signals,
    nearest_memory_line,
)
from research.backtests.microstructure.strategies.market_memory_reversion import (
    MarketMemoryReversionConfig,
    generate_market_memory_reversion_details,
    generate_market_memory_reversion_signals,
)
from research.backtests.microstructure.strategies.moving_average_crossover import (
    generate_moving_average_crossover_signals,
)
from research.backtests.microstructure.strategies.taker_imbalance import generate_taker_imbalance_signals


def _bar(
    timestamp: int,
    price: float,
    *,
    buy_quote: float = 50.0,
    sell_quote: float = 50.0,
) -> TradeBar:
    return TradeBar(
        bucket_start_ms=timestamp,
        close_price=price,
        quote_volume=buy_quote + sell_quote,
        taker_buy_quote_volume=buy_quote,
        taker_sell_quote_volume=sell_quote,
        trade_count=10,
    )


def test_taker_imbalance_strategy_generates_directional_signals() -> None:
    bars = [
        _bar(1, 100.0, buy_quote=70.0, sell_quote=30.0),
        _bar(2, 101.0, buy_quote=50.0, sell_quote=50.0),
        _bar(3, 99.0, buy_quote=25.0, sell_quote=75.0),
    ]

    signals = generate_taker_imbalance_signals(bars, threshold=0.20, force_flat_last=False)

    assert signals == [1, 0, -1]


def test_box_reversion_strategy_uses_box_edge_and_flow_confirmation() -> None:
    bars = [
        _bar(1, 100.0),
        _bar(2, 101.0),
        _bar(3, 99.0, buy_quote=65.0, sell_quote=35.0),
        _bar(4, 100.0),
        _bar(5, 99.0),
        _bar(6, 102.0, buy_quote=35.0, sell_quote=65.0),
    ]

    signals = generate_box_reversion_signals(
        bars,
        lookback=3,
        edge_threshold=0.15,
        flow_threshold=0.10,
        force_flat_last=False,
    )

    assert signals == [0, 0, 1, 0, 0, -1]


def test_bars_to_backtest_points_can_use_box_reversion_strategy() -> None:
    bars = [
        _bar(1, 100.0),
        _bar(2, 101.0),
        _bar(3, 99.0, buy_quote=65.0, sell_quote=35.0),
    ]

    signals = generate_box_reversion_signals(
        bars,
        lookback=3,
        flow_threshold=0.10,
        force_flat_last=False,
    )
    points = bars_to_backtest_points(
        bars,
        signals=signals,
        force_flat_last=False,
    )

    assert [point.signal for point in points] == [0, 0, 1]


def test_manual_memory_box_sorts_user_price_lines() -> None:
    box = ManualMemoryBox.from_lines([103.5, 99.5, 102.0, 100.8, 105.0])

    assert box.min_line == 99.5
    assert box.max_line == 105.0
    assert box.internal_lines == (100.8, 102.0, 103.5)
    assert nearest_memory_line(102.2, box.lines) == 102.0


def test_manual_memory_box_generates_edge_reversion_signals() -> None:
    bars = [
        _bar(1, 99.55, buy_quote=65.0, sell_quote=35.0),
        _bar(2, 102.0, buy_quote=80.0, sell_quote=20.0),
        _bar(3, 104.95, buy_quote=35.0, sell_quote=65.0),
        _bar(4, 106.0, buy_quote=35.0, sell_quote=65.0),
    ]

    signals = generate_manual_memory_box_signals(
        bars,
        lines=[99.5, 100.8, 102.0, 103.5, 105.0],
        line_tolerance_bps=8.0,
        flow_threshold=0.10,
        force_flat_last=False,
    )

    assert signals == [1, 0, -1, 0]


def test_manual_memory_box_can_trade_internal_lines_when_enabled() -> None:
    bars = [
        _bar(1, 102.0, buy_quote=70.0, sell_quote=30.0),
        _bar(2, 103.5, buy_quote=30.0, sell_quote=70.0),
    ]

    signals = generate_manual_memory_box_signals(
        bars,
        lines=[99.5, 100.8, 102.0, 103.5, 105.0],
        line_tolerance_bps=5.0,
        flow_threshold=0.10,
        trade_internal_lines=True,
        force_flat_last=False,
    )

    assert signals == [1, -1]


def test_market_memory_reversion_requires_edge_rejection_and_flow() -> None:
    bars = [
        _bar(1, 100.00, buy_quote=50.0, sell_quote=50.0),
        _bar(2, 99.55, buy_quote=65.0, sell_quote=35.0),
        _bar(3, 99.62, buy_quote=70.0, sell_quote=30.0),
        _bar(4, 104.98, buy_quote=35.0, sell_quote=65.0),
        _bar(5, 104.90, buy_quote=30.0, sell_quote=70.0),
    ]
    config = MarketMemoryReversionConfig.from_lines(
        [99.5, 100.8, 102.0, 103.5, 105.0],
        line_tolerance_bps=15.0,
        flow_threshold=0.10,
        min_rejection_bps=2.0,
        cooldown_bars=0,
        force_flat_last=False,
    )

    signals = generate_market_memory_reversion_signals(bars, config=config)

    assert signals == [0, 0, 1, 0, -1]


def test_market_memory_reversion_reports_explainable_signal_details() -> None:
    bars = [
        _bar(1, 99.45, buy_quote=50.0, sell_quote=50.0),
        _bar(2, 99.60, buy_quote=70.0, sell_quote=30.0),
    ]
    config = MarketMemoryReversionConfig.from_lines(
        [99.5, 102.0, 105.0],
        line_tolerance_bps=15.0,
        flow_threshold=0.10,
        min_rejection_bps=2.0,
        force_flat_last=False,
    )

    details = generate_market_memory_reversion_details(bars, config=config)

    assert details[-1].signal == 1
    assert details[-1].nearest_line == 99.5
    assert details[-1].line_role == "support"
    assert details[-1].reason == "support_rejection_with_buy_flow"


def test_market_memory_reversion_cooldown_suppresses_repeated_signals() -> None:
    bars = [
        _bar(1, 99.45, buy_quote=50.0, sell_quote=50.0),
        _bar(2, 99.60, buy_quote=70.0, sell_quote=30.0),
        _bar(3, 99.70, buy_quote=70.0, sell_quote=30.0),
    ]
    config = MarketMemoryReversionConfig.from_lines(
        [99.5, 102.0, 105.0],
        line_tolerance_bps=25.0,
        flow_threshold=0.10,
        min_rejection_bps=1.0,
        cooldown_bars=1,
        force_flat_last=False,
    )

    signals = generate_market_memory_reversion_signals(bars, config=config)

    assert signals == [0, 1, 0]


def test_moving_average_crossover_generates_trend_following_targets() -> None:
    bars = [_bar(idx, float(price)) for idx, price in enumerate([100, 101, 102, 103, 104, 105], start=1)]

    signals = generate_moving_average_crossover_signals(
        bars,
        fast_window=2,
        slow_window=3,
        neutral_band_bps=0.0,
        force_flat_last=False,
    )

    assert signals == [0, 0, 1, 1, 1, 1]


def test_moving_average_crossover_can_flip_short() -> None:
    bars = [_bar(idx, float(price)) for idx, price in enumerate([105, 104, 103, 102, 101, 100], start=1)]

    signals = generate_moving_average_crossover_signals(
        bars,
        fast_window=2,
        slow_window=3,
        neutral_band_bps=0.0,
        force_flat_last=False,
    )

    assert signals == [0, 0, -1, -1, -1, -1]
