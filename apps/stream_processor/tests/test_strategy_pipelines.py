from __future__ import annotations

import pytest

from schemas.signal import SignalDirection
from stream_processor.pipelines.order_intent_pipeline import signal_to_order_intent
from stream_processor.pipelines.signal_pipeline import publish_signals
from strategies.btc_oi_trend import BtcOiTrendStrategy
from strategies.btc_price_oi_box import BtcPriceOiBoxStrategy
from strategies.registry import StrategyRegistry, build_default_strategy_registry


class DummyProducer:
    def __init__(self) -> None:
        self.messages: list[tuple[str, dict]] = []

    async def produce(self, key: str, value: dict) -> None:
        self.messages.append((key, value))


class DisabledStrategyControlRepo:
    async def is_enabled(self, strategy_name: str, default: bool = True) -> bool:
        return False


def _trade(price: str, ts: int) -> dict:
    return {
        "exchange": "binance",
        "market_type": "perp",
        "data_type": "trade",
        "symbol": "BTCUSDT",
        "price": price,
        "size": "0.1",
        "exchange_ts": ts,
        "local_ts": ts,
    }


def _oi(value: str, ts: int) -> dict:
    return {
        "exchange": "binance",
        "market_type": "perp",
        "data_type": "open_interest",
        "symbol": "BTCUSDT",
        "open_interest": value,
        "exchange_ts": ts,
        "local_ts": ts,
    }


def test_btc_oi_trend_strategy_emits_long_signal_after_price_and_oi_confirm() -> None:
    strategy = BtcOiTrendStrategy(
        window_size=3,
        min_price_move_bps=5,
        min_oi_move_bps=2,
        cooldown_ms=0,
    )

    events = [
        _trade("100", 1_700_000_000_000),
        _oi("1000", 1_700_000_000_000),
        _trade("100.02", 1_700_000_001_000),
        _oi("1000.1", 1_700_000_001_000),
        _trade("100.10", 1_700_000_002_000),
        _oi("1001", 1_700_000_002_000),
    ]

    signals = []
    for event in events:
        signals.extend(strategy.on_market_event(event))

    assert len(signals) == 1
    assert signals[0].direction == SignalDirection.LONG
    assert signals[0].suggested_side == "BUY"
    assert signals[0].strategy_name == "btc_oi_trend_v1"


def test_btc_price_oi_box_strategy_emits_range_bounce_signal() -> None:
    strategy = BtcPriceOiBoxStrategy(
        window_size=6,
        min_box_points=5,
        entry_edge_ratio=1,
        bounce_bars=1,
        bounce_confirm_bps=1,
        cooldown_ms=1,
    )

    events = [
        _trade("100", 1),
        _oi("1000", 1),
        _trade("99", 2),
        _oi("1001", 2),
        _trade("101", 3),
        _oi("999", 3),
        _trade("99", 4),
        _oi("1000", 4),
        _trade("99", 5),
        _oi("1000", 5),
        _trade("99.3", 6),
        _oi("1000.5", 6),
    ]

    signals = []
    for event in events:
        signals.extend(strategy.on_market_event(event))

    assert len(signals) == 1
    assert signals[0].direction == SignalDirection.LONG
    assert signals[0].suggested_side == "BUY"
    assert signals[0].strategy_name == "btc_price_oi_box_v1"
    assert signals[0].suggested_entry_price is not None
    assert signals[0].suggested_stop_loss is not None
    assert signals[0].suggested_take_profit is not None


def test_btc_price_oi_box_strategy_emits_breakout_trend_signal() -> None:
    strategy = BtcPriceOiBoxStrategy(
        window_size=6,
        min_box_points=5,
        entry_edge_ratio=0,
        breakout_buffer_bps=5,
        min_trend_momentum_bps=5,
        oi_breakout_buffer_bps=5,
        cooldown_ms=0,
    )

    events = [
        _trade("100", 1),
        _oi("1000", 1),
        _trade("100.1", 2),
        _oi("1000.5", 2),
        _trade("100.2", 3),
        _oi("1000.2", 3),
        _trade("100.1", 4),
        _oi("999.8", 4),
        _trade("100.2", 5),
        _oi("1000", 5),
        _trade("102", 6),
        _oi("1010", 6),
    ]

    signals = []
    for event in events:
        signals.extend(strategy.on_market_event(event))

    assert len(signals) == 1
    assert signals[0].direction == SignalDirection.LONG
    assert signals[0].strategy_name == "btc_price_oi_box_v1"


def test_default_registry_registers_live_btc_strategies() -> None:
    registry = build_default_strategy_registry()

    assert [strategy.name for strategy in registry.strategies] == [
        "btc_oi_trend_v1",
        "btc_price_oi_box_v1",
    ]


@pytest.mark.asyncio
async def test_publish_signals_sends_strategy_signal_to_topic_payload() -> None:
    strategy = BtcPriceOiBoxStrategy(
        window_size=6,
        min_box_points=5,
        entry_edge_ratio=1,
        bounce_bars=1,
        bounce_confirm_bps=1,
        cooldown_ms=1,
    )
    registry = StrategyRegistry([strategy])
    producer = DummyProducer()

    events = [
        _trade("100", 1),
        _oi("1000", 1),
        _trade("99", 2),
        _oi("1001", 2),
        _trade("101", 3),
        _oi("999", 3),
        _trade("99", 4),
        _oi("1000", 4),
        _trade("99", 5),
        _oi("1000", 5),
        _trade("99.3", 6),
    ]
    payloads = []
    for event in events:
        payloads = await publish_signals(event=event, registry=registry, producer=producer)

    assert len(payloads) == 1
    assert len(producer.messages) == 1
    key, payload = producer.messages[0]
    assert key == payload["signal_id"]
    assert payload["suggested_order_type"] == "MARKET"
    assert payload["strategy_name"] == "btc_price_oi_box_v1"
    assert payload["suggested_entry_price"] is not None
    assert payload["suggested_stop_loss"] is not None
    assert payload["suggested_take_profit"] is not None


@pytest.mark.asyncio
async def test_publish_signals_suppresses_disabled_strategy() -> None:
    strategy = BtcPriceOiBoxStrategy(
        window_size=6,
        min_box_points=5,
        entry_edge_ratio=1,
        bounce_bars=1,
        bounce_confirm_bps=1,
        cooldown_ms=1,
    )
    registry = StrategyRegistry([strategy])
    producer = DummyProducer()

    events = [
        _trade("100", 1),
        _oi("1000", 1),
        _trade("99", 2),
        _oi("1001", 2),
        _trade("101", 3),
        _oi("999", 3),
        _trade("99", 4),
        _oi("1000", 4),
        _trade("99", 5),
        _oi("1000", 5),
        _trade("99.3", 6),
    ]
    payloads = []
    for event in events:
        payloads.extend(
            await publish_signals(
                event=event,
                registry=registry,
                producer=producer,
                strategy_control_repo=DisabledStrategyControlRepo(),
            )
        )

    assert payloads == []
    assert producer.messages == []


def test_signal_to_order_intent_builds_order_request_payload() -> None:
    strategy = BtcOiTrendStrategy(
        window_size=2,
        min_price_move_bps=1,
        min_oi_move_bps=1,
        cooldown_ms=0,
    )
    strategy.on_market_event(_trade("100", 1))
    strategy.on_market_event(_oi("1000", 1))
    strategy.on_market_event(_trade("100.02", 2))
    signal = strategy.on_market_event(_oi("1001", 2))[0]
    signal.suggested_entry_price = "100.02"
    signal.suggested_stop_loss = "99.90"
    signal.suggested_take_profit = "100.30"

    intent = signal_to_order_intent(signal)

    assert intent is not None
    assert intent["signal_id"] == signal.signal_id
    assert intent["strategy_name"] == "btc_oi_trend_v1"
    assert intent["exchange"] == "BINANCE"
    assert intent["side"] == "BUY"
    assert intent["order_type"] == "MARKET"
    assert intent["position_action"] == "OPEN"
    assert intent["entry_price"] == "100.02"
    assert intent["stop_loss_price"] == "99.90"
    assert intent["take_profit_price"] == "100.30"
