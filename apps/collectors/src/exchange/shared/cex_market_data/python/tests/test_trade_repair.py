from __future__ import annotations

from cex_market_data_collector.trade_repair import TradeRepairState


def test_trade_repair_state_detects_numeric_gap() -> None:
    state = TradeRepairState()

    assert state.observe(
        {
            "data_type": "trade",
            "exchange": "okx",
            "symbol": "BTC-USDT-SWAP",
            "trade_id": "100",
            "exchange_ts": 1_700_000_000_000,
        }
    ) is None
    gap = state.observe(
        {
            "data_type": "trade",
            "exchange": "okx",
            "symbol": "BTC-USDT-SWAP",
            "trade_id": "103",
            "exchange_ts": 1_700_000_000_100,
        }
    )

    assert gap is not None
    assert gap.from_trade_id == 101
    assert gap.to_trade_id == 102
    assert gap.last_exchange_ts == 1_700_000_000_000


def test_trade_repair_state_ignores_non_numeric_ids() -> None:
    state = TradeRepairState()

    state.observe(
        {
            "data_type": "trade",
            "exchange": "bybit",
            "symbol": "BTCUSDT",
            "trade_id": "abc",
            "exchange_ts": 1_700_000_000_000,
        }
    )
    assert state.observe(
        {
            "data_type": "trade",
            "exchange": "bybit",
            "symbol": "BTCUSDT",
            "trade_id": "def",
            "exchange_ts": 1_700_000_000_100,
        }
    ) is None


def test_trade_repair_state_detects_stream_resume_time_gap_for_non_numeric_ids() -> None:
    state = TradeRepairState()
    state.observe(
        {
            "data_type": "trade",
            "exchange": "bybit",
            "symbol": "BTCUSDT",
            "trade_id": "abc",
            "exchange_ts": 1_700_000_000_000,
        }
    )
    state.mark_stream_interrupted()
    gap = state.observe(
        {
            "data_type": "trade",
            "exchange": "bybit",
            "symbol": "BTCUSDT",
            "trade_id": "def",
            "exchange_ts": 1_700_000_010_000,
        }
    )

    assert gap is not None
    assert gap.from_trade_id is None
    assert gap.to_trade_id is None
    assert gap.reason == "stream_resume_time_gap"
    assert gap.last_exchange_ts == 1_700_000_000_000
