from __future__ import annotations

import pytest

from execution_gateway.adapters.binance.mapper.binance_algo_event_mapper import (
    map_binance_algo_status,
    normalize_binance_algo_update,
)
from schemas.market import Exchange, MarketType
from schemas.order import ConditionalStatus

from execution_gateway.adapters.binance.constant.binance_constant import BinanceConditionalOrderState


def test_map_binance_algo_status_new() -> None:
    assert map_binance_algo_status(BinanceConditionalOrderState.new) == ConditionalStatus.NEW


def test_map_binance_algo_status_triggering_to_triggered() -> None:
    assert map_binance_algo_status(BinanceConditionalOrderState.triggering) == ConditionalStatus.TRIGGERED


def test_map_binance_algo_status_triggered() -> None:
    assert map_binance_algo_status(BinanceConditionalOrderState.triggered) == ConditionalStatus.TRIGGERED


def test_map_binance_algo_status_finished() -> None:
    assert map_binance_algo_status(BinanceConditionalOrderState.finished) == ConditionalStatus.FINISHED


def test_map_binance_algo_status_canceled_maps_to_cancelled() -> None:
    assert map_binance_algo_status(BinanceConditionalOrderState.canceled) == ConditionalStatus.CANCELLED


def test_map_binance_algo_status_unknown_when_empty() -> None:
    assert map_binance_algo_status(None) == ConditionalStatus.UNKNOWN
    assert map_binance_algo_status("") == ConditionalStatus.UNKNOWN


def test_map_binance_algo_status_unknown_when_unrecognized() -> None:
    assert map_binance_algo_status("SOMETHING_NEW") == ConditionalStatus.UNKNOWN


def test_normalize_binance_algo_update_triggered_event() -> None:
    raw = {
        "e": "ALGO_UPDATE",
        "E": 1_700_000_000_100,
        "T": 1_700_000_000_050,
        "o": {
            "s": "BTCUSDT",
            "X": "TRIGGERED",
            "caid": "TKSTOP001",
            "aid": 123456,
            "ai": 987654,
            "aq": "0.01",
            "ap": "59000",
        },
    }

    event = normalize_binance_algo_update(
        raw_event=raw,
        market_type=MarketType.PERP,
    )

    assert event.exchange == Exchange.BINANCE
    assert event.market_type == MarketType.PERP
    assert event.symbol == "BTCUSDT"

    assert event.client_conditional_id == "TKSTOP001"
    assert event.exchange_conditional_id == "123456"

    assert event.target_status == ConditionalStatus.TRIGGERED
    assert event.exchange_conditional_status == "TRIGGERED"

    assert event.triggered_order_id == "987654"
    assert event.filled_quantity == "0.01"
    assert event.avg_fill_price == "59000"

    assert event.event_time == 1_700_000_000_100
    assert event.transaction_time == 1_700_000_000_050
    assert event.raw == raw


def test_normalize_binance_algo_update_new_event_without_triggered_order_id() -> None:
    raw = {
        "e": "ALGO_UPDATE",
        "E": 1_700_000_000_100,
        "T": 1_700_000_000_050,
        "o": {
            "s": "BTCUSDT",
            "X": "NEW",
            "caid": "TKSTOP002",
            "aid": 123457,
            "ai": 0,
        },
    }

    event = normalize_binance_algo_update(
        raw_event=raw,
        market_type=MarketType.PERP,
    )

    assert event.symbol == "BTCUSDT"
    assert event.client_conditional_id == "TKSTOP002"
    assert event.exchange_conditional_id == "123457"
    assert event.target_status == ConditionalStatus.NEW
    assert event.triggered_order_id is None


def test_normalize_binance_algo_update_missing_order_payload_raises() -> None:
    raw = {
        "e": "ALGO_UPDATE",
        "E": 1_700_000_000_100,
        "T": 1_700_000_000_050,
        "o": None,
    }

    with pytest.raises(ValueError):
        normalize_binance_algo_update(raw_event=raw, market_type=MarketType.PERP)


def test_normalize_binance_algo_update_missing_symbol_raises() -> None:
    raw = {
        "e": "ALGO_UPDATE",
        "E": 1_700_000_000_100,
        "T": 1_700_000_000_050,
        "o": {
            "X": "NEW",
            "caid": "TKSTOP003",
            "aid": 123458,
        },
    }

    with pytest.raises(ValueError):
        normalize_binance_algo_update(raw_event=raw, market_type=MarketType.PERP)
