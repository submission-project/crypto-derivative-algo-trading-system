from __future__ import annotations

from cex_market_data_collector.operational_adapters import (
    DEFAULT_OPERATIONAL_EXCHANGES,
    build_operational_specs,
)
from cex_market_data_collector.operational_runtime import _loads_message


def _first_ws(exchange: str):
    spec = build_operational_specs((exchange,))[0]
    assert spec.websocket_specs
    return spec.websocket_specs[0]


def test_operational_specs_cover_requested_exchanges() -> None:
    specs = build_operational_specs(DEFAULT_OPERATIONAL_EXCHANGES)

    assert {spec.exchange for spec in specs} == set(DEFAULT_OPERATIONAL_EXCHANGES)
    assert all(
        spec.rest_poll_specs or spec.websocket_specs[0].data_type == "trade_orderbook_oi"
        for spec in specs
        if spec.websocket_specs
    )


def test_operational_specs_use_configurable_market_topics(monkeypatch) -> None:
    monkeypatch.setenv("MARKET_TOPIC_PREFIX", "research.market")
    monkeypatch.setenv("MARKET_TOPIC_MARKET_TYPE", "swap")

    spec = build_operational_specs(("okx",), rest_oi_fallback=True)[0]

    assert spec.rest_poll_specs[0].topic == "research.market.open_interest.okx.swap"
    assert spec.websocket_specs[0].topic == "research.market.mixed.okx.swap"


def test_core_exchanges_have_trade_orderbook_websocket_specs() -> None:
    specs = build_operational_specs(
        ("binance", "bybit", "okx", "bitget", "gate", "mexc", "kraken", "htx")
    )

    assert all(spec.websocket_specs for spec in specs)


def test_core_non_binance_websocket_specs_have_rest_trade_repair() -> None:
    specs = build_operational_specs(("bybit", "okx", "bitget"))

    for spec in specs:
        assert spec.websocket_specs[0].trade_repair is not None


def test_rest_oi_fallback_can_be_disabled_for_ws_oi_exchanges() -> None:
    specs = build_operational_specs(
        ("bybit", "okx", "bitget", "gate", "mexc", "bitfinex", "kraken", "binance"),
        rest_oi_fallback=False,
    )
    by_exchange = {spec.exchange: spec for spec in specs}

    for exchange in ("bybit", "okx", "bitget", "gate", "mexc", "bitfinex", "kraken"):
        assert by_exchange[exchange].websocket_specs[0].data_type == "trade_orderbook_oi"
        assert by_exchange[exchange].rest_poll_specs == ()

    assert by_exchange["binance"].websocket_specs[0].data_type == "trade_orderbook"
    assert by_exchange["binance"].rest_poll_specs


def test_binance_websocket_normalizer_emits_trade_and_orderbook() -> None:
    spec = _first_ws("binance")
    assert spec.normalizer is not None

    trade = spec.normalizer(
        {
            "stream": "btcusdt@trade",
            "data": {
                "e": "trade",
                "s": "BTCUSDT",
                "t": 1,
                "p": "65000.0",
                "q": "0.1",
                "m": False,
                "T": 1_700_000_000_000,
            },
        }
    )[0]
    depth = spec.normalizer(
        {
            "stream": "btcusdt@depth20@100ms",
            "data": {
                "e": "depthUpdate",
                "s": "BTCUSDT",
                "b": [["65000.0", "1.0"]],
                "a": [["65001.0", "2.0"]],
                "u": 10,
                "T": 1_700_000_000_001,
            },
        }
    )[0]

    assert trade["data_type"] == "trade"
    assert trade["side"] == "buy"
    assert depth["data_type"] == "orderbook"
    assert depth["bids"][0] == {"price": "65000.0", "size": "1.0"}


def test_bybit_websocket_normalizer_emits_trade_and_orderbook() -> None:
    spec = _first_ws("bybit")
    assert spec.normalizer is not None

    trade = spec.normalizer(
        {
            "topic": "publicTrade.BTCUSDT",
            "data": [
                {
                    "s": "BTCUSDT",
                    "i": "abc",
                    "p": "65000.0",
                    "v": "0.1",
                    "S": "Buy",
                    "T": 1_700_000_000_000,
                }
            ],
        }
    )[0]
    depth = spec.normalizer(
        {
            "topic": "orderbook.50.BTCUSDT",
            "data": {
                "s": "BTCUSDT",
                "b": [["65000.0", "1.0"]],
                "a": [["65001.0", "2.0"]],
                "u": 10,
                "ts": 1_700_000_000_001,
            },
        }
    )[0]

    assert trade["trade_id"] == "abc"
    assert depth["sequence"] == "10"


def test_bybit_websocket_normalizer_emits_open_interest_from_ticker() -> None:
    spec = _first_ws("bybit")
    assert spec.normalizer is not None

    oi = spec.normalizer(
        {
            "topic": "tickers.BTCUSDT",
            "ts": 1_700_000_000_002,
            "data": {
                "symbol": "BTCUSDT",
                "openInterest": "100.0",
                "openInterestValue": "6500000.0",
            },
        }
    )[0]

    assert oi["data_type"] == "open_interest"
    assert oi["open_interest"] == "100.0"
    assert oi["open_interest_value_usd"] == "6500000.0"


def test_okx_websocket_normalizer_emits_trade_and_orderbook() -> None:
    spec = _first_ws("okx")
    assert spec.normalizer is not None

    trade = spec.normalizer(
        {
            "arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"},
            "data": [
                {
                    "instId": "BTC-USDT-SWAP",
                    "tradeId": "1",
                    "px": "65000.0",
                    "sz": "0.1",
                    "side": "buy",
                    "ts": "1700000000000",
                }
            ],
        }
    )[0]
    depth = spec.normalizer(
        {
            "arg": {"channel": "books5", "instId": "BTC-USDT-SWAP"},
            "data": [
                {
                    "bids": [["65000.0", "1.0", "0", "1"]],
                    "asks": [["65001.0", "2.0", "0", "1"]],
                    "ts": "1700000000001",
                    "seqId": 10,
                }
            ],
        }
    )[0]

    assert trade["symbol"] == "BTC-USDT-SWAP"
    assert depth["asks"][0]["price"] == "65001.0"


def test_okx_websocket_normalizer_emits_open_interest_channel() -> None:
    spec = _first_ws("okx")
    assert spec.normalizer is not None

    oi = spec.normalizer(
        {
            "arg": {"channel": "open-interest", "instId": "BTC-USDT-SWAP"},
            "data": [
                {
                    "instId": "BTC-USDT-SWAP",
                    "oi": "100",
                    "oiCcy": "10.0",
                    "oiUsd": "650000.0",
                    "ts": "1700000000002",
                }
            ],
        }
    )[0]

    assert oi["data_type"] == "open_interest"
    assert oi["symbol"] == "BTC-USDT-SWAP"
    assert oi["open_interest"] == "10.0"
    assert oi["open_interest_value_usd"] == "650000.0"


def test_bitget_websocket_normalizer_emits_open_interest_from_ticker() -> None:
    spec = _first_ws("bitget")
    assert spec.normalizer is not None

    oi = spec.normalizer(
        {
            "arg": {"instType": "USDT-FUTURES", "channel": "ticker", "instId": "BTCUSDT"},
            "data": [
                {
                    "instId": "BTCUSDT",
                    "openInterest": "25.0",
                    "openInterestValue": "1625000.0",
                    "markPrice": "65000.0",
                    "ts": "1700000000002",
                }
            ],
        }
    )[0]

    assert oi["data_type"] == "open_interest"
    assert oi["symbol"] == "BTCUSDT"
    assert oi["open_interest"] == "25.0"
    assert oi["open_interest_value_usd"] == "1625000.0"


def test_gate_websocket_normalizer_emits_open_interest_from_ticker() -> None:
    spec = _first_ws("gate")
    assert spec.normalizer is not None

    oi = spec.normalizer(
        {
            "channel": "futures.tickers",
            "time_ms": 1_700_000_000_002,
            "result": {
                "contract": "BTC_USDT",
                "total_size": "1000",
                "mark_price": "65000.0",
            },
        }
    )[0]

    assert oi["data_type"] == "open_interest"
    assert oi["symbol"] == "BTC_USDT"
    assert oi["open_interest"] == "1000"


def test_mexc_websocket_normalizer_emits_open_interest_from_ticker() -> None:
    spec = _first_ws("mexc")
    assert spec.normalizer is not None

    oi = spec.normalizer(
        {
            "channel": "push.ticker",
            "symbol": "BTC_USDT",
            "data": {
                "holdVol": "42",
                "fairPrice": "65000.0",
                "timestamp": 1_700_000_000_002,
            },
        }
    )[0]

    assert oi["data_type"] == "open_interest"
    assert oi["symbol"] == "BTC_USDT"
    assert oi["open_interest"] == "42"


def test_bitfinex_websocket_normalizer_emits_open_interest_from_status() -> None:
    spec = _first_ws("bitfinex")
    assert spec.normalizer is not None

    oi = spec.normalizer(
        [
            1,
            [
                1_700_000_000_002,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "65000.0",
                "123.45",
            ],
        ]
    )[0]

    assert oi["data_type"] == "open_interest"
    assert oi["symbol"] == "tBTCF0:USTF0"
    assert oi["open_interest"] == "123.45"


def test_kraken_websocket_normalizer_emits_open_interest_from_ticker() -> None:
    spec = _first_ws("kraken")
    assert spec.normalizer is not None

    oi = spec.normalizer(
        {
            "feed": "ticker",
            "product_id": "PF_XBTUSD",
            "time": 1_700_000_000_002,
            "openInterest": 2126.0737,
            "markPrice": 64410.3193,
        }
    )[0]

    assert oi["data_type"] == "open_interest"
    assert oi["symbol"] == "PF_XBTUSD"
    assert oi["open_interest"] == "2126.0737"


def test_gzip_message_loader_supports_htx_payloads() -> None:
    import gzip
    import orjson

    compressed = gzip.compress(orjson.dumps({"ping": 123}))

    assert _loads_message(compressed, gzip_binary=True) == {"ping": 123}
