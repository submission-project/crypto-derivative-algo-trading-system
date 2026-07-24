from common.market_naming import (
    build_market_redis_stream_key,
    build_market_topic,
    csv_values,
    default_market_topics,
)


def test_build_market_topic_defaults_to_existing_operational_shape(monkeypatch):
    monkeypatch.delenv("MARKET_TOPIC_PREFIX", raising=False)
    monkeypatch.delenv("MARKET_TOPIC_MARKET_TYPE", raising=False)

    assert build_market_topic(exchange="Binance", data_type="mixed") == "market.mixed.binance.perp"


def test_build_market_topic_uses_env_prefix_and_market_type(monkeypatch):
    monkeypatch.setenv("MARKET_TOPIC_PREFIX", "research.market")
    monkeypatch.setenv("MARKET_TOPIC_MARKET_TYPE", "swap")

    assert build_market_topic(exchange="OKX", data_type="open_interest") == "research.market.open_interest.okx.swap"


def test_default_market_topics_uses_configured_exchange_list(monkeypatch):
    monkeypatch.setenv("MARKET_PIPELINE_EXCHANGES", "binance, okx,binance")
    monkeypatch.delenv("MARKET_TOPIC_PREFIX", raising=False)
    monkeypatch.delenv("MARKET_TOPIC_MARKET_TYPE", raising=False)

    assert default_market_topics() == (
        "market.mixed.binance.perp",
        "market.mixed.okx.perp",
        "market.open_interest.binance.perp",
        "market.open_interest.okx.perp",
    )


def test_build_market_redis_stream_key_defaults_and_prefix_override(monkeypatch):
    monkeypatch.delenv("MARKET_REDIS_STREAM_PREFIX", raising=False)
    assert (
        build_market_redis_stream_key(
            data_type="orderbook",
            exchange="binance",
            market_type="perp",
            symbol="btcusdt",
        )
        == "market:orderbook:binance:perp:BTCUSDT"
    )

    monkeypatch.setenv("MARKET_REDIS_STREAM_PREFIX", "research:market")
    assert (
        build_market_redis_stream_key(
            data_type="trade",
            exchange="bybit",
            market_type="perp",
            symbol="ethusdt",
        )
        == "research:market:trade:bybit:perp:ETHUSDT"
    )


def test_csv_values_deduplicates_and_trims():
    assert csv_values(" a, b,a, ,c ") == ("a", "b", "c")
