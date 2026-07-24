from unittest.mock import AsyncMock, MagicMock

import pytest

from stream_processor.pipelines.market_pipeline import (
    MarketEventRouter,
    normalize_market_data_type,
    parse_market_topics,
)


def test_parse_market_topics_deduplicates_and_trims():
    assert parse_market_topics(" a, b,a, ,c ") == ("a", "b", "c")


def test_parse_market_topics_uses_default_builder_env(monkeypatch):
    monkeypatch.setenv("MARKET_PIPELINE_EXCHANGES", "binance,okx")
    monkeypatch.setenv("MARKET_TOPIC_PREFIX", "research.market")
    monkeypatch.setenv("MARKET_TOPIC_MARKET_TYPE", "swap")

    assert parse_market_topics(None) == (
        "research.market.mixed.binance.swap",
        "research.market.mixed.okx.swap",
        "research.market.open_interest.binance.swap",
        "research.market.open_interest.okx.swap",
    )


def test_normalize_market_data_type_aliases():
    assert normalize_market_data_type("depth") == "orderbook"
    assert normalize_market_data_type("order_book") == "orderbook"
    assert normalize_market_data_type("trades") == "trade"
    assert normalize_market_data_type("open_interest") == "open_interest"


@pytest.mark.asyncio
async def test_market_event_router_partitions_mixed_batch():
    trade_repo = MagicMock()
    trade_repo.publish_batch = AsyncMock()
    orderbook_repo = MagicMock()
    orderbook_repo.publish_batch = AsyncMock()
    oi_repo = MagicMock()
    oi_repo.publish_batch = AsyncMock()
    redis_repo = MagicMock()
    redis_repo.publish_batch = AsyncMock()

    router = MarketEventRouter(
        trade_repo=trade_repo,
        orderbook_repo=orderbook_repo,
        open_interest_repo=oi_repo,
        redis_repo=redis_repo,
    )

    counts = await router.publish_batch(
        [
            {"data_type": "trade", "symbol": "BTCUSDT"},
            {"data_type": "depth", "symbol": "BTCUSDT"},
            {"data_type": "open_interest", "symbol": "BTCUSDT"},
            {"data_type": "unknown", "symbol": "BTCUSDT"},
        ]
    )

    assert counts == {"trade": 1, "orderbook": 1, "open_interest": 1, "unknown": 1}
    trade_repo.publish_batch.assert_awaited_once_with([{"data_type": "trade", "symbol": "BTCUSDT"}])
    orderbook_repo.publish_batch.assert_awaited_once_with([{"data_type": "orderbook", "symbol": "BTCUSDT"}])
    oi_repo.publish_batch.assert_awaited_once_with([{"data_type": "open_interest", "symbol": "BTCUSDT"}])
    redis_repo.publish_batch.assert_awaited_once()
    assert len(redis_repo.publish_batch.await_args.args[0]) == 3
