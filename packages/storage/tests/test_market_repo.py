from unittest.mock import AsyncMock, MagicMock

import pytest

from storage.identifiers import QuestDBTable
from storage.repositories.market_repo import (
    MarketEventRedisBufferRepository,
    MarketTradeQuestDBRepository,
    OpenInterestQuestDBRepository,
    OrderBookQuestDBRepository,
)


def test_market_trade_encode_numeric_fields():
    repo = MarketTradeQuestDBRepository(MagicMock(), QuestDBTable.MARKET_TRADES)

    encoded = repo.encode(
        {
            "data_type": "trade",
            "exchange": "binance",
            "market_type": "perp",
            "symbol": "BTCUSDT",
            "side": "buy",
            "trade_id": "123",
            "price": "65000.5",
            "size": "0.02",
            "exchange_ts": 1700000000000,
            "local_ts": 1700000000005,
            "source": "rest_gap_fill",
            "verified_by_rest": True,
            "repair_from_trade_id": 120,
            "repair_to_trade_id": 122,
            "repair_reason": "numeric_trade_id_gap",
            "raw": {"p": "65000.5"},
        }
    )

    assert encoded["symbols"]["data_type"] == "trade"
    assert encoded["symbols"]["side"] == "buy"
    assert encoded["symbols"]["source"] == "rest_gap_fill"
    assert encoded["columns"]["price"] == 65000.5
    assert encoded["columns"]["size"] == 0.02
    assert encoded["columns"]["trade_id"] == 123
    assert encoded["columns"]["verified_by_rest"] is True
    assert encoded["columns"]["repair_from_trade_id"] == 120
    assert encoded["columns"]["repair_to_trade_id"] == 122
    assert encoded["columns"]["repair_reason"] == "numeric_trade_id_gap"
    assert encoded["columns"]["raw_json"] == '{"p":"65000.5"}'
    assert encoded["at"] == 1700000000000 * 1_000_000


def test_market_trade_invalid_numeric_is_skipped(caplog):
    repo = MarketTradeQuestDBRepository(MagicMock(), QuestDBTable.MARKET_TRADES)

    with caplog.at_level("WARNING"):
        encoded = repo.encode(
            {
                "exchange": "binance",
                "market_type": "perp",
                "symbol": "BTCUSDT",
                "price": "bad",
                "size": "0.01",
                "exchange_ts": 1700000000000,
            }
        )

    assert "price" not in encoded["columns"]
    assert encoded["columns"]["size"] == 0.01
    assert any("invalid numeric value for 'price'" in record.message for record in caplog.records)


def test_orderbook_encode_top_of_book_features():
    repo = OrderBookQuestDBRepository(MagicMock(), QuestDBTable.MARKET_ORDERBOOKS)

    encoded = repo.encode(
        {
            "data_type": "orderbook",
            "exchange": "binance",
            "market_type": "perp",
            "symbol": "BTCUSDT",
            "bids": [{"price": "64999.0", "size": "1.2"}],
            "asks": [{"price": "65001.0", "size": "0.8"}],
            "sequence": "999",
            "exchange_ts": 1700000000000,
            "local_ts": 1700000000010,
        }
    )

    assert encoded["symbols"]["data_type"] == "orderbook"
    assert encoded["columns"]["best_bid_price"] == 64999.0
    assert encoded["columns"]["best_ask_price"] == 65001.0
    assert encoded["columns"]["mid_price"] == 65000.0
    assert encoded["columns"]["spread"] == 2.0
    assert encoded["columns"]["bid_depth"] == 1
    assert encoded["columns"]["ask_depth"] == 1
    assert encoded["columns"]["sequence"] == "999"


def test_open_interest_encode_value_and_unit():
    repo = OpenInterestQuestDBRepository(MagicMock(), QuestDBTable.MARKET_OPEN_INTEREST)

    encoded = repo.encode(
        {
            "data_type": "open_interest",
            "exchange": "bitget",
            "market_type": "perp",
            "symbol": "BTCUSDT",
            "open_interest": "100.5",
            "open_interest_unit": "BTC",
            "open_interest_value_usd": "6500000.25",
            "exchange_ts": 1700000000000,
            "local_ts": 1700000000010,
            "note": "estimated",
        }
    )

    assert encoded["symbols"]["open_interest_unit"] == "BTC"
    assert encoded["columns"]["open_interest"] == 100.5
    assert encoded["columns"]["open_interest_value_usd"] == 6500000.25
    assert encoded["columns"]["note"] == "estimated"


def test_market_redis_key_and_json_encoding():
    repo = MarketEventRedisBufferRepository(MagicMock(), maxlen=100)
    event = {
        "data_type": "depth",
        "exchange": "binance",
        "market_type": "perp",
        "symbol": "btcusdt",
        "bids": [{"price": "1", "size": "2"}],
    }

    assert repo.get_stream_key(event) == "market:orderbook:binance:perp:BTCUSDT"
    assert repo.encode(event)["bids"] == '[{"price":"1","size":"2"}]'


def test_market_redis_key_uses_env_prefix(monkeypatch):
    monkeypatch.setenv("MARKET_REDIS_STREAM_PREFIX", "research:market")
    repo = MarketEventRedisBufferRepository(MagicMock(), maxlen=100)

    assert (
        repo.get_stream_key(
            {
                "data_type": "trade",
                "exchange": "okx",
                "market_type": "perp",
                "symbol": "btc-usdt-swap",
            }
        )
        == "research:market:trade:okx:perp:BTC-USDT-SWAP"
    )


@pytest.mark.asyncio
async def test_base_save_publishes_encoded_orderbook_row():
    questdb = MagicMock()
    questdb.write_batch = AsyncMock()
    repo = OrderBookQuestDBRepository(questdb, QuestDBTable.MARKET_ORDERBOOKS)
    event = {
        "exchange": "binance",
        "market_type": "perp",
        "symbol": "BTCUSDT",
        "bids": [],
        "asks": [],
        "exchange_ts": 1700000000000,
    }

    await repo.save(event)

    questdb.write_batch.assert_awaited_once()
    table, rows = questdb.write_batch.await_args.args
    assert table == QuestDBTable.MARKET_ORDERBOOKS
    assert rows[0]["symbols"]["data_type"] == "orderbook"
