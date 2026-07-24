"""
TradeQuestDBRepository.encode() 테스트.

핵심 검증 포인트:
- canonical trade의 price/size(십진 문자열)가 QuestDB 적재용 float으로 변환되는가
- 잘못된 numeric 값에 대해 안전하게 동작하는가 (skip + warning)
- 정밀도 보존이 필요한 wire format은 그대로이고, 변환은 storage 경계에서만 일어나는가
"""
from unittest.mock import AsyncMock, MagicMock

import pytest

from storage.identifiers import QuestDBTable
from storage.repositories.trade_questdb import TradeQuestDBRepository

@pytest.fixture
def repo():
    return TradeQuestDBRepository(
        questdb=MagicMock(), table_name=QuestDBTable.CANONICAL_TRADES
    )


def _canonical(**overrides) -> dict:
    """schemas.market 정책에 맞는 canonical trade dict를 생성."""
    base = {
        "exchange": "BINANCE",
        "market_type": "PERP",
        "symbol": "BTCUSDT",
        "source": "fstream_undocumented_trade",
        "is_buyer_maker": False,
        "trade_id": 12345,
        "price": "70000.5",     # 십진 문자열
        "size": "0.001",        # 십진 문자열
        "exchange_ts": 1700000000000,
        "local_ts": 1700000000010,
        "verified_by_rest": False,
        "lag_ms": 12.3,
    }
    base.update(overrides)
    return base


class TestEncodeNumericConversion:
    def test_price_size_converted_to_float(self, repo):
        """문자열 price/size가 float으로 변환되어 columns에 들어가야 한다."""
        encoded = repo.encode(_canonical(price="70000.5", size="0.001"))

        assert encoded["columns"]["price"] == 70000.5
        assert encoded["columns"]["size"] == 0.001
        assert isinstance(encoded["columns"]["price"], float)
        assert isinstance(encoded["columns"]["size"], float)

    def test_already_numeric_input_still_works(self, repo):
        """레거시(float) 입력도 정상 처리되어야 한다 — backward compat."""
        encoded = repo.encode(_canonical(price=70000.5, size=0.001))

        assert encoded["columns"]["price"] == 70000.5
        assert encoded["columns"]["size"] == 0.001

    def test_int_input_works(self, repo):
        """price/size가 int여도 float으로 변환된다."""
        encoded = repo.encode(_canonical(price=70000, size=1))

        assert encoded["columns"]["price"] == 70000.0
        assert encoded["columns"]["size"] == 1.0
        assert isinstance(encoded["columns"]["price"], float)

    def test_invalid_price_skipped_with_warning(self, repo, caplog):
        """잘못된 문자열은 skip되고 warning이 남아야 한다."""
        with caplog.at_level("WARNING"):
            encoded = repo.encode(_canonical(price="not-a-number"))

        assert "price" not in encoded["columns"]
        assert any("invalid numeric value for 'price'" in r.message for r in caplog.records)

    def test_invalid_size_skipped(self, repo):
        encoded = repo.encode(_canonical(size="abc"))

        assert "size" not in encoded["columns"]
        assert "price" in encoded["columns"]  # 다른 필드는 영향 없음

    def test_none_price_excluded(self, repo):
        encoded = repo.encode(_canonical(price=None))
        assert "price" not in encoded["columns"]


class TestEncodeStructure:
    def test_symbols_section(self, repo):
        encoded = repo.encode(_canonical())

        assert encoded["symbols"]["exchange"] == "BINANCE"
        assert encoded["symbols"]["market_type"] == "PERP"
        assert encoded["symbols"]["symbol"] == "BTCUSDT"
        assert encoded["symbols"]["source"] == "fstream_undocumented_trade"
        assert encoded["symbols"]["is_buyer_maker"] == "false"  # 소문자 변환

    def test_is_buyer_maker_lowercased(self, repo):
        encoded_true = repo.encode(_canonical(is_buyer_maker=True))
        encoded_false = repo.encode(_canonical(is_buyer_maker=False))

        assert encoded_true["symbols"]["is_buyer_maker"] == "true"
        assert encoded_false["symbols"]["is_buyer_maker"] == "false"

    def test_columns_passthrough_for_non_numeric(self, repo):
        """trade_id, exchange_ts, local_ts 등 numeric이 아닌 컬럼은 그대로 들어가야 한다."""
        encoded = repo.encode(_canonical())

        assert encoded["columns"]["trade_id"] == 12345
        assert encoded["columns"]["exchange_ts"] == 1700000000000
        assert encoded["columns"]["local_ts"] == 1700000000010
        assert encoded["columns"]["verified_by_rest"] is False
        assert encoded["columns"]["lag_ms"] == 12.3

    def test_at_nanos_from_exchange_ts(self, repo):
        """at(나노초) 는 exchange_ts(밀리초) * 1_000_000."""
        encoded = repo.encode(_canonical(exchange_ts=1700000000000))
        assert encoded["at"] == 1700000000000 * 1_000_000

    def test_missing_exchange_ts_falls_back_to_zero(self, repo):
        data = _canonical()
        del data["exchange_ts"]
        encoded = repo.encode(data)
        assert encoded["at"] == 0


class TestPrecisionTradeoff:
    def test_high_precision_string_loses_precision_in_storage(self, repo):
        """
        QuestDB DOUBLE은 IEEE 754라 19자리 정밀도는 보존되지 않는다.
        그러나 wire format(원본 dict)은 변경되지 않으므로 Kafka 메시지의 정밀도는 보존된다.
        """
        original = _canonical(price="70123.123456789012345")
        encoded = repo.encode(original)

        # storage용 변환은 float이므로 정밀도 손실 발생 (~15자리만 보존)
        assert encoded["columns"]["price"] != "70123.123456789012345"
        assert isinstance(encoded["columns"]["price"], float)

        # 원본 dict는 변경되지 않아야 함 — Kafka로 발행된 canonical은 byte-perfect 유지
        assert original["price"] == "70123.123456789012345"
        assert isinstance(original["price"], str)


class TestSavePublishesViaQuestDBClient:
    """BaseQuestDBRepository.save 가 하위 repo에서도 동일하게 동작하는지 검증."""

    @pytest.mark.asyncio
    async def test_save_passes_single_encoded_row_to_write_batch(self):
        questdb = MagicMock()
        questdb.write_batch = AsyncMock()

        repo = TradeQuestDBRepository(questdb=questdb, table_name=QuestDBTable.CANONICAL_TRADES)
        item = _canonical()
        encoded = repo.encode(item)

        await repo.save(item)

        questdb.write_batch.assert_awaited_once_with(QuestDBTable.CANONICAL_TRADES, [encoded])
