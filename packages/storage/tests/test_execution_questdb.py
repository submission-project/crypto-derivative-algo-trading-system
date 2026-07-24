import pytest
from unittest.mock import AsyncMock, MagicMock

from storage.identifiers import QuestDBTable
from storage.repositories.execution_questdb import ExecutionQuestDBRepository

@pytest.fixture
def repo():
    return ExecutionQuestDBRepository(
        questdb=MagicMock(), table_name=QuestDBTable.EXECUTION_LOGS
    )

def _execution(**overrides) -> dict:
    base = {
        "execution_id": "X123",
        "order_id": "O456",
        "source": "MANUAL",
        "exchange": "BINANCE",
        "market_type": "PERP",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "fill_price": "70000.5",
        "fill_quantity": "0.001",
        "commission": "0.028",
        "is_maker": False,
        "exchange_ts": 1700000000000,
        "local_ts": 1700000000010,
    }
    base.update(overrides)
    return base

class TestExecutionEncode:
    def test_numeric_conversion(self, repo):
        encoded = repo.encode(_execution(fill_price="70000.5", fill_quantity="0.001", commission="0.028"))
        
        assert encoded["columns"]["fill_price"] == 70000.5
        assert encoded["columns"]["fill_quantity"] == 0.001
        assert encoded["columns"]["commission"] == 0.028
        assert isinstance(encoded["columns"]["fill_price"], float)
        assert isinstance(encoded["columns"]["fill_quantity"], float)

    def test_symbols_section(self, repo):
        encoded = repo.encode(_execution(is_maker=True))
        
        assert encoded["symbols"]["exchange"] == "BINANCE"
        assert encoded["symbols"]["market_type"] == "PERP"
        assert encoded["symbols"]["symbol"] == "BTCUSDT"
        assert encoded["symbols"]["side"] == "BUY"
        assert encoded["symbols"]["source"] == "MANUAL"
        assert encoded["symbols"]["is_maker"] == "true"
        
    def test_invalid_numeric_skipped(self, repo, caplog):
        with caplog.at_level("WARNING"):
            encoded = repo.encode(_execution(fill_price="invalid"))
            
        assert "fill_price" not in encoded["columns"]
        assert "fill_quantity" in encoded["columns"]
        assert any("invalid numeric value for 'fill_price'" in r.message for r in caplog.records)
        
    def test_missing_fields(self, repo):
        data = _execution()
        del data["commission"]
        encoded = repo.encode(data)
        
        assert "commission" not in encoded["columns"]
        
    def test_at_nanos(self, repo):
        encoded = repo.encode(_execution(exchange_ts=1700000000000))
        assert encoded["at"] == 1700000000000 * 1_000_000


class TestSavePublishesViaQuestDBClient:
    """save() → publish_batch → encode → questdb.write_batch 경로 검증."""

    @pytest.mark.asyncio
    async def test_save_passes_single_encoded_row_to_write_batch(self):
        questdb = MagicMock()
        questdb.write_batch = AsyncMock()

        repo = ExecutionQuestDBRepository(questdb=questdb, table_name=QuestDBTable.EXECUTION_LOGS)
        item = _execution()
        encoded = repo.encode(item)

        await repo.save(item)

        questdb.write_batch.assert_awaited_once_with(QuestDBTable.EXECUTION_LOGS, [encoded])

    @pytest.mark.asyncio
    async def test_save_equivalent_to_publish_batch_with_one_item(self):
        questdb = MagicMock()
        questdb.write_batch = AsyncMock()

        repo = ExecutionQuestDBRepository(questdb=questdb, table_name=QuestDBTable.EXECUTION_LOGS)
        item = _execution()

        questdb.write_batch.reset_mock()
        await repo.save(item)
        call_save = questdb.write_batch.call_args

        questdb.write_batch.reset_mock()
        await repo.publish_batch([item])
        call_batch = questdb.write_batch.call_args

        assert call_save == call_batch
