"""
ExecutionQuestDBRepository — 체결 이벤트를 QuestDB 시계열 테이블에 저장.
"""

from typing import Dict, Any

from common.logging import setup_logger
from storage.questdb_client import QuestDBClient
from storage.identifiers import QuestDBTable
from .base_questdb import BaseQuestDBRepository

logger = setup_logger(__name__)


class ExecutionQuestDBRepository(BaseQuestDBRepository):
    """
    체결 이벤트(ExecutionReport)를 QuestDB에 저장하는 리포지토리.

    price/quantity 등 숫자형 문자열은 DecimalString에서 float로 변환하여
    QuestDB의 SUM, AVG 같은 numeric 쿼리가 동작
    """

    NUMERIC_COLUMNS = ("fill_price", "fill_quantity", "commission", "latency_ms")

    def __init__(
        self, questdb: QuestDBClient, table_name: str = QuestDBTable.EXECUTION_LOGS
    ):
        super().__init__(questdb, table_name)

        # QuestDB SYMBOL 타입 (인덱싱, 필터링 용도)
        self.symbols_keys = [
            "exchange",
            "market_type",
            "symbol",
            "side",
            "source",
            "is_maker",
        ]
        # QuestDB COLUMN 타입 (값 저장)
        self.columns_keys = [
            "execution_id",
            "order_id",
            "signal_id",
            "strategy_name",
            "fill_price",
            "fill_quantity",
            "commission",
            "commission_asset",
            "exchange_trade_id",
            "exchange_order_id",
            "exchange_ts",
            "local_ts",
            "latency_ms",
        ]
        self.ts_key = "exchange_ts"

    def encode(self, data: dict) -> Dict[str, Any]:
        """
        ExecutionReport 딕셔너리를 QuestDBClient가 이해하는 ILP 형태로 인코딩.
        """
        symbols = {}
        for k in self.symbols_keys:
            v = data.get(k)
            if k == "is_maker":
                symbols[k] = str(v).lower() if v is not None else "false"
            else:
                symbols[k] = str(v) if v is not None else "unknown"

        columns: Dict[str, Any] = {}
        for k in self.columns_keys:
            v = data.get(k)
            if v is None:
                continue

            if k in self.NUMERIC_COLUMNS:
                try:
                    v = float(v)
                except (TypeError, ValueError):
                    logger.warning(
                        f"ExecutionQuestDBRepository: invalid numeric value for '{k}': {v!r}; skipping field"
                    )
                    continue

            columns[k] = v

        ts_ms = int(data.get(self.ts_key, 0))
        at_nanos = ts_ms * 1_000_000

        return {"symbols": symbols, "columns": columns, "at": at_nanos}
