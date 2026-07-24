"""
TradeQuestDBRepository — Trade 도메인의 QuestDB 저장 규칙을 담당합니다.
"""

from typing import Dict, Any

from common.logging import setup_logger
from storage.questdb_client import QuestDBClient
from .base_questdb import BaseQuestDBRepository

logger = setup_logger(__name__)


class TradeQuestDBRepository(BaseQuestDBRepository):
    """
    Trade 데이터를 QuestDB에 저장하는 규칙을 매핑하는 리포지토리.

    canonical trade 메시지는 wire(Kafka) 단계에서 정밀도 보존을 위해
    price/size를 십진 문자열("70000.5")로 운반합니다 (schemas.market.DecimalString).
    QuestDB는 분석/집계 용도로 사용되므로 적재 직전 float(=DOUBLE)으로 변환하여
    SUM, AVG 같은 numeric 쿼리가 동작하도록 합니다.
    """

    NUMERIC_COLUMNS = ("price", "size")

    def __init__(self, questdb: QuestDBClient, table_name: str):
        super().__init__(questdb, table_name)

        self.symbols_keys = [
            "exchange",
            "market_type",
            "symbol",
            "source",
            "is_buyer_maker",
        ]
        self.columns_keys = [
            "trade_id",
            "price",
            "size",
            "exchange_ts",
            "local_ts",
            "verified_by_rest",
            "lag_ms",
        ]
        self.ts_key = "exchange_ts"

    def encode(self, data: dict) -> Dict[str, Any]:
        """
        Trade 딕셔너리를 QuestDBClient가 이해하는 ILP 형태의 dict로 인코딩
        """
        symbols = {
            k: (
                str(data.get(k, "unknown")).lower()
                if k == "is_buyer_maker"
                else str(data.get(k, "unknown"))
            )
            for k in self.symbols_keys
        }

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
                        f"TradeQuestDBRepository: invalid numeric value for '{k}': {v!r}; skipping field"
                    )
                    continue

            columns[k] = v

        ts_ms = int(data.get(self.ts_key, 0))
        at_nanos = ts_ms * 1_000_000

        return {"symbols": symbols, "columns": columns, "at": at_nanos}
