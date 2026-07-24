from abc import ABC, abstractmethod
from typing import List, Dict, Any

from storage.questdb_client import QuestDBClient


class BaseQuestDBRepository(ABC):
    """
    QuestDB를 이용하는 저장소용 기본 리포지토리
    공통 I/O(publish_batch)를 구현하고, 하위 클래스에서 인코딩 방식을 정의합니다.
    """

    def __init__(self, questdb: QuestDBClient, table_name: str):
        self.questdb = questdb
        self.table_name = table_name

    @abstractmethod
    def encode(self, data: dict) -> Dict[str, Any]:
        """
        단일 데이터를 QuestDBClient가 이해할 수 있는 형태의 딕셔너리로 변환합니다.
        반환 형식: {"symbols": {...}, "columns": {...}, "at": 1612345678000000000}
        """
        pass

    async def save(self, item: dict) -> None:
        """단일 행을 QuestDB에 발행합니다."""
        await self.publish_batch([item])

    async def publish_batch(self, items: List[dict]):
        """다수의 데이터를 QuestDB에 한 번에 발행합니다."""
        if not items:
            return

        rows = [self.encode(item) for item in items]
        await self.questdb.write_batch(self.table_name, rows)
