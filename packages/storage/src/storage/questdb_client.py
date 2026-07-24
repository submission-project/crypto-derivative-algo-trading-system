"""
QuestDB Client — QuestDB ILP(Influx Line Protocol)를 통한 통신만 담당하는 순수 인프라 계층
도메인 지식(테이블 명, 컬럼 등)을 포함하지 않습니다.
"""

import asyncio
import os
from typing import List, Dict, Any
from common.logging import setup_logger
from common.config import settings

from questdb.ingress import Sender, TimestampNanos

logger = setup_logger(__name__)

# [needs improvement] 추후 이런식으로 변경 바람
# QuestDB writer queue
# + dedicated background writer
# + persistent Sender


class QuestDBClient:
    """
    QuestDB 조작을 위한 래퍼 클라이언트
    비동기 환경에서 스레드 풀을 이용하여 안전하게 ILP Sender를 실행
    """

    def __init__(self, host: str, ilp_port: int):
        self.conf = f"tcp::addr={host}:{ilp_port};"

    async def connect(self):
        logger.info(f"QuestDBClient configured: {self.conf}")

    async def close(self):
        logger.info("QuestDBClient closed (no persistent sender)")

    def _write_batch_sync(self, table_name: str, rows: List[Dict[str, Any]]):
        """
        동기 ILP 전송. ThreadPool에서 실행됩니다.
        rows 구조:
        [
            {
                "symbols": {"key": "value", ...},
                "columns": {"key": 123.4, ...},
                "at": 1612345678000000000  # Nanos
            },
            ...
        ]
        """
        if not rows:
            return
        try:
            with Sender.from_conf(self.conf) as sender:
                for row in rows:
                    at_nanos = row.get("at")
                    sender.row(
                        table_name,
                        symbols=row.get("symbols", {}),
                        columns=row.get("columns", {}),
                        at=TimestampNanos(at_nanos) if at_nanos else None,
                    )
                sender.flush()
            logger.debug(f"Flushed {len(rows)} rows to QuestDB '{table_name}'")
        except Exception as e:
            logger.error(f"QuestDB write failed for '{table_name}': {e}")
            if settings.questdb_strict_ilp_errors:
                raise

    async def write_batch(self, table_name: str, rows: List[Dict[str, Any]]):
        """비동기 래퍼: Event Loop 차단 방지를 위해 ThreadPool에서 실행"""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._write_batch_sync, table_name, rows)
