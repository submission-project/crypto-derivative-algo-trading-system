"""
TradeRedisBufferRepository — Trade 도메인의 Redis Stream 저장 규칙을 담당합니다.
"""

from typing import List
from .base_redis import BaseHotBufferRepository


class TradeRedisBufferRepository(BaseHotBufferRepository):
    """
    Trade 데이터를 Redis Stream에 저장하고 조회하는 리포지토리
    """

    def get_stream_key(self, data: dict) -> str:
        """단일 Trade 객체에서 Stream Key 추출"""
        return self._build_key(
            data.get("exchange"),
            data.get("market_type"),
            data.get("symbol"),
        )

    def _build_key(self, exchange: str, market_type: str, symbol: str) -> str:
        """Stream Key 생성 규칙 (예: trades:binance:perp:BTCUSDT)"""
        return f"trades:{exchange}:{market_type}:{symbol.upper()}"

    def encode(self, data: dict) -> dict:
        """Trade 딕셔너리를 Redis에 저장할 수 있도록 문자열 기반으로 인코딩"""
        return {k: str(v) for k, v in data.items() if v is not None}

    async def read_latest(
        self, exchange: str, market_type: str, symbol: str, count: int = 100
    ) -> List[dict]:
        """특정 심볼의 최신 N건 Trade 데이터를 조회"""
        key = self._build_key(exchange, market_type, symbol)
        entries = await self.redis.xrevrange(key, count=count)

        # entries: list of (id, {field: value})
        return [fields for _, fields in entries]
