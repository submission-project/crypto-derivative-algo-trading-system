from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from common.logging import setup_logger
from schemas.position import Position, PositionStatus
from storage.redis_client import RedisStreamClient
from storage.repositories.redis.domain.position_projection_schema import (
    PositionRedisProjection,
)

DEFAULT_REDIS_PROJECTION_LIMIT = 500
DEFAULT_REDIS_SCAN_COUNT = 200


@dataclass
class PositionClearProjection:
    deleted_keys: int = 0


logger = setup_logger(__name__)

_KEY_PREFIX = "position:live"
_OPEN_SET_PREFIX = "position:open"
_SYMBOL_INDEX_PREFIX = "position:by:symbol"
_FLAT_TTL_SEC = 7 * 24 * 60 * 60


class PositionRedisRepository:
    """
    Redis position projection.

    Keys:
      - position:live:{position_id}
      - position:open:{exchange}
      - position:by:symbol:{exchange}:{market_type}:{symbol}

    PostgreSQL이 원본이고 Redis는 빠른 조회용 projection이다.
    """

    def __init__(self, redis: RedisStreamClient) -> None:
        self.redis = redis

    def _key(self, position_id: str) -> str:
        return f"{_KEY_PREFIX}:{position_id}"

    def _open_key(self, exchange: str, market_type: str) -> str:
        return f"{_OPEN_SET_PREFIX}:{exchange}:{market_type.lower()}"

    def _symbol_key(
        self,
        *,
        exchange: str,
        market_type: str,
        symbol: str,
    ) -> str:
        return (
            f"{_SYMBOL_INDEX_PREFIX}:"
            f"{exchange}:{market_type}:{symbol.upper()}"
        )

    # ----------------- helper methods -----------------
    async def _scan_set_ids(
        self,
        key: str,
        *,
        limit: int = DEFAULT_REDIS_PROJECTION_LIMIT,
        cursor: int = 0,
        scan_count: int = DEFAULT_REDIS_SCAN_COUNT,
    ) -> tuple[list[str], int]:
        ids: list[str] = []
        next_cursor = cursor

        while len(ids) < limit:
            next_cursor, batch = await self.redis.client.sscan(
                key,
                cursor=next_cursor,
                count=min(scan_count, limit - len(ids)),
            )

            ids.extend(_decode_redis_value(raw_id) for raw_id in batch)

            if int(next_cursor) == 0:
                break

        return ids[:limit], int(next_cursor)

    async def save(self, position: Position | dict[str, Any]) -> None:
        """
        Position projection 저장.

        OPEN:
          - position:open:{exchange}:{market_type}에 추가
          - position:by:symbol:{exchange}:{market_type}:{symbol}에 추가
          - TTL 제거

        FLAT:
          - open index에서 제거
          - symbol index에서 제거
          - live hash에 7일 TTL
        """
        projection = PositionRedisProjection.from_position(position)
        data = projection.to_hash()

        position_id = projection.position_id
        exchange = projection.exchange
        market_type = projection.market_type
        symbol = projection.symbol
        status = projection.status

        key = self._key(position_id)
        open_key = self._open_key(exchange, market_type)
        symbol_key = self._symbol_key(
            exchange=exchange,
            market_type=market_type,
            symbol=symbol,
        )

        client = self.redis.client

        async with client.pipeline(transaction=True) as pipe:
            pipe.hset(key, mapping=data)

            if status == PositionStatus.OPEN.value:
                pipe.sadd(open_key, position_id)
                pipe.sadd(symbol_key, position_id)
                pipe.persist(key)
            else:
                pipe.srem(open_key, position_id)
                pipe.srem(symbol_key, position_id)
                pipe.expire(key, _FLAT_TTL_SEC)

            await pipe.execute()

        logger.debug(
            f"Position Redis projection saved: "
            f"position_id={position_id}, "
            f"exchange={exchange}, "
            f"market_type={market_type}, "
            f"symbol={symbol}, "
            f"status={status}"
        )

    async def get(self, position_id: str) -> Optional[dict[str, Any]]:
        row = await self.redis.client.hgetall(self._key(position_id))

        if not row:
            return None

        return {
            _decode_redis_value(key): _decode_redis_value(value)
            for key, value in row.items()
        }

    async def list_open_positions(
        self,
        *,
        exchange: str,
        market_type: str,
        limit: int = DEFAULT_REDIS_PROJECTION_LIMIT,
    ) -> list[dict[str, Any]]:
        # ids = await self.redis.client.smembers(
        #     self._open_key(exchange, market_type)
        # )

        # return await self._load_many(ids, limit=limit)
        ids, _ = await self._scan_set_ids(
            self._open_key(exchange, market_type),
            limit=limit,
        )
        return await self._load_many(raw_ids=ids)



    async def list_by_symbol(
        self,
        *,
        exchange: str,
        market_type: str,
        symbol: str,
        limit: int = DEFAULT_REDIS_PROJECTION_LIMIT,
    ) -> list[dict[str, Any]]:
        # ids = await self.redis.client.smembers(
        #     self._symbol_key(
        #         exchange=exchange,
        #         market_type=market_type,
        #         symbol=symbol,
        #     )
        # )

        # return await self._load_many(ids, limit=limit)

        ids, _ = await self._scan_set_ids(
            self._symbol_key(
                exchange=exchange,
                market_type=market_type,
                symbol=symbol,
            ),
            limit=limit,
        )

        return await self._load_many(raw_ids=ids)

    async def _load_many(
        self,
        *,
        raw_ids: set[Any] | list[Any],
        # limit: int = 500,
    ) -> list[dict[str, Any]]:
        # position_ids = [
        #     _decode_redis_value(raw_id)
        #     for raw_id in raw_ids
        # ][:limit]
        position_ids = [
            _decode_redis_value(raw_id)
            for raw_id in raw_ids
        ]

        if not position_ids:
            return []

        pipe = self.redis.client.pipeline(transaction=False)

        for position_id in position_ids:
            pipe.hgetall(self._key(position_id))

        rows = await pipe.execute()

        result: list[dict[str, Any]] = []

        for row in rows:
            if row:
                result.append(
                    {
                        _decode_redis_value(key): _decode_redis_value(value)
                        for key, value in row.items()
                    }
                )

        return result

    async def delete(self, position_id: str) -> None:
        """
        projection 삭제.

        기존 row를 읽어서 open/symbol index에서도 제거한다.
        """
        row = await self.get(position_id)

        exchange = row.get("exchange") if row else None
        market_type = row.get("market_type") if row else None
        symbol = row.get("symbol") if row else None

        client = self.redis.client

        async with client.pipeline(transaction=True) as pipe:
            pipe.delete(self._key(position_id))

            if exchange and market_type:
                pipe.srem(self._open_key(exchange, market_type), position_id)

            if exchange and market_type and symbol:
                pipe.srem(
                    self._symbol_key(
                        exchange=exchange,
                        market_type=market_type,
                        symbol=symbol,
                    ),
                    position_id,
                )

            await pipe.execute()

    async def clear_projection(
        self,
        *,
        include_live_hashes: bool = True,
    ) -> PositionClearProjection:
        """
        Position Redis projection 초기화.

        startup rebuild / integration test 용도.
        """
        client = self.redis.client

        patterns: list[str] = [
            f"{_OPEN_SET_PREFIX}:*",
            f"{_SYMBOL_INDEX_PREFIX}:*",
        ]

        if include_live_hashes:
            patterns.insert(0, f"{_KEY_PREFIX}:*")

        result = PositionClearProjection()

        for pattern in patterns:
            cursor = 0

            while True:
                cursor, keys = await client.scan(
                    cursor=cursor,
                    match=pattern,
                    count=500,
                )

                if keys:
                    result.deleted_keys += int(await client.delete(*keys))

                if cursor == 0:
                    break

        return result

def _decode_redis_value(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode()

    return str(value)
