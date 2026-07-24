"""
OrderStateRedisRepository — Redis Hash 기반 실시간 주문 상태 관리.

구조:
    order:live:{order_id}  → Hash -> orders 테이블의 한 row projection
    order:open             → Set   open 주문만 빨리 찾기 위한 인덱스
    order:unknown          → ZSet  UNKNOWN 주문만 빨리 찾기 위한 인덱스
    order:recovery         → ZSet  SUBMITTED / PENDING_CANCEL / UNKNOWN 복구 대상
                              -> 복구 점검이 필요한 주문만 빨리 찾기 위한 인덱스

Terminal 상태:
    FILLED, CANCELLED, REJECTED, EXPIRED

Terminal 도달 시:
    - order:open에서 제거
    - order:unknown에서 제거
    - order:recovery에서 제거
    - order:live:{order_id}에 24시간 TTL 설정

주의:
    Redis Hash 값은 문자열 중심으로 저장한다.
    가격/수량/체결가/ID는 문자열로 보존한다.

중요:
    PostgreSQL이 원본 상태(Source of Truth)이고,
    Redis는 빠른 조회용 projection이다.

    projection 갱신은 version-aware upsert를 사용한다.
    즉, incoming version이 Redis의 current version보다 클 때만 반영한다.
"""

from __future__ import annotations

from dataclasses import dataclass

import json
import types
from typing import Any, Optional, Union, get_args, get_origin

from storage.redis_client import RedisStreamClient

from common.logging import setup_logger
from schemas.order import (
    Order,
    OrderStatus,
    TERMINAL_STATUSES,
    RECOVERY_STATUSES,
    CONDITIONAL_TRACKABLE_STATUSES,
    UNKNOWN_STATUSES
)
from storage.identifiers import (
    RedisKey,
    redis_order_live_key,
    redis_order_by_symbol_key,
    redis_order_open_key,
    redis_order_conditional_open_key,
    redis_order_unknown_key,
    redis_order_recovery_key,
)

from .base_redis import BaseStateRedisRepository

from storage.repositories.redis.domain.order_projection_schema import (
    OrderRedisProjection,
)
from collections.abc import Sequence
from collections.abc import AsyncIterator

DEFAULT_REDIS_PROJECTION_LIMIT = 500
DEFAULT_REDIS_SCAN_COUNT = 200

logger = setup_logger(__name__)

# Terminal 상태 도달 후 Redis에서 자동 만료되기까지의 시간 (초)
_TERMINAL_TTL_SEC = 86400  # 24시간

_REGULAR_TERMINAL_STATUS_VALUES = frozenset(s.value for s in TERMINAL_STATUSES)
_RECOVERY_STATUS_VALUES = frozenset(s.value for s in RECOVERY_STATUSES)
_CONDITIONAL_TRACKABLE_STATUS_VALUES = frozenset(
    s.value if hasattr(s, "value") else (str(s) if s else "")
    for s in CONDITIONAL_TRACKABLE_STATUSES
)
_UNKNOWN_STATUS_VALUES = frozenset(s.value for s in UNKNOWN_STATUSES)

_REGULAR_TERMINAL_STATUSES_ARG = "|".join(sorted(_REGULAR_TERMINAL_STATUS_VALUES))
_RECOVERY_STATUSES_ARG = "|".join(sorted(_RECOVERY_STATUS_VALUES))
_CONDITIONAL_TRACKABLE_STATUSES_ARG = "|".join(sorted(_CONDITIONAL_TRACKABLE_STATUS_VALUES))
_UNKNOWN_STATUSES_ARG = "|".join(sorted(_UNKNOWN_STATUS_VALUES))

DEFAULT_RECOVERY_BATCH_SIZE = 100

ORDER_STATE_INDEX_PATTEN_LIST: list[str] = [
    f"{RedisKey.ORDER_OPEN_SET}:*",
    f"{RedisKey.ORDER_CONDITIONAL_OPEN_SET}:*",
    f"{RedisKey.ORDER_UNKNOWN_ZSET}:*",
    f"{RedisKey.ORDER_RECOVERY_ZSET}:*",
    f"{RedisKey.ORDER_BY_SYMBOL_SET}:*",
]

@dataclass(slots=True)
class OrderClearProjectionResult:
    total_deleted: int = 0
    cleared_live_hashes: int = 0
    cleared_indexes: int = 0


def _extract_fields_by_type(model_class: type, target_type: type) -> set[str]:
    fields: set[str] = set()

    for name, info in model_class.model_fields.items():
        ann = info.annotation

        if ann is target_type:
            fields.add(name)
            continue

        origin = get_origin(ann)

        if origin is Union or type(ann) is types.UnionType:
            args = get_args(ann)
            if target_type in args:
                fields.add(name)

    return fields


_INT_FIELDS = _extract_fields_by_type(Order, int)
_BOOL_FIELDS = _extract_fields_by_type(Order, bool)


def _live_key(order_id: str) -> str:
    return redis_order_live_key(order_id)


def _regular_open_key(*, exchange: str, market_type: str) -> str:
    return redis_order_open_key(exchange, market_type)


def _conditional_open_key(*, exchange: str, market_type: str) -> str:
    return redis_order_conditional_open_key(exchange, market_type)


def _unknown_key(*, exchange: str, market_type: str) -> str:
    return redis_order_unknown_key(exchange, market_type)


def _recovery_key(*, exchange: str, market_type: str) -> str:
    return redis_order_recovery_key(exchange, market_type)


def _symbol_key(*, exchange: str, market_type: str, symbol: str) -> str:
    return redis_order_by_symbol_key(exchange, market_type, symbol)

def _reconcile_failure_key(
    *,
    exchange: str,
    market_type: str,
    order_id: str,
) -> str:
    return (
        f"order:reconcile:failure:"
        f"{exchange.upper()}:{market_type.upper()}:{order_id}"
    )


# KEYS:
#   1 = order hash key
#   2 = orders:open:{exchange}:{market_type}
#   3 = orders:conditional:open:{exchange}:{market_type}
#   4 = orders:unknown:{exchange}:{market_type}
#   5 = orders:recovery:{exchange}:{market_type}
#   6 = orders:by:symbol:{exchange}:{market_type}:{symbol}
#
# ARGV:
#   1  = incoming_version
#   2  = status
#   3  = order_id
#   4  = updated_ts
#   5  = terminal_ttl_sec
#   6  = regular_terminal_statuses_arg
#   7  = recovery_statuses_arg
#   8  = unknown_statuses_arg
#   9  = conditional_trackable_statuses_arg
#   10 = order_route
#   11 = conditional_status
#   12 = field_count
#   13... = field/value pairs
#
# Return:
#   {1, incoming_version} = applied
#   {0, current_version}  = ignored because incoming_version <= current_version
# 키 저장 조건
# recovery key에 저장되는 조건
# 1. OrderStatus가 SUBMITTED / PENDING_CANCEL / UNKNOWN 이거나
# 2. 조건부 주문이고 conditional_status가 open 상태이면
_UPSERT_PROJECTION_IF_NEWER_LUA = """
local order_key = KEYS[1]
local regular_open_key = KEYS[2]
local conditional_open_key = KEYS[3]
local unknown_key = KEYS[4]
local recovery_key = KEYS[5]
local symbol_key = KEYS[6]

local incoming_version = tonumber(ARGV[1])
local status = ARGV[2]
local order_id = ARGV[3]
local updated_ts = tonumber(ARGV[4])
local terminal_ttl_sec = tonumber(ARGV[5])

local regular_terminal_statuses_arg = ARGV[6]
local recovery_statuses_arg = ARGV[7]
local unknown_statuses_arg = ARGV[8]
local conditional_trackable_statuses_arg = ARGV[9]

local order_route = ARGV[10]
local conditional_status = ARGV[11]

local field_count = tonumber(ARGV[12])

local function contains_status(statuses, target)
    if statuses == nil or statuses == '' then
        return false
    end

    local haystack = '|' .. statuses .. '|'
    local needle = '|' .. target .. '|'

    return string.find(haystack, needle, 1, true) ~= nil
end

local current_version_raw = redis.call('HGET', order_key, 'version')
local current_version = -1

if current_version_raw then
    current_version = tonumber(current_version_raw) or -1
end

if incoming_version <= current_version then
    return {0, current_version}
end

local fields = {}
local arg_index = 13

for i = 1, field_count do
    table.insert(fields, ARGV[arg_index])
    table.insert(fields, ARGV[arg_index + 1])
    arg_index = arg_index + 2
end

redis.call('HSET', order_key, unpack(fields))

-- 기존 index에서 먼저 제거 후 다시 추가한다.
redis.call('SREM', regular_open_key, order_id)
redis.call('SREM', conditional_open_key, order_id)
redis.call('SREM', symbol_key, order_id)

redis.call('ZREM', unknown_key, order_id)
redis.call('ZREM', recovery_key, order_id)

local is_terminal = contains_status(regular_terminal_statuses_arg, status)

local is_regular_open =
    order_route == "REGULAR"
    and not is_terminal

local is_conditional_open =
    order_route == "CONDITIONAL"
    and not is_terminal
    and contains_status(conditional_trackable_statuses_arg, conditional_status)

local is_recovery =
    contains_status(recovery_statuses_arg, status)
    or is_conditional_open

local is_unknown =
    status == unknown_statuses_arg
    or conditional_status == "UNKNOWN"

if is_terminal then
    redis.call('EXPIRE', order_key, terminal_ttl_sec)
else
    redis.call('PERSIST', order_key)
    redis.call('SADD', symbol_key, order_id)
end

if is_regular_open then
    redis.call('SADD', regular_open_key, order_id)
end

if is_conditional_open then
    redis.call('SADD', conditional_open_key, order_id)
end

if is_unknown then
    redis.call('ZADD', unknown_key, updated_ts, order_id)
end

if is_recovery then
    redis.call('ZADD', recovery_key, updated_ts, order_id)
end

return {1, incoming_version}
"""


class OrderStateRedisRepository(BaseStateRedisRepository):
    """
    Redis Hash를 사용한 실시간 주문 상태 관리.

    역할:
      - order live projection 저장
      - open / unknown / recovery 인덱스 관리
      - terminal 상태 TTL 설정
      - terminal 상태 덮어쓰기 방지
      - PostgreSQL 원본에서 온 version-aware projection upsert
    """

    def __init__(self, redis: RedisStreamClient, max_retries: int = 5) -> None:
        self._max_retries = max_retries

        self.scan_num_once = 500
        super().__init__(redis)

    # ----------------- helper methods -----------------
    async def _scan_set_ids(
        self,
        *,
        key: str,
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

    # ----------------- read methods -----------------
    async def _load_many_order_ids(
        self,
        raw_order_ids: Sequence[Any],
        # raw_order_ids: list[Any] | set[Any],
        # *,
        # limit: int = 500,
    ) -> list[dict[str, Any]]:
        ids = [_decode_redis_value(order_id) for order_id in raw_order_ids]
        # ids = ids[:limit]

        if not ids:
            return []

        pipe = self.redis.client.pipeline(transaction=False)

        for order_id in ids:
            pipe.hgetall(_live_key(order_id))

        rows = await pipe.execute()

        result: list[dict[str, Any]] = []

        for row in rows:
            if row:
                result.append(self._deserialize_hash(row))

        return result

    # ----------------- write methods -----------------

    # order_dict -> order로 변경
    async def save(self, order: Order) -> bool:
        """
        주문 projection을 Redis에 저장.
        """
        return await self.upsert_projection_if_newer(order)

    async def upsert_projection_if_newer(
        self,
        order: Order,
    ) -> bool:
        """
        PostgreSQL 원본에서 온 projection을 Redis에 반영.

        incoming version이 현재 Redis version보다 클 때만 반영한다.

        규칙:
        - Redis에 주문이 없으면 저장
        - incoming version > current version 이면 저장
        - incoming version <= current version 이면 무시
        - Hash / open / unknown / recovery / TTL을 Lua로 원자적으로 함께 갱신

        Returns:
            True  = Redis projection에 반영됨
            False = 더 오래된 projection이라 무시됨
        """

        projection: OrderRedisProjection = OrderRedisProjection.from_order(order)
        fields = projection.to_hash()

        argv: list[str] = [
            str(projection.version),
            projection.status,
            projection.order_id,
            str(projection.updated_ts),
            str(_TERMINAL_TTL_SEC),
            _REGULAR_TERMINAL_STATUSES_ARG,
            _RECOVERY_STATUSES_ARG,
            _UNKNOWN_STATUSES_ARG,
            _CONDITIONAL_TRACKABLE_STATUSES_ARG,
            projection.order_route,
            projection.conditional_status,
            str(len(fields)),
        ]


        for field, value in fields.items():
            argv.append(str(field))
            argv.append(str(value))

        result = await self.redis.client.eval(
            _UPSERT_PROJECTION_IF_NEWER_LUA,
            6,
            _live_key(projection.order_id),
            _regular_open_key(
                exchange=projection.exchange,
                market_type=projection.market_type,
            ),
            _conditional_open_key(
                exchange=projection.exchange,
                market_type=projection.market_type,
            ),
            _unknown_key(
                exchange=projection.exchange,
                market_type=projection.market_type,
            ),
            _recovery_key(
                exchange=projection.exchange,
                market_type=projection.market_type,
            ),
            _symbol_key(
                exchange=projection.exchange,
                market_type=projection.market_type,
                symbol=projection.symbol,
            ),
            *argv,
        )

        if isinstance(result, (list, tuple)):
            applied = int(result[0]) == 1
            redis_version = int(result[1])
        else:
            applied = int(result) == 1
            redis_version = projection.version

        if applied:
            logger.debug(
                f"Redis projection upserted: "
                f"order_id={projection.order_id}, "
                f"route={projection.order_route}, "
                f"status={projection.status}, "
                f"conditional_status={projection.conditional_status}, "
                f"version={projection.version}"
            )
        else:
            logger.debug(
                f"Stale Redis projection ignored: "
                f"order_id={projection.order_id}, "
                f"route={projection.order_route}, "
                f"status={projection.status}, "
                f"incoming_version={projection.version}, "
                f"current_version={redis_version}"
            )

        return applied

    # ----------------- deserialize -----------------

    def _deserialize_hash(self, data: dict[Any, Any]) -> dict[str, Any]:
        result: dict[str, Any] = {}

        for raw_key, raw_value in data.items():
            key = _decode_redis_value(raw_key)
            value = _decode_redis_value(raw_value)

            if value == "":
                result[key] = None
                continue

            if key in {"raw_exchange_response", "raw_request"}:
                try:
                    result[key] = json.loads(value)
                except json.JSONDecodeError:
                    result[key] = value
                continue

            if key in _BOOL_FIELDS:
                result[key] = value.lower() in {"1", "true", "yes", "y"}
                continue

            if key in _INT_FIELDS:
                try:
                    result[key] = int(value)
                except ValueError:
                    result[key] = value
                continue

            result[key] = value

        return result

    # ----------------- read methods -----------------
    async def get(self, order_id: str) -> Optional[dict[str, Any]]:
        data = await self.redis.client.hgetall(_live_key(order_id))

        if not data:
            return None

        return self._deserialize_hash(data)

    async def list_open_regular_orders(
        self,
        *,
        exchange: str,
        market_type: str,
        limit: int = DEFAULT_REDIS_PROJECTION_LIMIT,
    ) -> list[dict[str, Any]]:
        # order_ids = await self.redis.client.smembers(_regular_open_key(exchange=exchange, market_type=market_type))
        # return await self._load_many_order_ids(order_ids, limit=limit)
        order_ids, _ = await self._scan_set_ids(
            key=_regular_open_key(exchange=exchange, market_type=market_type),
            limit=limit,
        )
        return await self._load_many_order_ids(order_ids)

    async def iter_open_conditional_order_batches(
        self,
        *,
        exchange: str,
        market_type: str,
        batch_size: int = DEFAULT_REDIS_PROJECTION_LIMIT,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        cursor = 0
        key = _conditional_open_key(exchange=exchange, market_type=market_type)

        while True:
            order_ids, cursor = await self._scan_set_ids(
                key=key,
                cursor=cursor,
                limit=batch_size,
            )

            rows = await self._load_many_order_ids(order_ids)

            if rows:
                yield rows

            if cursor == 0:
                break

    async def list_open_conditional_orders(
        self,
        *,
        exchange: str,
        market_type: str,
        limit: int = DEFAULT_REDIS_PROJECTION_LIMIT,
    ) -> list[dict[str, Any]]:
        # order_ids = await self.redis.client.smembers(_conditional_open_key(exchange=exchange, market_type=market_type))

        # return await self._load_many_order_ids(order_ids, limit=limit)
        order_ids, _ = await self._scan_set_ids(
            key=_conditional_open_key(exchange=exchange, market_type=market_type),
            limit=limit,
        )
        return await self._load_many_order_ids(order_ids)

    # [claim] 수정 했으니 관련 참고하는 코드 및 테스트코드 수정 바람
    # [claim] list_open_orders는 deprecated 하고 list_open_regular_orders를 사용하도록 
    # async def list_open_orders(
    #     self,
    #     *,
    #     exchange: str,
    #     market_type: str,
    #     limit: int = DEFAULT_REDIS_PROJECTION_LIMIT,
    # ) -> list[dict[str, Any]]:
    #     """
    #     현재 active/open 주문 목록 조회.

    #     주의:
    #         여기서 open은 Binance NEW 상태만 의미하지 않고,
    #         terminal이 아닌 주문 전체를 의미한다.
    #     """
    #     return await self.list_open_regular_orders(
    #         exchange=exchange,
    #         market_type=market_type,
    #         limit=limit,
    #     )

    async def list_recovery_orders(
        self,
        *,
        exchange: str,
        market_type: str,
        batch_size: int = DEFAULT_RECOVERY_BATCH_SIZE,
        older_than_ts: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        복구 대상 주문 조회.

        대상 상태:
          - SUBMITTED
          - PENDING_CANCEL
          - UNKNOWN
        """
        max_score = older_than_ts if older_than_ts is not None else "+inf"

        # Redis Sorted Set(ZSET) 에서 score[_recovery_key 인]가 -inf ~ max_score 사이인 값들을 가져옴
        order_ids = await self.redis.client.zrangebyscore(
            _recovery_key(exchange=exchange, market_type=market_type),
            min="-inf",
            max=max_score,
            start=0,
            num=batch_size,
        )

        return await self._load_many_order_ids(order_ids)

    async def list_unknown_orders(
        self,
        *,
        exchange: str,
        market_type: str,
        batch_size: int = DEFAULT_RECOVERY_BATCH_SIZE,
        older_than_ts: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        UNKNOWN 주문 목록 조회.

        older_than_ts가 있으면 해당 timestamp 이하의 UNKNOWN만 조회.
        """
        max_score = older_than_ts if older_than_ts is not None else "+inf"

        # timestamp 이하의 UNKNOWN만 조회
        order_ids = await self.redis.client.zrangebyscore(
            _unknown_key(exchange=exchange, market_type=market_type),
            min="-inf",
            max=max_score,
            start=0,
            num=batch_size,
        )

        return await self._load_many_order_ids(order_ids)

    async def postpone_recovery_order(
        self,
        *,
        exchange: str,
        market_type: str,
        order_id: str,
        next_attempt_ts: int,
    ) -> None:
        await self.redis.client.zadd(
            _recovery_key(exchange=exchange, market_type=market_type),
            {order_id: next_attempt_ts},
        )

    async def postpone_unknown_order(
        self,
        *,
        exchange: str,
        market_type: str,
        order_id: str,
        next_attempt_ts: int,
    ) -> None:
        await self.redis.client.zadd(
            _unknown_key(exchange=exchange, market_type=market_type),
            {order_id: next_attempt_ts},
        )

    async def list_open_by_symbol(
        self,
        *,
        exchange: str,
        market_type: str,
        symbol: str,
        include_conditional: bool = False,
        limit: int = DEFAULT_REDIS_PROJECTION_LIMIT,
    ) -> list[dict[str, Any]]:
        """
        심볼별 non-terminal 주문 목록 조회.

        include_conditional=False:
            REGULAR 주문만 반환

        include_conditional=True:
            REGULAR + CONDITIONAL projection 모두 반환
        """
        # order_ids = await self.redis.client.smembers(
        #     _symbol_key(exchange=exchange, market_type=market_type, symbol=symbol)
        # )

        # rows = await self._load_many_order_ids(order_ids, limit=limit)
        order_ids, _ = await self._scan_set_ids(
            key=_symbol_key(exchange=exchange, market_type=market_type, symbol=symbol),
            limit=limit,
        )
        rows = await self._load_many_order_ids(order_ids)

        if include_conditional:
            return rows

        return [row for row in rows if row.get("order_route") == "REGULAR"]

    # async def list_by_symbol(
    #     self,
    #     *,
    #     exchange: str,
    #     market_type: str,
    #     symbol: str,
    #     include_conditional: bool = False,
    #     limit: int = 500,
    # ) -> list[dict[str, Any]]:
    #     """
    #     list_open_by_symbol alias.
    #     """
    #     return await self.list_open_by_symbol(
    #         exchange=exchange,
    #         market_type=market_type,
    #         symbol=symbol,
    #         include_conditional=include_conditional,
    #         limit=limit,
    #     )

    # ----------------- delete / clear -----------------
    async def delete(self, order_id: str) -> None:
        client = self.redis.client

        existing = await self.get(order_id)

        async with client.pipeline(transaction=True) as pipe:
            pipe.delete(_live_key(order_id))

            if existing:
                exchange = str(existing.get("exchange") or "")
                market_type = str(existing.get("market_type") or "")
                symbol = str(existing.get("symbol") or "").upper()

                if exchange:
                    pipe.srem(_regular_open_key(exchange=exchange, market_type=market_type), order_id)
                    pipe.srem(_conditional_open_key(exchange=exchange, market_type=market_type), order_id)
                    pipe.zrem(_unknown_key(exchange=exchange, market_type=market_type), order_id)
                    pipe.zrem(_recovery_key(exchange=exchange, market_type=market_type), order_id)

                if exchange and market_type and symbol:
                    pipe.srem(_symbol_key(exchange=exchange, market_type=market_type, symbol=symbol), order_id)

            await pipe.execute()

    async def clear_projection(
        self,
        *,
        include_live_hashes: bool = True,
    ) -> OrderClearProjectionResult:
        """
        Redis 주문 projection 초기화.

        삭제 대상:
        - order:live:{id}
        - orders:open:{exchange}:{market_type}
        - orders:conditional:open:{exchange}:{market_type}
        - orders:unknown:{exchange}:{market_type}
        - orders:recovery:{exchange}:{market_type}
        - orders:by:symbol:{exchange}:{market_type}:{symbol}
        """

        live_hashes_deleted = 0
        indexes_deleted = 0

        result = OrderClearProjectionResult()

        # index_patterns: list[str] = [
        #     f"{RedisKey.ORDER_OPEN_SET}:*",
        #     f"{RedisKey.ORDER_CONDITIONAL_OPEN_SET}:*",
        #     f"{RedisKey.ORDER_UNKNOWN_ZSET}:*",
        #     f"{RedisKey.ORDER_RECOVERY_ZSET}:*",
        #     f"{RedisKey.ORDER_BY_SYMBOL_SET}:*",
        # ]

        if include_live_hashes:
            live_hashes_deleted = await self._delete_by_pattern(
                f"{RedisKey.ORDER_LIVE_PREFIX}:*"
            )
            result.cleared_live_hashes = live_hashes_deleted

        for pattern in ORDER_STATE_INDEX_PATTEN_LIST:
            result.cleared_indexes += await self._delete_by_pattern(pattern)

        result.total_deleted = live_hashes_deleted + indexes_deleted

        return result

    async def _delete_by_pattern(self, pattern: str) -> int:
        client = self.redis.client
        cursor = 0
        deleted = 0

        while True:
            cursor, keys = await client.scan(
                cursor=cursor,
                match=pattern,
                count=self.scan_num_once,
            )

            if keys:
                deleted += int(await client.delete(*keys))

            if cursor == 0:
                break

        return deleted

    async def increment_reconcile_failure(
        self,
        *,
        exchange: str,
        market_type: str,
        order_id: str,
        ttl_sec: int,
    ) -> int:
        key = _reconcile_failure_key(
            exchange=exchange,
            market_type=market_type,
            order_id=order_id,
        )

        count = await self.redis.client.incr(key)
        await self.redis.client.expire(key, ttl_sec)
        return int(count)

    async def clear_reconcile_failure(
        self,
        *,
        exchange: str,
        market_type: str,
        order_id: str,
    ) -> None:
        key = _reconcile_failure_key(
            exchange=exchange,
            market_type=market_type,
            order_id=order_id,
        )

        await self.redis.client.delete(key)

    # [deprecated]
    async def update_status(
        self,
        order_id: str,
        status: OrderStatus | str,
        updated_ts: int,
        **extra_fields: Any,
    ) -> Optional[dict[str, Any]]:
        """
        Deprecated.

        PostgreSQL 원본 구조에서는 Redis 단독 상태 전이를 금지한다.
        반드시 OrderStateService.transition_order()
        -> upsert_projection_if_newer() 경로를 사용해야 한다.
        """
        raise RuntimeError(
            "OrderStateRedisRepository.update_status() is deprecated. "
            "Use OrderStateService.transition_order() instead."
        )

    # [deprecated]
    async def transition_status(
        self,
        *,
        order_id: str,
        expected_status: OrderStatus | str | None,
        target_status: OrderStatus | str,
        updated_ts: int,
        **extra_fields: Any,
    ) -> bool:
        """
        Deprecated.

        Redis 중심 상태 전이는 REGULAR / CONDITIONAL 분리 index를 깨뜨릴 수 있다.
        """
        raise RuntimeError(
            "OrderStateRedisRepository.transition_status() is deprecated. "
            "Use OrderStateService.transition_order() instead."
        )


def _decode_redis_value(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode()
    return str(value)
