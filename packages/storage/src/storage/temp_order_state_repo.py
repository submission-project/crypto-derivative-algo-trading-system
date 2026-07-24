# PostgreSQL 원본에서 온 Redis projection upsert용 Lua script.
#
# KEYS:
#   1 = order hash key
#   2 = order:open
#   3 = order:unknown
#   4 = order:recovery
#
# ARGV:
#   1 = incoming_version
#   2 = status
#   3 = order_id
#   4 = updated_ts
#   5 = terminal_ttl_sec
#   6 = field_count
#   7... = field/value pairs
#
# Return:
#   1 = applied
#   0 = ignored because incoming_version <= current_version
_UPSERT_PROJECTION_IF_NEWER_LUA = """
local order_key = KEYS[1]
local open_key = KEYS[2]
local unknown_key = KEYS[3]
local recovery_key = KEYS[4]

local incoming_version = tonumber(ARGV[1]) # 현재 Redis version보다 최신인지 비교
local status = ARGV[2] # terminal인지, recovery 대상인지 판단
local order_id = ARGV[3] # order:open, order:unknown, order:recovery 인덱스에 넣거나 제거할 member
local updated_ts = tonumber(ARGV[4]) # ZSet score로 사용
local terminal_ttl_sec = tonumber(ARGV[5]) # terminal 상태일 때 EXPIRE TTL 값
local field_count = tonumber(ARGV[6]) # ARGV[7]부터 field/value 쌍을 몇 개 읽을지 결정

local current_version_raw = redis.call('HGET', order_key, 'version')
local current_version = -1

if current_version_raw then
    current_version = tonumber(current_version_raw) or -1
end

if incoming_version <= current_version then
    return 0
end

local fields = {}
local arg_index = 7

for i = 1, field_count do
    table.insert(fields, ARGV[arg_index])  # field
    table.insert(fields, ARGV[arg_index + 1])  # value
    arg_index = arg_index + 2
end

redis.call('HSET', order_key, unpack(fields))

local is_terminal =
    status == 'FILLED'
    or status == 'CANCELLED'
    or status == 'REJECTED'
    or status == 'EXPIRED'

local is_recovery =
    status == 'SUBMITTED'
    or status == 'PENDING_CANCEL'
    or status == 'UNKNOWN'

if is_terminal then
    redis.call('SREM', open_key, order_id)
    redis.call('ZREM', unknown_key, order_id)
    redis.call('ZREM', recovery_key, order_id)
    redis.call('EXPIRE', order_key, terminal_ttl_sec)
else
    redis.call('SADD', open_key, order_id)
    redis.call('PERSIST', order_key)

    if status == 'UNKNOWN' then
        redis.call('ZADD', unknown_key, updated_ts, order_id)
    else
        redis.call('ZREM', unknown_key, order_id)
    end

    if is_recovery then
        redis.call('ZADD', recovery_key, updated_ts, order_id)
    else
        redis.call('ZREM', recovery_key, order_id)
    end
end

return 1
"""


async def upsert_projection_if_newer(
    self,
    order_dict: dict[str, Any],
) -> bool:
    """
    PostgreSQL 원본에서 온 최신 주문 상태를 Redis projection에 반영.

    규칙:
      - Redis에 주문이 없으면 반영
      - incoming version > current version 이면 반영
      - incoming version <= current version 이면 무시
      - Hash / open / unknown / recovery / TTL을 Lua로 원자적으로 함께 갱신

    Returns:
        True  = Redis projection에 반영됨
        False = stale projection이라 무시됨
    """
    order_id = order_dict["order_id"]
    key = self._key(order_id)

    status = self._normalize_status(order_dict.get("status"))
    status_value = status.value

    if order_dict.get("version") is None:
        raise ValueError("order.version is required for projection upsert")

    incoming_version = int(order_dict["version"])
    updated_ts = int(order_dict.get("updated_ts") or 0)

    fields = self._build_fields(
        order_dict,
        status_value=status_value,
    )

    # hash 필드
    argv: list[str] = [
        str(incoming_version),  # incoming_version
        status_value,  # status
        order_id,  # order_id
        str(updated_ts),  # updated_ts
        str(_TERMINAL_TTL_SEC),  # terminal_ttl_sec
        str(len(fields)),  # field_count
    ]

    # 필드 정보
    for field, value in fields.items():
        argv.append(str(field))
        argv.append(str(value))

    result = await self.redis.client.eval(
        _UPSERT_PROJECTION_IF_NEWER_LUA,
        4,
        key,
        _OPEN_SET_KEY,
        _UNKNOWN_ZSET_KEY,
        _RECOVERY_ZSET_KEY,
        *argv,
    )

    applied = int(result) == 1

    if applied:
        logger.debug(
            f"Redis projection upserted: "
            f"order_id={order_id}, "
            f"status={status_value}, "
            f"version={incoming_version}"
        )
    else:
        logger.debug(
            f"Stale Redis projection ignored: "
            f"order_id={order_id}, "
            f"status={status_value}, "
            f"version={incoming_version}"
        )

    return applied
