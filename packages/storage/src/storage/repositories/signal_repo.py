"""
SignalRedisRepository — 반자동 주문 워크플로우를 위한 시그널 상태 관리.

구조:
    signal:pending:{signal_id}  → Hash (Signal JSON 필드)
    signal:pending:index        → Sorted Set (score=generated_ts, member=signal_id)

TTL:
    각 시그널 Hash는 expires_ts 기준으로 자동 만료.
    기본 TTL이 설정되지 않은 경우 DEFAULT_SIGNAL_TTL_SEC 적용.
"""

from typing import Optional, List, Dict, Any

from common.logging import setup_logger
from common.time import current_time_ms
from storage.identifiers import RedisKey, redis_signal_pending_key
from storage.redis_client import RedisStreamClient

logger = setup_logger(__name__)

# 기본 시그널 승인 대기 시간 (초) — expires_ts 미설정 시 사용
DEFAULT_SIGNAL_TTL_SEC = 1800  # 30분


class SignalRedisRepository:
    """
    반자동 주문을 위한 시그널 상태 관리.

    전략이 시그널을 생성하면 PENDING 상태로 Redis에 저장하고,
    사용자가 승인/거부할 때까지 대기합니다.
    """

    def __init__(self, redis: RedisStreamClient, default_ttl_sec: int = DEFAULT_SIGNAL_TTL_SEC):
        self.redis = redis
        self.default_ttl_sec = default_ttl_sec

    def _key(self, signal_id: str) -> str:
        return redis_signal_pending_key(signal_id)

    async def save_pending(self, signal_dict: Dict[str, Any]) -> None:
        """
        신규 시그널을 PENDING 상태로 저장.
        expires_ts가 없으면 default_ttl_sec 기준으로 자동 설정.
        """
        signal_id = signal_dict["signal_id"]
        generated_ts = signal_dict.get("generated_ts", current_time_ms())
        key = self._key(signal_id)

        # expires_ts 미설정 시 기본값
        if not signal_dict.get("expires_ts"):
            signal_dict["expires_ts"] = generated_ts + (self.default_ttl_sec * 1000)

        # TTL 계산 (현재 시각 기준)
        now_ms = current_time_ms()
        remaining_ms = signal_dict["expires_ts"] - now_ms
        ttl_sec = max(int(remaining_ms / 1000), 60)  # 최소 60초

        # 상태 강제 설정
        signal_dict["status"] = "PENDING"

        fields = {k: str(v) if v is not None else "" for k, v in signal_dict.items()}

        client = self.redis.client
        async with client.pipeline(transaction=False) as pipe:
            pipe.hset(key, mapping=fields)
            pipe.expire(key, ttl_sec)
            pipe.zadd(RedisKey.SIGNAL_PENDING_INDEX, {signal_id: float(generated_ts)})
            await pipe.execute()

        logger.info(
            f"Signal saved as PENDING: {signal_id} "
            f"(TTL={ttl_sec}s, expires_ts={signal_dict['expires_ts']})"
        )

    async def get_pending(self, signal_id: str) -> Optional[Dict[str, Any]]:
        """승인 대기 중인 시그널 조회. 없거나 만료되면 None."""
        data = await self.redis.client.hgetall(self._key(signal_id))
        if not data:
            # 인덱스에서도 정리
            await self.redis.client.zrem(RedisKey.SIGNAL_PENDING_INDEX, signal_id)
            return None
        return data

    async def list_pending(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        승인 대기 중인 시그널 목록 (최신순).
        만료된 시그널은 자동 정리됩니다.
        """
        # Sorted Set에서 최신순으로 signal_id 가져오기
        signal_ids = await self.redis.client.zrevrange(RedisKey.SIGNAL_PENDING_INDEX, 0, limit - 1)
        if not signal_ids:
            return []

        signals = []
        expired_ids = []

        for signal_id in signal_ids:
            data = await self.redis.client.hgetall(self._key(signal_id))
            if data and data.get("status") == "PENDING":
                signals.append(data)
            else:
                expired_ids.append(signal_id)

        # 만료된 항목 인덱스 정리
        if expired_ids:
            await self.redis.client.zrem(RedisKey.SIGNAL_PENDING_INDEX, *expired_ids)

        return signals

    async def approve(
        self,
        signal_id: str,
        order_id: str,
        approved_ts: int,
    ) -> Optional[Dict[str, Any]]:
        """
        시그널 승인 → APPROVED 상태로 전환 + 생성된 주문 ID 매핑.
        반환: 업데이트된 시그널 dict, 없으면 None.
        """
        key = self._key(signal_id)
        client = self.redis.client

        data = await client.hgetall(key)
        if not data:
            logger.warning(f"Signal not found for approval: {signal_id}")
            return None

        if data.get("status") != "PENDING":
            logger.warning(
                f"Signal {signal_id} is not PENDING (status={data.get('status')}), cannot approve"
            )
            return None

        update = {
            "status": "APPROVED",
            "approved_order_id": order_id,
            "approved_ts": str(approved_ts),
        }

        async with client.pipeline(transaction=False) as pipe:
            pipe.hset(key, mapping=update)
            # 승인된 시그널은 인덱스에서 제거 (더 이상 pending이 아님)
            pipe.zrem(RedisKey.SIGNAL_PENDING_INDEX, signal_id)
            # 승인된 시그널도 24시간 후 만료
            pipe.expire(key, 86400)
            await pipe.execute()

        logger.info(f"Signal approved: {signal_id} → order_id={order_id}")

        # 최신 데이터 반환
        return await client.hgetall(key)

    async def dismiss(self, signal_id: str) -> Optional[Dict[str, Any]]:
        """시그널 거부/무시 → DISMISSED 상태로 전환."""
        key = self._key(signal_id)
        client = self.redis.client

        data = await client.hgetall(key)
        if not data:
            logger.warning(f"Signal not found for dismissal: {signal_id}")
            return None

        if data.get("status") != "PENDING":
            logger.warning(
                f"Signal {signal_id} is not PENDING (status={data.get('status')}), cannot dismiss"
            )
            return None

        async with client.pipeline(transaction=False) as pipe:
            pipe.hset(key, mapping={"status": "DISMISSED"})
            pipe.zrem(RedisKey.SIGNAL_PENDING_INDEX, signal_id)
            # 거부된 시그널은 1시간 후 만료
            pipe.expire(key, 3600)
            await pipe.execute()

        logger.info(f"Signal dismissed: {signal_id}")
        return await client.hgetall(key)

    async def cleanup_expired(self) -> int:
        """
        만료된 시그널 인덱스 정리.
        TTL에 의해 Hash는 자동 삭제되지만, Sorted Set 인덱스는 수동 정리 필요.
        반환: 정리된 항목 수.
        """
        signal_ids = await self.redis.client.zrange(RedisKey.SIGNAL_PENDING_INDEX, 0, -1)
        if not signal_ids:
            return 0

        expired = []
        for signal_id in signal_ids:
            exists = await self.redis.client.exists(self._key(signal_id))
            if not exists:
                expired.append(signal_id)

        if expired:
            await self.redis.client.zrem(RedisKey.SIGNAL_PENDING_INDEX, *expired)
            logger.info(f"Cleaned up {len(expired)} expired signal index entries")

        return len(expired)
