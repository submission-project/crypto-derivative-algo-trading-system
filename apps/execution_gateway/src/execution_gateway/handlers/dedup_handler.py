from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class DedupStore(Protocol):
    async def reserve(self, key: str, ttl_seconds: int) -> bool:
        """Return True only when key was not seen before."""


@dataclass(slots=True)
class InMemoryDedupStore:
    seen: set[str]

    def __init__(self) -> None:
        self.seen = set()

    async def reserve(self, key: str, ttl_seconds: int) -> bool:
        if key in self.seen:
            return False
        self.seen.add(key)
        return True


class RedisDedupStore:
    def __init__(self, redis_client, prefix: str = "dedup:order_intent") -> None:
        self.redis_client = redis_client
        self.prefix = prefix

    async def reserve(self, key: str, ttl_seconds: int) -> bool:
        redis = self.redis_client.client
        redis_key = f"{self.prefix}:{key}"
        return bool(await redis.set(redis_key, "1", ex=ttl_seconds, nx=True))


@dataclass(frozen=True, slots=True)
class DedupDecision:
    accepted: bool
    key: str


class OrderIntentDedupHandler:
    def __init__(self, store: DedupStore, ttl_seconds: int = 60 * 60) -> None:
        self.store = store
        self.ttl_seconds = ttl_seconds

    async def evaluate(self, intent: dict) -> DedupDecision:
        key = self.key_for_intent(intent)
        accepted = await self.store.reserve(key, self.ttl_seconds)
        return DedupDecision(accepted=accepted, key=key)

    @staticmethod
    def key_for_intent(intent: dict) -> str:
        signal_id = str(intent.get("signal_id") or "").strip()
        if signal_id:
            return signal_id
        strategy = str(intent.get("strategy_name") or "unknown").strip()
        exchange = str(intent.get("exchange") or "unknown").strip()
        symbol = str(intent.get("symbol") or "unknown").strip()
        generated_ts = str(intent.get("generated_ts") or "0").strip()
        side = str(intent.get("side") or "unknown").strip()
        return f"{strategy}:{exchange}:{symbol}:{side}:{generated_ts}"
