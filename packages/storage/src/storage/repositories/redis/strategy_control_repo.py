from __future__ import annotations


class StrategyControlRedisRepository:
    def __init__(self, redis_client, prefix: str = "strategy:control") -> None:
        self.redis_client = redis_client
        self.prefix = prefix

    async def set_enabled(self, strategy_name: str, enabled: bool) -> dict[str, object]:
        key = self._key(strategy_name)
        await self.redis_client.client.set(key, "1" if enabled else "0")
        return {"strategy_name": strategy_name, "enabled": enabled}

    async def is_enabled(self, strategy_name: str, default: bool = True) -> bool:
        value = await self.redis_client.client.get(self._key(strategy_name))
        if value is None:
            return default
        return str(value).strip().lower() in {"1", "true", "yes", "on", "enabled"}

    async def get_status(self, strategy_name: str, default: bool = True) -> dict[str, object]:
        return {
            "strategy_name": strategy_name,
            "enabled": await self.is_enabled(strategy_name, default=default),
        }

    def _key(self, strategy_name: str) -> str:
        cleaned = str(strategy_name or "").strip()
        if not cleaned:
            raise ValueError("strategy_name is required")
        return f"{self.prefix}:{cleaned}:enabled"
