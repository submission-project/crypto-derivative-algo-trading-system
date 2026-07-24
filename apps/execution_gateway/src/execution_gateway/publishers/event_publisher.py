from __future__ import annotations

from typing import Any

from abc import ABC, abstractmethod


class EventPublisher(ABC):
    @abstractmethod
    async def start(self) -> None:
        pass

    @abstractmethod
    async def stop(self) -> None:
        pass

    @abstractmethod
    async def publish(
        self,
        *,
        topic: str,
        key: str,
        value: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> None:
        pass