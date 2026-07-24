from typing import Protocol


class ExecutionRateLimiter(Protocol):
    """
    ExecutionGateway에서 사용할 rate limiter 인터페이스.
    """

    async def acquire_costs(
        self,
        *,
        order_10s: int = 0,
        order_1m: int = 0,
        request_weight: int = 0,
        local_order_count: int = 0,
    ) -> None: ...

    async def acquire_request_weight(self, weight: int = 1) -> None: ...

    async def acquire_order_slot(self, count: int = 1) -> None: ...

    async def acquire_single_order(self) -> None: ...

    async def acquire_batch_orders(self) -> None: ...