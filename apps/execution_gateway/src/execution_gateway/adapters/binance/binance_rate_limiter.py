from __future__ import annotations

from aiolimiter import AsyncLimiter

class BinanceRateLimiter:
    """
    단일 노드용 In-memory Binance USD-M Futures Rate Limiter.

    주의:
      - 단일 프로세스 기준 로컬 제한기
      - 여러 프로세스/서버/API key 공유 환경에서는 Redis 기반 limiter 필요
      - 응답 헤더 기반 보정은 별도 구현 필요

    참고 비용:
      - 단건 주문: order_10s=1, order_1m=1, request_weight=0 # https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api
      - 배치 주문: order_10s=5, order_1m=1, request_weight=5 # https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Place-Multiple-Orders#request-weight
      - 단건/배치 취소: request_weight=1 #https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Cancel-Order
      - openOrders(symbol 지정): request_weight=1 #https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Query-Open-Orders
      - openOrders(symbol 생략): request_weight=40 #https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Query-Open-Orders
      - get_order: request_weight=1 #https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Query-Order
    """

    def __init__(
        self,
        request_weight_per_min: int = 2400,
        orders_per_10s: int = 240,   # 공식 300보다 보수적으로
        orders_per_min: int = 960,   # 공식 1200보다 보수적으로
        local_orders_per_day: int | None = None,
    ):
        self.weight_limiter = AsyncLimiter(
            max_rate=request_weight_per_min,
            time_period=60,
        )

        self.order_10s_limiter = AsyncLimiter(
            max_rate=orders_per_10s,
            time_period=10,
        )

        self.order_1m_limiter = AsyncLimiter(
            max_rate=orders_per_min,
            time_period=60,
        )

        self.order_1d_limiter = (
            AsyncLimiter(
                max_rate=local_orders_per_day,
                time_period=86400,
            )
            if local_orders_per_day is not None
            else None
        )

    async def acquire_costs(
        self,
        *,
        order_10s: int = 0,
        order_1m: int = 0,
        request_weight: int = 0,
        local_order_count: int = 0,
    ) -> None:
        """
        Binance endpoint 비용을 그대로 반영하여 제한 슬롯 확보.

        Args:
            order_10s:
                X-MBX-ORDER-COUNT-10S에 반영할 비용.
            order_1m:
                X-MBX-ORDER-COUNT-1M에 반영할 비용.
            request_weight:
                X-MBX-USED-WEIGHT-1M에 반영할 비용.
            local_order_count:
                내부 일간 주문 제한에 반영할 수량.
                local_orders_per_day를 설정한 경우에만 사용.
        """
        if request_weight > 0:
            await self.weight_limiter.acquire(request_weight)

        if order_10s > 0:
            await self.order_10s_limiter.acquire(order_10s)

        if order_1m > 0:
            await self.order_1m_limiter.acquire(order_1m)

        if self.order_1d_limiter is not None and local_order_count > 0:
            await self.order_1d_limiter.acquire(local_order_count)

    async def acquire_request_weight(self, weight: int = 1) -> None:
        """
        일반 request weight만 소비하는 endpoint용.
        """
        if weight <= 0:
            return

        await self.acquire_costs(request_weight=weight)

    async def acquire_order_slot(self, count: int = 1) -> None:
        """
        기존 호환용 메서드.

        단건 주문 N개를 개별 호출한다고 가정한 비용:
          - order_10s = N
          - order_1m  = N
          - request_weight = 0

        주의:
          - batchOrders에는 이 메서드를 쓰면 안 된다.
        """
        if count <= 0:
            return

        await self.acquire_costs(
            order_10s=count,
            order_1m=count,
            local_order_count=count,
        )
    
    async def acquire_single_order(self) -> None:
        """
        POST /fapi/v1/order 비용.
        """
        await self.acquire_costs(
            order_10s=1,
            order_1m=1,
            request_weight=0,
            local_order_count=1,
        )

    async def acquire_batch_orders(self) -> None:
        """
        POST /fapi/v1/batchOrders 비용.

        batch 내 주문 수가 1~5건이어도 endpoint 비용은 동일하게 반영.
        """
        await self.acquire_costs(
            order_10s=5,
            order_1m=1,
            request_weight=5,
            local_order_count=5,
        )

    async def acquire_modify_order(self) -> None:
        """
        PUT /fapi/v1/order 비용.
        """
        await self.acquire_costs(
            order_10s=1,
            order_1m=1,
            request_weight=0,
            local_order_count=1,
        )

    async def acquire_batch_modify_orders(self) -> None:
        """
        PUT /fapi/v1/batchOrders 비용.

        Binance batch modify 비용도 단건과 다르게 별도 관리하는 편이 안전하다.
        """
        await self.acquire_costs(
            order_10s=5,
            order_1m=1,
            request_weight=5,
            local_order_count=5,
        )
