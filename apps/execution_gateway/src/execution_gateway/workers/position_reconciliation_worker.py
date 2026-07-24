from __future__ import annotations

import asyncio
from typing import Optional

from common.logging import setup_logger
# from execution_gateway.adapters.binance.binance_rest_adapter import BinanceRestAdapter
# from execution_gateway.limiter.execution_rate_limiter import ExecutionRateLimiter
from execution_gateway.services.position_state_service import PositionStateService

from execution_gateway.exchange.registry import ExchangeExecutionClientRegistry
from execution_gateway.exchange.client import ExchangeExecutionClient
from schemas.market import Exchange, MarketType

logger = setup_logger(__name__)


class PositionReconciliationWorker:
    """
    Binance positionRisk snapshot을 주기적으로 가져와
    PostgreSQL positions + Redis projection을 동기화한다.
    """

    def __init__(
        self,
        *,
        exchange_clients: ExchangeExecutionClientRegistry,
        # adapter: BinanceRestAdapter,
        position_state_service: PositionStateService,
        markets: list[tuple[Exchange, MarketType]],
        # rate_limiter: ExecutionRateLimiter,
        interval_sec: float = 30.0,
        active_symbols: Optional[set[str]] = None,
    ) -> None:
        # self.adapter = adapter
        self.exchange_clients = exchange_clients
        self.position_state_service = position_state_service
        self.markets = tuple(markets)
        # self.rate_limiter = rate_limiter
        self.interval_sec = interval_sec
        self.active_symbols = active_symbols

        self._running = False
        self._stop_event = asyncio.Event()
        self._task: Optional[asyncio.Task[None]] = None

    async def start(self) -> None:
        if self._running:
            logger.warning("PositionReconciliationWorker가 이미 실행 중입니다.")
            return

        self._running = True
        self._stop_event.clear()
        self._task = asyncio.create_task(
            self._run_loop(),
            name="position-reconciliation-worker",
        )

        logger.info(f"PositionReconciliationWorker 시작: interval={self.interval_sec}s")

    async def stop(self) -> None:
        self._running = False
        self._stop_event.set()

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            finally:
                self._task = None

        logger.info("PositionReconciliationWorker 종료 완료")

    async def _run_loop(self) -> None:
        while self._running:
            try:
                await self.reconcile_once()

                stopped = await self._sleep_or_stop(self.interval_sec)
                if stopped:
                    break

            except asyncio.CancelledError:
                break

            except Exception as e:
                logger.error(
                    f"PositionReconciliationWorker loop error: {e}",
                    exc_info=True,
                )

                stopped = await self._sleep_or_stop(self.interval_sec)
                if stopped:
                    break

    async def _sleep_or_stop(self, delay_sec: float) -> bool:
        try:
            await asyncio.wait_for(
                self._stop_event.wait(),
                timeout=delay_sec,
            )
            return True
        except asyncio.TimeoutError:
            return False

    async def reconcile_once(self) -> None:
        total = 0

        for exchange, market_type in self.markets:
            client = self._client(exchange, market_type)

            if not client.capabilities.supports_position_snapshot:
                continue

            if not self.active_symbols:
                snapshots = await client.get_positions()
                updated = await self.position_state_service.refresh_position_snapshots(
                    snapshots
                )

                total += len(updated)

                logger.info(
                    f"position snapshot 전체 refresh 완료: "
                    f"exchange={exchange.value}, "
                    f"market_type={market_type.value}, "
                    f"count={len(updated)}"
                )
                continue

            market_total = 0

            for symbol in self.active_symbols:
                snapshots = await client.get_positions(symbol=symbol)
                updated = await self.position_state_service.refresh_position_snapshots(
                    snapshots
                )

                market_total += len(updated)

            total += market_total

            logger.info(
                f"position snapshot active_symbols refresh 완료: "
                f"exchange={exchange.value}, "
                f"market_type={market_type.value}, "
                f"count={market_total}"
            )

        logger.info(f"position reconciliation 완료: total_count={total}")

        # if not self.active_symbols:
        #     await self.rate_limiter.acquire_request_weight(weight=5)

        #     rows = await self.adapter.get_position_risk_v3()

        #     updated = await self.position_state_service.refresh_positions_from_exchange(
        #         rows
        #     )

        #     logger.info(f"positionRisk 전체 refresh 완료: count={len(updated)}")
        #     return

        # for symbol in self.active_symbols:
        #     await self.rate_limiter.acquire_request_weight(weight=5)

        #     rows = await self.adapter.get_position_risk_v3(symbol=symbol)

        #     updated = await self.position_state_service.refresh_positions_from_exchange(
        #         rows
        #     )

        #     total += len(updated)

        # logger.info(f"positionRisk active_symbols refresh 완료: count={total}")

    def _client(
        self,
        exchange: Exchange,
        market_type: MarketType,
    ) -> ExchangeExecutionClient:
        return self.exchange_clients.get(
            exchange=exchange,
            market_type=market_type,
        )
