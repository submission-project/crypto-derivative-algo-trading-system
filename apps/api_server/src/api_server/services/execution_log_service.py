"""
QuestDB execution log persistence with Redis fill deduplication.

User Data Stream listener/mapper가 정규화한 NormalizedOrderUpdateEvent를 받아
실제 fill 이벤트만 QuestDB에 저장한다.
"""

from __future__ import annotations

import time
from decimal import Decimal
from typing import Any

from common.converters import enum_value
from common.logging import setup_logger
from common.metrics import (
    FILL_DEDUP_KEY_DELETE_TOTAL,
    FILL_DUPLICATE_SKIPPED_TOTAL,
    FILL_EVENTS_TOTAL,
    FILL_QUESTDB_SAVE_TOTAL,
)
from schemas.order import Order
from schemas.order_update_event import NormalizedOrderUpdateEvent
from storage.redis_client import RedisStreamClient
from storage.repositories.execution_questdb import ExecutionQuestDBRepository

logger = setup_logger(__name__)

_FILL_DEDUP_TTL_SEC = 24 * 60 * 60


class ExecutionLogService:
    """체결 QuestDB 기록 + Redis 기반 fill 중복 제거."""

    def __init__(
        self,
        *,
        exec_repo: ExecutionQuestDBRepository | None,
        redis: RedisStreamClient | None,
        fill_dedup_ttl_sec: int = _FILL_DEDUP_TTL_SEC,
    ) -> None:
        self._exec_repo = exec_repo
        self._redis = redis
        self.fill_dedup_ttl_sec = fill_dedup_ttl_sec

    @staticmethod
    def _enum_value(value: Any) -> Any:
        return enum_value(value)

    @staticmethod
    def _is_real_trade_fill(event: NormalizedOrderUpdateEvent) -> bool:
        if event.execution_type and event.execution_type.upper() != "TRADE":
            return False

        if not event.trade_id or event.trade_id in {"0", "-1", "None"}:
            return False

        if not event.last_fill_quantity:
            return False

        try:
            return Decimal(event.last_fill_quantity) > 0
        except Exception:
            return False

    @staticmethod
    def _build_execution_report(
        *,
        order: Order,
        event_data: NormalizedOrderUpdateEvent,
    ) -> dict[str, Any]:
        now_ms = time.time_ns() // 1_000_000

        return {
            "exchange": ExecutionLogService._enum_value(event_data.exchange),
            "market_type": ExecutionLogService._enum_value(event_data.market_type),
            "symbol": event_data.symbol.upper(),
            "side": ExecutionLogService._enum_value(order.side),
            "source": ExecutionLogService._enum_value(order.source),
            "is_maker": bool(event_data.is_maker),
            "execution_id": event_data.trade_id or "",
            "exchange_trade_id": event_data.trade_id or "",
            "order_id": order.order_id,
            "exchange_order_id": event_data.exchange_order_id
            or order.exchange_order_id
            or "",
            "signal_id": order.signal_id,
            "strategy_name": order.strategy_name,
            "fill_price": event_data.last_fill_price,
            "fill_quantity": event_data.last_fill_quantity,
            "commission": event_data.commission,
            "commission_asset": event_data.commission_asset,
            "exchange_ts": (
                event_data.transaction_time
                or event_data.event_time
                or now_ms
            ),
            "local_ts": now_ms,
        }

    @staticmethod
    def _build_fill_dedup_key(
        *,
        order: Order,
        trade_id: str,
    ) -> str:
        exchange = ExecutionLogService._enum_value(order.exchange)
        market_type = ExecutionLogService._enum_value(order.market_type)
        symbol = order.symbol.upper()

        return (
            f"fill:seen:"
            f"{exchange}:"
            f"{market_type}:"
            f"{symbol}:"
            f"{order.order_id}:"
            f"{trade_id}"
        )

    async def _mark_fill_seen_if_new(
        self,
        *,
        order: Order,
        trade_id: str,
    ) -> bool:
        redis = self._redis
        if redis is None or redis.client is None:
            return True

        key = ExecutionLogService._build_fill_dedup_key(
            order=order,
            trade_id=trade_id,
        )

        result = await redis.client.set(
            key,
            "1",
            ex=self.fill_dedup_ttl_sec,
            nx=True,
        )

        return bool(result)

    async def _delete_fill_seen_key(
        self,
        *,
        order: Order,
        trade_id: str,
    ) -> None:
        redis = self._redis
        if redis is None or redis.client is None:
            return

        key = ExecutionLogService._build_fill_dedup_key(
            order=order,
            trade_id=trade_id,
        )

        try:
            await redis.client.delete(key)
            FILL_DEDUP_KEY_DELETE_TOTAL.labels(result="success").inc()
        except Exception as e:
            FILL_DEDUP_KEY_DELETE_TOTAL.labels(result="failed").inc()
            logger.error(
                f"fill dedup key 삭제 실패: "
                f"order_id={order.order_id}, "
                f"trade_id={trade_id}, "
                f"err={e}",
                exc_info=True,
            )

    async def save_if_needed(
        self,
        *,
        order: Order,
        event_data: NormalizedOrderUpdateEvent,
    ) -> None:
        if not self._is_real_trade_fill(event_data):
            return

        if self._exec_repo is None:
            logger.error(
                "exec_repo 미초기화 상태에서 execution log 저장 시도 무시: "
                f"order_id={order.order_id}"
            )
            return

        trade_id = event_data.trade_id
        if not trade_id:
            return

        exchange = str(self._enum_value(event_data.exchange))
        market_type = str(self._enum_value(event_data.market_type))
        symbol = event_data.symbol.upper()

        FILL_EVENTS_TOTAL.labels(
            exchange=exchange,
            market_type=market_type,
            symbol=symbol,
        ).inc()

        try:
            is_new_fill = await self._mark_fill_seen_if_new(
                order=order,
                trade_id=trade_id,
            )
        except Exception as e:
            logger.warning(
                "fill dedup Redis 오류 — 중복 가능성을 감수하고 QuestDB 저장 진행: "
                f"order_id={order.order_id}, trade_id={trade_id}, err={e}",
                exc_info=True,
            )
            is_new_fill = True

        if not is_new_fill:
            FILL_DUPLICATE_SKIPPED_TOTAL.labels(
                exchange=exchange,
                market_type=market_type,
                symbol=symbol,
            ).inc()
            logger.info(
                f"중복 fill 이벤트 무시: "
                f"order_id={order.order_id}, "
                f"trade_id={trade_id}, "
                f"symbol={event_data.symbol}"
            )
            return

        execution_report = self._build_execution_report(
            order=order,
            event_data=event_data,
        )

        try:
            await self._exec_repo.save(execution_report)
            FILL_QUESTDB_SAVE_TOTAL.labels(result="success").inc()

            logger.info(
                f"QuestDB Execution log 저장 완료: "
                f"order_id={order.order_id}, "
                f"trade_id={trade_id}"
            )

        except Exception as e:
            await self._delete_fill_seen_key(
                order=order,
                trade_id=trade_id,
            )
            FILL_QUESTDB_SAVE_TOTAL.labels(result="failed").inc()

            logger.error(
                f"QuestDB Execution log 저장 실패: "
                f"order_id={order.order_id}, "
                f"trade_id={trade_id}, "
                f"event={event_data.model_dump(mode='json')}, "
                f"err={e}",
                exc_info=True,
            )
