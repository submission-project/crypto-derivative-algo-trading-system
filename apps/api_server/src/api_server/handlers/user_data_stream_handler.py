"""
User Data Stream 콜백 핸들러.

listener.start()에 등록되어 주문/계정/조건부 주문 이벤트를 처리한다.
"""

from __future__ import annotations

from common.logging import setup_logger
from schemas.order_update_event import NormalizedOrderUpdateEvent
from schemas.conditional_order_event import NormalizedConditionalOrderEvent
from schemas.position_update_event import NormalizedPositionSnapshot
from schemas.order import Order

from ..runtime import state

logger = setup_logger(__name__)


async def on_trade_update(event_data: NormalizedOrderUpdateEvent) -> None:
    """
    정규화된 주문 update 이벤트 처리.

    핵심:
    - 상태 변경은 gateway.apply_order_update_event()에서만 처리
    - QuestDB execution log는 state.execution_log_service에서 처리
    """
    client_order_id = event_data.client_order_id
    if not client_order_id:
        logger.warning(f"client_order_id 누락된 주문 이벤트: {event_data}")
        return

    gateway = state.gateway
    if gateway is None:
        logger.error(
            f"gateway 미초기화 상태에서 UserDataStream 주문 이벤트 수신: "
            f"client_order_id={client_order_id}"
        )
        return

    order: Order | None = await gateway.apply_order_update_event(event_data)
    if not order:
        logger.error(
            f"로컬 주문을 찾지 못한 UserDataStream 이벤트: "
            f"exchange={event_data.exchange.value}, "
            f"market_type={event_data.market_type.value}, "
            f"client_order_id={client_order_id}"
        )
        return

    execution_log_service = state.execution_log_service
    if execution_log_service is None:
        logger.error(
            f"execution_log_service 미초기화 상태에서 체결 이벤트 수신: "
            f"client_order_id={client_order_id}"
        )
        return

    await execution_log_service.save_if_needed(
        order=order,
        event_data=event_data,
    )

# [check] on_account_update -> on_position_update
async def on_position_update(
    snapshots: list[NormalizedPositionSnapshot],
) -> None:
    position_state_service = state.position_state_service
    if position_state_service is None:
        logger.error("position_state_service 미초기화 상태에서 position update 수신")
        return

    if not snapshots:
        return

    updated_positions = await position_state_service.apply_position_snapshots(
        snapshots=snapshots,
    )

    logger.info(
        f"position update 반영 완료: "
        f"exchange={snapshots[0].exchange.value}, "
        f"market_type={snapshots[0].market_type.value}, "
        f"updated_positions={len(updated_positions)}"
    )

# async def on_account_update(event_data: AccountUpdateEnvelope) -> None:
#     """
#     ACCOUNT_UPDATE 이벤트 처리.

#     여기서는 주문 상태를 건드리지 않고,
#     잔고/포지션 상태 캐시 또는 리스크 엔진 갱신에 사용한다.
#     """
#     raw = event_data.raw
#     account_data = raw.get("a", {})
#     if not isinstance(account_data, dict):
#         logger.warning(f"ACCOUNT_UPDATE account payload가 dict가 아님: {raw}")
#         return

#     reason = account_data.get("m")
#     balances = account_data.get("B", [])
#     positions = account_data.get("P", [])

#     logger.info(
#         f"ACCOUNT_UPDATE 수신: "
#         f"reason={reason}, "
#         f"balances={len(balances)}, "
#         f"positions={len(positions)}"
#     )
#     try:
#         position_state_service = state.position_state_service
#         if position_state_service is None:
#             logger.error(
#                 f"position_state_service 미초기화 상태에서 ACCOUNT_UPDATE 수신: "
#                 f"reason={reason}"
#             )
#             return

#         updated_positions = await position_state_service.apply_account_update(event_data)

#         logger.info(
#             f"ACCOUNT_UPDATE position 반영 완료: "
#             f"updated_positions={len(updated_positions)}"
#         )

#     except Exception as e:
#         logger.error(
#             f"ACCOUNT_UPDATE position 반영 실패: "
#             f"reason={reason}, err={e}",
#             exc_info=True,
#         )

#     # [CLAIM]
#     # 나중에 추가할 곳:
#     # await state.account_repo.apply_balance_updates(balances)
#     # await state.risk_manager.refresh_from_account_update(...)

#     # 주의:
#     # positions는 전체 포지션 스냅샷이 아니라 "변경된 포지션만" 들어올 수 있으므로
#     # 전체 덮어쓰기 방식이 아니라 patch update로 처리해야 한다.

async def on_algo_update(event_data: NormalizedConditionalOrderEvent) -> None:
    """
    조건부 주문 이벤트 처리.

    거래소별 listener/mapper가 이미 NormalizedConditionalOrderEvent로 변환한
    이벤트만 받아 Gateway에 반영한다.
    """
    try:
        gateway = state.gateway
        if gateway is None:
            logger.error(
                f"gateway 미초기화 상태에서 조건부 주문 이벤트 수신: "
                f"raw={event_data.raw}"
            )
            return

        # normalized = normalize_binance_algo_update(
        #     raw_event=event_data.raw,
        #     market_type=MarketType.PERP,
        # )

        # updated = await gateway.apply_conditional_order_event(normalized)
        updated = await gateway.apply_conditional_order_event(event_data)

        if updated:
            logger.info(
                f"조건부 주문 이벤트 반영 완료: "
                f"exchange={event_data.exchange.value}, "
                f"market_type={event_data.market_type.value}, "
                f"order_id={updated.order_id}, "
                f"conditional_status="
                f"{updated.conditional_status.value if updated.conditional_status else None}, "
            )

    except Exception as e:
        logger.error(
            f"조건부 주문 이벤트 처리 실패: raw={event_data.raw}, err={e}",
            exc_info=True,
        )
