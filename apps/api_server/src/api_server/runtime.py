from __future__ import annotations

from storage.projection.position_projection_rebuilder import PositionProjectionRebuilder

import asyncio
from dataclasses import dataclass, field

from storage.postgres_client import PostgresClient
from storage.redis_client import RedisStreamClient
from storage.repositories.redis.order_state_repo import OrderStateRedisRepository
from storage.repositories.signal_repo import SignalRedisRepository
from storage.repositories.redis.strategy_control_repo import StrategyControlRedisRepository
from storage.questdb_client import QuestDBClient
from storage.repositories.execution_questdb import ExecutionQuestDBRepository
from storage.repositories.postgres.order_intent_repo import OrderIntentPostgresRepository
from storage.repositories.postgres.order_repo import OrderPostgresRepository
from storage.repositories.postgres.outbox_repo import OutboxPostgresRepository
from storage.repositories.postgres.strategy_repo import StrategyPostgresRepository
from storage.repositories.postgres.strategy_risk_config_repo import StrategyRiskConfigPostgresRepository

# from execution_gateway.adapters.binance.binance_rest_adapter import BinanceRestAdapter
# from execution_gateway.adapters.binance.binance_rate_limiter import BinanceRateLimiter
from execution_gateway.gateway import ExecutionGateway
from execution_gateway.workers.reconciliation_worker import ReconciliationWorker
from execution_gateway.workers.recovery_worker import RecoveryWorker
from storage.projection.order_projection_rebuilder import OrderProjectionRebuilder
# from execution_gateway.listeners.binance.binance_user_data_stream import BinanceUserDataStreamListener
from execution_gateway.publishers.outbox_publisher import OutboxPublisher
from execution_gateway.publishers.redpanda_event_publisher import RedpandaEventPublisher

from storage.repositories.postgres.position_repo import PositionPostgresRepository
from storage.repositories.redis.position_state_repo import PositionRedisRepository
from execution_gateway.services.order_state_service import OrderStateService
from execution_gateway.services.position_state_service import PositionStateService
from execution_gateway.workers.position_reconciliation_worker import PositionReconciliationWorker
from execution_gateway.services.position_order_service import PositionOrderService

from .services.execution_log_service import ExecutionLogService
from .services.order.order_service import OrderService
from .services.signal.signal_service import SignalService
from .services.strategy_control_service import StrategyControlService
from .services.account.account_service import AccountService
from .services.position.position_service import PositionService

from execution_gateway.exchange.registry import ExchangeExecutionClientRegistry

from execution_gateway.listeners.user_data_stream import UserDataStreamListener
from schemas.market import Exchange, MarketType

from execution_gateway.exchange import AsyncClosable
from .websocket_manager import WebSocketManager

@dataclass(slots=True)
class AppState:
    # PostgreSQL
    postgres: PostgresClient | None = None
    order_intent_pg_repo: OrderIntentPostgresRepository | None = None
    order_pg_repo: OrderPostgresRepository | None = None
    outbox_pg_repo: OutboxPostgresRepository | None = None
    strategy_pg_repo: StrategyPostgresRepository | None = None
    strategy_risk_config_pg_repo: StrategyRiskConfigPostgresRepository | None = None

    # Redis
    redis: RedisStreamClient | None = None
    order_repo: OrderStateRedisRepository | None = None
    signal_repo: SignalRedisRepository | None = None
    strategy_control_repo: StrategyControlRedisRepository | None = None

    # State Service
    order_state_service: OrderStateService | None = None
    order_projection_rebuilder: OrderProjectionRebuilder | None = None

    # QuestDB
    questdb: QuestDBClient | None = None
    exec_repo: ExecutionQuestDBRepository | None = None

    # Execution logging (QuestDB + Redis fill dedup)
    execution_log_service: ExecutionLogService | None = None

    order_service: OrderService | None = None
    signal_service: SignalService | None = None
    strategy_control_service: StrategyControlService | None = None
    account_service: AccountService | None = None
    position_service: PositionService | None = None

    # # Binance Adapter
    # adapter: BinanceRestAdapter | None = None
    # rate_limiter: BinanceRateLimiter | None = None
    gateway: ExecutionGateway | None = None

    # # User Data Stream Listener
    # listener: BinanceUserDataStreamListener | None = None
    # listener_task: asyncio.Task[None] | None = None

    # User Data Stream Listener
    listeners: dict[tuple[Exchange, MarketType], UserDataStreamListener] = field(
        default_factory=dict
    )
    listener_tasks: dict[tuple[Exchange, MarketType], asyncio.Task[None]] = field(
        default_factory=dict
    )

    # Background Workers
    reconciliation_worker: ReconciliationWorker | None = None
    recovery_worker: RecoveryWorker | None = None

    # Event publishing
    event_publisher: RedpandaEventPublisher | None = None
    outbox_publisher: OutboxPublisher | None = None

    # Position
    position_pg_repo: PositionPostgresRepository | None = None
    position_repo: PositionRedisRepository | None = None
    position_state_service: PositionStateService | None = None
    position_reconciliation_worker: PositionReconciliationWorker | None = None
    position_order_service: PositionOrderService | None = None

    position_projection_rebuilder: PositionProjectionRebuilder | None = None

    # Runtime flags
    is_ready: bool = False

    # Exchange Client Registry
    exchange_clients: ExchangeExecutionClientRegistry | None = None

    exchange_closables: list[AsyncClosable] = field(default_factory=list)

    # WebSocket & Collector
    ws_manager: WebSocketManager | None = None
    collector_task: asyncio.Task[None] | None = None
    signal_ws_task: asyncio.Task[None] | None = None
    order_intent_consumer_task: asyncio.Task[None] | None = None



state = AppState()

def is_app_ready() -> bool:
    return state.is_ready
