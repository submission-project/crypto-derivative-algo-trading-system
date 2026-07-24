from .runtime import state
from .config import settings as app_settings

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from common.logging import setup_logger
from common.config import settings as common_settings

from schemas.market import Exchange, MarketType

from execution_gateway.gateway import ExecutionGateway
from storage.redis_client import RedisStreamClient
from storage.repositories.redis.order_state_repo import OrderStateRedisRepository
from storage.repositories.redis.strategy_control_repo import StrategyControlRedisRepository
from storage.repositories.signal_repo import SignalRedisRepository
from storage.questdb_client import QuestDBClient
from storage.repositories.execution_questdb import ExecutionQuestDBRepository
from execution_gateway.workers.reconciliation_worker import ReconciliationWorker
from execution_gateway.workers.recovery_worker import RecoveryWorker

from storage.projection.order_projection_rebuilder import OrderProjectionRebuilder
from storage.projection.position_projection_rebuilder import PositionProjectionRebuilder

from .config.execution_markets import parse_enabled_execution_markets

from .routes import orders, signals, health, account, positions, orderbook, websocket, strategies
import asyncio

from cex_market_data_collector.module_loader import ensure_exchange_package_paths
from cex_market_data_collector.operational_adapters import build_operational_specs
from cex_market_data_collector.operational_runtime import run_operational_specs
from .websocket_manager import WebSocketManager, WebSocketManagerSink
import contextlib

from execution_gateway.listeners.user_data_stream import UserDataStreamListener

from .factories.user_data_stream_listener_factory import (
    create_user_data_stream_listeners,
)

from storage.postgres_client import PostgresClient
from storage.repositories.postgres.order_intent_repo import (
    OrderIntentPostgresRepository,
)
from storage.repositories.postgres.order_repo import (
    OrderPostgresRepository,
)
from storage.repositories.postgres.outbox_repo import (
    OutboxPostgresRepository,
)
from storage.repositories.postgres.strategy_repo import StrategyPostgresRepository
from storage.repositories.postgres.strategy_risk_config_repo import StrategyRiskConfigPostgresRepository
from execution_gateway.services.order_state_service import OrderStateService

from execution_gateway.publishers.outbox_publisher import OutboxPublisher
from execution_gateway.publishers.redpanda_event_publisher import RedpandaEventPublisher

from .services.execution_log_service import ExecutionLogService
from .services.order.order_service import OrderService
from .services.signal.signal_service import SignalService
from .services.strategy_control_service import StrategyControlService
from .services.account.account_service import AccountService
from .services.position.position_service import PositionService

from .helper import log_background_task_result

from .handlers.user_data_stream_handler import (
    # on_account_update,
    on_trade_update,
    on_algo_update,
    on_position_update,
)

from storage.repositories.postgres.position_repo import PositionPostgresRepository
from storage.repositories.redis.position_state_repo import PositionRedisRepository
from execution_gateway.services.position_state_service import PositionStateService
from execution_gateway.workers.position_reconciliation_worker import PositionReconciliationWorker
from execution_gateway.services.position_order_service import PositionOrderService

from prometheus_client import make_asgi_app

from api_server.factories.exchange_bootstrap import build_exchange_runtime
from api_server.websocket.signal_stream import run_signal_websocket_broadcaster
from execution_gateway.consumers.order_intent_consumer import run_order_intent_consumer
from execution_gateway.handlers.dedup_handler import OrderIntentDedupHandler, RedisDedupStore
from execution_gateway.handlers.order_submit_handler import OrderSubmitHandler
from execution_gateway.handlers.risk_handler import PreTradeRiskHandler

logger = setup_logger(__name__)


# ───────────────────────────── Lifespan ─────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        state.is_ready = False
        await _startup_app_state()
        # 12. 초기화 완료
        state.is_ready = True
        logger.info("API Server & Execution Gateway 시작 완료")

        yield

    finally:
        state.is_ready = False
        logger.info("API Server & Execution Gateway 종료 중...")

        # in-flight 요청 drain 대기.
        # is_ready=False 이후 새 요청은 health check 실패로 라우팅 중단되지만,
        # 이미 처리 중인 요청이 PG/Redis 커넥션 종료로 실패하는 것을 방지한다.
        drain_sec = app_settings.shutdown_drain_sec
        logger.info(f"in-flight 요청 drain 대기: {drain_sec}s")
        await asyncio.sleep(drain_sec)

        await _shutdown_app_state()

        logger.info("API Server & Execution Gateway 종료 완료")


# ───────────────────────────── FastAPI App ─────────────────────────────

app = FastAPI(
    title="Takora Trading API",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ───────────────────────────── Prometheus mount ─────────────────────────────

metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


# ───────────────────────────── Dependency Overrides ─────────────────────────────


def get_order_service() -> OrderService:
    if state.order_service is None:
        raise HTTPException(
            status_code=503,
            detail="OrderService is not ready",
        )
    return state.order_service


def get_signal_service() -> SignalService:
    if state.signal_service is None:
        raise HTTPException(
            status_code=503,
            detail="SignalService is not ready",
        )
    return state.signal_service


def get_account_service() -> AccountService:
    if state.account_service is None:
        raise HTTPException(
            status_code=503,
            detail="AccountService is not ready",
        )
    return state.account_service


def get_position_service() -> PositionService:
    if state.position_service is None:
        raise HTTPException(
            status_code=503,
            detail="PositionService is not ready",
        )
    return state.position_service


def get_gateway() -> ExecutionGateway:
    if state.gateway is None:
        raise HTTPException(
            status_code=503,
            detail="ExecutionGateway is not ready",
        )
    return state.gateway


def get_strategy_control_service() -> StrategyControlService:
    if state.strategy_control_service is None:
        raise HTTPException(
            status_code=503,
            detail="StrategyControlService is not ready",
        )
    return state.strategy_control_service


app.dependency_overrides[orders.get_order_service] = get_order_service
app.dependency_overrides[signals.get_signal_service] = get_signal_service
app.dependency_overrides[account.get_account_service] = get_account_service
app.dependency_overrides[positions.get_position_service] = get_position_service
app.dependency_overrides[orderbook.get_gateway] = get_gateway
app.dependency_overrides[strategies.get_strategy_control_service] = get_strategy_control_service


def get_websocket_manager() -> WebSocketManager:
    if state.ws_manager is None:
        raise HTTPException(
            status_code=503,
            detail="WebSocketManager is not ready",
        )
    return state.ws_manager


app.dependency_overrides[websocket.get_websocket_manager] = get_websocket_manager


# ───────────────────────────── Routers ─────────────────────────────

app.include_router(health.router)
app.include_router(orders.router)
app.include_router(signals.router)
app.include_router(account.router)
app.include_router(positions.router)
app.include_router(orderbook.router)
app.include_router(websocket.router)
app.include_router(strategies.router)


async def _startup_app_state() -> None:
    # markets: list[tuple[Exchange, MarketType]] = [
    #     (Exchange.BINANCE, MarketType.PERP),
    # ]
    markets: list[tuple[Exchange, MarketType]] = parse_enabled_execution_markets(
        common_settings.enabled_execution_markets
    )
    
    # 0. PostgreSQL 초기화
    postgres_dsn = common_settings.postgres_dsn
    if not postgres_dsn:
        raise RuntimeError(
            "postgres_dsn 미설정: 환경 변수 POSTGRES_DSN을 설정하거나 "
            "ENV_FILE(.env / .env.dev 등)에 POSTGRES_DSN=postgresql://… 를 넣어 주세요."
        )

    state.postgres = PostgresClient(
        dsn=postgres_dsn,
        min_size=common_settings.postgres_min_size,
        max_size=common_settings.postgres_max_size,
    )
    await state.postgres.connect()

    state.order_intent_pg_repo = OrderIntentPostgresRepository()
    state.order_pg_repo = OrderPostgresRepository()
    state.outbox_pg_repo = OutboxPostgresRepository()
    state.strategy_risk_config_pg_repo = StrategyRiskConfigPostgresRepository()

    # 1. Redis 초기화
    state.redis = RedisStreamClient(
        host=common_settings.redis_host,
        port=common_settings.redis_port,
        db=common_settings.redis_db,
    )
    await state.redis.connect()

    state.order_repo = OrderStateRedisRepository(state.redis)
    state.signal_repo = SignalRedisRepository(state.redis)
    state.strategy_control_repo = StrategyControlRedisRepository(state.redis)

    # Position Repo
    state.position_pg_repo = PositionPostgresRepository()
    state.position_repo = PositionRedisRepository(state.redis)

    state.position_state_service = PositionStateService(
        postgres=state.postgres,
        position_repo=state.position_pg_repo,
        outbox_repo=state.outbox_pg_repo,
        redis_position_repo=state.position_repo,
    )

    # 2. OrderStateService 초기화
    state.order_state_service = OrderStateService(
        postgres=state.postgres,
        intent_repo=state.order_intent_pg_repo,
        postgres_order_repo=state.order_pg_repo,
        outbox_repo=state.outbox_pg_repo,
        redis_order_repo=state.order_repo,
    )
    # 3. Redis 주문 projection rebuild 초기화
    state.order_projection_rebuilder = OrderProjectionRebuilder(
        postgres=state.postgres,
        postgres_order_repo=state.order_pg_repo,
        redis_order_repo=state.order_repo,
    )

    rebuild_result = await state.order_projection_rebuilder.rebuild_active_projection(
        reset_existing=True,
    )

    logger.info(
        f"Startup Redis order projection rebuild 완료: "
        f"total={rebuild_result.total_rows}, "
        f"rebuilt={rebuild_result.rebuilt}, "
        f"skipped={rebuild_result.skipped}, "
        f"failed={rebuild_result.failed}"
    )

    if rebuild_result.failed > 0:
        logger.warning(
            f"Redis projection rebuild 중 실패 row 존재: "
            f"failed={rebuild_result.failed}. "
            f"서비스는 계속 시작하지만 점검 필요."
        )

    # 3-1. Redis 포지션 projection rebuild 초기화
    state.position_projection_rebuilder = PositionProjectionRebuilder(
        postgres=state.postgres,
        position_repo=state.position_pg_repo,
        redis_position_repo=state.position_repo,
    )

    position_rebuild_result = await state.position_projection_rebuilder.rebuild_active_projection(
        reset_existing=True,
    )

    logger.info(
        f"Startup Redis position projection rebuild 완료: "
        f"total={position_rebuild_result.total_rows}, "
        f"rebuilt={position_rebuild_result.rebuilt}, "
        f"failed={position_rebuild_result.failed}"
    )

    if position_rebuild_result.failed > 0:
        logger.warning(
            f"Redis position projection rebuild 중 실패 row 존재: "
            f"failed={position_rebuild_result.failed}. "
            f"서비스는 계속 시작하지만 점검 필요."
        )

    # 4. QuestDB 초기화
    state.questdb = QuestDBClient(
        host=common_settings.questdb_host,
        ilp_port=common_settings.questdb_ilp_port,
    )
    await state.questdb.connect()
    state.exec_repo = ExecutionQuestDBRepository(state.questdb)
    state.execution_log_service = ExecutionLogService(
        exec_repo=state.exec_repo,
        redis=state.redis,
    )

    # 5. Exchange Client Registry 초기화
    # state.exchange_clients = ExchangeExecutionClientRegistry()

    # 5.1 Binance REST Adapter 초기화
    # state.adapter = create_binance_adapter()

    # 5.2. Binance Execution Client 초기화
    # binance_execution_client = BinanceExecutionClient(
    #     adapter=state.adapter,
    #     order_router=BinanceOrderRouter(state.adapter),
    # )
    # state.rate_limiter = binance_execution_client.rate_limiter

    # 5-4. Registry 등록
    # state.exchange_clients.register(binance_execution_client)
    state.exchange_clients, listener_registry, state.exchange_closables = (
        await build_exchange_runtime(markets)
    )

    # 6-1. Execution Gateway 초기화
    state.gateway = ExecutionGateway(
        exchange_clients=state.exchange_clients,
        # adapter=state.adapter,
        state_repo=state.order_repo,
        state_service=state.order_state_service,
    )

    state.position_order_service = PositionOrderService(
        position_state_service=state.position_state_service,
        gateway=state.gateway,
    )

    state.order_service = OrderService(gateway=state.gateway)
    state.signal_service = SignalService(repo=state.signal_repo, gateway=state.gateway)
    state.strategy_pg_repo = StrategyPostgresRepository()
    state.strategy_control_service = StrategyControlService(
        repo=state.strategy_control_repo,
        postgres=state.postgres,
        strategy_pg_repo=state.strategy_pg_repo,
        strategy_risk_config_pg_repo=state.strategy_risk_config_pg_repo,
    )

    # PostgreSQL → Redis 전략 상태 동기화 (서버 시작 시)
    await state.strategy_control_service.sync_pg_to_redis()
    state.account_service = AccountService(gateway=state.gateway)
    state.position_service = PositionService(
        position_order_service=state.position_order_service,
        position_repo=state.position_repo,
    )

    if app_settings.order_intent_consumer_enabled:
        order_submit_handler = OrderSubmitHandler(
            gateway=state.gateway,
            risk_handler=PreTradeRiskHandler(config=None),
            dedup_handler=OrderIntentDedupHandler(RedisDedupStore(state.redis)),
            postgres=state.postgres,
            strategy_risk_config_repo=state.strategy_risk_config_pg_repo,
        )
        state.order_intent_consumer_task = asyncio.create_task(
            run_order_intent_consumer(handler=order_submit_handler),
            name="order-intent-consumer-task",
        )
        state.order_intent_consumer_task.add_done_callback(log_background_task_result)
        logger.warning("Order intent consumer enabled: strategy intents can submit live orders.")

    # 8. Outbox Publisher 초기화
    state.event_publisher = RedpandaEventPublisher(
        bootstrap_servers=common_settings.redpanda_brokers,
        client_id="takora-outbox-publisher",
    )

    OUTBOX_INTERVAL_SEC = 1.0
    OUTBOX_BATCH_SIZE = 100
    OUTBOX_LOCK_TTL_MS = 30_000
    OUTBOX_RETRY_DELAY_MS = 5_000
    OUTBOX_MAX_RETRY_COUNT = 20

    state.outbox_publisher = OutboxPublisher(
        postgres=state.postgres,
        outbox_repo=state.outbox_pg_repo,
        event_publisher=state.event_publisher,
        topic="takora.order.events",
        interval_sec=OUTBOX_INTERVAL_SEC,
        batch_size=OUTBOX_BATCH_SIZE,
        lock_ttl_ms=OUTBOX_LOCK_TTL_MS,
        retry_delay_ms=OUTBOX_RETRY_DELAY_MS,
        max_retry_count=OUTBOX_MAX_RETRY_COUNT,
    )

    await state.outbox_publisher.start()

    # 9. User Data Stream Listener 초기화
    ############################################################
    # state.listener = BinanceUserDataStreamListener(
    #     rest_adapter=state.adapter,
    #     ws_base_url=get_user_data_ws_base_url(),
    # )

    # state.listener.on_order_update(on_trade_update),
    # state.listener.on_account_update(on_account_update)
    # state.listener.on_algo_update(on_algo_update)

    # state.listener_task = asyncio.create_task(
    #     state.listener.start(),
    #     name="user-data-stream-listener",
    # )

    # state.listener_task.add_done_callback(log_background_task_result)

    ############################################################
    # listener_registry = UserDataStreamListenerRegistry()

    # listener_factory_list:list[UserDataStreamListenerFactory] = [
    #     BinanceUserDataStreamListenerFactory(
    #         rest_adapter=state.adapter,
    #         ws_base_url=get_user_data_ws_base_url(),
    #     )
    # ]
    # for listener_factory in listener_factory_list:
    #     listener_registry.register(listener_factory)

    listeners: dict[tuple[Exchange, MarketType], UserDataStreamListener] = create_user_data_stream_listeners(
        markets=markets,
        registry=listener_registry,
    )

    for key, listener in listeners.items():
        exchange, market_type = key

        listener.on_order_update(on_trade_update)
        # listener.on_account_update(on_account_update)
        listener.on_algo_update(on_algo_update)
        listener.on_position_update(on_position_update)

        task = asyncio.create_task(
            listener.start(),
            name=f"user-data-stream-{exchange.value}-{market_type.value}",
        )
        task.add_done_callback(log_background_task_result)

        state.listeners[key] = listener
        state.listener_tasks[key] = task

    ############################################################

    # 10. Recovery Worker 초기화
    state.recovery_worker = RecoveryWorker(
        exchange_clients=state.exchange_clients,
        gateway=state.gateway,
        repo=state.order_repo,
        markets=markets,
        interval_sec=app_settings.recovery_worker_interval_sec,
        older_than_ms=app_settings.recovery_worker_older_than_ms,
        batch_size=app_settings.recovery_worker_batch_size,
        failure_backoff_ms=app_settings.recovery_worker_failure_backoff_ms,
    )
    await state.recovery_worker.start()

    # 11. Reconciliation Worker 초기화

    state.reconciliation_worker = ReconciliationWorker(
        exchange_clients=state.exchange_clients,
        gateway=state.gateway,
        order_state_service=state.order_state_service,
        redis_order_repo=state.order_repo,
        markets=markets,
        interval_sec=app_settings.reconciliation_worker_interval_sec,
        active_symbols=None,
        recent_grace_ms=app_settings.reconciliation_worker_recent_grace_ms,
        external_orphan_policy=app_settings.reconciliation_worker_orphan_policy,
        all_orders_threshold=app_settings.reconciliation_worker_all_orders_threshold,
        all_orders_lookback_ms=app_settings.reconciliation_worker_all_orders_lookback_ms,
        all_orders_limit=app_settings.reconciliation_worker_all_orders_limit,
    )
    await state.reconciliation_worker.start()


    state.position_reconciliation_worker = PositionReconciliationWorker(
        exchange_clients=state.exchange_clients,
        position_state_service=state.position_state_service,
        markets=markets,
        interval_sec=app_settings.position_reconciliation_worker_interval_sec,
        active_symbols=None,
    )
    await state.position_reconciliation_worker.start()

    # 13. WebSocket 및 CEX Market Data Collector 초기화 및 시작
    ensure_exchange_package_paths()
    state.ws_manager = WebSocketManager()

    state.signal_ws_task = asyncio.create_task(
        run_signal_websocket_broadcaster(
            manager=state.ws_manager,
            bootstrap_servers=common_settings.redpanda_brokers,
        ),
        name="signal-websocket-broadcaster-task",
    )
    state.signal_ws_task.add_done_callback(log_background_task_result)

    exchanges_str = common_settings.market_pipeline_exchanges
    exchanges = tuple(ex.strip() for ex in exchanges_str.split(",") if ex.strip())
    
    oi_interval = app_settings.oi_collect_interval_s
    logger.info(f"CEX Market Data Collector 시작 중 (Exchanges: {exchanges}, OI Interval: {oi_interval}s)...")
    specs = build_operational_specs(
        exchanges,
        oi_interval_s=oi_interval,
        rest_oi_fallback=app_settings.oi_rest_fallback,
    )
    sink = WebSocketManagerSink(state.ws_manager)
    
    state.collector_task = asyncio.create_task(
        run_operational_specs(specs, sink=sink),
        name="operational-collector-task"
    )
    state.collector_task.add_done_callback(log_background_task_result)


async def _shutdown_app_state() -> None:
    if state.order_intent_consumer_task is not None:
        try:
            logger.info("Order intent consumer 종료 중...")
            state.order_intent_consumer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await state.order_intent_consumer_task
            logger.info("Order intent consumer 종료 완료")
        except Exception as e:
            logger.warning(f"Order intent consumer 종료 실패: {e}", exc_info=True)
        finally:
            state.order_intent_consumer_task = None

    if state.signal_ws_task is not None:
        try:
            logger.info("Signal websocket broadcaster 종료 중...")
            state.signal_ws_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await state.signal_ws_task
            logger.info("Signal websocket broadcaster 종료 완료")
        except Exception as e:
            logger.warning(f"Signal websocket broadcaster 종료 실패: {e}", exc_info=True)
        finally:
            state.signal_ws_task = None

    # 0. CEX Market Data Collector 종료
    if state.collector_task is not None:
        try:
            logger.info("CEX Market Data Collector 종료 중...")
            state.collector_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await state.collector_task
            logger.info("CEX Market Data Collector 종료 완료")
        except Exception as e:
            logger.warning(f"CEX Market Data Collector 종료 실패: {e}", exc_info=True)

    # 1. User Data Stream 종료
    # listener = state.listener
    # listener_task = state.listener_task

    # if listener is not None:
    #     try:
    #         await listener.stop()
    #     except Exception as e:
    #         logger.warning(f"BinanceUserDataStreamListener stop 실패: {e}", exc_info=True)

    # if listener_task is not None:
    #     try:
    #         await asyncio.wait_for(listener_task, timeout=5)
    #     except asyncio.TimeoutError:
    #         logger.warning("BinanceUserDataStreamListener 종료 timeout. task cancel 수행.")
    #         listener_task.cancel()
    #         with contextlib.suppress(asyncio.CancelledError):
    #             await listener_task
    #     except Exception as e:
    #         logger.warning(f"BinanceUserDataStreamListener task 종료 실패: {e}", exc_info=True)
    for key, listener in list(state.listeners.items()):
        exchange, market_type = key

        try:
            await listener.stop()
        except Exception as e:
            logger.warning(
                f"UserDataStreamListener stop 실패: "
                f"exchange={exchange.value}, "
                f"market_type={market_type.value}, "
                f"err={e}",
                exc_info=True,
            )

    for key, task in list(state.listener_tasks.items()):
        exchange, market_type = key

        try:
            await asyncio.wait_for(task, timeout=5)
        except asyncio.TimeoutError:
            logger.warning(
                f"UserDataStreamListener 종료 timeout. task cancel 수행: "
                f"exchange={exchange.value}, market_type={market_type.value}"
            )
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        except Exception as e:
            logger.warning(
                f"UserDataStreamListener task 종료 실패: "
                f"exchange={exchange.value}, "
                f"market_type={market_type.value}, "
                f"err={e}",
                exc_info=True,
            )

    state.listeners.clear()
    state.listener_tasks.clear()

    # Position Reconciliation Worker 종료
    if state.position_reconciliation_worker is not None:
        try:
            await state.position_reconciliation_worker.stop()
        except Exception as e:
            logger.warning(
                f"PositionReconciliationWorker 종료 실패: {e}",
                exc_info=True,
            )

    # 2. RecoveryWorker 종료
    if state.recovery_worker is not None:
        try:
            await state.recovery_worker.stop()
        except Exception as e:
            logger.warning(f"RecoveryWorker 종료 실패: {e}", exc_info=True)

    # 3. ReconciliationWorker 종료
    if state.reconciliation_worker is not None:
        try:
            await state.reconciliation_worker.stop()
        except Exception as e:
            logger.warning(f"ReconciliationWorker 종료 실패: {e}", exc_info=True)

    # 4. OutboxPublisher 종료
    if state.outbox_publisher is not None:
        try:
            await state.outbox_publisher.stop()
        except Exception as e:
            logger.warning(f"OutboxPublisher 종료 실패: {e}", exc_info=True)

    # 5. Binance Adapter 종료
    # if state.adapter is not None:
    #     try:
    #         await state.adapter.close()
    #     except Exception as e:
    #         logger.warning(f"BinanceRestAdapter 종료 실패: {e}", exc_info=True)

    # 5. Exchange Resource 종료
    for resource in state.exchange_closables:
        try:
            await resource.close()
        except Exception as e:
            logger.warning(f"exchange resource 종료 실패: {e}", exc_info=True)

    state.exchange_closables.clear()

    # 6. QuestDB 종료
    if state.questdb is not None:
        try:
            await state.questdb.close()
        except AttributeError:
            logger.debug("QuestDBClient.close() 없음. skip.")
        except Exception as e:
            logger.warning(f"QuestDB 종료 실패: {e}", exc_info=True)

    # 7. Redis 종료
    if state.redis is not None:
        try:
            await state.redis.close()
        except Exception as e:
            logger.warning(f"Redis 종료 실패: {e}", exc_info=True)

    # 8. PostgreSQL 종료
    if state.postgres is not None:
        try:
            await state.postgres.close()
        except Exception as e:
            logger.warning(f"PostgreSQL 종료 실패: {e}", exc_info=True)
