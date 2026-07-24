from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    app_name: str = "api_server"
    environment: str = "dev"

    # ORDER_INTENT_CONSUMER_ENABLED
    order_intent_consumer_enabled: bool = False

    # Recovery Worker Settings
    recovery_worker_interval_sec: float = 3.0
    recovery_worker_older_than_ms: int = 2000
    recovery_worker_batch_size: int = 100
    recovery_worker_failure_backoff_ms: int = 10000

    # Reconciliation Worker Settings
    reconciliation_worker_interval_sec: int = 60
    reconciliation_worker_recent_grace_ms: int = 3000
    reconciliation_worker_orphan_policy: str = "cancel"
    reconciliation_worker_all_orders_threshold: int = 6
    reconciliation_worker_all_orders_lookback_ms: int = 60000
    reconciliation_worker_all_orders_limit: int = 1000

    # Position Reconciliation Worker Settings
    position_reconciliation_worker_interval_sec: int = 30

    # CEX Market Data Collector Settings
    oi_collect_interval_s: float = 10.0
    oi_rest_fallback: bool = False

    # Shutdown Settings
    shutdown_drain_sec: float = 3.0

    class Config:
        env_file = ".env"


settings = Settings()
