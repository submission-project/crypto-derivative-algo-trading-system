import os
from common.config import settings

# ── Environment Variable Keys ──
# Maintained for backwards compatibility if any module imports these constants directly
ENV_KEY_ENVIRONMENT = "ENVIRONMENT"
ENV_KEY_REDPANDA_BROKERS = "REDPANDA_BROKERS"
ENV_KEY_BINANCE_API_KEY = "BINANCE_API_KEY"
ENV_KEY_REDIS_HOST = "REDIS_HOST"
ENV_KEY_REDIS_PORT = "REDIS_PORT"
ENV_KEY_QUESTDB_HOST = "QUESTDB_HOST"
ENV_KEY_QUESTDB_PORT = "QUESTDB_PORT"
ENV_KEY_QUESTDB_ILP_PORT = "QUESTDB_ILP_PORT"
ENV_KEY_API_HOST = "API_HOST"
ENV_KEY_API_PORT = "API_PORT"
ENV_KEY_BINANCE_PERP_TOPIC_TRADES = "BINANCE_PERP_TOPIC_TRADES"
ENV_KEY_BINANCE_SPOT_TOPIC_TRADES = "BINANCE_SPOT_TOPIC_TRADES"
ENV_KEY_APP_NODE_ID = "APP_NODE_ID"
ENV_KEY_BINANCE_SPOT_SBE_WS = "BINANCE_SPOT_SBE_WS"

# ── Environments ──
ENV_PROD = "prod"
ENV_DEV = "dev"

def get_env() -> str:
    """Get the current environment"""
    return os.getenv(ENV_KEY_ENVIRONMENT, settings.environment).lower()

def is_dev() -> bool:
    return get_env() == ENV_DEV

def is_prod() -> bool:
    return get_env() == ENV_PROD

def get_redpanda_brokers() -> str:
    return settings.redpanda_brokers

def get_redis_host() -> str:
    return settings.redis_host

def get_redis_port() -> int:
    return settings.redis_port
