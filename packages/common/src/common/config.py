from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
import os
import socket


class Settings(BaseSettings):
    # App Settings
    app_name: str = "common"
    environment: str
    # 실행되는 서버의 호스트네임을 기본값으로 사용
    app_node_id: str = Field(default_factory=lambda: socket.gethostname())

    discord_webhook_url: Optional[str] = None
    slack_webhook_url: Optional[str] = None

    pushover_user_key: Optional[str] = None
    pushover_api_token: Optional[str] = None

    # Redpanda / Kafka
    redpanda_brokers: str
    binance_perp_topic_trades: Optional[str] = None  # Legacy: 필수가 아닐 경우
    binance_spot_topic_trades: str
    binance_perp_topic_raw_trades: str
    binance_perp_topic_agg_trades: str
    binance_perp_topic_canonical: str
    binance_perp_topic_repair_jobs: str

    # Redis
    redis_host: str
    redis_port: int
    redis_db: int

    # PostgreSQL (asyncpg DSN). 미설정이면 테스트·일부 패키지만 import 하는 경우를 허용하고,
    # API 서버 기동 시 main 에서 명시적으로 검증한다.
    postgres_dsn: Optional[str] = None
    postgres_min_size: int = 1
    postgres_max_size: int = 10

    # QuestDB
    questdb_host: str
    questdb_port: int
    questdb_ilp_port: int
    questdb_strict_ilp_errors: bool = False

    # API
    api_host: str
    api_port: int

    # Binance
    binance_spot_sbe_ws: str
    binance_perp_ws: str
    # MARKET_DATA 등급 엔드포인트(/fapi/v1/historicalTrades 등)에 필요한 X-MBX-APIKEY.
    # 없으면 인증 필요 호출 시 RestAuthError 발생.
    binance_api_key: Optional[str] = None
    binance_max_leverage: int

    binance_key_type: str

    model_config = SettingsConfigDict(
        env_file=os.environ.get("ENV_FILE", ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    enabled_execution_markets: str | None = None
    
    market_pipeline_exchanges: str


settings = Settings()
