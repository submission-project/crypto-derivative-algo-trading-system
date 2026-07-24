from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    app_name: str = "stream_processor"
    environment: str = "dev"
    flush_interval_sec: float = 0.01
    batch_size: int = 100
    trade_maxlen: int = 2000
    market_event_maxlen: int = 2000
    market_topics: str | None = None
    binance_perp_topic_canonical: str

    # # V2 방식: SettingsConfigDict 사용
    # model_config = SettingsConfigDict(
    #     env_file=os.environ.get("ENV_FILE", ".env.dev"),
    #     env_file_encoding="utf-8",
    #     case_sensitive=False # 대소문자 구분 없이 환경변수 매핑 (기본값)
    # )

settings = Settings()
