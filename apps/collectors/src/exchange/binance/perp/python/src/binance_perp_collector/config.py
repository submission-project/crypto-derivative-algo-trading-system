import os
from typing import List
from pydantic_settings import SettingsConfigDict
from common.config import Settings as CommonSettings

class Settings(CommonSettings):
    app_name: str = "binance_perp_collector"

    # 이 컴포넌트 전용 설정 추가 (필요시)
    binance_perp_symbols: List[str]
    
    
    model_config = SettingsConfigDict(
        env_file=os.environ.get("ENV_FILE", ".env.dev"),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )

settings = Settings()
