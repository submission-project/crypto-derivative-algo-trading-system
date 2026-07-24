from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
import os
import socket

class Settings(BaseSettings):
    environment: str
    app_node_id: str = Field(default_factory=lambda: socket.gethostname())

    binance_testnet_ed25519_key_pem: str
    binance_testnet_api_key: str

    model_config = SettingsConfigDict(
        env_file=os.environ.get("ENV_FILE", ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False
    )

settings = Settings()