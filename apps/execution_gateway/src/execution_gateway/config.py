import os
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "execution_gateway"
    environment: str = "dev"

    # ── Binance API 인증 (Mainnet) ──
    binance_api_key: str = Field(description="Binance API Key (HMAC/Ed25519)")
    binance_api_secret: str = Field(description="Binance API Secret (HMAC-SHA256)")
    # REST 서명 방식: HMAC | ED25519 (환경변수 BINANCE_KEY_TYPE)
    binance_key_type: str = Field(description="HMAC or ED25519")

    # ── Binance API 인증 (Testnet) ──
    binance_testnet_api_key: Optional[str] = Field(
        default=None, description="Testnet API Key"
    )
    binance_testnet_api_secret: Optional[str] = Field(
        default=None, description="Testnet API Secret"
    )

    # ── Binance Endpoints ──
    # Testnet toggle: True이면 testnet URL 사용
    use_testnet: bool = Field(
        default=True, description="True면 testnet, False면 mainnet"
    )

    # Mainnet
    binance_rest_base_url: str = "https://fapi.binance.com"
    binance_ws_base_url: str = "wss://fstream.binance.com"
    binance_ws_trade_url: str = "wss://ws-fapi.binance.com/ws-fapi/v1"

    # Testnet
    # binance_testnet_rest_url: str = "https://testnet.binancefuture.com"
    binance_testnet_rest_url: str = "https://demo-fapi.binance.com"

    binance_testnet_ws_url: str = "wss://fstream.binancefuture.com/private"
    binance_testnet_ws_trade_url: str = "wss://testnet.binancefuture.com/ws-fapi/v1"

    # ── Infra ──
    redis_host: str = "localhost"
    redis_port: int = 6379
    redpanda_brokers: str = "localhost:9092"

    # ── Ed25519 ──
    binance_ed25519_key_pem: Optional[str] = Field(
        default=None, description="Ed25519 private key PEM 경로 (Mainnet)"
    )
    binance_testnet_ed25519_key_pem: Optional[str] = Field(
        default=None, description="Ed25519 private key PEM 경로 (Testnet)"
    )

    model_config = SettingsConfigDict(
        env_file=os.environ.get("ENV_FILE", ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    @property
    def rest_base_url(self) -> str:
        if self.use_testnet:
            return self.binance_testnet_rest_url
        return self.binance_rest_base_url

    @property
    def ws_base_url(self) -> str:
        if self.use_testnet:
            return self.binance_testnet_ws_url
        return self.binance_ws_base_url

    @property
    def active_api_key(self) -> str:
        if self.use_testnet and self.binance_testnet_api_key:
            return self.binance_testnet_api_key
        return self.binance_api_key

    @property
    def active_api_secret(self) -> Optional[str]:
        if self.use_testnet and self.binance_testnet_api_secret:
            return self.binance_testnet_api_secret
        return self.binance_api_secret

    @property
    def active_ed25519_key_pem(self) -> Optional[str]:
        if self.use_testnet and self.binance_testnet_ed25519_key_pem:
            return self.binance_testnet_ed25519_key_pem
        return self.binance_ed25519_key_pem


settings = Settings()
