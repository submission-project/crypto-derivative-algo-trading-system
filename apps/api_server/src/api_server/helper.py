import asyncio
from typing import Any
from execution_gateway.adapters.binance.binance_rest_adapter import (
    BinanceRestAdapter,
    BinanceKeyType,
)

from common.config import settings as common_settings
from execution_gateway.config import settings as gw_settings
from common.file import read_file_if_exists
from common.logging import setup_logger

from execution_gateway.exchange import ExchangeApiError

from fastapi import HTTPException


logger = setup_logger(__name__)


# ───────────────────────────── Helpers ─────────────────────────────


def create_binance_adapter() -> BinanceRestAdapter:
    """
    HMAC / ED25519 설정에 따라 BinanceRestAdapter 생성.
    """
    key_type = BinanceKeyType(str(common_settings.binance_key_type).upper())

    adapter_kwargs: dict[str, Any] = {
        "base_url": gw_settings.rest_base_url,
        "api_key": gw_settings.active_api_key,
        "key_type": key_type,
    }

    if key_type == BinanceKeyType.HMAC:
        api_secret = getattr(gw_settings, "active_api_secret", None)

        if not api_secret:
            api_secret = getattr(common_settings, "binance_api_secret", None)

        if not api_secret:
            raise ValueError(
                "HMAC key_type인데 api_secret이 없습니다. "
                "gw_settings.active_api_secret 또는 common_settings.binance_api_secret을 설정하세요."
            )

        adapter_kwargs["api_secret"] = api_secret

    elif key_type == BinanceKeyType.ED25519:
        pem_data = read_file_if_exists(gw_settings.active_ed25519_key_pem)

        if not pem_data:
            raise ValueError(
                "ED25519 key_type인데 private_key_pem이 없습니다. "
                "gw_settings.active_ed25519_key_pem 경로를 확인하세요."
            )

        adapter_kwargs["private_key_pem"] = pem_data

    return BinanceRestAdapter(**adapter_kwargs)


def log_background_task_result(task: asyncio.Task) -> None:
    """
    백그라운드 task가 예외로 죽었는지 로깅.
    """
    try:
        task.result()

    except asyncio.CancelledError:
        pass

    except Exception as e:
        logger.error(f"Background task crashed: {e}", exc_info=True)


def get_user_data_ws_base_url() -> str:
    base = gw_settings.ws_base_url.rstrip("/")
    return base if base.endswith("/private") else f"{base}/private"


def exchange_error_to_http(e: ExchangeApiError) -> HTTPException:
    status_code = e.status_code or 400

    return HTTPException(
        status_code=status_code,
        detail={
            "error": "exchange_api",
            "exchange": e.exchange.value,
            "category": e.category.value,
            "code": e.code,
            "message": e.message,
        },
    )