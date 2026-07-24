from __future__ import annotations

import asyncio
import os
from pathlib import Path
from unittest.mock import MagicMock

import time

import pytest

from execution_gateway.adapters.binance.binance_order_router import BinanceOrderRouter
from execution_gateway.adapters.binance.binance_rest_adapter import (
    BinanceKeyType,
    BinanceRestAdapter,
)
from execution_gateway.adapters.binance.binance_execution_client import BinanceExecutionClient
from execution_gateway.config import settings as gw_settings
from execution_gateway.exchange.registry import ExchangeExecutionClientRegistry
from execution_gateway.gateway import ExecutionGateway
from schemas.market import Exchange, MarketType
from storage.repositories.redis.order_state_repo import OrderStateRedisRepository


pytestmark = pytest.mark.integration


def _require_real_binance_tests_enabled() -> None:
    if os.getenv("RUN_BINANCE_REAL_TESTS") != "1":
        pytest.skip(
            "Real Binance testnet tests are disabled. "
            "Set RUN_BINANCE_REAL_TESTS=1 to run."
        )


def _assert_testnet() -> None:
    base = gw_settings.binance_testnet_rest_url.rstrip("/")
    allowed = {
        "https://demo-fapi.binance.com",
        "https://testnet.binancefuture.com",
    }
    if base not in allowed:
        pytest.skip(f"Testnet endpoint가 아닙니다: {base}")


def _load_pem() -> str:
    pem_path = gw_settings.active_ed25519_key_pem
    if not pem_path:
        pytest.skip("gw_settings.active_ed25519_key_pem is not configured")

    path = Path(pem_path)
    if not path.exists():
        pytest.skip(f"ED25519 private key file does not exist: {path}")

    return path.read_text()


async def _make_real_adapter() -> BinanceRestAdapter:
    _require_real_binance_tests_enabled()
    _assert_testnet()

    return BinanceRestAdapter(
        base_url=gw_settings.binance_testnet_rest_url,
        api_key=gw_settings.active_api_key,
        key_type=BinanceKeyType.ED25519,
        private_key_pem=_load_pem(),
    )


def _make_gateway(
    *,
    adapter: BinanceRestAdapter,
) -> ExecutionGateway:
    client = BinanceExecutionClient(
        adapter=adapter,
        order_router=BinanceOrderRouter(adapter),
    )

    registry = ExchangeExecutionClientRegistry()
    registry.register(client)

    return ExecutionGateway(
        state_repo=MagicMock(spec=OrderStateRedisRepository),
        state_service=MagicMock(),
        exchange_clients=registry,
    )


async def _current_leverage(
    *,
    adapter: BinanceRestAdapter,
    symbol: str,
) -> int:
    rows = await adapter.get_symbol_config(symbol=symbol)
    if not rows:
        raise AssertionError(f"symbolConfig returned no rows for symbol={symbol}")

    for row in rows:
        row.leverage
        if row.symbol and row.symbol.upper() == symbol.upper():
            return row.leverage

    return rows[0].leverage


def _pick_target_leverage(current: int) -> int:
    max_allowed = int(getattr(gw_settings, "binance_max_leverage", 20))
    candidates = [3, 5, 2, 1]

    for candidate in candidates:
        if candidate <= max_allowed and candidate != current:
            return candidate

    pytest.skip(
        f"No alternate leverage candidate available: "
        f"current={current}, max_allowed={max_allowed}"
    )


async def _wait_until_leverage(
    *,
    adapter: BinanceRestAdapter,
    symbol: str,
    expected: int,
    timeout_sec: float = 10.0,
    interval_sec: float = 0.5,
) -> int:
    deadline = asyncio.get_running_loop().time() + timeout_sec
    last_seen: int | None = None

    while asyncio.get_running_loop().time() < deadline:
        last_seen = await _current_leverage(adapter=adapter, symbol=symbol)
        if last_seen == expected:
            return last_seen
        await asyncio.sleep(interval_sec)

    raise AssertionError(
        f"leverage did not become {expected} for {symbol}; last_seen={last_seen}"
    )

@pytest.mark.stable
@pytest.mark.asyncio
async def test_real_testnet_gateway_change_leverage_updates_position_risk() -> None:
    """
    Binance Futures Testnet에서 Gateway.change_leverage()가 레버리지를 바꾸는지 검증
    """
    symbol = os.getenv("BINANCE_TESTNET_LEVERAGE_SYMBOL", "BTCUSDT").upper()

    adapter = await _make_real_adapter()
    gateway = _make_gateway(adapter=adapter)

    original: int | None = None
    try:
        original = await _current_leverage(adapter=adapter, symbol=symbol)
        print("현재 레버리지: ",original)
        target = _pick_target_leverage(original)
        print("바꿀 레버리지: ",target)

        result = await gateway.change_leverage(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol=symbol,
            leverage=target,
        )

        assert result.exchange is Exchange.BINANCE
        assert result.market_type is MarketType.PERP
        assert result.symbol == symbol
        assert result.leverage == target
        assert int(result.raw["leverage"]) == target

        observed = await _wait_until_leverage(
            adapter=adapter,
            symbol=symbol,
            expected=target,
        )
        assert observed == target

        time.sleep(2)

    finally:
        if original is not None:
            await gateway.change_leverage(
                exchange=Exchange.BINANCE,
                market_type=MarketType.PERP,
                symbol=symbol,
                leverage=original,
            )
            await _wait_until_leverage(
                adapter=adapter,
                symbol=symbol,
                expected=original,
            )
        await adapter.close()
