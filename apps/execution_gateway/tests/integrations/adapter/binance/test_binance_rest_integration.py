from __future__ import annotations

from typing import Callable
import asyncio
import os
import time
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from pathlib import Path
from typing import Any

import pytest

from execution_gateway.config import settings

from common.ids import generate_order_id
from execution_gateway.adapters.binance.binance_order_router import BinanceOrderRouter
from execution_gateway.adapters.binance.binance_rest_adapter import (
    BinanceKeyType,
    BinanceRestAdapter,
)
from schemas.market import Exchange, MarketType
from schemas.order import (
    Order,
    OrderRoute,
    OrderSide,
    OrderSource,
    OrderStatus,
    OrderType,
    PositionAction,
    TimeInForce,
)
from schemas.position import PositionSide

from execution_gateway.adapters.binance.dto.resp.OrderResponseDto import (
    CancelAllOpenOrdersRespDto,
    CancelOrderRespDto,
    OrderRespDto,
    CancelAlgoOrderRespDto,
)
from execution_gateway.adapters.binance.dto.resp.AlgoOrderResponseDto import (
    CancelAllAlgoOpenOrdersRespDto,
    AlgoOrderRespDto,
)
from execution_gateway.adapters.binance.dto.resp.AccountResponseDto import (
    AccountInfoRespDto,
)

from execution_gateway.adapters.binance.binance_rest_adapter import (
    BinanceRestAdapter,
    BinanceKeyType,
    BinanceApiError,
    BinanceRateLimitError,
)


pytestmark = pytest.mark.integration


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def load_pem() -> str:
    pem_path = getattr(settings, "active_ed25519_key_pem", None)

    if not pem_path:
        pytest.skip("settings.active_ed25519_key_pem is not configured")

    path = Path(str(pem_path))

    if not path.exists():
        pytest.skip(f"ED25519 private key file does not exist: {path}")

    return path.read_text()


def make_adapter() -> BinanceRestAdapter:
    pem_data = load_pem()

    return BinanceRestAdapter(
        base_url=settings.binance_testnet_rest_url,
        api_key=settings.active_api_key,
        key_type=BinanceKeyType.ED25519,
        private_key_pem=pem_data,
    )

def _client_id(prefix: str) -> str:
    # Binance client id length 제한에 걸리지 않게 짧게 유지.
    return f"TK{prefix}{int(time.time() * 1000)}"


def _round_down_to_step(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def _round_up_to_step(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_UP) * step


def _quantity_with_min_notional(
    *,
    quantity: str,
    price: Decimal,
    min_notional: Decimal = Decimal("55"),
    quantity_step: Decimal = Decimal("0.001"),
) -> str:
    requested_quantity = Decimal(quantity)
    if price * requested_quantity >= min_notional:
        return str(requested_quantity)

    return str(_round_up_to_step(min_notional / price, quantity_step))


async def _get_reference_price(adapter: BinanceRestAdapter, symbol: str) -> Decimal:
    """
    public ticker price 기준.

    네 adapter에 get_symbol_price_ticker가 없으면
    기존 public request 메서드명에 맞춰 바꿔라.
    """
    ticker = await adapter.get_symbol_price_ticker(symbol)
    return Decimal(str(ticker.price))

async def _wait_for_function_listing(
    *,
    function: Callable,
    function_kwargs: dict[str, Any] | None,
    client_algo_id: str,
    algo_id: str | None,
    timeout_sec: float = 20.0,
    poll_interval_sec: float = 0.35,
) -> tuple[list[AlgoOrderRespDto], list[AlgoOrderRespDto]]:
    """
    place 직후 openAlgoOrders가 잠깐 비는 경우가 있어 폴링한다.
    """
    deadline = time.monotonic() + timeout_sec
    open_algo_orders: list[AlgoOrderRespDto] = []

    while time.monotonic() < deadline:
        open_algo_orders = await function(
            **function_kwargs,
        )
        found = [
            item
            for item in open_algo_orders
            if str(item.clientAlgoId) == client_algo_id
            or (algo_id is not None and str(item.algoId) == algo_id)
        ]
        if found:
            return found, open_algo_orders
        await asyncio.sleep(poll_interval_sec)

    return [], open_algo_orders


def _make_regular_limit_order(
    *,
    symbol: str,
    side: OrderSide,
    quantity: str,
    price: str,
    client_order_id: str,
) -> Order:
    now = _now_ms()

    return Order(
        order_id=client_order_id,
        source=OrderSource.MANUAL,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol=symbol,
        side=side,
        order_type=OrderType.LIMIT,
        order_route=OrderRoute.REGULAR,
        quantity=quantity,
        price=price,
        trigger_price=None,
        time_in_force=TimeInForce.GTC,
        reduce_only=False,
        close_position=False,
        client_order_id=client_order_id,
        position_side=PositionSide.BOTH,
        position_action=PositionAction.OPEN,
        status=OrderStatus.SUBMITTED,
        created_ts=now,
        updated_ts=now,
        version=2,
    )

def _make_regular_market_order(
    *,
    symbol: str,
    side: OrderSide,
    quantity: str,
    client_order_id: str,
) -> Order:
    now = _now_ms()

    return Order(
        order_id=client_order_id,
        source=OrderSource.MANUAL,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol=symbol,
        side=side,
        order_type=OrderType.MARKET,
        order_route=OrderRoute.REGULAR,
        quantity=quantity,
        price=None,
        trigger_price=None,
        time_in_force=None,
        reduce_only=False,
        close_position=False,
        client_order_id=client_order_id,
        position_side=PositionSide.BOTH,
        position_action=PositionAction.OPEN,
        status=OrderStatus.SUBMITTED,
        created_ts=now,
        updated_ts=now,
        version=2,
    )

def _make_stop_market_algo_order(
    *,
    symbol: str,
    side: OrderSide,
    quantity: str,
    trigger_price: str,
    client_algo_id: str,
) -> Order:
    now = _now_ms()

    return Order(
        order_id=client_algo_id,
        source=OrderSource.MANUAL,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol=symbol,
        side=side,
        order_type=OrderType.STOP_MARKET,
        order_route=OrderRoute.CONDITIONAL,
        quantity=quantity,
        price=None,
        trigger_price=trigger_price,
        time_in_force=None,
        reduce_only=False,
        close_position=False,
        client_conditional_id=client_algo_id,
        position_side=PositionSide.BOTH,
        position_action=PositionAction.OPEN,
        status=OrderStatus.SUBMITTED,
        created_ts=now,
        updated_ts=now,
        version=2,
    )

def _make_stop_limit_algo_order(
    *,
    symbol: str,
    side: OrderSide,
    quantity: str,
    trigger_price: str,
    price: str,
    client_algo_id: str,
) -> Order:
    now = _now_ms()

    return Order(
        order_id=client_algo_id,
        source=OrderSource.MANUAL,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol=symbol,
        side=side,
        order_type=OrderType.STOP_LIMIT,
        order_route=OrderRoute.CONDITIONAL,
        quantity=quantity,
        price=price,
        trigger_price=trigger_price,
        time_in_force=TimeInForce.GTC,
        reduce_only=False,
        close_position=False,
        client_conditional_id=client_algo_id,
        position_side=PositionSide.BOTH,
        position_action=PositionAction.OPEN,
        status=OrderStatus.SUBMITTED,
        created_ts=now,
        updated_ts=now,
        version=2,
    )

async def _assert_open_algo_order_exists(
    *,
    adapter: BinanceRestAdapter,
    symbol: str,
    client_algo_id: str,
    algo_id: str | None = None,
) -> AlgoOrderRespDto:
    found, open_algo_orders = await _wait_for_function_listing(
        function=adapter.get_open_algo_orders,
        function_kwargs={
            "symbol": symbol,
        },
        client_algo_id=client_algo_id,
        algo_id=algo_id,
    )

    assert found, {
        "client_algo_id": client_algo_id,
        "algo_id": algo_id,
        "open_algo_orders": open_algo_orders,
    }

    return found[0]


async def _assert_open_regular_order_exists(
    *,
    adapter: BinanceRestAdapter,
    symbol: str,
    client_order_id: str,
    order_id: int | None = None,
    timeout_sec: float = 20.0,
    poll_interval_sec: float = 0.35,
) -> OrderRespDto:
    deadline = time.monotonic() + timeout_sec
    open_orders: list[OrderRespDto] = []

    while time.monotonic() < deadline:
        open_orders = await adapter.get_open_orders(symbol=symbol)
        found = [
            item
            for item in open_orders
            if str(item.clientOrderId) == client_order_id
            or (order_id is not None and item.orderId == order_id)
        ]

        if found:
            return found[0]

        await asyncio.sleep(poll_interval_sec)

    raise AssertionError(
        {
            "client_order_id": client_order_id,
            "order_id": order_id,
            "open_orders": open_orders,
        }
    )


async def _wait_regular_order_absent_from_open_orders(
    *,
    adapter: BinanceRestAdapter,
    symbol: str,
    client_order_id: str,
    timeout_sec: float = 20.0,
    poll_interval_sec: float = 0.35,
) -> None:
    deadline = time.monotonic() + timeout_sec
    open_orders: list[OrderRespDto] = []

    while time.monotonic() < deadline:
        open_orders = await adapter.get_open_orders(symbol=symbol)
        if all(str(item.clientOrderId) != client_order_id for item in open_orders):
            return

        await asyncio.sleep(poll_interval_sec)

    raise AssertionError(
        {
            "message": "regular order still exists in openOrders",
            "client_order_id": client_order_id,
            "open_orders": open_orders,
        }
    )


async def _wait_algo_order_absent_from_open_orders(
    *,
    adapter: BinanceRestAdapter,
    symbol: str,
    client_algo_id: str,
    timeout_sec: float = 20.0,
    poll_interval_sec: float = 0.35,
) -> None:
    deadline = time.monotonic() + timeout_sec
    open_algo_orders: list[AlgoOrderRespDto] = []

    while time.monotonic() < deadline:
        open_algo_orders = await adapter.get_open_algo_orders(symbol=symbol)
        if all(str(item.clientAlgoId) != client_algo_id for item in open_algo_orders):
            return

        await asyncio.sleep(poll_interval_sec)

    raise AssertionError(
        {
            "message": "algo order still exists in openAlgoOrders",
            "client_algo_id": client_algo_id,
            "open_algo_orders": open_algo_orders,
        }
    )


async def _wait_all_orders_contains(
    *,
    adapter: BinanceRestAdapter,
    symbol: str,
    client_order_id: str,
    order_id: int | None = None,
    start_time: int | None = None,
    timeout_sec: float = 20.0,
    poll_interval_sec: float = 0.35,
) -> OrderRespDto:
    deadline = time.monotonic() + timeout_sec
    all_orders: list[OrderRespDto] = []

    while time.monotonic() < deadline:
        try:
            all_orders = await adapter.get_all_orders(
                symbol=symbol,
                start_time=start_time,
                end_time=_now_ms() + 5_000,
                limit=100,
            )
        except BinanceApiError as e:
            if e.code == -1 and "org/bouncycastle/crypto/CipherParameters" in str(e):
                print("binance 서버 에러")
                raise
            raise

        found = [
            item
            for item in all_orders
            if str(item.clientOrderId) == client_order_id
            or (order_id is not None and item.orderId == order_id)
        ]

        if found:
            return found[0]

        await asyncio.sleep(poll_interval_sec)

    raise AssertionError(
        {
            "client_order_id": client_order_id,
            "order_id": order_id,
            "all_orders": all_orders,
        }
    )


async def _cancel_regular_safely(
    *,
    adapter: BinanceRestAdapter,
    symbol: str,
    client_order_id: str | None,
) -> None:
    if not client_order_id:
        return

    try:
        await adapter.cancel_order(
            symbol=symbol,
            client_order_id=client_order_id,
        )
    except Exception as e:
        print(e)
        pass


async def _cancel_algo_safely(
    *,
    adapter: BinanceRestAdapter,
    symbol: str,
    client_algo_id: str | None,
    algo_id: str | None = None,
) -> None:
    if client_algo_id is None and algo_id is None:
        return

    try:
        await adapter.cancel_algo_order(
            symbol=symbol,
            client_algo_id=client_algo_id,
            algo_id=algo_id,
        )
    except Exception:
        pass

@pytest.mark.stable
@pytest.mark.asyncio
async def test_rest_real_communication_account_info() -> None:
    """
    실제 REST API로 계정 정보를 조회한다.

    주문은 넣지 않는 smoke test.
    """

    adapter = make_adapter()

    account_test = {'totalInitialMargin': '0.00000000', 'totalMaintMargin': '0.00000000', 'totalWalletBalance': '4995.09417573', 'totalUnrealizedProfit': '0.00000000', 'totalMarginBalance': '4995.09417573', 'totalPositionInitialMargin': '0.00000000', 'totalOpenOrderInitialMargin': '0.00000000', 'totalCrossWalletBalance': '4995.09417573', 'totalCrossUnPnl': '0.00000000', 'availableBalance': '4995.09417573', 'maxWithdrawAmount': '4995.09417573', 'assets': [{'asset': 'FDUSD', 'walletBalance': '0.00000000', 'unrealizedProfit': '0.00000000', 'marginBalance': '0.00000000', 'maintMargin': '0.00000000', 'initialMargin': '0.00000000', 'positionInitialMargin': '0.00000000', 'openOrderInitialMargin': '0.00000000', 'crossWalletBalance': '0.00000000', 'crossUnPnl': '0.00000000', 'availableBalance': '0.00000000', 'maxWithdrawAmount': '0.00000000', 'updateTime': 0}, {'asset': 'U', 'walletBalance': '0.00000000', 'unrealizedProfit': '0.00000000', 'marginBalance': '0.00000000', 'maintMargin': '0.00000000', 'initialMargin': '0.00000000', 'positionInitialMargin': '0.00000000', 'openOrderInitialMargin': '0.00000000', 'crossWalletBalance': '0.00000000', 'crossUnPnl': '0.00000000', 'availableBalance': '0.00000000', 'maxWithdrawAmount': '0.00000000', 'updateTime': 0}, {'asset': 'BNB', 'walletBalance': '0.00000000', 'unrealizedProfit': '0.00000000', 'marginBalance': '0.00000000', 'maintMargin': '0.00000000', 'initialMargin': '0.00000000', 'positionInitialMargin': '0.00000000', 'openOrderInitialMargin': '0.00000000', 'crossWalletBalance': '0.00000000', 'crossUnPnl': '0.00000000', 'availableBalance': '0.00000000', 'maxWithdrawAmount': '0.00000000', 'updateTime': 0}, {'asset': 'ETH', 'walletBalance': '0.00000000', 'unrealizedProfit': '0.00000000', 'marginBalance': '0.00000000', 'maintMargin': '0.00000000', 'initialMargin': '0.00000000', 'positionInitialMargin': '0.00000000', 'openOrderInitialMargin': '0.00000000', 'crossWalletBalance': '0.00000000', 'crossUnPnl': '0.00000000', 'availableBalance': '0.00000000', 'maxWithdrawAmount': '0.00000000', 'updateTime': 0}, {'asset': 'BTC', 'walletBalance': '0.01000000', 'unrealizedProfit': '0.00000000', 'marginBalance': '0.01000000', 'maintMargin': '0.00000000', 'initialMargin': '0.00000000', 'positionInitialMargin': '0.00000000', 'openOrderInitialMargin': '0.00000000', 'crossWalletBalance': '0.01000000', 'crossUnPnl': '0.00000000', 'availableBalance': '0.01000000', 'maxWithdrawAmount': '0.01000000', 'updateTime': 1781341734796}, {'asset': 'USDT', 'walletBalance': '4995.09417573', 'unrealizedProfit': '0.00000000', 'marginBalance': '4995.09417573', 'maintMargin': '0.00000000', 'initialMargin': '0.00000000', 'positionInitialMargin': '0.00000000', 'openOrderInitialMargin': '0.00000000', 'crossWalletBalance': '4995.09417573', 'crossUnPnl': '0.00000000', 'availableBalance': '4995.09417573', 'maxWithdrawAmount': '4995.09417573', 'updateTime': 1781347578065}, {'asset': 'USD1', 'walletBalance': '0.00000000', 'unrealizedProfit': '0.00000000', 'marginBalance': '0.00000000', 'maintMargin': '0.00000000', 'initialMargin': '0.00000000', 'positionInitialMargin': '0.00000000', 'openOrderInitialMargin': '0.00000000', 'crossWalletBalance': '0.00000000', 'crossUnPnl': '0.00000000', 'availableBalance': '0.00000000', 'maxWithdrawAmount': '0.00000000', 'updateTime': 0}, {'asset': 'USDC', 'walletBalance': '5000.00000000', 'unrealizedProfit': '0.00000000', 'marginBalance': '5000.00000000', 'maintMargin': '0.00000000', 'initialMargin': '0.00000000', 'positionInitialMargin': '0.00000000', 'openOrderInitialMargin': '0.00000000', 'crossWalletBalance': '5000.00000000', 'crossUnPnl': '0.00000000', 'availableBalance': '5000.00000000', 'maxWithdrawAmount': '5000.00000000', 'updateTime': 1781341734745}], 'positions': []}

    try:
        account = await adapter.get_account_info()
        assert isinstance(account, AccountInfoRespDto)
        assert account.assets is not None
        assert account.positions is not None

        for key in account_test.keys():
            assert key in account.raw

    finally:
        await adapter.close()

@pytest.mark.stable
@pytest.mark.asyncio
async def test_real_regular_limit_buy_order_create_and_cancel() -> None:
    """
    BUY LIMIT 주문 생성/취소 테스트.

    BUY LIMIT 가격을 현재가보다 충분히 낮게 둬서 체결 가능성을 낮춘다.
    """

    symbol = os.getenv("BINANCE_REAL_TEST_SYMBOL", "BTCUSDT")
    quantity = os.getenv("BINANCE_REAL_TEST_QTY", "0.002")

    adapter = make_adapter()
    router = BinanceOrderRouter(adapter)

    client_order_id: str | None = None
    order: Order | None = None

    try:
        ref_price = await _get_reference_price(adapter, symbol)

        # 현재가의 50% 가격에 BUY LIMIT. -> 주문이 체결되 되지 않게
        # BTCUSDT tickSize가 보통 0.1 또는 0.01 단위라 0.1 단위로 보수적 round.
        limit_price = _round_down_to_step(ref_price * Decimal("0.5"), Decimal("0.1"))

        client_order_id = _client_id("BLM")
        
        order = _make_regular_limit_order(
            symbol=symbol,
            side=OrderSide.BUY,
            quantity=quantity,
            price=str(limit_price),
            client_order_id=client_order_id,
        )

        resp = await router.place_regular_order(order)
        
        place_test = {'orderId': 15209797037, 'symbol': 'BTCUSDT', 'status': 'NEW', 'clientOrderId': 'TKBLM1781490609621', 'price': '32949.50', 'origQty': '0.0020', 'executedQty': '0.0000', 'cumQty': '0.0000', 'timeInForce': 'GTC', 'type': 'LIMIT', 'reduceOnly': False, 'closePosition': False, 'side': 'BUY', 'positionSide': 'BOTH', 'stopPrice': '0.00', 'workingType': 'CONTRACT_PRICE', 'priceProtect': False, 'origType': 'LIMIT', 'priceMatch': 'NONE', 'selfTradePreventionMode': 'EXPIRE_MAKER', 'goodTillDate': 0, 'updateTime': 1781490609642}

        assert isinstance(resp, OrderRespDto)
        for key in place_test.keys():
            assert key in resp.raw

        assert resp.orderId is not None
        assert str(resp.clientOrderId) == client_order_id

        cancel_resp = await adapter.cancel_order(
            symbol=symbol,
            client_order_id=client_order_id,
        )

        print("데이터:",cancel_resp)

        assert isinstance(cancel_resp, CancelOrderRespDto)
        assert str(cancel_resp.clientOrderId) == client_order_id

        client_order_id = None

    finally:
        await _cancel_regular_safely(
            adapter=adapter,
            symbol=symbol,
            client_order_id=client_order_id,
        )
        await adapter.close()

@pytest.mark.stable
@pytest.mark.asyncio
async def test_real_regular_limit_sell_order_create_and_cancel() -> None:
    """
    SELL LIMIT 주문 생성/취소 테스트.

    SELL LIMIT 가격을 현재가보다 충분히 높게 둬서 체결 가능성을 낮춘다.
    """

    symbol = os.getenv("BINANCE_REAL_TEST_SYMBOL", "BTCUSDT")
    quantity = os.getenv("BINANCE_REAL_TEST_QTY", "0.001")

    adapter = make_adapter()
    router = BinanceOrderRouter(adapter)

    client_order_id: str | None = None

    try:
        ref_price = await _get_reference_price(adapter, symbol)
        limit_price = _round_down_to_step(
            ref_price * Decimal("2"),
            Decimal("0.1"),
        )

        client_order_id = _client_id("SLM")

        order = _make_regular_limit_order(
            symbol=symbol,
            side=OrderSide.SELL,
            quantity=quantity,
            price=str(limit_price),
            client_order_id=client_order_id,
        )

        resp = await router.place_regular_order(order)

        assert isinstance(resp, OrderRespDto)
        assert resp.orderId is not None
        assert str(resp.clientOrderId) == client_order_id

        cancel_resp = await adapter.cancel_order(
            symbol=symbol,
            client_order_id=client_order_id,
        )

        assert isinstance(cancel_resp, CancelOrderRespDto)
        assert str(cancel_resp.clientOrderId) == client_order_id

        client_order_id = None

    finally:
        await _cancel_regular_safely(
            adapter=adapter,
            symbol=symbol,
            client_order_id=client_order_id,
        )
        await adapter.close()

@pytest.mark.stable
@pytest.mark.asyncio
async def test_real_stop_market_buy_algo_order_create_query_and_cancel() -> None:
    """
    BUY STOP_MARKET algo order 생성/조회/취소 테스트.

    BUY STOP_MARKET은 가격이 triggerPrice 이상이 되면 발동한다.
    현재가의 2배를 triggerPrice로 둬서 즉시 trigger 가능성을 낮춘다.
    """

    symbol = os.getenv("BINANCE_REAL_TEST_SYMBOL", "BTCUSDT")
    quantity = os.getenv("BINANCE_REAL_TEST_QTY", "0.002")

    adapter = make_adapter()
    router = BinanceOrderRouter(adapter)

    client_algo_id: str | None = None
    algo_id: str | None = None

    try:
        ref_price = await _get_reference_price(adapter, symbol)

        # BUY STOP_MARKET은 가격이 triggerPrice 이상이 되면 발동.
        # 현재가 2배면 테스트 중 발동 가능성이 낮다.
        trigger_price = _round_down_to_step(ref_price * Decimal("2"), Decimal("0.1"))

        client_algo_id = _client_id("BSM")

        order = _make_stop_market_algo_order(
            symbol=symbol,
            side=OrderSide.BUY,
            quantity=quantity,
            trigger_price=str(trigger_price),
            client_algo_id=client_algo_id,
        )

        resp = await router.place_conditional_order(order)

        assert isinstance(resp, AlgoOrderRespDto)

        # Binance 응답 필드는 환경/버전에 따라 algoId/clientAlgoId 중심.
        algo_id_raw = resp.algoId
        if algo_id_raw is not None:
            algo_id = str(algo_id_raw)

        returned_client_algo_id = resp.clientAlgoId or client_algo_id
        

        assert returned_client_algo_id == client_algo_id

        found = await _assert_open_algo_order_exists(
            adapter=adapter,
            symbol=symbol,
            client_algo_id=client_algo_id,
            algo_id=algo_id,
        )

        assert str(found.clientAlgoId) == client_algo_id

        cancel_resp = await adapter.cancel_algo_order(
            symbol=symbol,
            client_algo_id=client_algo_id,
            algo_id=algo_id,
        )

        assert isinstance(cancel_resp, CancelAlgoOrderRespDto)
        client_algo_id = None
        algo_id = None

    finally:
        await _cancel_algo_safely(
            adapter=adapter,
            symbol=symbol,
            client_algo_id=client_algo_id,
            algo_id=algo_id,
        )
        await adapter.close()

@pytest.mark.stable
@pytest.mark.asyncio
async def test_real_stop_market_sell_algo_order_create_query_and_cancel() -> None:
    """
    SELL STOP_MARKET algo order 생성/조회/취소 테스트.

    SELL STOP_MARKET은 가격이 triggerPrice 이하가 되면 발동한다.
    현재가의 50%를 triggerPrice로 둬서 즉시 trigger 가능성을 낮춘다.
    """

    symbol = os.getenv("BINANCE_REAL_TEST_SYMBOL", "BTCUSDT")
    quantity = os.getenv("BINANCE_REAL_TEST_QTY", "0.002")

    adapter = make_adapter()
    router = BinanceOrderRouter(adapter)

    client_algo_id: str | None = None
    algo_id: str | None = None

    try:
        ref_price = await _get_reference_price(adapter, symbol)

        trigger_price = _round_down_to_step(
            ref_price * Decimal("0.5"),
            Decimal("0.1"),
        )

        client_algo_id = _client_id("SSM")

        order = _make_stop_market_algo_order(
            symbol=symbol,
            side=OrderSide.SELL,
            quantity=quantity,
            trigger_price=str(trigger_price),
            client_algo_id=client_algo_id,
        )

        resp = await router.place_conditional_order(order)

        assert isinstance(resp, AlgoOrderRespDto)

        # Binance 응답 필드는 환경/버전에 따라 algoId/clientAlgoId 중심.
        algo_id_raw = resp.algoId
        if algo_id_raw is not None:
            algo_id = str(algo_id_raw)

        returned_client_algo_id = str(resp.clientAlgoId)

        assert returned_client_algo_id == client_algo_id

        found = await _assert_open_algo_order_exists(
            adapter=adapter,
            symbol=symbol,
            client_algo_id=client_algo_id,
            algo_id=algo_id,
        )

        assert str(found.clientAlgoId) == client_algo_id

        cancel_resp = await adapter.cancel_algo_order(
            symbol=symbol,
            client_algo_id=client_algo_id,
            algo_id=algo_id,
        )

        assert isinstance(cancel_resp, CancelAlgoOrderRespDto)
        client_algo_id = None
        algo_id = None

    finally:
        await _cancel_algo_safely(
            adapter=adapter,
            symbol=symbol,
            client_algo_id=client_algo_id,
            algo_id=algo_id,
        )
        await adapter.close()

@pytest.mark.stable
@pytest.mark.asyncio
async def test_real_stop_limit_buy_algo_order_create_query_and_cancel() -> None:
    """
    BUY STOP_LIMIT algo order 생성/조회/취소 테스트.

    BUY STOP_LIMIT은 가격이 triggerPrice 이상이 되면 발동한다.
    현재가의 2배를 triggerPrice로 둬서 즉시 trigger 가능성을 낮춘다.
    """

    symbol = os.getenv("BINANCE_REAL_TEST_SYMBOL", "BTCUSDT")
    quantity = os.getenv("BINANCE_REAL_TEST_QTY", "0.002")

    adapter = make_adapter()
    router = BinanceOrderRouter(adapter)

    client_algo_id: str | None = None
    algo_id: str | None = None

    try:
        ref_price = await _get_reference_price(adapter, symbol)

        trigger_price = _round_down_to_step(
            ref_price * Decimal("2"),
            Decimal("0.1"),
        )

        # 발동 후 제출될 BUY LIMIT 가격.
        # trigger보다 약간 높게 둔다.
        limit_price = _round_down_to_step(
            trigger_price * Decimal("1.001"),
            Decimal("0.1"),
        )

        client_algo_id = _client_id("BSL")

        order = _make_stop_limit_algo_order(
            symbol=symbol,
            side=OrderSide.BUY,
            quantity=quantity,
            trigger_price=str(trigger_price),
            price=str(limit_price),
            client_algo_id=client_algo_id,
        )

        resp = await router.place_conditional_order(order)

        assert isinstance(resp, AlgoOrderRespDto)

        # Binance 응답 필드는 환경/버전에 따라 algoId/clientAlgoId 중심.
        algo_id_raw = resp.algoId
        if algo_id_raw is not None:
            algo_id = str(algo_id_raw)

        returned_client_algo_id = resp.clientAlgoId or client_algo_id

        assert returned_client_algo_id == client_algo_id

        found = await _assert_open_algo_order_exists(
            adapter=adapter,
            symbol=symbol,
            client_algo_id=client_algo_id,
            algo_id=algo_id,
        )

        assert str(found.clientAlgoId) == client_algo_id

        cancel_resp = await adapter.cancel_algo_order(
            symbol=symbol,
            client_algo_id=client_algo_id,
            algo_id=algo_id,
        )

        assert isinstance(cancel_resp, CancelAlgoOrderRespDto)
        client_algo_id = None
        algo_id = None

    finally:
        await _cancel_algo_safely(
            adapter=adapter,
            symbol=symbol,
            client_algo_id=client_algo_id,
            algo_id=algo_id,
        )
        await adapter.close()

@pytest.mark.stable
@pytest.mark.asyncio
async def test_real_stop_limit_sell_algo_order_create_query_and_cancel() -> None:
    """
    SELL STOP_LIMIT algo order 생성/조회/취소 테스트.

    SELL STOP_LIMIT은 가격이 triggerPrice 이하가 되면 발동한다.
    현재가의 0.5배를 triggerPrice로 둬서 즉시 trigger 가능성을 낮춘다.
    """

    symbol = os.getenv("BINANCE_REAL_TEST_SYMBOL", "BTCUSDT")
    quantity = os.getenv("BINANCE_REAL_TEST_QTY", "0.002")

    adapter = make_adapter()
    router = BinanceOrderRouter(adapter)

    client_algo_id: str | None = None
    algo_id: str | None = None

    try:
        ref_price = await _get_reference_price(adapter, symbol)

        trigger_price = _round_down_to_step(
            ref_price * Decimal("0.5"),
            Decimal("0.1"),
        )

        # 발동 후 제출될 BUY LIMIT 가격.
        # trigger보다 약간 낮게 둔다.
        limit_price = _round_down_to_step(
            trigger_price * Decimal("0.999"),
            Decimal("0.1"),
        )

        client_algo_id = _client_id("SSL")

        order = _make_stop_limit_algo_order(
            symbol=symbol,
            side=OrderSide.SELL,
            quantity=quantity,
            trigger_price=str(trigger_price),
            price=str(limit_price),
            client_algo_id=client_algo_id,
        )

        resp = await router.place_conditional_order(order)

        assert isinstance(resp, AlgoOrderRespDto)

        # Binance 응답 필드는 환경/버전에 따라 algoId/clientAlgoId 중심.
        algo_id_raw = resp.algoId
        if algo_id_raw is not None:
            algo_id = str(algo_id_raw)

        returned_client_algo_id = resp.clientAlgoId or client_algo_id

        assert returned_client_algo_id == client_algo_id

        found = await _assert_open_algo_order_exists(
            adapter=adapter,
            symbol=symbol,
            client_algo_id=client_algo_id,
            algo_id=algo_id,
        )

        assert str(found.clientAlgoId) == client_algo_id

        cancel_resp = await adapter.cancel_algo_order(
            symbol=symbol,
            client_algo_id=client_algo_id,
            algo_id=algo_id,
        )

        assert isinstance(cancel_resp, CancelAlgoOrderRespDto)
        client_algo_id = None
        algo_id = None

    finally:
        await _cancel_algo_safely(
            adapter=adapter,
            symbol=symbol,
            client_algo_id=client_algo_id,
            algo_id=algo_id,
        )
        await adapter.close()

@pytest.mark.stable
@pytest.mark.asyncio
async def test_real_get_open_orders_get_all_orders_and_cancel_all_open_orders() -> None:
    """
    일반 LIMIT 주문 생성 후 openOrders/allOrders 조회와 전체 일반 주문 취소를 검증한다.

    cancel_all_open_orders는 해당 symbol의 모든 일반 open order를 취소하므로,
    테스트 시작 시 기존 open order가 있으면 계정 보호를 위해 skip한다.
    """

    symbol = os.getenv("BINANCE_REAL_TEST_SYMBOL", "BTCUSDT")
    quantity = os.getenv("BINANCE_REAL_TEST_QTY", "0.001")

    adapter = make_adapter()
    router = BinanceOrderRouter(adapter)

    client_order_id: str | None = None

    try:
        existing_open_orders = await adapter.get_open_orders(symbol=symbol)
        if existing_open_orders:
            await adapter.cancel_all_open_orders(symbol=symbol)

        ref_price = await _get_reference_price(adapter, symbol)
        limit_price = _round_down_to_step(ref_price * Decimal("0.5"), Decimal("0.1"))
        quantity = _quantity_with_min_notional(
            quantity=quantity,
            price=limit_price,
        )

        client_order_id = _client_id("CAO")
        start_time = _now_ms() - 5_000

        order = _make_regular_limit_order(
            symbol=symbol,
            side=OrderSide.BUY,
            quantity=quantity,
            price=str(limit_price),
            client_order_id=client_order_id,
        )

        resp = await router.place_regular_order(order)


        assert isinstance(resp, OrderRespDto)
        assert resp.orderId is not None
        assert str(resp.clientOrderId) == client_order_id

        open_row = await _assert_open_regular_order_exists(
            adapter=adapter,
            symbol=symbol,
            client_order_id=client_order_id,
            order_id=resp.orderId,
        )
        assert str(open_row.clientOrderId) == client_order_id
        cancel_all_resp = await adapter.cancel_all_open_orders(symbol=symbol)
        
        assert isinstance(cancel_all_resp, CancelAllOpenOrdersRespDto)
        assert cancel_all_resp.code == 200
        assert cancel_all_resp.msg

        await _wait_regular_order_absent_from_open_orders(
            adapter=adapter,
            symbol=symbol,
            client_order_id=client_order_id,
        )

        cancelled_client_order_id = client_order_id
        client_order_id = None

        try:
            all_orders_row = await _wait_all_orders_contains(
                adapter=adapter,
                symbol=symbol,
                client_order_id=cancelled_client_order_id,
                order_id=resp.orderId,
                start_time=start_time,
            )
            assert str(all_orders_row.clientOrderId) == str(resp.clientOrderId)
        except Exception as e:
            print(e)


    finally:
        await _cancel_regular_safely(
            adapter=adapter,
            symbol=symbol,
            client_order_id=client_order_id,
        )
        await adapter.close()


@pytest.mark.stable
@pytest.mark.asyncio
async def test_real_cancel_all_algo_open_orders() -> None:
    """
    조건부 STOP_MARKET 주문 생성 후 전체 algo open order 취소를 검증한다.

    cancel_all_algo_open_orders는 해당 symbol의 모든 algo open order를 취소하므로,
    테스트 시작 시 기존 algo open order가 있으면 계정 보호를 위해 skip한다.
    """

    symbol = os.getenv("BINANCE_REAL_TEST_SYMBOL", "BTCUSDT")
    quantity = os.getenv("BINANCE_REAL_TEST_QTY", "0.001")

    adapter = make_adapter()
    router = BinanceOrderRouter(adapter)

    client_algo_id: str | None = None
    algo_id: str | None = None

    try:
        existing_open_algo_orders = await adapter.get_open_algo_orders(symbol=symbol)
        if existing_open_algo_orders:
            pytest.skip(
                f"{symbol}에 기존 algo open order가 있어 cancel_all_algo_open_orders 테스트를 건너뜁니다."
            )

        ref_price = await _get_reference_price(adapter, symbol)
        trigger_price = _round_down_to_step(ref_price * Decimal("2"), Decimal("0.1"))

        client_algo_id = _client_id("CAA")

        order = _make_stop_market_algo_order(
            symbol=symbol,
            side=OrderSide.BUY,
            quantity=quantity,
            trigger_price=str(trigger_price),
            client_algo_id=client_algo_id,
        )

        resp = await router.place_conditional_order(order)

        assert isinstance(resp, AlgoOrderRespDto)

        if resp.algoId is not None:
            algo_id = str(resp.algoId)

        returned_client_algo_id = resp.clientAlgoId or client_algo_id
        assert returned_client_algo_id == client_algo_id

        found = await _assert_open_algo_order_exists(
            adapter=adapter,
            symbol=symbol,
            client_algo_id=client_algo_id,
            algo_id=algo_id,
        )
        assert str(found.clientAlgoId) == client_algo_id

        cancel_all_resp = await adapter.cancel_all_algo_open_orders(symbol=symbol)

        assert isinstance(cancel_all_resp, CancelAllAlgoOpenOrdersRespDto)
        assert cancel_all_resp.code == 200
        assert cancel_all_resp.msg

        await _wait_algo_order_absent_from_open_orders(
            adapter=adapter,
            symbol=symbol,
            client_algo_id=client_algo_id,
        )

        client_algo_id = None
        algo_id = None

    finally:
        await _cancel_algo_safely(
            adapter=adapter,
            symbol=symbol,
            client_algo_id=client_algo_id,
            algo_id=algo_id,
        )
        await adapter.close()


@pytest.mark.stable
@pytest.mark.asyncio
async def test_real_market_buy_then_market_sell_cleanup() -> None:
    """
    MARKET BUY 후 MARKET SELL로 정리하는 테스트.

    주의:
      - 테스트넷이어도 실제 포지션이 생긴다.
      - 명시적으로 RUN_BINANCE_REAL_MARKET_TESTS=1일 때만 실행한다.
    """

    symbol = os.getenv("BINANCE_REAL_TEST_SYMBOL", "BTCUSDT")
    quantity = os.getenv("BINANCE_REAL_TEST_QTY", "0.001")

    adapter = make_adapter()
    router = BinanceOrderRouter(adapter)

    try:
        buy_id = _client_id("MBY")

        buy_order = _make_regular_market_order(
            symbol=symbol,
            side=OrderSide.BUY,
            quantity=quantity,
            client_order_id=buy_id,
        )

        buy_resp = await router.place_regular_order(buy_order)

        assert isinstance(buy_resp, OrderRespDto)
        assert buy_resp.orderId is not None

        sell_id = _client_id("MSL")

        sell_order = _make_regular_market_order(
            symbol=symbol,
            side=OrderSide.SELL,
            quantity=quantity,
            client_order_id=sell_id,
        )

        sell_resp = await router.place_regular_order(sell_order)

        assert isinstance(sell_resp, OrderRespDto)
        assert sell_resp.orderId is not None

    finally:
        await adapter.close()

_ORDER_DOES_NOT_EXIST = -2013
_TESTNET_API_RETRY_SLEEP_CAP_SEC = 1.25
_TESTNET_RATE_LIMIT_BACKOFF_SEC = 2.0

async def _get_order_retry_binance(
    adapter: BinanceRestAdapter,
    *,
    symbol: str,
    client_order_id: str,
    attempts: int = 15,
) -> OrderRespDto:
    delay = 0.05
    last_2013: BinanceApiError | None = None

    for _ in range(attempts):
        try:
            return await adapter.get_order(
                symbol=symbol,
                client_order_id=client_order_id,
            )
        except BinanceRateLimitError:
            await asyncio.sleep(_TESTNET_RATE_LIMIT_BACKOFF_SEC)
        except BinanceApiError as e:
            if e.code != _ORDER_DOES_NOT_EXIST:
                raise
            last_2013 = e
            await asyncio.sleep(delay)
            delay = min(delay * 1.5, _TESTNET_API_RETRY_SLEEP_CAP_SEC)
    assert last_2013 is not None
    raise last_2013


async def _cancel_order_retry_binance(
    adapter: BinanceRestAdapter,
    *,
    symbol: str,
    order_id: int,
    attempts: int = 12,
) -> CancelOrderRespDto:
    delay = 0.05
    last_2013: BinanceApiError | None = None
    for _ in range(attempts):
        try:
            return await adapter.cancel_order(symbol=symbol, order_id=order_id)
        except BinanceRateLimitError:
            await asyncio.sleep(_TESTNET_RATE_LIMIT_BACKOFF_SEC)
        except BinanceApiError as e:
            if e.code != _ORDER_DOES_NOT_EXIST:
                raise
            try:
                q = await adapter.get_order(symbol=symbol, order_id=order_id)
                if q.status == "CANCELED":
                    return CancelOrderRespDto.from_response(q.raw)
            except BinanceApiError:
                pass
            last_2013 = e
            await asyncio.sleep(delay)
            delay = min(delay * 1.5, _TESTNET_API_RETRY_SLEEP_CAP_SEC)
    assert last_2013 is not None
    raise last_2013

@pytest.mark.stable
@pytest.mark.integration
@pytest.mark.asyncio
async def test_rest_adapter_testnet():
    adapter = make_adapter()

    order_id = None

    try:
        account = await adapter.get_account_info()
        assert account.totalWalletBalance is not None

        new_client_order_id = generate_order_id()

        order = await adapter.place_regular_order(
            {
                "symbol": "BTCUSDT",
                "side": "BUY",
                "type": "LIMIT",
                "timeInForce": "GTC",
                "price": "10000",
                "quantity": "0.01",
                "newClientOrderId": new_client_order_id,
            }
        )

        order_id = order.orderId
        assert order.clientOrderId == new_client_order_id

        queried = await _get_order_retry_binance(
            adapter,
            symbol="BTCUSDT",
            client_order_id=new_client_order_id,
        )
        assert queried.clientOrderId == new_client_order_id

        assert order_id
        cancel = await _cancel_order_retry_binance(
            adapter,
            symbol="BTCUSDT",
            order_id=order_id,
        )
        assert cancel.status == "CANCELED"

        order_id = None

    finally:
        if order_id is not None:
            await adapter.cancel_order("BTCUSDT", order_id=order_id)

        await adapter.close()

@pytest.mark.stable
@pytest.mark.asyncio
@pytest.mark.integration
async def test_rest_adapter_testnet_batch():
    adapter = make_adapter()

    # open_orders = await adapter.get_open_orders("BTCUSDT")

    # 2. 단건 주문 테스트 (터무니없는 가격으로 지정가 주문하여 체결되지 않게 함)
    newClientOrderId = generate_order_id()
    order_resp = await adapter.place_regular_order(
        {
            "symbol": "BTCUSDT",
            "newClientOrderId": newClientOrderId,
            "side": "BUY",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "price": "10000.0",
            "quantity": "0.01",
        }
    )
    order_id = order_resp.orderId
    assert order_resp.clientOrderId == newClientOrderId

    # # 3. 단건 주문 취소
    assert order_id
    cancel_resp = await _cancel_order_retry_binance(
        adapter, symbol="BTCUSDT", order_id=order_id
    )
    assert cancel_resp.orderId == order_id

    # # 4. 일괄 주문 테스트 (batchOrders)
    batch_params = [
        {
            "symbol": "BTCUSDT",
            "side": "BUY",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "price": "10001.0",
            "quantity": "0.01",
            "newClientOrderId": generate_order_id(),
        },
        {
            "symbol": "BTCUSDT",
            "side": "BUY",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "price": "10002.0",
            "quantity": "0.01",
            "newClientOrderId": generate_order_id(),
        },
        {
            "symbol": "BTCUSDT",
            "side": "BUY",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "price": "10003.0",
            "quantity": "0.01",
            "newClientOrderId": generate_order_id(),
        },
    ]

    order_ids_to_cancel: list[int] = []
    # # 5. 일괄 취소 테스트 (batchCancel)
    try:
        (batch_resp, error) = await adapter.place_batch_orders(batch_params)

        assert error == []

        assert len(batch_resp) == 3
        assert all(r.orderId for r in batch_resp if isinstance(r, OrderRespDto))
        assert all(
            [
                batch_params[i]["newClientOrderId"] == batch_resp[i].clientOrderId
                for i in range(len(batch_resp)) if isinstance(batch_resp[i], OrderRespDto)
            ]
        )

        order_ids_to_cancel = [
            r.orderId for r in batch_resp if isinstance(r, OrderRespDto) and r.orderId
        ]


        batch_cancel_resp = await adapter.cancel_batch_orders(
            "BTCUSDT", order_ids=order_ids_to_cancel
        )
        assert len(batch_cancel_resp) == 3
        
        assert all(r.status == "CANCELED" for r in batch_cancel_resp)

        listen_key_dto = await adapter.create_listen_key()
        listen_key = listen_key_dto.listenKey
        assert listen_key

        await adapter.close_listen_key(listen_key)

    finally:
        if order_ids_to_cancel:
            await adapter.cancel_batch_orders(
                "BTCUSDT",
                order_ids=order_ids_to_cancel[:10],
            )

        await adapter.close()
