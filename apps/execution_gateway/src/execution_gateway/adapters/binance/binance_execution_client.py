from __future__ import annotations

from typing import Any, Optional

from schemas.market import Exchange, MarketType
from schemas.order import (
    ConditionalStatus,
    Order,
    OrderRoute,
    OrderStatus,
    RejectReason,
)
from schemas.position import PositionSide

from execution_gateway.adapters.binance.binance_order_router import BinanceOrderRouter
from execution_gateway.adapters.binance.binance_rest_adapter import (
    BinanceApiError,
    BinanceInternalRetryableError,
    BinanceIpBanError,
    BinanceNetworkError,
    BinanceRateLimitError,
    BinanceRestAdapter,
    BinanceServiceUnavailableError,
    BinanceSystemThrottleError,
    BinanceUnknownExecutionError,
    BinanceWafError,
    BinanceLeveragePolicyError
)
from execution_gateway.adapters.binance.binance_rate_limiter import BinanceRateLimiter
from execution_gateway.adapters.binance.binance_ws_trade_adapter import BinanceWsTradeAdapter
from execution_gateway.exchange import (
    ExchangeApiError,
    ExchangeBatchOrderResult,
    ExchangeCancelResult,
    ExchangeCapabilities,
    ExchangeConditionalAck,
    ExchangeConditionalSnapshot,
    ExchangeErrorCategory,
    ExchangeLeverageResult,
    ExchangeOrderAck,
    ExchangeOrderReject,
    ExchangeOrderSnapshot,
    ExchangePositionSnapshot,
    ExchangeExecutionClient
)

from .constant.binance_constant import BinanceConditionalOrderState, BINANCE_EXCHANGE_CONDITIONAL_ORDER_UNKNOWN_STATUS

from .mapper.binance_order_event_mapper import _BINANCE_ORDER_STATUS_MAP, _MAPPER_INTERNAL_TO_BINANCE_ORDER_STATUS
from .mapper.binance_algo_event_mapper import _BINANCE_ALGO_STATUS_MAP, _MAPPER_INTERNAL_TO_BINANCE_ALGO_STATUS

from .dto.resp.OrderResponseDto import CancelAlgoOrderRespDto, OrderRespDto
from .dto.resp.AlgoOrderResponseDto import AlgoOrderRespDto
from .dto.resp.PositionResponseDto import PositionRiskRespDto

from common.time import epoch_ms

from common.logging import setup_logger

logger = setup_logger(__name__)

MAX_ALL_ORDERS_WINDOW_MS = 7 * 24 * 60 * 60 * 1000 - 1_000

class BinanceExecutionClient(ExchangeExecutionClient):
    """
    ExchangeExecutionClient implementation for Binance USD-M Futures.

    This wrapper keeps Binance-specific parameter, status, and error mapping out
    of the future exchange-neutral ExecutionGateway path while reusing the
    existing BinanceRestAdapter and BinanceOrderRouter.
    """

    exchange = Exchange.BINANCE
    market_type = MarketType.PERP

    capabilities = ExchangeCapabilities(
        supports_batch_order=True,
        max_batch_order_size=5,
        supports_batch_cancel=True,
        max_batch_cancel_size=10,
        supports_cancel_all=True,
        supports_ws_trade=False,
        supports_conditional_order=True,
        supports_conditional_batch=False,
        supports_conditional_reconciliation=True,
        supports_bulk_order_lookup=True,
        supports_hedge_mode=True,
        supports_reduce_only=True,
        supports_close_position=True,
        supports_leverage_change=True,
        supports_position_snapshot=True,
        bulk_order_lookup_threshold=6,
    )

    def __init__(
        self, 
        *,
        adapter: BinanceRestAdapter, 
        order_router: BinanceOrderRouter,
        ws_adapter: Optional[BinanceWsTradeAdapter] = None
    ) -> None:
        self.adapter = adapter
        self.order_router = order_router
        self.rate_limiter: BinanceRateLimiter = BinanceRateLimiter()
        self.ws_adapter: Optional[BinanceWsTradeAdapter] = ws_adapter

    async def place_order(self, order: Order) -> ExchangeOrderAck:
        # if order.order_route == OrderRoute.CONDITIONAL:
        #     raise ValueError("place_order requires a REGULAR order")

        try:
            await self.rate_limiter.acquire_single_order()
            resp = await self.order_router.place_regular_order(order)
        except BinanceApiError as e:
            raise self._map_error(e) from e

        return self._order_ack_from_response(order=order, resp=resp)

    async def place_conditional_order(
        self,
        order: Order,
    ) -> ExchangeConditionalAck:
        # if order.order_route != OrderRoute.CONDITIONAL:
        #     raise ValueError("place_conditional_order requires a CONDITIONAL order")

        try:
            await self.rate_limiter.acquire_single_order()
            resp = await self.order_router.place_conditional_order(order)
        except BinanceApiError as e:
            raise self._map_error(e) from e

        return self._conditional_ack_from_response(order=order, resp=resp)

    async def place_batch_orders(
        self,
        orders: list[Order],
    ) -> list[ExchangeBatchOrderResult]:
        if not orders:
            return []

        params = [self.order_router._map_regular_order_params(order) for order in orders]

        try:
            await self.rate_limiter.acquire_batch_orders()
            success, errors = await self.adapter.place_batch_orders(params)
        except BinanceApiError as e:
            raise self._map_error(e) from e

        results: list[ExchangeBatchOrderResult] = []
        for order, row in zip(orders, success):
            results.append(self._order_ack_from_response(order=order, resp=row))
            
        for order, row in zip(orders, errors):
            item_code = row.get("code")
            results.append(
                ExchangeOrderReject(
                    exchange=order.exchange,
                    market_type=order.market_type,
                    symbol=order.symbol,
                    client_order_id=order.client_order_id or order.order_id or "",
                    reject_reason=self._reject_reason_from_code(
                        item_code,
                        str(row.get("msg", "")),
                    ),
                    message=str(row.get("msg", "")),
                    code=item_code,
                    raw=row,
                )
            )

        return results

    async def cancel_order(self, order: Order) -> ExchangeCancelResult:
        try:
            if order.order_route == OrderRoute.CONDITIONAL:
                await self.rate_limiter.acquire_request_weight(weight=1)
                algo_resp = await self.adapter.cancel_algo_order(
                    symbol=order.symbol,
                    client_algo_id=order.client_conditional_id,
                    algo_id=order.exchange_conditional_id,
                )

                return self._conditional_cancel_result_from_dto(
                    exchange=order.exchange,
                    market_type=order.market_type,
                    symbol=order.symbol,
                    client_conditional_id=order.client_conditional_id,
                    dto=algo_resp,
                )
            else:
                await self.rate_limiter.acquire_request_weight(weight=1)
                regular_resp = await self.adapter.cancel_order(
                    symbol=order.symbol,
                    client_order_id=order.client_order_id or order.order_id,
                )
                return self._regular_cancel_from_response(
                    exchange=order.exchange,
                    market_type=order.market_type,
                    symbol=order.symbol,
                    client_order_id=regular_resp.clientOrderId or order.client_order_id,
                    exchange_order_id=regular_resp.orderId or order.exchange_order_id,
                    raw_status=regular_resp.status,
                    raw=regular_resp.raw,
                )
        except BinanceApiError as e:
            raise self._map_error(e) from e


    async def cancel_regular_order_by_client_id(
        self,
        *,
        symbol: str,
        client_order_id: str,
    ) -> ExchangeCancelResult:
        try:
            await self.rate_limiter.acquire_request_weight(weight=1)
            resp = await self.adapter.cancel_order(
                symbol=symbol,
                client_order_id=client_order_id,
            )
        except BinanceApiError as e:
            raise self._map_error(e) from e

        return self._regular_cancel_from_response(
            exchange=self.exchange,
            market_type=self.market_type,
            symbol=symbol,
            client_order_id=resp.clientOrderId or client_order_id,
            exchange_order_id=resp.orderId,
            raw_status=resp.status,
            raw=resp.raw,
        )

    async def cancel_batch_orders(
        self,
        orders: list[Order],
    ) -> list[ExchangeCancelResult]:
        if not orders:
            return []

        if any(order.order_route != OrderRoute.REGULAR for order in orders):
            raise ValueError("Binance batch cancel supports only REGULAR orders")

        symbols = {order.symbol.upper() for order in orders}
        if len(symbols) != 1:
            raise ValueError("Binance batch cancel requires all orders to share symbol")

        symbol = next(iter(symbols))
        client_order_ids = [
            order.client_order_id or order.order_id or ""
            for order in orders
        ]

        try:
            await self.rate_limiter.acquire_request_weight(weight=1)
            cancel_order_resp_list = await self.adapter.cancel_batch_orders(
                symbol=symbol,
                client_order_ids=client_order_ids,
            )
        except BinanceApiError as e:
            raise self._map_error(e) from e

        return [
            self._regular_cancel_from_response(
                exchange=order.exchange,
                market_type=order.market_type,
                symbol=order.symbol,
                client_order_id=resp.clientOrderId or order.client_order_id,
                exchange_order_id=resp.orderId or order.exchange_order_id,
                raw_status=resp.status,
                raw=resp.raw,
            )
            for order, resp in zip(orders, cancel_order_resp_list)
        ]

    async def cancel_all_regular_open_orders(
        self,
        *,
        symbol: str,
    ) -> ExchangeCancelResult:
        try:
            await self.rate_limiter.acquire_request_weight(weight=1)
            resp = await self.adapter.cancel_all_open_orders(symbol=symbol)
        except BinanceApiError as e:
            raise self._map_error(e) from e

        return ExchangeCancelResult(
            exchange=self.exchange,
            market_type=self.market_type,
            symbol=symbol.upper(),
            raw=resp.raw,
        )

    async def cancel_conditional_order_by_id(
        self,
        *,
        symbol: str,
        client_conditional_id: str | None = None,
        exchange_conditional_id: str | None = None,
    ) -> ExchangeCancelResult:
        if not client_conditional_id and not exchange_conditional_id:
            raise ValueError(
                "client_conditional_id 또는 exchange_conditional_id 중 하나는 필요합니다."
            )

        try:
            await self.rate_limiter.acquire_request_weight(weight=1)
            resp = await self.adapter.cancel_algo_order(
                symbol=symbol,
                client_algo_id=client_conditional_id,
                algo_id=exchange_conditional_id,
            )
        except BinanceApiError as e:
            raise self._map_error(e) from e

        return self._conditional_cancel_result_from_dto(
            exchange=self.exchange,
            market_type=self.market_type,
            symbol=symbol,
            client_conditional_id=client_conditional_id,
            dto=resp,
        )

    async def cancel_all_conditional_open_orders(
        self,
        *,
        symbol: str,
    ) -> ExchangeCancelResult:
        try:
            await self.rate_limiter.acquire_request_weight(weight=1)
            resp = await self.adapter.cancel_all_algo_open_orders(symbol=symbol)
        except BinanceApiError as e:
            raise self._map_error(e) from e

        return ExchangeCancelResult(
            exchange=self.exchange,
            market_type=self.market_type,
            symbol=symbol.upper(),
            raw=resp.raw,
        )

    async def get_order(self, order: Order) -> ExchangeOrderSnapshot:
        try:
            await self.rate_limiter.acquire_request_weight(weight=1)
            resp = await self.adapter.get_order(
                symbol=order.symbol,
                client_order_id=order.client_order_id or order.order_id,
            )
        except BinanceApiError as e:
            raise self._map_error(e) from e

        return self._order_snapshot_from_response(resp.raw, fallback_order=order)

    async def get_open_orders(
        self,
        *,
        symbol: str | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        try:
            weight = 1 if symbol else 40
            await self.rate_limiter.acquire_request_weight(weight=weight)
            rows = await self.adapter.get_open_orders(symbol=symbol)
        except BinanceApiError as e:
            raise self._map_error(e) from e

        return [self._order_snapshot_from_response(row.raw) for row in rows]

    async def cancel_conditional_order(
        self,
        order: Order,
    ) -> ExchangeCancelResult:
        if order.order_route != OrderRoute.CONDITIONAL:
            raise ValueError("cancel_conditional_order requires a CONDITIONAL order")

        return await self.cancel_order(order)

    async def get_open_conditional_orders(
        self,
        symbol: str
    ) -> list[ExchangeConditionalSnapshot]:
        try:
            await self.rate_limiter.acquire_request_weight(weight=1)
            rows = await self.adapter.get_open_algo_orders(symbol=symbol)
        except BinanceApiError as e:
            raise self._map_error(e) from e

        return [self._conditional_snapshot_from_response(row.raw) for row in rows]

    async def get_conditional_order(
        self,
        order: Order,
    ) -> ExchangeConditionalSnapshot | None:
        if order.order_route != OrderRoute.CONDITIONAL:
            raise ValueError("get_conditional_order requires a CONDITIONAL order")

        try:
            await self.rate_limiter.acquire_request_weight(weight=1)
            open_rows:list[AlgoOrderRespDto] = await self.adapter.get_open_algo_orders(symbol=order.symbol)
            matched = self._find_matching_algo_row(rows=open_rows, order=order)
            if matched is not None:
                return self._conditional_snapshot_from_response(matched.raw, order)

            await self.rate_limiter.acquire_request_weight(weight=5)
            all_rows = await self.adapter.get_all_algo_orders(
                symbol=order.symbol,
                algo_id=order.exchange_conditional_id,
                limit=1000,
            )
            matched = self._find_matching_algo_row(rows=all_rows, order=order)
            if matched is not None:
                return self._conditional_snapshot_from_response(matched.raw, order)
        except BinanceApiError as e:
            raise self._map_error(e) from e

        return None

    async def change_leverage(
        self,
        *,
        symbol: str,
        leverage: int,
    ) -> ExchangeLeverageResult:     
        try:
            await self.rate_limiter.acquire_request_weight(weight=1)
            resp = await self.adapter.change_leverage(
                symbol=symbol,
                leverage=leverage,
            )
        except BinanceLeveragePolicyError as e:
            raise ExchangeApiError(
                exchange=self.exchange,
                category=ExchangeErrorCategory.INVALID_PARAMETER,
                message=str(e),
                code="LEVERAGE_POLICY_EXCEEDED",
                status_code=400,
                raw={
                    "type": type(e).__name__,
                    "requested": e.requested,
                    "max_allowed": e.max_allowed,
                },
            ) from e
        except BinanceApiError as e:
            raise self._map_error(e) from e

        return ExchangeLeverageResult(
            exchange=self.exchange,
            market_type=self.market_type,
            symbol=str(resp.symbol or symbol).upper(),
            leverage=int(resp.leverage or leverage),
            raw=resp.raw,
        )

    async def get_symbol_price_ticker(self, symbol: str) -> dict[str, Any]:
        try:
            ticker = await self.adapter.get_symbol_price_ticker(symbol)
            return {
                "symbol": ticker.symbol,
                "price": ticker.price,
                "time": ticker.time
            }
        except BinanceApiError as e:
            raise self._map_error(e) from e

    async def get_positions(
        self,
        *,
        symbol: str | None = None,
    ) -> list[ExchangePositionSnapshot]:
        try:
            await self.rate_limiter.acquire_request_weight(weight=5)
            rows:list[PositionRiskRespDto] = await self.adapter.get_position_risk_v3(symbol=symbol)
        except BinanceApiError as e:
            raise self._map_error(e) from e

        return [self._position_snapshot_from_response(row) for row in rows]

    async def find_order_snapshots(
        self,
        *,
        symbol: str,
        orders: list[Order],
        lookback_ms: int = 60_000,
        limit: int = 1000,
    ) -> dict[str, ExchangeOrderSnapshot]:
        """
        Binance allOrders로 여러 주문 snapshot을 한 번에 조회한다.

        반환 key는 local order.order_id.
        즉 ReconciliationWorker는 result[order.order_id]로 바로 찾을 수 있다.
        """
        valid_orders = [
            order
            for order in orders
            if order.order_id and order.symbol.upper() == symbol.upper()
        ]

        if not valid_orders:
            return {}

        lookup_client_id_by_order_id: dict[str, str] = {}

        for order in valid_orders:
            order_id = str(order.order_id)
            client_order_id = order.client_order_id or order.order_id

            if not client_order_id:
                continue

            lookup_client_id_by_order_id[order_id] = str(client_order_id)

        if not lookup_client_id_by_order_id:
            return {}

        target_client_ids = set(lookup_client_id_by_order_id.values())

        oldest_created_ts = min(order.created_ts for order in valid_orders)
        end_time = epoch_ms()
        start_time = max(
            0,
            max(
                oldest_created_ts - lookback_ms,
                end_time - MAX_ALL_ORDERS_WINDOW_MS,
            ),
        )

        order_resp_list = await self._fetch_all_orders_until_client_ids_found(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            target_client_ids=target_client_ids,
            limit=limit,
        )

        order_resp_by_client_id = {
            order_resp.clientOrderId: order_resp
            for order_resp in order_resp_list
            if order_resp.clientOrderId
        }

        result: dict[str, ExchangeOrderSnapshot] = {}

        for order in valid_orders:
            if not order.order_id:
                continue

            order_id = str(order.order_id)
            client_order_id = lookup_client_id_by_order_id.get(order_id)

            if not client_order_id:
                continue

            order_resp = order_resp_by_client_id.get(client_order_id)

            if order_resp is None:
                continue

            result[order_id] = self._order_snapshot_from_response(
                order_resp.raw,
                fallback_order=order,
            )

        return result

    async def close(self) -> None:
        await self.adapter.close()

    def _order_ack_from_response(
        self,
        *,
        order: Order,
        resp: OrderRespDto,
    ) -> ExchangeOrderAck:
        status_val = resp.status
        return ExchangeOrderAck(
            exchange=order.exchange,
            market_type=order.market_type,
            symbol=order.symbol,
            client_order_id=order.client_order_id or order.order_id or "",
            exchange_order_id=self._optional_str(resp.orderId),
            status=self._map_order_status(status_val) or OrderStatus.ACKNOWLEDGED,
            raw_status=self._optional_str(status_val),
            raw=resp.raw,
        )

    def _conditional_ack_from_response(
        self,
        *,
        order: Order,
        resp: AlgoOrderRespDto,
    ) -> ExchangeConditionalAck:
        algo_status = resp.algoStatus
        return ExchangeConditionalAck(
            exchange=order.exchange,
            market_type=order.market_type,
            symbol=order.symbol,
            client_conditional_id=order.client_conditional_id or order.order_id or "",
            exchange_conditional_id=self._optional_str(resp.algoId),
            conditional_status=(
                self._map_conditional_status(algo_status)
                or ConditionalStatus.NEW
            ),
            raw_status=self._optional_str(algo_status),
            raw=resp.raw,
        )

    def _regular_cancel_from_response(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
        symbol: str,
        client_order_id: str | None,
        exchange_order_id: str | int | None,
        raw_status: str | None,
        raw: dict[str, Any],
    ) -> ExchangeCancelResult:
        return ExchangeCancelResult(
            exchange=exchange,
            market_type=market_type,
            symbol=symbol.upper(),
            client_order_id=client_order_id,
            exchange_order_id=self._optional_str(exchange_order_id),
            status=self._map_order_status(raw_status) or OrderStatus.CANCELLED,
            raw_status=self._optional_str(raw_status),
            raw=raw,
        )

    def _conditional_cancel_result_from_dto(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
        symbol: str,
        client_conditional_id: str | None,
        dto: CancelAlgoOrderRespDto,
    ) -> ExchangeCancelResult:
        return ExchangeCancelResult(
            exchange=exchange,
            market_type=market_type,
            symbol=symbol.upper(),
            client_conditional_id=dto.clientAlgoId or client_conditional_id,
            exchange_conditional_id=(
                str(dto.algoId) if dto.algoId is not None else None
            ),
            conditional_status=ConditionalStatus.CANCELLED,
            raw_status=(
                dto.raw.get("algoStatus")
                or dto.raw.get("status")
                or BinanceConditionalOrderState.canceled
            ),
            raw=dto.raw,
        )

    def _order_snapshot_from_response(
        self,
        row: dict[str, Any],
        fallback_order: Order | None = None,
    ) -> ExchangeOrderSnapshot:
        raw_status = row.get("status")
        symbol = str(row.get("symbol") or (fallback_order.symbol if fallback_order else ""))
        market_type = fallback_order.market_type if fallback_order else self.market_type

        return ExchangeOrderSnapshot(
            exchange=self.exchange,
            market_type=market_type,
            symbol=symbol.upper(),
            client_order_id=self._optional_str(
                row.get("clientOrderId")
                or (fallback_order.client_order_id if fallback_order else None)
            ),
            exchange_order_id=self._optional_str(row.get("orderId")),
            status=self._map_order_status(raw_status) or OrderStatus.UNKNOWN,
            filled_quantity=str(row.get("executedQty", "0")),
            avg_fill_price=str(row.get("avgPrice", "0")),
            raw_status=self._optional_str(raw_status),
            raw=row,
        )

    def _conditional_snapshot_from_response(
        self,
        row: dict[str, Any],
        fallback_order: Order | None = None,
    ) -> ExchangeConditionalSnapshot:
        raw_status = row.get("algoStatus") or row.get("status")

        symbol = (
            fallback_order.symbol
            if fallback_order is not None
            else str(row.get("symbol") or "")
        )

        exchange = (
            fallback_order.exchange
            if fallback_order is not None
            else self.exchange
        )

        market_type = (
            fallback_order.market_type
            if fallback_order is not None
            else self.market_type
        )

        client_conditional_id = (
            row.get("clientAlgoId")
            or (
                fallback_order.client_conditional_id
                if fallback_order is not None
                else None
            )
        )

        exchange_conditional_id = (
            row.get("algoId")
            or (
                fallback_order.exchange_conditional_id
                if fallback_order is not None
                else None
            )
        )

        return ExchangeConditionalSnapshot(
            exchange=exchange,
            market_type=market_type,
            symbol=str(symbol).upper(),
            client_conditional_id=self._optional_str(client_conditional_id),
            exchange_conditional_id=self._optional_str(exchange_conditional_id),
            conditional_status=(
                self._map_conditional_status(raw_status) or ConditionalStatus.UNKNOWN
            ),
            triggered_order_id=self._optional_str(row.get("triggeredOrderId")),
            triggered_client_order_id=self._optional_str(row.get("triggeredClientOrderId")),
            filled_quantity=self._optional_str(row.get("executedQty")),
            avg_fill_price=self._optional_str(row.get("avgPrice")),
            raw_status=self._optional_str(raw_status),
            raw=row,
        )

    def _position_snapshot_from_response(
        self,
        row: PositionRiskRespDto,
    ) -> ExchangePositionSnapshot:
        return ExchangePositionSnapshot(
            exchange=self.exchange,
            market_type=self.market_type,
            symbol=str(row.symbol).upper(),
            position_side=self._map_position_side(row.positionSide),
            position_amt=str(row.positionAmt),
            entry_price=self._optional_str(row.entryPrice),
            mark_price=self._optional_str(row.markPrice),
            unrealized_pnl=self._optional_str(row.unRealizedProfit),
            leverage=self._optional_int(row.raw.get("leverage")),
            liquidation_price=self._optional_str(row.liquidationPrice),
            break_even_price=self._optional_str(row.breakEvenPrice),
            isolated_margin=self._optional_str(row.isolatedMargin),
            isolated_wallet=self._optional_str(row.isolatedWallet),
            margin_type=self._optional_str(row.raw.get("marginType")),
            notional=self._optional_str(row.notional),
            updated_ts=self._optional_int(row.updateTime),
            raw=row.raw,
        )

    def _map_error(self, e: BinanceApiError) -> ExchangeApiError:
        return ExchangeApiError(
            exchange=self.exchange,
            category=self._error_category(e),
            message=e.msg,
            code=e.code,
            status_code=e.status_code,
            raw={"type": type(e).__name__},
        )

    def _error_category(self, e: BinanceApiError) -> ExchangeErrorCategory:
        if isinstance(e, BinanceUnknownExecutionError):
            return ExchangeErrorCategory.UNKNOWN_EXECUTION
        if isinstance(e, BinanceRateLimitError):
            return ExchangeErrorCategory.RATE_LIMITED
        if isinstance(e, BinanceIpBanError):
            return ExchangeErrorCategory.IP_BANNED
        if isinstance(e, BinanceWafError):
            return ExchangeErrorCategory.WAF_BLOCKED
        if isinstance(e, BinanceServiceUnavailableError):
            return ExchangeErrorCategory.SERVICE_UNAVAILABLE
        if isinstance(e, BinanceInternalRetryableError):
            return ExchangeErrorCategory.INTERNAL_RETRYABLE
        if isinstance(e, BinanceSystemThrottleError):
            return ExchangeErrorCategory.SYSTEM_THROTTLE
        if isinstance(e, BinanceNetworkError):
            return ExchangeErrorCategory.NETWORK

        if e.code in {-2011, -2013}:
            return ExchangeErrorCategory.ORDER_NOT_FOUND

        if e.code in (-2018, -2019):
            return ExchangeErrorCategory.INSUFFICIENT_BALANCE
        if e.code in (-1100, -1102, -1111, -1116, -1121):
            return ExchangeErrorCategory.INVALID_SYMBOL

        return ExchangeErrorCategory.EXCHANGE_REJECTED

    def _reject_reason_from_code(
        self,
        code: int | str | None,
        message: str,
    ) -> RejectReason:
        if code in (-2018, -2019):
            return RejectReason.INSUFFICIENT_BALANCE
        if code == -2010 and "insufficient" in message.lower():
            return RejectReason.INSUFFICIENT_BALANCE
        if code in (-1100, -1102, -1111, -1116, -1121):
            return RejectReason.INVALID_SYMBOL
        return RejectReason.EXCHANGE_REJECTED

    def _map_order_status(self, status: Any) -> Optional[OrderStatus]:
        status_map = _BINANCE_ORDER_STATUS_MAP

        if status is None:
            return None

        return status_map.get(str(status).upper())

    def _map_conditional_status(self, status: Any) -> ConditionalStatus | None:
        status_map = _BINANCE_ALGO_STATUS_MAP

        if status is None:
            return None

        return status_map.get(str(status).upper())

    def _find_matching_algo_row(
        self,
        *,
        rows: list[AlgoOrderRespDto],
        order: Order,
    ) -> AlgoOrderRespDto | None:
        for row in rows:
            client_id = self._optional_str(row.clientAlgoId)
            exchange_id = self._optional_str(row.algoId)

            if order.client_conditional_id and client_id == order.client_conditional_id:
                return row
            if order.exchange_conditional_id and exchange_id == order.exchange_conditional_id:
                return row

        return None

    def _map_position_side(self, value: Any) -> PositionSide:
        if value == PositionSide.LONG.value:
            return PositionSide.LONG
        if value == PositionSide.SHORT.value:
            return PositionSide.SHORT
        return PositionSide.BOTH

    def _optional_str(self, value: Any) -> str | None:
        if value is None:
            return None
        text = str(value)
        return text if text else None

    def _optional_int(self, value: Any) -> int | None:
        if value in (None, ""):
            return None
        return int(value)


    async def _fetch_all_orders_until_client_ids_found(
        self,
        *,
        symbol: str,
        start_time: int,
        end_time: int,
        target_client_ids: set[str],
        limit: int,
    ) -> list[OrderRespDto]:
        rows: list[OrderRespDto] = []
        found: set[str] = set()
        next_order_id: int | None = None

        while True:
            await self.rate_limiter.acquire_request_weight(weight=5)
            order_list:list[OrderRespDto] = await self.adapter.get_all_orders(
                symbol=symbol,
                order_id=next_order_id,
                start_time=start_time,
                end_time=end_time,
                limit=limit,
            )

            if not order_list:
                break

            rows = rows + order_list

            for order in order_list:
                client_order_id = order.clientOrderId

                if client_order_id and client_order_id in target_client_ids:
                    found.add(client_order_id)

            if target_client_ids <= found:
                break

            if len(order_list) < limit:
                break

            last_order_id_raw = order_list[-1].orderId

            if last_order_id_raw is None:
                logger.warning(
                    f"allOrders pagination 중 orderId 누락. 중단: "
                    f"symbol={symbol}, last_row={order_list[-1]}"
                )
                break

            next_order_id = int(last_order_id_raw) + 1

        return rows

    def get_mapper_internal_conditional_order_status(
        self,
        exchange_conditional_status: str,
    ) -> ConditionalStatus | None:
        return _BINANCE_ALGO_STATUS_MAP.get(exchange_conditional_status)

    def get_mapper_internal_order_status(
        self,
        exchange_order_status: str,
    ) -> OrderStatus:
        return _BINANCE_ORDER_STATUS_MAP.get(exchange_order_status)


    def get_mapper_exchange_conditional_order_status(
        self,
        internal_conditional_status: ConditionalStatus,
    ) -> str:
        return _MAPPER_INTERNAL_TO_BINANCE_ALGO_STATUS.get(internal_conditional_status)

    def get_mapper_exchange_order_status(
        self,
        internal_order_status: OrderStatus,
    ) -> str:
        return _MAPPER_INTERNAL_TO_BINANCE_ORDER_STATUS.get(internal_order_status)


    def get_exchange_conditional_order_unknown_status_value(self):
        return BINANCE_EXCHANGE_CONDITIONAL_ORDER_UNKNOWN_STATUS
