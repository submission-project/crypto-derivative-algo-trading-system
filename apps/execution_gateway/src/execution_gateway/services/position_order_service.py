from __future__ import annotations

from typing import Optional

from decimal import Decimal

from common.logging import setup_logger
from execution_gateway.gateway import ExecutionGateway
from execution_gateway.services.position_state_service import PositionStateService
from schemas.market import Exchange, MarketType
from schemas.order import (
    Order,
    OrderRequest,
    OrderSide,
    OrderSource,
    OrderType,
    OrderRoute,
    PositionAction,
    TimeInForce
)
from schemas.position import Position, PositionSide, PositionStatus, make_position_id

logger = setup_logger(__name__)

# FLAT
# 포지션 row는 존재하지만, 실제 열린 포지션 수량은 0인 상태


class PositionOrderError(RuntimeError):
    pass


class PositionCloseError(PositionOrderError):
    pass


class PositionOpenError(PositionOrderError):
    pass


class PositionFlipError(PositionOrderError):
    pass


def _to_decimal(value: str | int | float | Decimal) -> Decimal:
    return Decimal(str(value))


def _decimal_abs_str(value: str | int | float | Decimal) -> str:
    return format(abs(_to_decimal(value)), "f")


def _validate_positive_quantity(
    value: str | int | float | Decimal,
    *,
    field_name: str,
) -> Decimal:
    qty = _to_decimal(value)

    if qty <= 0:
        raise PositionOrderError(f"{field_name} must be positive")

    return qty

# [claim] 꼭 써야 되는 가?
def _validate_perp_or_futures(market_type: MarketType) -> None:
    if market_type not in {MarketType.PERP, MarketType.FUTURES}:
        raise PositionOrderError(
            f"PositionOrderService supports only PERP/FUTURES, "
            f"got={market_type.value}"
        )



def _opposite_side_for_position(position: Position) -> OrderSide:
    """
    현재 포지션을 줄이거나 닫기 위한 반대 방향 주문 side 계산.

    Hedge:
      LONG  -> SELL
      SHORT -> BUY

    One-way BOTH:
      position_amt > 0 -> SELL
      position_amt < 0 -> BUY
    """
    amt = _to_decimal(position.position_amt)

    if position.position_side == PositionSide.LONG:
        return OrderSide.SELL

    if position.position_side == PositionSide.SHORT:
        return OrderSide.BUY

    # One-way BOTH
    if position.position_side == PositionSide.BOTH:
        if amt > 0:
            return OrderSide.SELL

        if amt < 0:
            return OrderSide.BUY

    raise PositionCloseError(
        f"flat or unsupported position has no opposite side: "
        f"position_id={position.position_id}, "
        f"position_side={position.position_side.value}, "
        f"position_amt={position.position_amt}"
    )

def _same_direction_side_for_position(position: Position) -> OrderSide:
    """
    현재 포지션 방향으로 수량을 늘리기 위한 주문 side 계산.

    Hedge:
      LONG  -> BUY
      SHORT -> SELL

    One-way BOTH:
      position_amt > 0 -> BUY
      position_amt < 0 -> SELL
    """
    amt = _to_decimal(position.position_amt)

    if position.position_side == PositionSide.LONG:
        return OrderSide.BUY

    if position.position_side == PositionSide.SHORT:
        return OrderSide.SELL

    if position.position_side == PositionSide.BOTH:
        if amt > 0:
            return OrderSide.BUY

        if amt < 0:
            return OrderSide.SELL

    raise PositionOrderError(
        f"flat or unsupported position has no same-direction side: "
        f"position_id={position.position_id}, "
        f"position_side={position.position_side.value}, "
        f"position_amt={position.position_amt}"
    )

def _reduce_only_for_position(position: Position) -> bool:
    """
    Binance Hedge Mode에서는 reduceOnly를 보내면 안 되는 경우가 있다.

    정책:
      - One-way BOTH: reduce_only=True
      - Hedge LONG/SHORT: reduce_only=False
    """
    return position.position_side == PositionSide.BOTH

def _validate_open_side(
    *,
    position_side: PositionSide,
    side: OrderSide,
) -> None:
    """
    신규 OPEN 주문의 side와 position_side 정합성 검증.

    One-way BOTH:
      BUY/SELL 모두 허용

    Hedge:
      LONG  -> BUY만 허용
      SHORT -> SELL만 허용
    """
    if position_side == PositionSide.BOTH:
        return

    if position_side == PositionSide.LONG and side != OrderSide.BUY:
        raise PositionOpenError(
            f"LONG hedge position can only be opened with BUY, "
            f"requested_side={side.value}"
        )

    if position_side == PositionSide.SHORT and side != OrderSide.SELL:
        raise PositionOpenError(
            f"SHORT hedge position can only be opened with SELL, "
            f"requested_side={side.value}"
        )


class PositionOrderService:
    """
    현재 position 상태를 기준으로 포지션 의도 주문을 생성하는 서비스.

    원칙:
      - 반드시 ExecutionGateway.submit_order()를 사용한다.
      - PositionOrderService는 OrderRequest 생성 계층이다.
      - 실제 체결/포지션 결과는 fills / ACCOUNT_UPDATE / PositionStateService로 판단한다.
    """

    def __init__(
        self,
        *,
        position_state_service: PositionStateService,
        gateway: ExecutionGateway,
    ) -> None:
        self.position_state_service = position_state_service
        self.gateway = gateway

    # 포지션 종료 시장가 주문 [일반 주문]
    async def close_position_market(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
        symbol: str,
        position_side: PositionSide,
        source: OrderSource,
        signal_id: Optional[str] = None,
        strategy_name: Optional[str] = None,
    ) -> Order:
        """
        현재 열려 있는 position 전체를 MARKET 주문으로 닫는다.

        One-way:
          LONG  -> SELL reduceOnly
          SHORT -> BUY reduceOnly

        Hedge:
          LONG  -> SELL positionSide=LONG
          SHORT -> BUY positionSide=SHORT
          reduceOnly는 보내지 않음
        """
        position = await self._load_position(
            exchange=exchange,
            market_type=market_type,
            symbol=symbol,
            position_side=position_side,
        )

        self._assert_position_open(position)

        quantity = _decimal_abs_str(position.position_amt)
        side = _opposite_side_for_position(position)
        reduce_only = _reduce_only_for_position(position)

        req = OrderRequest(
            exchange=exchange,
            market_type=market_type,
            symbol=position.symbol,
            side=side,
            order_type=OrderType.MARKET,
            order_route=OrderRoute.REGULAR,
            quantity=quantity,
            price=None,
            trigger_price=None,
            reduce_only=reduce_only,
            close_position=False,
            position_side=position.position_side,
            position_action=PositionAction.CLOSE,
        )

        logger.warning(
            f"포지션 MARKET close 주문 생성: "
            f"position_id={position.position_id}, "
            f"symbol={position.symbol}, "
            f"position_side={position.position_side.value}, "
            f"amt={position.position_amt}, "
            f"close_side={side.value}, "
            f"quantity={quantity}, "
            f"reduce_only={reduce_only}"
        )

        return await self.gateway.submit_order(
            req=req,
            source=source,
            signal_id=signal_id,
            strategy_name=strategy_name,
        )

    # 포지션 종료 지정가 주문 [일반 주문]
    async def close_position_limit(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
        symbol: str,
        price: str,
        position_side: PositionSide,
        time_in_force: TimeInForce,
        source: OrderSource,
        signal_id: Optional[str] = None,
        strategy_name: Optional[str] = None,
    ) -> Order:
        """
        현재 열린 position 전체를 LIMIT 주문으로 닫는다.

        주의:
        - LIMIT close는 미체결 위험이 있다.
        - 급한 청산이면 close_position_market 또는 STOP_MARKET 계열을 사용한다.
        """
        position = await self._load_position(
            exchange=exchange,
            market_type=market_type,
            symbol=symbol,
            position_side=position_side,
        )

        self._assert_position_open(position)

        quantity = _decimal_abs_str(position.position_amt)
        side = _opposite_side_for_position(position)
        reduce_only = _reduce_only_for_position(position)

        req = OrderRequest(
            exchange=exchange,
            market_type=market_type,
            symbol=position.symbol,
            side=side,
            order_type=OrderType.LIMIT,
            order_route=OrderRoute.REGULAR,
            time_in_force=time_in_force,
            quantity=quantity,
            price=price,
            trigger_price=None,
            reduce_only=reduce_only,
            close_position=False,
            position_side=position.position_side,
            position_action=PositionAction.CLOSE,
        )

        logger.info(
            f"포지션 LIMIT close 주문 생성: "
            f"position_id={position.position_id}, "
            f"symbol={position.symbol}, "
            f"side={side.value}, "
            f"quantity={quantity}, "
            f"price={price}, "
            f"reduce_only={reduce_only}"
        )

        return await self.gateway.submit_order(
            req=req,
            source=source,
            signal_id=signal_id,
            strategy_name=strategy_name,
        )

    # [claim] use_close_position 의 경우, 이걸 참고하는 메소드는 명시해야 될듯
    # 포지션 종료 조건부 시장가 주문 [조건부 주문]
    async def close_position_stop_market(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
        symbol: str,
        trigger_price: str,
        position_side: PositionSide,
        source: OrderSource,
        use_close_position: bool = True,
        signal_id: Optional[str] = None,
        strategy_name: Optional[str] = None,
    ) -> Order:
        """
        현재 열린 position 전체를 STOP_MARKET 조건부 주문으로 닫는다.

        use_close_position=True:
          - 내부 quantity='0'
          - close_position=True
          - reduce_only=False
          - Binance 전송 시 Router가 quantity를 보내지 않아야 한다.

        use_close_position=False:
          - 현재 position 수량을 quantity로 명시
          - one-way BOTH일 때 reduce_only=True
        """
        position = await self._load_position(
            exchange=exchange,
            market_type=market_type,
            symbol=symbol,
            position_side=position_side,
        )

        self._assert_position_open(position)

        if use_close_position:
            quantity = "0"
            reduce_only = False
            close_position = True
        else:
            quantity = _decimal_abs_str(position.position_amt)
            reduce_only = _reduce_only_for_position(position)
            close_position = False

        side = _opposite_side_for_position(position)

        req = OrderRequest(
            exchange=exchange,
            market_type=market_type,
            symbol=position.symbol,
            side=side,
            order_type=OrderType.STOP_MARKET,
            order_route=OrderRoute.CONDITIONAL,
            time_in_force=None,
            quantity=quantity,
            price=None,
            trigger_price=trigger_price,
            reduce_only=reduce_only,
            close_position=close_position,
            position_side=position.position_side,
            position_action=PositionAction.CLOSE,
        )

        logger.warning(
            f"포지션 STOP_MARKET close 주문 생성: "
            f"position_id={position.position_id}, "
            f"symbol={position.symbol}, "
            f"side={side.value}, "
            f"quantity={quantity}, "
            f"trigger_price={trigger_price}, "
            f"reduce_only={reduce_only}"
        )

        return await self.gateway.submit_order(
            req=req,
            source=source,
            signal_id=signal_id,
            strategy_name=strategy_name,
        )

    # 포지션 종료 조건부 지정가 주문 [조건부 주문]
    async def close_position_stop_limit(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
        symbol: str,
        trigger_price: str,
        price: str,
        position_side: PositionSide,
        time_in_force: TimeInForce,
        source: OrderSource,
        signal_id: Optional[str] = None,
        strategy_name: Optional[str] = None,
    ) -> Order:
        """
        현재 열린 position 전체를 STOP_LIMIT 조건부 주문으로 닫는다.

        trigger_price 도달 후 price 지정가 주문 생성.
        """
        position = await self._load_position(
            exchange=exchange,
            market_type=market_type,
            symbol=symbol,
            position_side=position_side,
        )

        self._assert_position_open(position)

        quantity = _decimal_abs_str(position.position_amt)
        side = _opposite_side_for_position(position)
        reduce_only = _reduce_only_for_position(position)

        req = OrderRequest(
            exchange=exchange,
            market_type=market_type,
            symbol=position.symbol,
            side=side,
            order_type=OrderType.STOP_LIMIT,
            order_route=OrderRoute.CONDITIONAL,
            time_in_force=time_in_force,
            quantity=quantity,
            price=price,
            trigger_price=trigger_price,
            reduce_only=reduce_only,
            close_position=False,
            position_side=position.position_side,
            position_action=PositionAction.CLOSE,
        )

        logger.warning(
            f"포지션 STOP_LIMIT close 주문 생성: "
            f"position_id={position.position_id}, "
            f"symbol={position.symbol}, "
            f"side={side.value}, "
            f"quantity={quantity}, "
            f"trigger_price={trigger_price}, "
            f"price={price}, "
            f"reduce_only={reduce_only}"
        )

        return await self.gateway.submit_order(
            req=req,
            source=source,
            signal_id=signal_id,
            strategy_name=strategy_name,
        )

    # 포지션 줄이기 시장가 주문 [일반 주문]
    async def reduce_position_market(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
        symbol: str,
        quantity: str,
        position_side: PositionSide,
        source: OrderSource,
        signal_id: Optional[str] = None,
        strategy_name: Optional[str] = None,
    ) -> Order:
        """
        현재 열려 있는 position 일부를 MARKET 주문으로 줄인다.

        quantity는 position abs amt보다 클 수 없다.
        """
        position = await self._load_position(
            exchange=exchange,
            market_type=market_type,
            symbol=symbol,
            position_side=position_side,
        )

        self._assert_position_open(position)

        current_abs = abs(_to_decimal(position.position_amt))
        reduce_qty = _validate_positive_quantity(
            quantity,
            field_name="reduce quantity",
        )

        if reduce_qty > current_abs:
            raise PositionCloseError(
                f"reduce quantity exceeds position size: "
                f"position_id={position.position_id}, "
                f"current_abs={current_abs}, reduce_qty={reduce_qty}"
            )

        side = _opposite_side_for_position(position)
        reduce_only = _reduce_only_for_position(position)

        req = OrderRequest(
            exchange=exchange,
            market_type=market_type,
            symbol=position.symbol,
            side=side,
            order_type=OrderType.MARKET,
            order_route=OrderRoute.REGULAR,
            quantity=format(reduce_qty, "f"),
            price=None,
            trigger_price=None,
            reduce_only=reduce_only,
            close_position=False,
            position_side=position.position_side,
            position_action=PositionAction.REDUCE,
        )

        logger.info(
            f"포지션 MARKET reduce 주문 생성: "
            f"position_id={position.position_id}, "
            f"symbol={position.symbol}, "
            f"position_side={position.position_side.value}, "
            f"current_amt={position.position_amt}, "
            f"reduce_qty={quantity}, "
            f"side={side.value}, "
            f"reduce_only={reduce_only}"
        )

        return await self.gateway.submit_order(
            req=req,
            source=source,
            signal_id=signal_id,
            strategy_name=strategy_name,
        )

    # 포지션 줄이기 지정가 주문 [일반 주문]
    async def reduce_position_limit(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
        symbol: str,
        quantity: str,
        price: str,
        position_side: PositionSide,
        time_in_force: TimeInForce,
        source: OrderSource,
        signal_id: Optional[str] = None,
        strategy_name: Optional[str] = None,
    ) -> Order:
        """
        현재 열려 있는 position 일부를 MARKET 주문으로 줄인다.

        quantity는 abs(position_amt)보다 클 수 없다.
        """
        position = await self._load_position(
            exchange=exchange,
            market_type=market_type,
            symbol=symbol,
            position_side=position_side,
        )

        self._assert_position_open(position)

        current_abs = abs(_to_decimal(position.position_amt))
        reduce_qty = _validate_positive_quantity(
            quantity,
            field_name="reduce quantity",
        )

        if reduce_qty > current_abs:
            raise PositionCloseError(
                f"reduce quantity exceeds position size: "
                f"position_id={position.position_id}, "
                f"current_abs={current_abs}, reduce_qty={reduce_qty}"
            )

        side = _opposite_side_for_position(position)
        reduce_only = _reduce_only_for_position(position)

        req = OrderRequest(
            exchange=exchange,
            market_type=market_type,
            symbol=position.symbol,
            side=side,
            order_type=OrderType.LIMIT,
            order_route=OrderRoute.REGULAR,
            time_in_force=time_in_force,
            quantity=format(reduce_qty, "f"),
            price=price,
            trigger_price=None,
            reduce_only=reduce_only,
            close_position=False,
            position_side=position.position_side,
            position_action=PositionAction.REDUCE,
        )

        logger.info(
            f"포지션 LIMIT reduce 주문 생성: "
            f"position_id={position.position_id}, "
            f"symbol={position.symbol}, "
            f"side={side.value}, "
            f"quantity={quantity}, "
            f"price={price}, "
            f"reduce_only={reduce_only}"
        )

        return await self.gateway.submit_order(
            req=req,
            source=source,
            signal_id=signal_id,
            strategy_name=strategy_name,
        )

    # 포지션 줄이기 조건부 시장가 주문 [조건부 주문]
    async def reduce_position_stop_market(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
        symbol: str,
        quantity: str,
        trigger_price: str,
        position_side: PositionSide,
        source: OrderSource,
        signal_id: Optional[str] = None,
        strategy_name: Optional[str] = None,
    ) -> Order:
        """
        현재 열린 position 일부를 STOP_MARKET 조건부 주문으로 줄인다.
        """
        position = await self._load_position(
            exchange=exchange,
            market_type=market_type,
            symbol=symbol,
            position_side=position_side,
        )

        self._assert_position_open(position)

        current_abs = abs(_to_decimal(position.position_amt))
        reduce_qty = _validate_positive_quantity(
            quantity,
            field_name="stop market reduce quantity",
        )

        if reduce_qty > current_abs:
            raise PositionCloseError(
                f"reduce quantity exceeds position size: "
                f"position_id={position.position_id}, "
                f"current_abs={current_abs}, reduce_qty={reduce_qty}"
            )

        side = _opposite_side_for_position(position)
        reduce_only = _reduce_only_for_position(position)

        req = OrderRequest(
            exchange=exchange,
            market_type=market_type,
            symbol=position.symbol,
            side=side,
            order_type=OrderType.STOP_MARKET,
            order_route=OrderRoute.CONDITIONAL,
            time_in_force=None,
            quantity=format(reduce_qty, "f"),
            price=None,
            trigger_price=trigger_price,
            reduce_only=reduce_only,
            close_position=False,
            position_side=position.position_side,
            position_action=PositionAction.REDUCE,
        )

        logger.warning(
            f"포지션 STOP_MARKET reduce 주문 생성: "
            f"position_id={position.position_id}, "
            f"symbol={position.symbol}, "
            f"side={side.value}, "
            f"quantity={quantity}, "
            f"trigger_price={trigger_price}, "
            f"reduce_only={reduce_only}"
        )

        return await self.gateway.submit_order(
            req=req,
            source=source,
            signal_id=signal_id,
            strategy_name=strategy_name,
        )

    async def reduce_position_stop_limit(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
        symbol: str,
        quantity: str,
        trigger_price: str,
        price: str,
        position_side: PositionSide,
        time_in_force: TimeInForce,
        source: OrderSource,
        signal_id: Optional[str] = None,
        strategy_name: Optional[str] = None,
    ) -> Order:
        """
        현재 열린 position 일부를 STOP_LIMIT 조건부 주문으로 줄인다.
        """
        position = await self._load_position(
            exchange=exchange,
            market_type=market_type,
            symbol=symbol,
            position_side=position_side,
        )

        self._assert_position_open(position)

        current_abs = abs(_to_decimal(position.position_amt))
        reduce_qty = _validate_positive_quantity(
            quantity,
            field_name="stop limit reduce quantity",
        )

        if reduce_qty > current_abs:
            raise PositionCloseError(
                f"reduce quantity exceeds position size: "
                f"position_id={position.position_id}, "
                f"current_abs={current_abs}, reduce_qty={reduce_qty}"
            )

        side = _opposite_side_for_position(position)
        reduce_only = _reduce_only_for_position(position)

        req = OrderRequest(
            exchange=exchange,
            market_type=market_type,
            symbol=position.symbol,
            side=side,
            order_type=OrderType.STOP_LIMIT,
            order_route=OrderRoute.CONDITIONAL,
            time_in_force=time_in_force,
            quantity=format(reduce_qty, "f"),
            price=price,
            trigger_price=trigger_price,
            reduce_only=reduce_only,
            close_position=False,
            position_side=position.position_side,
            position_action=PositionAction.REDUCE,
        )

        logger.warning(
            f"포지션 STOP_LIMIT reduce 주문 생성: "
            f"position_id={position.position_id}, "
            f"symbol={position.symbol}, "
            f"side={side.value}, "
            f"quantity={quantity}, "
            f"trigger_price={trigger_price}, "
            f"price={price}, "
            f"reduce_only={reduce_only}"
        )

        return await self.gateway.submit_order(
            req=req,
            source=source,
            signal_id=signal_id,
            strategy_name=strategy_name,
        )

    # 시장가 주문 - open [일반 주문]
    async def open_position_market(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
        symbol: str,
        side: OrderSide,
        quantity: str,
        position_side: PositionSide,
        source: OrderSource,
        signal_id: Optional[str] = None,
        strategy_name: Optional[str] = None,
    ) -> Order:
        """
        신규 포지션 진입 MARKET 주문.

        One-way:
          LONG 진입  -> BUY positionSide=BOTH
          SHORT 진입 -> SELL positionSide=BOTH

        Hedge:
          LONG 진입  -> BUY positionSide=LONG
          SHORT 진입 -> SELL positionSide=SHORT
        """

        _validate_perp_or_futures(market_type)
        _validate_open_side(position_side=position_side, side=side)

        open_qty = _validate_positive_quantity(
            quantity,
            field_name="open quantity",
        )

        req = OrderRequest(
            exchange=exchange,
            market_type=market_type,
            symbol=symbol.upper(),
            side=side,
            order_type=OrderType.MARKET,
            order_route=OrderRoute.REGULAR,
            quantity=format(open_qty, "f"),
            price=None,
            trigger_price=None,
            reduce_only=False,
            position_side=position_side,
            position_action=PositionAction.OPEN,
        )

        logger.info(
            f"포지션 MARKET open 주문 생성: "
            f"symbol={symbol.upper()}, "
            f"position_side={position_side.value}, "
            f"side={side.value}, "
            f"quantity={quantity}"
        )

        return await self.gateway.submit_order(
            req=req,
            source=source,
            signal_id=signal_id,
            strategy_name=strategy_name,
        )

    # 포지션 추가 시장가 주문, [일반 주문]
    async def increase_position_market(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
        symbol: str,
        side: OrderSide,
        quantity: str,
        position_side: PositionSide,
        source: OrderSource,
        signal_id: Optional[str] = None,
        strategy_name: Optional[str] = None,
    ) -> Order:
        """
        현재 열린 포지션과 같은 방향으로 수량을 늘리는 MARKET 주문.

        One-way:
          LONG 증가  -> BUY
          SHORT 증가 -> SELL

        Hedge:
          LONG 증가  -> BUY positionSide=LONG
          SHORT 증가 -> SELL positionSide=SHORT

        주의:
          - 포지션이 없으면 INCREASE가 아니라 OPEN이므로 실패시킨다.
          - 반대 방향 주문이면 REDUCE/CLOSE/FLIP 영역이므로 실패시킨다.
        """
        increase_qty = _validate_positive_quantity(
            quantity,
            field_name="increase quantity",
        )

        position = await self._load_position(
            exchange=exchange,
            market_type=market_type,
            symbol=symbol,
            position_side=position_side,
        )

        self._assert_position_open(position)

        amt = _to_decimal(position.position_amt)

        self._validate_increase_side(
            position=position,
            requested_side=side,
            amt=amt,
        )

        req = OrderRequest(
            exchange=exchange,
            market_type=market_type,
            symbol=position.symbol,
            side=side,
            order_type=OrderType.MARKET,
            order_route=OrderRoute.REGULAR,
            quantity=format(increase_qty, "f"),
            price=None,
            trigger_price=None,
            reduce_only=False,
            close_position=False,
            position_side=position.position_side,
            position_action=PositionAction.INCREASE,
        )

        logger.info(
            f"포지션 MARKET increase 주문 생성: "
            f"position_id={position.position_id}, "
            f"symbol={position.symbol}, "
            f"position_side={position.position_side.value}, "
            f"current_amt={position.position_amt}, "
            f"increase_side={side.value}, "
            f"quantity={quantity}"
        )

        return await self.gateway.submit_order(
            req=req,
            source=source,
            signal_id=signal_id,
            strategy_name=strategy_name,
        )

    # 포지션 추가 혹은 증가 시장가 주문 [일반 주문]
    async def open_or_increase_position_market(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
        symbol: str,
        side: OrderSide,
        quantity: str,
        position_side: PositionSide,
        source: OrderSource,
        signal_id: Optional[str] = None,
        strategy_name: Optional[str] = None,
    ) -> Order:
        """
        포지션이 없거나 FLAT이면 OPEN,
        같은 방향 포지션이 이미 열려 있으면 INCREASE 주문을 생성한다.

        반대 방향 포지션이 열려 있으면 REDUCE/CLOSE/FLIP 영역이므로 실패시킨다.
        """
        order_qty = _validate_positive_quantity(
            quantity,
            field_name="open/increase quantity",
        )

        position = await self._try_load_position(
            exchange=exchange,
            market_type=market_type,
            symbol=symbol,
            position_side=position_side,
        )

        # 1. 포지션이 없으면 신규 OPEN
        if position is None:
            _validate_open_side(position_side=position_side, side=side)
            position_action = PositionAction.OPEN
            order_symbol = symbol.upper()
            order_position_side = position_side

        else: 
            amt = Decimal(str(position.position_amt))

            # 2. 포지션 row는 있지만 FLAT이면 신규 OPEN
            if position.status != PositionStatus.OPEN or amt == 0:
                _validate_open_side(position_side=position.position_side, side=side)

                position_action = PositionAction.OPEN
                order_symbol = position.symbol
                order_position_side = position.position_side

            # 3. 포지션이 열려 있으면 같은 방향인지 확인 후 INCREASE
            else:
                self._validate_increase_side(
                    position=position,
                    requested_side=side,
                    amt=amt,
                )

                position_action = PositionAction.INCREASE
                order_symbol = position.symbol
                order_position_side = position.position_side

        req = OrderRequest(
            exchange=exchange,
            market_type=market_type,
            symbol=order_symbol,
            side=side,
            order_type=OrderType.MARKET,
            order_route=OrderRoute.REGULAR,
            quantity=format(order_qty, "f"),
            price=None,
            trigger_price=None,
            reduce_only=False,
            close_position=False,
            position_side=order_position_side,
            position_action=position_action,
        )

        logger.info(
            f"포지션 MARKET open_or_increase 주문 생성: "
            f"symbol={order_symbol}, "
            f"position_side={order_position_side.value}, "
            f"side={side.value}, "
            f"quantity={quantity}, "
            f"position_action={position_action.value}, "
            f"existing_position_id={getattr(position, 'position_id', None)}, "
            f"existing_amt={getattr(position, 'position_amt', None)}"
        )

        return await self.gateway.submit_order(
            req=req,
            source=source,
            signal_id=signal_id,
            strategy_name=strategy_name,
        )

    # 시장가 주문 - flip [일반 주문]
    async def flip_position_market(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
        symbol: str,
        target_quantity: str,
        position_side: PositionSide,
        source: OrderSource,
        signal_id: Optional[str] = None,
        strategy_name: Optional[str] = None,
    ) -> Order:
        """
        현재 포지션을 닫고 반대 방향 포지션까지 여는 MARKET 주문.

        One-way 예:
          현재 LONG 0.01
          target_quantity=0.03
          -> SELL 0.03
          -> LONG 0.01 close + SHORT 0.02 open

          현재 SHORT -0.01
          target_quantity=0.03
          -> BUY 0.03
          -> SHORT 0.01 close + LONG 0.02 open

        주의:
          - reduceOnly=False
          - target_quantity는 현재 abs(position_amt)보다 커야 한다.
          - position_side=BOTH 기준으로 먼저 운영하는 것을 추천한다.
        """
        target_qty = _validate_positive_quantity(
            target_quantity,
            field_name="flip target_quantity",
        )

        position = await self._load_position(
            exchange=exchange,
            market_type=market_type,
            symbol=symbol,
            position_side=position_side,
        )

        self._assert_position_open(position)

        if not _reduce_only_for_position(position=position):
            raise PositionFlipError(
                "single-order flip is supported only for one-way position_side=BOTH. "
                "For hedge mode, close one side and open the opposite side separately."
            )

        current_abs = abs(_to_decimal(position.position_amt))

        if current_abs <= 0:
            raise PositionCloseError(
                f"position amount is zero, cannot flip: "
                f"position_id={position.position_id}"
            )
            
        order_qty = current_abs + target_qty
        side = _opposite_side_for_position(position)

        req = OrderRequest(
            exchange=exchange,
            market_type=market_type,
            symbol=position.symbol,
            side=side,
            order_type=OrderType.MARKET,
            order_route=OrderRoute.REGULAR,
            quantity=format(order_qty, "f"),
            price=None,
            trigger_price=None,
            reduce_only=False,
            close_position=False,
            position_side=position.position_side,
            position_action=PositionAction.FLIP,
        )

        logger.warning(
            f"포지션 MARKET flip 주문 생성: "
            f"position_id={position.position_id}, "
            f"symbol={position.symbol}, "
            f"position_side={position.position_side.value}, "
            f"current_amt={position.position_amt}, "
            f"flip_side={side.value}, "
            f"target_quantity={target_quantity}, "
            f"order_qty={order_qty}, "
            f"reduce_only=False"
        )

        return await self.gateway.submit_order(
            req=req,
            source=source,
            signal_id=signal_id,
            strategy_name=strategy_name,
        )

    # --- helpers ---
    def _validate_increase_side(
        self,
        *,
        position: Position,
        requested_side: OrderSide,
        amt: Decimal
    ) -> None:
        """
        INCREASE 주문이 현재 포지션과 같은 방향인지 검증한다.

        One-way BOTH:
          amt > 0  => LONG  => BUY만 증가
          amt < 0  => SHORT => SELL만 증가

        Hedge:
          LONG  => BUY만 증가
          SHORT => SELL만 증가
        """

        if position.position_side == PositionSide.BOTH:
            if amt > 0 and requested_side != OrderSide.BUY:
                raise PositionCloseError(
                    f"LONG one-way position can only be increased with BUY: "
                    f"position_id={position.position_id}, "
                    f"amt={position.position_amt}, "
                    f"requested_side={requested_side.value}"
                )

            if amt < 0 and requested_side != OrderSide.SELL:
                raise PositionCloseError(
                    f"SHORT one-way position can only be increased with SELL: "
                    f"position_id={position.position_id}, "
                    f"amt={position.position_amt}, "
                    f"requested_side={requested_side.value}"
                )

            if amt == 0:
                raise PositionCloseError(
                    f"flat one-way position cannot be increased: "
                    f"position_id={position.position_id}"
                )

            return

        if position.position_side == PositionSide.LONG:
            if requested_side != OrderSide.BUY:
                raise PositionCloseError(
                    f"LONG hedge position can only be increased with BUY: "
                    f"position_id={position.position_id}, "
                    f"requested_side={requested_side.value}"
                )
            return

        if position.position_side == PositionSide.SHORT:
            if requested_side != OrderSide.SELL:
                raise PositionCloseError(
                    f"SHORT hedge position can only be increased with SELL: "
                    f"position_id={position.position_id}, "
                    f"requested_side={requested_side.value}"
                )
            return

        raise PositionCloseError(
            f"unsupported position_side for increase: "
            f"position_id={position.position_id}, "
            f"position_side={position.position_side.value}"
        )

    async def _load_position(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
        symbol: str,
        position_side: PositionSide,
        # is_try_raise: bool = True,
    ) -> Position | None:
        _validate_perp_or_futures(market_type)

        position_id = make_position_id(
            exchange=exchange,
            market_type=market_type,
            symbol=symbol,
            position_side=position_side,
        )

        position = await self.position_state_service.load_position(
            position_id=position_id,
            refresh_projection=True,
        )

        if position is None:
            raise PositionCloseError(f"position not found: {position_id}")

        return position

    async def _try_load_position(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
        symbol: str,
        position_side: PositionSide,
    ) -> Optional[Position]:
        try:
            return await self._load_position(
                exchange=exchange,
                market_type=market_type,
                symbol=symbol,
                position_side=position_side,
                # is_try_raise=False,
            )
        except PositionCloseError:
            return None

    def _assert_position_open(self, position: Position) -> None:
        if position.status != PositionStatus.OPEN:
            raise PositionCloseError(
                f"position is not open: "
                f"position_id={position.position_id}, "
                f"status={position.status.value}, "
                f"amt={position.position_amt}"
            )

        amt = _to_decimal(position.position_amt)

        if amt == 0:
            raise PositionCloseError(
                f"position quantity is zero: "
                f"position_id={position.position_id}"
            )