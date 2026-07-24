from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from execution_gateway.consumers.order_intent_consumer import handle_order_intent
from execution_gateway.handlers.dedup_handler import InMemoryDedupStore, OrderIntentDedupHandler
from execution_gateway.handlers.order_submit_handler import OrderSubmitHandler
from execution_gateway.handlers.risk_handler import PreTradeRiskHandler, RiskConfig, RiskRejectReason
from schemas.market import Exchange, MarketType
from schemas.order import OrderSide, OrderStatus, OrderType, PositionAction
from schemas.position import PositionSide


def _intent(**overrides) -> dict:
    payload = {
        "exchange": Exchange.BINANCE.value,
        "market_type": MarketType.PERP.value,
        "symbol": "BTCUSDT",
        "side": OrderSide.BUY.value,
        "order_type": OrderType.MARKET.value,
        "quantity": "0.001",
        "position_side": PositionSide.LONG.value,
        "position_action": PositionAction.OPEN.value,
        "signal_id": "S-BINANCE-PERP-TEST",
        "strategy_name": "btc_price_oi_box_v1",
        "entry_price": "100000",
        "stop_loss_price": "99000",
        "take_profit_price": "101000",
    }
    payload.update(overrides)
    return payload


class FakeGateway:
    def __init__(self) -> None:
        self.calls = []

    async def submit_order(self, req, source, signal_id=None, strategy_name=None):
        self.calls.append(
            {
                "req": req,
                "source": source,
                "signal_id": signal_id,
                "strategy_name": strategy_name,
            }
        )
        return SimpleNamespace(
            order_id="O-BINANCE-PERP-TEST",
            status=OrderStatus.ACKNOWLEDGED,
            exchange_order_id="12345",
        )


def _risk_handler() -> PreTradeRiskHandler:
    return PreTradeRiskHandler(
        RiskConfig(
            account_equity=Decimal("10000000"),
            risk_per_trade=Decimal("0.002"),
            max_leverage=Decimal("0.7"),
            max_position_notional=Decimal("7000000"),
            min_notional=Decimal("5000"),
            min_stop_bps=Decimal("5"),
            min_reward_risk=Decimal("0.8"),
            quantity_step=Decimal("0.000001"),
            fee_bps=Decimal("4"),
            slippage_bps=Decimal("2"),
            spread_bps=Decimal("1"),
        )
    )


def test_risk_handler_accepts_valid_long_and_recalculates_quantity() -> None:
    decision = _risk_handler().evaluate(_intent(quantity="99"))

    assert decision.accepted is True
    assert decision.order_request is not None
    assert decision.order_request.quantity == "20"
    assert decision.metadata["stop_bps"] == "100"
    assert decision.metadata["reward_risk"] == "1"


def test_risk_handler_rejects_invalid_short_structure() -> None:
    decision = _risk_handler().evaluate(
        _intent(
            side=OrderSide.SELL.value,
            position_side=PositionSide.SHORT.value,
            entry_price="100000",
            stop_loss_price="99000",
            take_profit_price="101000",
        )
    )

    assert decision.accepted is False
    assert decision.reason == RiskRejectReason.INVALID_PRICE_STRUCTURE


def test_risk_handler_rejects_missing_stop_take_profit() -> None:
    intent = _intent()
    intent.pop("stop_loss_price")

    decision = _risk_handler().evaluate(intent)

    assert decision.accepted is False
    assert decision.reason == RiskRejectReason.MISSING_RISK_LEVELS


@pytest.mark.asyncio
async def test_order_submit_handler_runs_dedup_risk_and_gateway_submit() -> None:
    gateway = FakeGateway()
    handler = OrderSubmitHandler(
        # pyrefly: ignore [bad-argument-type]
        gateway=gateway,
        risk_handler=_risk_handler(),
        dedup_handler=OrderIntentDedupHandler(InMemoryDedupStore()),
    )

    result = await handler.process(_intent())

    assert result.accepted is True
    assert result.order
    assert result.risk_metadata
    assert result.order.order_id == "O-BINANCE-PERP-TEST"
    assert result.risk_metadata["quantity"] == "20"
    assert gateway.calls[0]["signal_id"] == "S-BINANCE-PERP-TEST"
    assert gateway.calls[0]["strategy_name"] == "btc_price_oi_box_v1"
    assert gateway.calls[0]["req"].quantity == "20"


@pytest.mark.asyncio
async def test_order_submit_handler_rejects_duplicate_signal_id() -> None:
    gateway = FakeGateway()
    handler = OrderSubmitHandler(
        # pyrefly: ignore [bad-argument-type]
        gateway=gateway,
        risk_handler=_risk_handler(),
        dedup_handler=OrderIntentDedupHandler(InMemoryDedupStore()),
    )

    assert (await handler.process(_intent())).accepted is True
    duplicate = await handler.process(_intent())

    assert duplicate.accepted is False
    assert duplicate.stage == "dedup"
    assert duplicate.reason == "DUPLICATE"
    assert len(gateway.calls) == 1


@pytest.mark.asyncio
async def test_order_intent_consumer_payload_reports_reject_reason() -> None:
    gateway = FakeGateway()
    # pyrefly: ignore [bad-argument-type]
    handler = OrderSubmitHandler(gateway=gateway, risk_handler=_risk_handler())

    payload = await handle_order_intent(
        intent=_intent(stop_loss_price="99990", take_profit_price="100010"),
        handler=handler,
    )

    assert payload["accepted"] is False
    assert payload["stage"] == "risk"
    assert payload["reason"] == RiskRejectReason.STOP_TOO_TIGHT.value


class MockPool:
    def acquire(self):
        class MockConnContext:
            async def __aenter__(self):
                return "conn"
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass
        return MockConnContext()


class MockPostgres:
    def __init__(self):
        self.pool = MockPool()


class MockStrategyRiskConfigRepo:
    def __init__(self, row):
        self.row = row
        self.calls = []

    async def get_by_strategy(self, conn, strategy_name):
        self.calls.append(strategy_name)
        return self.row


@pytest.mark.asyncio
async def test_order_submit_handler_retrieves_risk_config_from_postgres() -> None:
    postgres = MockPostgres()
    row = {
        "strategy_name": "btc_price_oi_box_v1",
        "account_equity": 5000000.0,
        "risk_per_trade": 0.002,
        "max_leverage": 0.7,
        "max_position_notional": 7000000.0,
        "min_notional": 5000.0,
        "min_stop_bps": 5.0,
        "min_reward_risk": 0.8,
        "quantity_step": 0.000001,
        "fee_bps": 4.0,
        "slippage_bps": 2.0,
        "spread_bps": 1.0,
    }
    repo = MockStrategyRiskConfigRepo(row)

    gateway = FakeGateway()
    handler = OrderSubmitHandler(
        gateway=gateway,
        risk_handler=_risk_handler(),
        postgres=postgres,
        strategy_risk_config_repo=repo,
    )

    result = await handler.process(_intent(quantity="99"))

    assert result.accepted is True
    # With 5,000,000 equity (instead of default 10,000,000), recalculated quantity should be 10!
    
    assert result.risk_metadata
    assert result.risk_metadata["quantity"] == "10"
    assert gateway.calls[0]["req"].quantity == "10"
    assert repo.calls == ["btc_price_oi_box_v1"]
