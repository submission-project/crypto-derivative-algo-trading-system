from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api_server.routes import strategies


from typing import Any

class FakeStrategyControlService:
    def __init__(self) -> None:
        self.enabled = True
        self.risk_config = {
            "strategy_name": "btc_price_oi_box_v1",
            "account_equity": 10000000.0,
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
            "created_ts": 12345,
            "updated_ts": 12345,
        }

    async def get_status(self, strategy_name: str) -> dict[str, object]:
        return {"strategy_name": strategy_name, "enabled": self.enabled}

    async def set_enabled(self, strategy_name: str, enabled: bool) -> dict[str, object]:
        self.enabled = enabled
        return {"strategy_name": strategy_name, "enabled": enabled}

    async def get_risk_config(self, strategy_name: str) -> dict[str, Any] | None:
        if strategy_name != self.risk_config["strategy_name"]:
            return None
        return self.risk_config

    async def update_risk_config(self, strategy_name: str, config_data: dict[str, Any]) -> dict[str, Any]:
        self.risk_config.update(config_data)
        return self.risk_config


def test_strategy_status_routes_toggle_enabled_flag() -> None:
    app = FastAPI()
    service = FakeStrategyControlService()
    app.dependency_overrides[strategies.get_strategy_control_service] = lambda: service
    app.include_router(strategies.router)
    client = TestClient(app)

    resp = client.get("/api/strategies/btc_price_oi_box_v1/status")
    assert resp.status_code == 200
    assert resp.json() == {"strategy_name": "btc_price_oi_box_v1", "enabled": True}

    resp = client.put("/api/strategies/btc_price_oi_box_v1/status", json={"enabled": False})
    assert resp.status_code == 200
    assert resp.json() == {"strategy_name": "btc_price_oi_box_v1", "enabled": False}

    resp = client.get("/api/strategies/btc_price_oi_box_v1/status")
    assert resp.json() == {"strategy_name": "btc_price_oi_box_v1", "enabled": False}


def test_strategy_risk_config_routes() -> None:
    app = FastAPI()
    service = FakeStrategyControlService()
    app.dependency_overrides[strategies.get_strategy_control_service] = lambda: service
    app.include_router(strategies.router)
    client = TestClient(app)

    resp = client.get("/api/strategies/btc_price_oi_box_v1/risk-config")
    assert resp.status_code == 200
    assert resp.json()["account_equity"] == 10000000.0

    resp = client.put("/api/strategies/btc_price_oi_box_v1/risk-config", json={"account_equity": 5000000.0})
    assert resp.status_code == 200
    assert resp.json()["account_equity"] == 5000000.0

    resp = client.get("/api/strategies/btc_price_oi_box_v1/risk-config")
    assert resp.json()["account_equity"] == 5000000.0

    resp = client.get("/api/strategies/unknown_strat/risk-config")
    assert resp.status_code == 404
