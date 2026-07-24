from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True, slots=True)
class PaperTradingCheck:
    name: str
    passed: bool
    evidence: str


@dataclass(frozen=True, slots=True)
class PaperTradingValidationReport:
    venue: str
    checks: list[PaperTradingCheck]

    @property
    def pass_rate(self) -> float:
        if not self.checks:
            return 0.0
        return sum(1 for check in self.checks if check.passed) / len(self.checks)

    @property
    def is_ready(self) -> bool:
        return bool(self.checks) and all(check.passed for check in self.checks)


def build_paper_trading_checklist(
    *,
    signal_to_order: bool = False,
    testnet_submit: bool = False,
    order_state_tracking: bool = False,
    user_data_stream: bool = False,
    position_reconciliation: bool = False,
    stop_order_validation: bool = False,
    risk_limit_validation: bool = False,
    unknown_recovery: bool = False,
    evidence_prefix: str = "pending",
) -> list[PaperTradingCheck]:
    specs = [
        ("Signal to OrderRequest conversion", signal_to_order),
        ("Binance testnet order submit", testnet_submit),
        ("ACK / REJECTED / UNKNOWN state tracking", order_state_tracking),
        ("User data stream fill reflection", user_data_stream),
        ("Position reconciliation", position_reconciliation),
        ("Stop / reduce-only order validation", stop_order_validation),
        ("Pre-trade risk limit validation", risk_limit_validation),
        ("UNKNOWN order recovery without duplicate submit", unknown_recovery),
    ]
    return [
        PaperTradingCheck(
            name=name,
            passed=passed,
            evidence=f"{evidence_prefix}: {name}",
        )
        for name, passed in specs
    ]


def summarize_checks(checks: Sequence[PaperTradingCheck]) -> dict[str, float]:
    total = len(checks)
    passed = sum(1 for check in checks if check.passed)
    return {
        "total": float(total),
        "passed": float(passed),
        "failed": float(total - passed),
        "pass_rate": passed / total if total else 0.0,
    }
