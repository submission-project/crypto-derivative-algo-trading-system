from __future__ import annotations

import sys

from ._compat import warn_deprecated

warn_deprecated()

if "binance_perp_trade.config" in sys.modules and "binance_perp_collector.config" not in sys.modules:
    sys.modules["binance_perp_collector.config"] = sys.modules["binance_perp_trade.config"]

import binance_perp_collector.main as _main

AggTradeStream = _main.AggTradeStream
FallbackController = _main.FallbackController
GapDetector = _main.GapDetector
GapFillFetcher = _main.GapFillFetcher
GapFillSource = _main.GapFillSource
HealthMonitor = _main.HealthMonitor
KafkaConsumer = _main.KafkaConsumer
KafkaProducer = _main.KafkaProducer
REPAIR_WORKER_GROUP_ID = _main.REPAIR_WORKER_GROUP_ID
RepairJob = _main.RepairJob
RestApiError = _main.RestApiError
RestAuthError = _main.RestAuthError
RestTradeClient = _main.RestTradeClient
TradeSource = _main.TradeSource
TradeStream = _main.TradeStream
logger = _main.logger
normalize_rest_trade = _main.normalize_rest_trade
settings = _main.settings


def _sync_patched_globals() -> None:
    for name in (
        "AggTradeStream",
        "FallbackController",
        "GapDetector",
        "GapFillFetcher",
        "GapFillSource",
        "HealthMonitor",
        "KafkaConsumer",
        "KafkaProducer",
        "REPAIR_WORKER_GROUP_ID",
        "RepairJob",
        "RestApiError",
        "RestAuthError",
        "RestTradeClient",
        "TradeSource",
        "TradeStream",
        "logger",
        "normalize_rest_trade",
        "settings",
    ):
        setattr(_main, name, globals()[name])


def _log_repair_outcome(job, result, restored_count: int, expected_count: int):
    _sync_patched_globals()
    return _main._log_repair_outcome(job, result, restored_count, expected_count)


async def main():
    _sync_patched_globals()
    return await _main.main()


def run() -> None:
    _sync_patched_globals()
    return _main.run()


if __name__ == "__main__":
    run()
