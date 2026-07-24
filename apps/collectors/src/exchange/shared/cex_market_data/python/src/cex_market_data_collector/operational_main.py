from __future__ import annotations

import argparse
import asyncio
import os

from .operational_adapters import (
    DEFAULT_OPERATIONAL_EXCHANGES,
    build_operational_specs,
)
from .operational_runtime import run_operational_specs
from .sinks import RedpandaSink, StdoutSink


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run operational cross-CEX trade/orderbook/open-interest collectors."
    )
    parser.add_argument(
        "--exchanges",
        default=",".join(DEFAULT_OPERATIONAL_EXCHANGES),
        help="Comma-separated exchange names.",
    )
    parser.add_argument("--oi-interval-s", type=float, default=60.0)
    parser.add_argument(
        "--rest-oi-fallback",
        action="store_true",
        help="Also run REST open-interest polling for exchanges that already expose OI over WebSocket.",
    )
    parser.add_argument("--timeout-s", type=float, default=10.0)
    parser.add_argument(
        "--sink",
        choices=("stdout", "redpanda"),
        default="stdout",
        help="Output sink. stdout is useful for local validation.",
    )
    parser.add_argument(
        "--redpanda-brokers",
        default=os.environ.get("REDPANDA_BROKERS", "localhost:9092"),
    )
    return parser.parse_args()


async def _amain() -> int:
    args = _parse_args()
    exchanges = tuple(
        exchange.strip().lower()
        for exchange in args.exchanges.split(",")
        if exchange.strip()
    )
    specs = build_operational_specs(
        exchanges,
        oi_interval_s=args.oi_interval_s,
        rest_oi_fallback=args.rest_oi_fallback,
    )
    sink = (
        RedpandaSink(args.redpanda_brokers)
        if args.sink == "redpanda"
        else StdoutSink()
    )
    await run_operational_specs(specs, sink=sink, timeout_s=args.timeout_s)
    return 0


def run() -> None:
    raise SystemExit(asyncio.run(_amain()))


if __name__ == "__main__":
    run()
