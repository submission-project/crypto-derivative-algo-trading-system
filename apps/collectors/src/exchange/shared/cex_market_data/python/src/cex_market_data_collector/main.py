from __future__ import annotations

import argparse
import asyncio
import sys

import orjson

from .adapters import DEFAULT_EXCHANGES, supported_exchanges
from .collector import collect_market_snapshots
from .http import HttpJsonClient


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect cross-CEX orderbook and open-interest snapshots."
    )
    parser.add_argument(
        "--exchanges",
        default=",".join(DEFAULT_EXCHANGES),
        help=f"Comma-separated exchanges. Supported: {', '.join(supported_exchanges())}",
    )
    parser.add_argument("--depth", type=int, default=20)
    parser.add_argument("--timeout-s", type=float, default=10.0)
    parser.add_argument(
        "--jsonl",
        action="store_true",
        help="Emit one JSON line per exchange instead of one JSON array.",
    )
    return parser.parse_args()


async def _amain() -> int:
    args = _parse_args()
    exchanges = tuple(
        exchange.strip().lower()
        for exchange in args.exchanges.split(",")
        if exchange.strip()
    )
    async with HttpJsonClient(timeout_s=args.timeout_s) as client:
        snapshots = await collect_market_snapshots(
            exchanges,
            client=client,
            depth=args.depth,
        )

    records = [snapshot.to_record() for snapshot in snapshots]
    if args.jsonl:
        for record in records:
            sys.stdout.buffer.write(orjson.dumps(record))
            sys.stdout.buffer.write(b"\n")
    else:
        sys.stdout.buffer.write(orjson.dumps(records, option=orjson.OPT_INDENT_2))
        sys.stdout.buffer.write(b"\n")
    return 0


def run() -> None:
    raise SystemExit(asyncio.run(_amain()))


if __name__ == "__main__":
    run()
