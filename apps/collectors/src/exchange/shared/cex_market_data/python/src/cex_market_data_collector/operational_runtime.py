from __future__ import annotations

import asyncio
import gzip
import logging
from contextlib import suppress
from typing import Any

import orjson
import websockets

from .http import HttpJsonClient
from .operational_models import OperationalSpecs, RestPollSpec, WebSocketSpec
from .sinks import EventSink
from .trade_repair import TradeRepairState


logger = logging.getLogger(__name__)


def _loads_message(message: str | bytes, *, gzip_binary: bool) -> Any:
    if isinstance(message, bytes):
        if gzip_binary:
            message = gzip.decompress(message)
        return orjson.loads(message)
    return orjson.loads(message)


def _event_key(event: dict[str, Any]) -> str:
    return str(event.get("symbol") or event.get("exchange") or "market")


async def run_operational_specs(
    specs: list[OperationalSpecs],
    *,
    sink: EventSink,
    timeout_s: float = 10.0,
) -> None:
    async with HttpJsonClient(timeout_s=timeout_s) as client:
        tasks: list[asyncio.Task] = []
        for spec in specs:
            tasks.extend(
                asyncio.create_task(_run_websocket_stream(ws_spec, sink, client))
                for ws_spec in spec.websocket_specs
            )
            tasks.extend(
                asyncio.create_task(_run_rest_poll(poll_spec, client, sink))
                for poll_spec in spec.rest_poll_specs
            )
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await sink.close()


async def _run_websocket_stream(
    spec: WebSocketSpec,
    sink: EventSink,
    client: HttpJsonClient,
) -> None:
    repair_state = TradeRepairState()
    while True:
        try:
            async with websockets.connect(spec.url, ping_interval=20, ping_timeout=20) as ws:
                for message in spec.subscribe_messages:
                    await ws.send(orjson.dumps(message).decode())

                async for raw_message in ws:
                    packet = _loads_message(raw_message, gzip_binary=spec.gzip_binary)
                    if _is_ping(packet):
                        await ws.send(orjson.dumps(_pong(packet, spec)).decode())
                        continue
                    if _is_subscription_ack(packet):
                        continue
                    if spec.normalizer is None:
                        continue
                    for event in spec.normalizer(packet):
                        gap = repair_state.observe(event)
                        if gap is not None and spec.trade_repair is not None:
                            try:
                                repaired = await spec.trade_repair.fetch_repair_trades(client, gap)
                                for repaired_event in repaired:
                                    await sink.emit(spec.topic, _event_key(repaired_event), repaired_event)
                            except Exception as exc:
                                logger.warning(
                                    "trade repair failed exchange=%s symbol=%s gap=%s: %s",
                                    spec.exchange,
                                    gap.symbol,
                                    gap,
                                    exc,
                                )
                        await sink.emit(spec.topic, _event_key(event), event)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            repair_state.mark_stream_interrupted()
            logger.warning("websocket stream error exchange=%s data_type=%s: %s", spec.exchange, spec.data_type, exc)
            await asyncio.sleep(3)


async def _run_rest_poll(
    spec: RestPollSpec,
    client: HttpJsonClient,
    sink: EventSink,
) -> None:
    while True:
        try:
            events = await spec.poller(client)
            for event in events:
                await sink.emit(spec.topic, _event_key(event), event)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        await asyncio.sleep(spec.interval_s)


def _is_subscription_ack(packet: Any) -> bool:
    if not isinstance(packet, dict):
        return False
    if packet.get("event") in {"subscribe", "subscribed"}:
        return True
    if packet.get("op") in {"subscribe", "sub"} and packet.get("success") is not False:
        return True
    if packet.get("ret_msg") in {"subscribe", "success"}:
        return True
    return False


def _is_ping(packet: Any) -> bool:
    return isinstance(packet, dict) and ("ping" in packet or packet.get("op") == "ping")


def _pong(packet: dict[str, Any], spec: WebSocketSpec) -> dict[str, Any]:
    if spec.ping_message is not None:
        return dict(spec.ping_message)
    if "ping" in packet:
        return {"pong": packet["ping"]}
    return {"op": "pong"}
