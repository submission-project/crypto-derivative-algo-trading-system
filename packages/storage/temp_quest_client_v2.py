"""
QuestDB Client — QuestDB ILP(Influx Line Protocol)를 통한 통신만 담당하는 순수 인프라 계층.

개선 구조:
    - async write_batch()는 내부 queue에 write request를 넣고 flush 완료를 기다림
    - dedicated writer thread가 persistent Sender를 소유
    - batch 단위로 row() 후 flush()
    - Event Loop를 QuestDB ILP write로 직접 block하지 않음

주의:
    - Sender는 writer thread 내부에서만 사용한다.
    - 외부 async 코드에서는 Sender를 직접 만지지 않는다.
"""

from __future__ import annotations

import asyncio
import queue
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from questdb.ingress import Sender, TimestampNanos

from common.logging import setup_logger

logger = setup_logger(__name__)


@dataclass(frozen=True, slots=True)
class _QuestDBWriteItem:
    table_name: str
    rows: List[Dict[str, Any]]
    loop: asyncio.AbstractEventLoop
    future: asyncio.Future[None]


_STOP = object()


class QuestDBClient:
    """
    QuestDB ILP writer client.

    기존 구조:
      - write_batch마다 Sender 생성/종료

    개선 구조:
      - connect() 시 dedicated writer thread 시작
      - writer thread 내부에서 persistent Sender 유지
      - write_batch()는 queue에 넣고 flush 완료까지 await
    """

    def __init__(
        self,
        host: str,
        ilp_port: int,
        *,
        queue_maxsize: int = 10_000,
        flush_interval_sec: float = 0.05,
        max_buffered_rows: int = 1_000,
        startup_timeout_sec: float = 5.0,
        shutdown_timeout_sec: float = 5.0,
    ) -> None:
        self.conf = f"tcp::addr={host}:{ilp_port};"

        self.queue_maxsize = queue_maxsize
        self.flush_interval_sec = flush_interval_sec
        self.max_buffered_rows = max_buffered_rows
        self.startup_timeout_sec = startup_timeout_sec
        self.shutdown_timeout_sec = shutdown_timeout_sec

        self._queue: queue.Queue[_QuestDBWriteItem | object] | None = None
        self._thread: threading.Thread | None = None
        self._ready_event = threading.Event()
        self._startup_error: BaseException | None = None
        self._closing = False
        self._connected = False

    async def connect(self) -> None:
        """
        Writer thread를 시작하고 persistent Sender를 준비한다.
        """
        if self._connected:
            return

        self._queue = queue.Queue(maxsize=self.queue_maxsize)
        self._ready_event.clear()
        self._startup_error = None
        self._closing = False

        self._thread = threading.Thread(
            target=self._writer_loop,
            name="questdb-ilp-writer",
            daemon=True,
        )
        self._thread.start()

        ready = await asyncio.to_thread(
            self._ready_event.wait,
            self.startup_timeout_sec,
        )

        if not ready:
            raise TimeoutError(
                f"QuestDB writer startup timeout: {self.startup_timeout_sec}s"
            )

        if self._startup_error is not None:
            raise RuntimeError(
                f"QuestDB writer startup failed: {self._startup_error}"
            ) from self._startup_error

        self._connected = True
        logger.info(
            f"QuestDBClient connected with persistent writer: "
            f"conf={self.conf}, "
            f"queue_maxsize={self.queue_maxsize}, "
            f"flush_interval_sec={self.flush_interval_sec}, "
            f"max_buffered_rows={self.max_buffered_rows}"
        )

    async def close(self) -> None:
        """
        Writer thread에 stop signal을 보내고, 남은 batch를 flush한 뒤 종료한다.
        """
        if not self._connected:
            return

        self._closing = True

        if self._queue is not None:
            await asyncio.to_thread(self._queue.put, _STOP)

        if self._thread is not None:
            await asyncio.to_thread(
                self._thread.join,
                self.shutdown_timeout_sec,
            )

            if self._thread.is_alive():
                logger.warning(
                    f"QuestDB writer thread did not stop within "
                    f"{self.shutdown_timeout_sec}s"
                )

        self._connected = False
        self._thread = None
        self._queue = None

        logger.info("QuestDBClient closed")

    async def write_batch(
        self,
        table_name: str,
        rows: List[Dict[str, Any]],
    ) -> None:
        """
        async write API.

        rows 구조:
        [
            {
                "symbols": {"key": "value", ...},
                "columns": {"key": 123.4, ...},
                "at": 1612345678000000000  # nanos
            },
            ...
        ]

        이 메서드는 queue에 넣은 뒤, writer thread가 flush할 때까지 await한다.
        """
        if not rows:
            return

        if not self._connected or self._queue is None:
            raise RuntimeError("QuestDBClient is not connected")

        if self._closing:
            raise RuntimeError("QuestDBClient is closing")

        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()

        item = _QuestDBWriteItem(
            table_name=table_name,
            rows=list(rows),
            loop=loop,
            future=future,
        )

        try:
            self._queue.put_nowait(item)
        except queue.Full:
            logger.warning(
                f"QuestDB write queue full. Blocking until slot is available. "
                f"table={table_name}, rows={len(rows)}"
            )
            await asyncio.to_thread(self._queue.put, item)

        await future

    # ───────────────────────────── Writer thread internals ─────────────────────────────

    def _writer_loop(self) -> None:
        """
        Dedicated writer thread loop.

        Sender는 이 thread 안에서만 생성/사용/종료된다.
        """
        pending: list[_QuestDBWriteItem] = []
        pending_rows = 0

        try:
            with Sender.from_conf(self.conf) as sender:
                self._ready_event.set()

                next_flush_at = time.monotonic() + self.flush_interval_sec

                while True:
                    timeout = max(0.0, next_flush_at - time.monotonic())

                    try:
                        item = self._queue.get(timeout=timeout)  # type: ignore[union-attr]
                    except queue.Empty:
                        item = None

                    if item is _STOP:
                        if pending:
                            self._flush_pending(sender, pending)
                            pending.clear()
                            pending_rows = 0

                        self._queue.task_done()  # type: ignore[union-attr]
                        break

                    if isinstance(item, _QuestDBWriteItem):
                        pending.append(item)
                        pending_rows += len(item.rows)
                        self._queue.task_done()  # type: ignore[union-attr]

                    now = time.monotonic()

                    should_flush_by_size = pending_rows >= self.max_buffered_rows
                    should_flush_by_time = pending and now >= next_flush_at

                    if should_flush_by_size or should_flush_by_time:
                        self._flush_pending(sender, pending)
                        pending.clear()
                        pending_rows = 0
                        next_flush_at = time.monotonic() + self.flush_interval_sec

        except BaseException as e:
            self._startup_error = e
            self._ready_event.set()

            logger.error(
                f"QuestDB writer loop failed: {e}",
                exc_info=True,
            )

            self._fail_pending(pending, e)
            self._drain_queue_with_error(e)

    def _flush_pending(
        self,
        sender: Sender,
        pending: list[_QuestDBWriteItem],
    ) -> None:
        """
        pending write items를 하나의 flush로 전송.
        """
        if not pending:
            return

        total_rows = 0

        try:
            for item in pending:
                for row in item.rows:
                    at_nanos = row.get("at")

                    sender.row(
                        item.table_name,
                        symbols=row.get("symbols", {}),
                        columns=row.get("columns", {}),
                        at=TimestampNanos(at_nanos) if at_nanos else None,
                    )
                    total_rows += 1

            sender.flush()

        except BaseException as e:
            logger.error(
                f"QuestDB batch flush failed: rows={total_rows}, err={e}",
                exc_info=True,
            )
            self._fail_pending(pending, e)
            return

        for item in pending:
            self._resolve_future(item, result=None)

        logger.debug(f"QuestDB flushed {total_rows} rows")

    def _resolve_future(
        self,
        item: _QuestDBWriteItem,
        *,
        result: None = None,
    ) -> None:
        def _set_result() -> None:
            if item.future.done():
                return
            item.future.set_result(result)

        item.loop.call_soon_threadsafe(_set_result)

    def _reject_future(
        self,
        item: _QuestDBWriteItem,
        exc: BaseException,
    ) -> None:
        def _set_exception() -> None:
            if item.future.done():
                return
            item.future.set_exception(exc)

        item.loop.call_soon_threadsafe(_set_exception)

    def _fail_pending(
        self,
        pending: list[_QuestDBWriteItem],
        exc: BaseException,
    ) -> None:
        for item in pending:
            self._reject_future(item, exc)

    def _drain_queue_with_error(self, exc: BaseException) -> None:
        """
        writer fatal error 발생 시 queue에 남은 요청들을 실패 처리.
        """
        if self._queue is None:
            return

        while True:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                break

            try:
                if isinstance(item, _QuestDBWriteItem):
                    self._reject_future(item, exc)
            finally:
                self._queue.task_done()
