"""
QuestDB 에 실제 TCP ILP 로 적재 후 REST `/exec` 로 행 존재를 검증한다 (mock 미사용).

선행 조건:
  - `make dev-up` 등으로 QuestDB 컨테이너 실행
  - 기본: `QUESTDB_HOST=127.0.0.1`, `QUESTDB_ILP_PORT=9009`, REST는
    `.env`(QUESTDB_HTTP_PORT 또는 QUESTDB_PORT, 보통 9000) 에 맞출 것

실행 예:
  make run-pytest-storage-questdb-integration

직접:
  uv run pytest packages/storage/tests/integration/test_questdb_save_integration.py \\
      -v -m integration

ILP TCP 연결 실패 시 pytest.skip 한다.
테스트에서는 `QUESTDB_STRICT_ILP_ERRORS=1` 을 켜 ILP 적재 예외가 삼켜지지 않게 한다.

wal 커밋 등으로 행이 잠깐 안 보일 수 있어, `table does not exist` 또는 빈 결과는 짧게 폴링한다.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import urllib.error
import urllib.parse
import urllib.request
import uuid

import pytest

pytest.importorskip("pytest_asyncio")
import pytest_asyncio

from storage.questdb_client import QuestDBClient
from storage.repositories.execution_questdb import ExecutionQuestDBRepository
from storage.repositories.trade_questdb import TradeQuestDBRepository

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def _strict_questdb_ilp_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """ILP 적재 오류 시 조용히 무시하면 실패 원인 파악이 불가능하므로 테스트에서 재전파."""
    monkeypatch.setenv("QUESTDB_STRICT_ILP_ERRORS", "1")


def _env_host() -> str:
    return os.getenv("QUESTDB_HOST", "127.0.0.1")


def _env_ilp_port() -> int:
    return int(os.getenv("QUESTDB_ILP_PORT", "9009"))


def _env_http_port() -> int:
    # compose: QUESTDB_PORT -> 컨테이너 9000 (REST /exec)
    return int(os.getenv("QUESTDB_HTTP_PORT", os.getenv("QUESTDB_PORT", "9000")))


def _require_questdb_ilp_socket() -> tuple[str, int, int]:
    host = _env_host()
    ilp = _env_ilp_port()
    http = _env_http_port()
    try:
        with socket.create_connection((host, ilp), timeout=3.0):
            pass
    except OSError as e:
        pytest.skip(
            f"QuestDB ILP 에 연결되지 않습니다 ({host}:{ilp}). "
            f"예: cd infra && ENV_FILE=../.env.dev docker-compose up -d questdb "
            f"— 상세: {e}"
        )
    return host, ilp, http


def _is_retryable_missing_table(exc: RuntimeError) -> bool:
    msg = str(exc).lower()
    return (
        "table does not exist" in msg
        or "cannot find entity" in msg
    )


def _http_exec_sql(*, host: str, http_port: int, sql: str) -> dict:
    q = urllib.parse.urlencode({"query": sql})
    url = f"http://{host}:{http_port}/exec?{q}"
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=15.0) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"QuestDB HTTP {e.code}: {body}") from e


async def _poll_dataset(
    *,
    host: str,
    http_port: int,
    sql: str,
    attempts: int = 40,
    delay_sec: float = 0.15,
) -> list:
    """WAL 플러시 등으로 행 노출이 약간 늦을 수 있어 짧게 폴링한다."""

    last: dict | None = None
    for _ in range(attempts):
        try:
            last = await asyncio.to_thread(
                _http_exec_sql, host=host, http_port=http_port, sql=sql
            )
        except RuntimeError as e:
            if _is_retryable_missing_table(e):
                await asyncio.sleep(delay_sec)
                continue
            raise
        ds = last.get("dataset") or []
        if ds:
            return ds
        await asyncio.sleep(delay_sec)
    pytest.fail(
        f"QuestDB 쿼리 결과가 비어 있습니다. 마지막 응답: {last!r} query={sql!r}"
    )


@pytest.fixture(scope="module")
def questdb_network() -> tuple[str, int, int]:
    return _require_questdb_ilp_socket()


@pytest_asyncio.fixture
async def live_questdb_client(questdb_network: tuple[str, int, int]) -> QuestDBClient:
    host, ilp, _ = questdb_network
    client = QuestDBClient(host=host, ilp_port=ilp)
    await client.connect()
    try:
        yield client
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_execution_questdb_save_inserts_readable_row(
    questdb_network: tuple[str, int, int],
    live_questdb_client: QuestDBClient,
) -> None:
    host, _, http_port = questdb_network
    table = "integration_test_execution_log_test"
    order_id = f"IT-EXEC-{uuid.uuid4().hex}"

    repo = ExecutionQuestDBRepository(questdb=live_questdb_client, table_name=table)
    payload = {
        "execution_id": "999888777",
        "order_id": order_id,
        "source": "MANUAL",
        "exchange": "BINANCE",
        "market_type": "PERP",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "fill_price": "70000.5",
        "fill_quantity": "0.001",
        "commission": "0.01",
        "commission_asset": "USDT",
        "exchange_order_id": "888777666",
        "exchange_trade_id": "999888777",
        "is_maker": False,
        "exchange_ts": 1_738_273_728_123,
        "local_ts": 1_738_273_728_200,
        "latency_ms": 1.0,
        "signal_id": None,
        "strategy_name": None,
    }

    await repo.save(payload)

    escaped = order_id.replace("'", "''")
    sql = f"SELECT order_id FROM {table} WHERE order_id = '{escaped}' LIMIT 10"
    rows = await _poll_dataset(host=host, http_port=http_port, sql=sql)
    assert rows[0][0] == order_id


@pytest.mark.asyncio
async def test_trade_questdb_save_inserts_readable_row(
    questdb_network: tuple[str, int, int],
    live_questdb_client: QuestDBClient,
) -> None:
    host, _, http_port = questdb_network
    table = "integration_test_canonical_trade_test"
    trade_key = uuid.uuid4().hex

    repo = TradeQuestDBRepository(questdb=live_questdb_client, table_name=table)
    payload = {
        "exchange": "BINANCE",
        "market_type": "PERP",
        "symbol": "BTCUSDT",
        "source": "integration_test",
        "is_buyer_maker": False,
        "trade_id": trade_key,
        "price": "70001.25",
        "size": "0.002",
        "exchange_ts": 1_738_273_730_456,
        "local_ts": 1_738_273_730_500,
        "verified_by_rest": False,
        "lag_ms": 5.5,
    }

    await repo.save(payload)

    escaped = trade_key.replace("'", "''")
    sql = f"SELECT trade_id FROM {table} WHERE trade_id = '{escaped}' LIMIT 10"
    rows = await _poll_dataset(host=host, http_port=http_port, sql=sql)
    assert rows[0][0] == trade_key
