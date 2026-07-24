"""
중앙 정의: PostgreSQL 테이블명, QuestDB 테이블명, Redis 키 문자열.

DDL 과의 동기화:
  - PostgreSQL: ``infra/postgres.init.sql``
  - QuestDB: ``infra/questdb.init.sql``

스키마 변경 시 위 SQL과 이 모듈을 함께 수정한다.
"""

# [claim] 다른 여러 컴포넌트가 섞여 있어서, 관리가 다소 복잡한 맛이 있음 -> ORDER_STATE_INDEX_PATTEN_LIST 같이 order_state_repo.py 에서만 쓰는 요소의 경우, 그쪽에서 관리하는 게 낮지 않음

from __future__ import annotations

from typing import Final

class QuestDBTable:
    """QuestDB 테이블명 (ILP / HTTP 조회 공통)."""

    EXECUTION_LOGS: Final[str] = "execution_logs"
    CANONICAL_TRADES: Final[str] = "canonical_trades"
    MARKET_TRADES: Final[str] = "market_trades"
    MARKET_ORDERBOOKS: Final[str] = "market_orderbooks"
    MARKET_OPEN_INTEREST: Final[str] = "market_open_interest"


class RedisKey:
    """주문 projection / 시그널 대기용 Redis 키."""

    ORDER_LIVE_PREFIX: Final[str] = "order:live"
    ORDER_OPEN_SET: Final[str] = "order:open"
    ORDER_CONDITIONAL_OPEN_SET: Final[str] = "order:conditional:open"
    ORDER_BY_SYMBOL_SET: Final[str] = "order:by:symbol"
    ORDER_UNKNOWN_ZSET: Final[str] = "order:unknown"
    ORDER_RECOVERY_ZSET: Final[str] = "order:recovery"

    SIGNAL_PENDING_PREFIX: Final[str] = "signal:pending"
    SIGNAL_PENDING_INDEX: Final[str] = "signal:pending:index"

# ORDER_STATE_INDEX_PATTEN_LIST: list[str] = [
#     f"{RedisKey.ORDER_OPEN_SET}:*",
#     f"{RedisKey.ORDER_CONDITIONAL_OPEN_SET}:*",
#     f"{RedisKey.ORDER_UNKNOWN_ZSET}:*",
#     f"{RedisKey.ORDER_RECOVERY_ZSET}:*",
#     f"{RedisKey.ORDER_BY_SYMBOL_SET}:*",
# ]


def redis_order_live_key(order_id: str) -> str:
    """``order:live:{order_id}`` Hash 키."""
    return f"{RedisKey.ORDER_LIVE_PREFIX}:{order_id}"


# index 해시 테이블
def redis_order_open_key(exchange: str, market_type: str) -> str:
    """``order:open:{exchange}:{market_type}`` ZSet 키.

    정규 미체결 주문 집합.
    """
    return f"{RedisKey.ORDER_OPEN_SET}:{exchange}:{market_type}"


def redis_order_conditional_open_key(exchange: str, market_type: str) -> str:
    """``order:conditional:open:{exchange}:{market_type}`` ZSet 키.

    조건부 미체결 주문 집합.
    """
    return f"{RedisKey.ORDER_CONDITIONAL_OPEN_SET}:{exchange}:{market_type}"


def redis_order_unknown_key(exchange: str, market_type: str) -> str:
    """``order:unknown:{exchange}:{market_type}`` ZSet 키.

    실행 불명확 주문 집합.
    """
    return f"{RedisKey.ORDER_UNKNOWN_ZSET}:{exchange}:{market_type}"


def redis_order_recovery_key(exchange: str, market_type: str) -> str:
    """``order:recovery:{exchange}:{market_type}`` ZSet 키.

    복구 대상 주문 집합.
    """
    return f"{RedisKey.ORDER_RECOVERY_ZSET}:{exchange}:{market_type}"


def redis_order_by_symbol_key(exchange: str, market_type: str, symbol: str) -> str:
    """``order:by:symbol:{exchange}:{market_type}:{symbol.upper()}`` ZSet 키.

    종목별 미체결 주문 집합.
    """
    return f"{RedisKey.ORDER_BY_SYMBOL_SET}:{exchange}:{market_type}:{symbol.upper()}"


def redis_signal_pending_key(signal_id: str) -> str:
    """``signal:pending:{signal_id}`` Hash 키."""
    return f"{RedisKey.SIGNAL_PENDING_PREFIX}:{signal_id}"
