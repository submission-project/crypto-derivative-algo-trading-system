from __future__ import annotations

import time
from decimal import Decimal
from typing import Any

from common.logging import setup_logger
from schemas.market import Exchange, MarketType
from schemas.position import (
    Position,
    PositionSide,
    PositionStatus,
    make_position_id,
)
from storage.postgres_client import PostgresClient
from storage.repositories.redis.position_state_repo import PositionRedisRepository
from storage.repositories.postgres.outbox_repo import OutboxPostgresRepository
from storage.repositories.postgres.position_repo import PositionPostgresRepository

from schemas.position_update_event import NormalizedPositionSnapshot
from execution_gateway.exchange import ExchangePositionSnapshot


logger = setup_logger(__name__)


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


class PositionStateService:
    """
    Position source of truth + Redis projection orchestration.

    실시간:
      ACCOUNT_UPDATE patch 반영

    복구/초기화:
      positionRisk REST snapshot 반영
    """

    def __init__(
        self,
        *,
        postgres: PostgresClient,
        position_repo: PositionPostgresRepository,
        outbox_repo: OutboxPostgresRepository,
        redis_position_repo: PositionRedisRepository,
    ) -> None:
        self.postgres = postgres
        self.position_repo = position_repo
        self.outbox_repo = outbox_repo
        self.redis_position_repo = redis_position_repo

    # async def apply_account_update(
    #     self,
    #     envelope: AccountUpdateEnvelope,
    # ) -> list[Position]:
    #     """
    #     Deprecated: Binance raw ACCOUNT_UPDATE wrapper.
    #     """
    #     """
    #     Binance ACCOUNT_UPDATE의 position patch를 반영.

    #     envelope.raw 구조:
    #       {
    #         "e": "ACCOUNT_UPDATE",
    #         "E": event_time,
    #         "T": transaction_time,
    #         "a": {
    #           "m": reason,
    #           "P": [...]
    #         }
    #       }
    #     """

    #     from execution_gateway.adapters.binance.binance_position_event_mapper import (
    #         normalize_binance_account_update_positions,
    #     )

    #     snapshots = normalize_binance_account_update_positions(envelope.raw)

    #     return await self.apply_position_snapshots(
    #         snapshots=snapshots,
    #         event_type="POSITION_UPDATED",
    #     )
    #     # raw = envelope.raw
    #     # event_time = envelope.event_time
    #     # transaction_time = raw.get("T")

    #     # account_data = raw.get("a", {})
    #     # reason = account_data.get("m")
    #     # position_rows = account_data.get("P", [])

    #     # if not position_rows:
    #     #     logger.debug(f"ACCOUNT_UPDATE position patch 없음: reason={reason}")
    #     #     return []

    #     # updated_positions: list[Position] = []

    #     # for row in position_rows:
    #     #     if not isinstance(row, dict):
    #     #         logger.warning(
    #     #             f"ACCOUNT_UPDATE position row가 dict가 아님: "
    #     #             f"type={type(row).__name__}, value={row!r}; skip"
    #     #         )
    #     #         continue

    #     #     try:
    #     #         position = self._position_from_account_update_row(
    #     #             row=row,
    #     #             reason=reason,
    #     #             event_time=event_time,
    #     #             transaction_time=transaction_time,
    #     #         )

    #     #         persisted = await self._persist_position(
    #     #             position=position,
    #     #             event_type="POSITION_UPDATED",
    #     #         )

    #     #         updated_positions.append(persisted)

    #     #     except Exception as e:
    #     #         logger.error(
    #     #             f"ACCOUNT_UPDATE position row 반영 실패: "
    #     #             f"reason={reason}, row={row}, err={e}",
    #     #             exc_info=True,
    #     #         )

    #     # return updated_positions

    # async def refresh_positions_from_exchange(
    #     self,
    #     rows: list[dict[str, Any]],
    #     *,
    #     event_time: int | None = None,
    # ) -> list[Position]:
    #     """
    #     Binance /fapi/v3/positionRisk snapshot 반영.

    #     주의:
    #       positionRisk V3는 포지션이 있거나 open order가 있는 symbol만 반환한다.
    #       따라서 응답에 없는 포지션을 무조건 FLAT으로 처리하면 안 된다.
    #     """
    #     now_ms = event_time or _now_ms()
    #     updated_positions: list[Position] = []

    #     for row in rows:
    #         position = self._position_from_position_risk_row(
    #             row=row,
    #             event_time=now_ms,
    #         )

    #         persisted = await self._persist_position(
    #             position=position,
    #             event_type="POSITION_SNAPSHOT_REFRESHED",
    #         )

    #         updated_positions.append(persisted)

    #     return updated_positions

    async def refresh_position_snapshots(
        self,
        snapshots: list[ExchangePositionSnapshot],
        *,
        event_time: int | None = None,
    ) -> list[Position]:
        """
        거래소에서 가져온 포지션 정보를
        postgres[position, outbox], redis[position] 에 반영.
        """
        now_ms = event_time or _now_ms()
        updated_positions: list[Position] = []

        for snapshot in snapshots:
            position = self._parse_position_from_exchange_snapshot(
                snapshot=snapshot,
                event_time=now_ms,
            )

            persisted = await self._persist_position(
                position=position,
                event_type="POSITION_SNAPSHOT_REFRESHED",
            )
            updated_positions.append(persisted)

        return updated_positions

    async def load_open_positions(self) -> list[Position]:
        pool = self.postgres.require_pool()

        async with pool.acquire() as conn:
            rows = await self.position_repo.list_open(conn)

        return [Position.model_validate(row) for row in rows]

    async def _persist_position(
        self,
        *,
        position: Position,
        event_type: str,
    ) -> Position:
        pool = self.postgres.require_pool()

        async with pool.acquire() as conn:
            async with conn.transaction():
                persisted = await self.position_repo.upsert(
                    conn,
                    position=position,
                )

                # [claim] outbox table의 aggratate_type 의 default 가 'order'로 되어 있는 데,
                # default는 제거 하고, 이후 order 넣는 부분은 order로 변경 아니면, enum 으로 바꾸어도 됨
                await self.outbox_repo.insert(
                    conn=conn,
                    aggregate_id=persisted.position_id,
                    aggregate_type="POSITION",
                    event_type=event_type,
                    payload={
                        "position": persisted.model_dump(
                            mode="json",
                            exclude_none=True,
                        )
                    },
                    created_ts=persisted.updated_ts,
                )

        try:
            await self.redis_position_repo.save(persisted)
        except Exception as e:
            logger.error(
                f"Redis position projection 저장 실패: "
                f"position_id={persisted.position_id}, "
                f"symbol={persisted.symbol}, "
                f"err={e}",
                exc_info=True,
            )

        logger.info(
            f"Position 상태 반영 완료: "
            f"position_id={persisted.position_id}, "
            f"status={persisted.status.value}, "
            f"amt={persisted.position_amt}, "
            f"version={persisted.version}"
        )

        return persisted

    def _position_from_account_update_row(
        self,
        *,
        row: dict[str, Any],
        reason: str | None,
        event_time: int | None,
        transaction_time: int | None,
    ) -> Position:
        now_ms = _now_ms()

        symbol = str(row.get("s") or "").upper()
        if not symbol:
            raise ValueError(f"ACCOUNT_UPDATE position row missing symbol: {row}")

        position_side = _normalize_position_side(row.get("ps"))
        position_amt = str(row.get("pa", "0"))

        status = _position_status_from_amt(position_amt)
        updated_ts = transaction_time or event_time or now_ms

        return Position(
            position_id=make_position_id(
                exchange=Exchange.BINANCE,
                market_type=MarketType.PERP,
                symbol=symbol,
                position_side=position_side,
            ),
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol=symbol,
            position_side=position_side,
            status=status,
            position_amt=position_amt,
            entry_price=_optional_str(row.get("ep")),
            break_even_price=_optional_str(row.get("bep")),
            # ACCOUNT_UPDATE에는 mark price가 없다.
            # positionRisk snapshot으로 별도 보강 가능.
            mark_price=None,
            unrealized_pnl=_optional_str(row.get("up")),
            # Binance ACCOUNT_UPDATE의 iw는 isolated wallet에 가까움.
            # isolated_margin과 동일시하지 않는다.
            isolated_margin=None,
            isolated_wallet=_optional_str(row.get("iw")),
            margin_type=_optional_str(row.get("mt")),
            # ACCOUNT_UPDATE에는 보통 아래 필드가 없음.
            leverage=None,
            liquidation_price=None,
            notional=None,
            update_reason=reason,
            last_event_time=event_time,
            last_transaction_time=transaction_time,
            # opened_ts / closed_ts는 repository.upsert()에서 계산.
            opened_ts=None,
            closed_ts=None,
            updated_ts=updated_ts,
        )

    async def load_position(
        self,
        *,
        position_id: str,
        refresh_projection: bool = True,
    ) -> Position | None:
        """
        PostgreSQL 원본에서 position을 로드한다.
        """
        pool = self.postgres.require_pool()

        async with pool.acquire() as conn:
            row = await self.position_repo.get(conn, position_id)

        if not row:
            return None

        position = Position.model_validate(row)

        if refresh_projection:
            try:
                await self.redis_position_repo.save(position)
            except Exception as e:
                logger.error(
                    f"Redis position projection refresh 실패: "
                    f"position_id={position_id}, err={e}",
                    exc_info=True,
                )

        return position

    async def apply_position_snapshots(
        self,
        *,
        snapshots: list[NormalizedPositionSnapshot],
        event_type: str = "POSITION_UPDATED",
    ) -> list[Position]:
        """
        거래소별 position update event를 정규화한 snapshot을 반영한다.

        Binance ACCOUNT_UPDATE, OKX position event, Bitget position event 모두
        mapper에서 NormalizedPositionSnapshot으로 변환한 뒤 이 메소드로 들어온다.
        """
        updated_positions: list[Position] = []

        for snapshot in snapshots:
            try:
                position = snapshot.to_position()

                persisted = await self._persist_position(
                    position=position,
                    event_type=event_type,
                )

                updated_positions.append(persisted)

            except Exception as e:
                logger.error(
                    f"position snapshot 반영 실패: "
                    f"exchange={snapshot.exchange.value}, "
                    f"market_type={snapshot.market_type.value}, "
                    f"symbol={snapshot.symbol}, "
                    f"position_side={snapshot.position_side.value}, "
                    f"err={e}",
                    exc_info=True,
                )

        return updated_positions

    def _parse_position_from_exchange_snapshot(
        self,
        *,
        snapshot: ExchangePositionSnapshot,
        event_time: int,
    ) -> Position:
        position_amt = snapshot.position_amt
        updated_ts = snapshot.updated_ts or event_time

        return Position(
            exchange=snapshot.exchange,
            market_type=snapshot.market_type,
            symbol=snapshot.symbol,
            position_side=snapshot.position_side,
            status=_position_status_from_amt(position_amt),
            position_amt=position_amt,
            entry_price=snapshot.entry_price,
            mark_price=snapshot.mark_price,
            unrealized_pnl=snapshot.unrealized_pnl,
            leverage=snapshot.leverage,
            liquidation_price=snapshot.liquidation_price,
            update_reason="POSITION_SNAPSHOT_REFRESH",
            last_event_time=updated_ts,
            last_transaction_time=updated_ts,
            updated_ts=updated_ts,
            break_even_price=snapshot.break_even_price,
            isolated_margin=snapshot.isolated_margin,
            isolated_wallet=snapshot.isolated_wallet,
            margin_type=snapshot.margin_type,
            notional=snapshot.notional,
        )
        

    # def _position_from_position_risk_row(
    #     self,
    #     *,
    #     row: dict[str, Any],
    #     event_time: int,
    # ) -> Position:

    #     position_amt = str(row.get("positionAmt", "0"))

    #     return Position(
    #         exchange=Exchange.BINANCE,
    #         market_type=MarketType.PERP,
    #         symbol=str(row.get("symbol", "")).upper(),
    #         position_side=PositionSide(str(row.get("positionSide", "BOTH"))),
    #         position_amt=position_amt,
    #         entry_price=_optional_str(row.get("entryPrice")),
    #         break_even_price=_optional_str(row.get("breakEvenPrice")),
    #         mark_price=_optional_str(row.get("markPrice")),
    #         unrealized_pnl=_optional_str(row.get("unRealizedProfit")),
    #         isolated_margin=_optional_str(row.get("isolatedMargin")),
    #         isolated_wallet=_optional_str(row.get("isolatedWallet")),
    #         liquidation_price=_optional_str(row.get("liquidationPrice")),
    #         notional=_optional_str(row.get("notional")),
    #         leverage=(
    #             int(row.get("leverage")) if row.get("leverage") is not None else None
    #         ),
    #         update_reason="POSITION_RISK_REFRESH",
    #         last_event_time=int(row.get("updateTime") or event_time),
    #         last_transaction_time=int(row.get("updateTime") or event_time),
    #         updated_ts=int(row.get("updateTime") or event_time),
    #     )


def _position_status_from_amt(
    position_amt: str | int | float | Decimal,
) -> PositionStatus:
    amt = Decimal(str(position_amt))

    if amt == 0:
        return PositionStatus.FLAT

    return PositionStatus.OPEN


def _normalize_position_side(raw: Any) -> PositionSide:
    if raw in (None, ""):
        return PositionSide.BOTH

    value = str(raw).upper()

    try:
        return PositionSide(value)
    except ValueError:
        logger.warning(f"Unknown Binance positionSide={raw!r}; fallback to BOTH")
        return PositionSide.BOTH


def _optional_str(value: Any) -> str | None:
    if value in (None, ""):
        return None

    return str(value)

