import asyncio
from .config import settings
from messaging.producer import KafkaProducer
from messaging.consumer import KafkaConsumer

from .core import (
    GapDetector,
    HealthMonitor,
    FallbackController,
    RepairJob,
    GapFillFetcher,
    GapFillSource,
    normalize_rest_trade,
)
from .streams import TradeStream, AggTradeStream
from .trade_rest import RestApiError, RestAuthError, RestTradeClient
from schemas.market import TradeSource

from common.logging import setup_logger

logger = setup_logger(__name__)

REPAIR_WORKER_GROUP_ID = "binance-perp-repair-worker"


def _log_repair_outcome(job, result, restored_count: int, expected_count: int):
    """
    GapFillFetcher 결과를 source 별로 의미 있게 로깅.

    실측 결과 두 endpoint 모두 ADL/insurance 등의 trade 타입을 응답에서 제외하기에,
    RECENT_MEMORY 와 HISTORICAL_DB 모두 restored < expected 가 정상 케이스로
    발생할 수 있다. 그래서 두 source 의 로깅 구조는 본질적으로 같다.

    - RECENT_MEMORY: Stage1 cover (1회 호출). 부족분은 ADL/insurance 추정.
    - HISTORICAL_DB: Stage2 cover (retry 포함). 부족분은 ADL/insurance 추정.
    - PARTIAL: Stage2 모든 retry 후에도 to_id 미도달 — lag > backoff 또는
      ADL/insurance 가 range 끝부분 (to_id 인접) 에 모여 있는 경우.
    """
    base = (
        f"{job.symbol} {job.from_trade_id}~{job.to_trade_id} "
        f"({restored_count}/{expected_count}, attempts={result.attempts})"
    )
    label = {
        GapFillSource.RECENT_MEMORY: "recent",
        GapFillSource.HISTORICAL_DB: "historical",
    }.get(result.source)

    if label is not None:  # 정상 cover (RECENT_MEMORY 또는 HISTORICAL_DB)
        if restored_count == expected_count:
            logger.info(f"✅ Repaired via {label}: {base}")
        else:
            missing = expected_count - restored_count
            logger.info(
                f"✅ Repaired via {label}: {base}. "
                f"missing={missing} likely ADL/insurance (excluded by Binance)."
            )

    elif result.source == GapFillSource.PARTIAL:
        logger.warning(
            f"⚠️ Partial repair after retries: {base}. "
            f"Cause: indexing lag > backoff window OR ADL/insurance at end of range."
        )


async def main():
    logger.info("🟢 Starting Binance Perp Trade Collector (3-Tier)")

    symbols = settings.binance_perp_symbols

    # ── Producers ──
    producer_raw_trade = KafkaProducer(
        bootstrap_servers=settings.redpanda_brokers,
        topic=settings.binance_perp_topic_raw_trades,
    )
    producer_raw_aggtrade = KafkaProducer(
        bootstrap_servers=settings.redpanda_brokers,
        topic=settings.binance_perp_topic_agg_trades,
    )
    producer_canonical = KafkaProducer(
        bootstrap_servers=settings.redpanda_brokers,
        topic=settings.binance_perp_topic_canonical,
    )
    producer_repair = KafkaProducer(
        bootstrap_servers=settings.redpanda_brokers,
        topic=settings.binance_perp_topic_repair_jobs,
    )

    await producer_raw_trade.connect()
    await producer_raw_aggtrade.connect()
    await producer_canonical.connect()
    await producer_repair.connect()

    # ── Consumer (repair jobs) ──
    consumer_repair = KafkaConsumer(
        bootstrap_servers=settings.redpanda_brokers,
        topic=settings.binance_perp_topic_repair_jobs,
        group_id=REPAIR_WORKER_GROUP_ID,
    )
    await consumer_repair.connect()

    # ── Core Components ──
    health = HealthMonitor()
    gap = GapDetector()
    fallback = FallbackController()
    rest_client = RestTradeClient(api_key=settings.binance_api_key)
    await rest_client.connect()
    gap_fill = GapFillFetcher(rest_client)

    # ── Streams ──
    trade_stream = TradeStream(
        symbols=symbols,
        base_ws_url=settings.binance_perp_ws,
        producer_raw=producer_raw_trade,
        producer_canonical=producer_canonical,
        health=health,
        fallback=fallback,
        gap=gap,
    )

    aggtrade_stream = AggTradeStream(
        symbols=symbols,
        base_ws_url=settings.binance_perp_ws,
        producer_raw=producer_raw_aggtrade,
        producer_repair=producer_repair,
        fallback=fallback,
    )

    # ── Repair Worker ──
    async def repair_worker():
        """
        Redpanda repair_jobs 토픽에서 RepairJob을 소비하고,
        GapFillFetcher (Stage1 recent → Stage2 historical+retry) 로 f~l 범위의
        개별 trade를 복원하여 canonical 토픽으로 발행합니다.
        """
        logger.info("🔧 Repair worker started. Consuming from repair_jobs topic...")

        async for job_data in consumer_repair.consume_stream():
            try:
                job = RepairJob.from_dict(job_data)
                expected_count = job.to_trade_id - job.from_trade_id + 1
                logger.info(
                    f"🔧 Repair job: {job.symbol} "
                    f"trade_id {job.from_trade_id}~{job.to_trade_id} "
                    f"(agg={job.source_agg_trade_id}, expected={expected_count})"
                )

                try:
                    result = await gap_fill.fetch_range(
                        symbol=job.symbol,
                        from_id=job.from_trade_id,
                        to_id=job.to_trade_id,
                    )
                except RestAuthError as e:
                    # 인증 실패는 silent하게 넘어가면 안 됨 — 운영자 즉시 개입 필요.
                    logger.error(
                        f"❌ REST auth failed for {job.symbol} repair "
                        f"({job.from_trade_id}~{job.to_trade_id}): {e}. "
                        f"Set BINANCE_API_KEY and verify it. Skipping job."
                    )
                    continue
                except RestApiError as e:
                    logger.error(
                        f"❌ REST error for {job.symbol} repair "
                        f"({job.from_trade_id}~{job.to_trade_id}): {e}. Skipping job."
                    )
                    continue

                restored_count = 0
                for raw_trade in result.trades:
                    canonical = normalize_rest_trade(
                        raw_trade,
                        job.symbol,
                        TradeSource.REST_GAP_FILL,
                        source_agg_trade_id=job.source_agg_trade_id,
                    )
                    await producer_canonical.produce(job.symbol, canonical)
                    restored_count += 1

                _log_repair_outcome(job, result, restored_count, expected_count)

            except Exception as e:
                logger.error(f"Repair job failed: {job_data}, error={e}")

    # ── Run all tasks ──
    tasks = [
        asyncio.create_task(trade_stream.run()),
        asyncio.create_task(aggtrade_stream.run()),
        asyncio.create_task(repair_worker()),
    ]

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("Shutdown signal received.")
    finally:
        await trade_stream.stop()
        await aggtrade_stream.stop()
        for t in tasks:
            t.cancel()
        # cancel 이후 task 들의 cleanup (websocket close handshake, SSL teardown 등)
        # 이 끝날 때까지 기다린다. 안 그러면 rest_client / producer 를 먼저 닫고
        # 루프가 종료되는 사이에 SSL transport 의 connection_lost 콜백이
        # 'Event loop is closed' 로 터지는 race 가 발생.
        await asyncio.gather(*tasks, return_exceptions=True)
        await rest_client.close()
        await consumer_repair.stop()
        await producer_raw_trade.stop()
        await producer_raw_aggtrade.stop()
        await producer_canonical.stop()
        await producer_repair.stop()


def run() -> None:
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    run()
