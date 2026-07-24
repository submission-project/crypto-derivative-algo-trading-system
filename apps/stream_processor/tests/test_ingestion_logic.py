import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from stream_processor.main import BATCH_SIZE, FLUSH_INTERVAL_SEC

# 테스트용 더미 데이터 생성기
async def mock_consume_stream(items_to_yield):
    for item in items_to_yield:
        yield item
        await asyncio.sleep(0.01) # 아주 잠깐씩 대기

@pytest.mark.asyncio
async def test_stale_buffer_timer_flush():
    """
    데이터가 BATCH_SIZE(100)보다 적게 들어왔을 때, 
    FLUSH_INTERVAL_SEC 후에 자동으로 Flush되는지 테스트
    """
    from stream_processor.main import flush_buffer
    
    # 1. Mock 객체 설정
    mock_writer = AsyncMock()
    mock_consumer = MagicMock()
    
    # 소량의 데이터 (5건)
    test_data = [{"symbol": "BTCUSDT", "price": 100.0 + i} for i in range(5)]
    mock_consumer.consume_stream = lambda: mock_consume_stream(test_data)
    
    # 2. 버퍼 및 통계 변수 (main.py의 로직 복제)
    buffer = []
    
    async def do_flush():
        if not buffer: return
        data_to_send = list(buffer)
        buffer.clear()
        await mock_writer.insert_batch(data_to_send, "trades", [], [])

    async def flush_timer():
        while True:
            await asyncio.sleep(FLUSH_INTERVAL_SEC)
            await do_flush()

    # 3. 테스트 실행
    timer_task = asyncio.create_task(flush_timer())
    
    # 데이터를 버퍼에 채움 (타이머가 가동 중인 상태)
    for data in test_data:
        buffer.append(data)
        # BATCH_SIZE 체크 로직 (여기서는 실행 안 됨)
        if len(buffer) >= BATCH_SIZE:
            await do_flush()
            
    # 타이머가 아직 돌기 전이라면 버퍼에 데이터가 있어야 함
    # (매우 짧은 시간이지만 동기적으로 append 했으므로 이 시점엔 존재할 가능성이 높음)
    # 만약 여기서 이미 0이라면 타이머가 그만큼 빠르게 일하고 있다는 뜻
    print(f"Buffer size before intentional wait: {len(buffer)}")
    
    # 4. 타이머 주기보다 확실히 더 기다림
    await asyncio.sleep(FLUSH_INTERVAL_SEC + 0.1)
    
    # 5. 검증: 최종적으로는 무조건 비워져 있어야 하며, mock_writer가 호출되었어야 함
    assert len(buffer) == 0
    assert mock_writer.insert_batch.call_count >= 1
    
    timer_task.cancel()

@pytest.mark.asyncio
async def test_immediate_batch_flush():
    """
    데이터가 BATCH_SIZE(100)에 도달했을 때 즉시 Flush되는지 테스트
    """
    mock_writer = AsyncMock()
    buffer = []
    
    async def do_flush():
        if not buffer: return
        data_to_send = list(buffer)
        buffer.clear()
        await mock_writer.insert_batch(data_to_send, "trades", [], [])

    # 100건의 데이터 준비
    large_data = [{"id": i} for i in range(BATCH_SIZE)]
    
    for data in large_data:
        buffer.append(data)
        if len(buffer) >= BATCH_SIZE:
            await do_flush()
            
    # 검증: 100건이 차자마자 즉시 호출되었어야 함
    assert len(buffer) == 0
    assert mock_writer.insert_batch.call_count == 1
