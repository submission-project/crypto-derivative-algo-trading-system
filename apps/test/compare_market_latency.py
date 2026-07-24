import asyncio
import websockets
import json
import time
import orjson


async def listen_stream(url, stream_type, arrival_dict):
    """특정 URL에서 스트림을 듣고 도착 시간을 기록"""
    print(f"Connecting to {stream_type} at {url}...")
    try:
        async with websockets.connect(url) as websocket:
            print(f"Connected to {stream_type}!")
            while True:
                message = await websocket.recv()
                recv_time = time.time_ns()
                data = orjson.loads(message)
                print(data)

                # Trade ID 추출
                if stream_type == "trade":
                    tid = data.get("t")
                else:  # aggTrade
                    tid = data.get("l")  # 마지막 trade id와 매칭

                if tid:
                    if tid not in arrival_dict:
                        arrival_dict[tid] = {}
                    arrival_dict[tid][stream_type] = recv_time
    except Exception as e:
        print(f"Error in {stream_type}: {e}")


async def compare_market_latency():
    # 1. aggTrade는 사용자가 주신 'public path'(/market/ws/) 사용
    url_agg = "wss://fstream.binance.com/market/ws/btcusdt@aggTrade"
    # 2. trade는 일반 경로 사용 (market 경로에서 안 될 가능성 대비)
    url_trade = "wss://fstream.binance.com/ws/btcusdt@trade"

    arrival_times = {}

    # 두 스트림을 각각의 태스크로 실행
    task_agg = asyncio.create_task(listen_stream(url_agg, "aggTrade", arrival_times))
    task_trade = asyncio.create_task(listen_stream(url_trade, "trade", arrival_times))

    print("Starting latency comparison between Market Path and Normal Path...")

    matches = 0
    try:
        while matches < 50:
            await asyncio.sleep(0.1)
            # 매칭된 ID 찾기
            matched_ids = [
                tid for tid, times in arrival_times.items() if len(times) == 2
            ]

            for tid in sorted(matched_ids):
                times = arrival_times[tid]
                t_time = times["trade"]
                a_time = times["aggTrade"]
                diff_ms = (t_time - a_time) / 1_000_000

                winner = "aggTrade(Market)" if diff_ms > 0 else "trade(Normal)"
                print(
                    f"Match {matches+1:2d} | ID: {tid} | Winner: {winner:16s} | Diff: {abs(diff_ms):.4f}ms"
                )

                matches += 1
                del arrival_times[tid]

    finally:
        task_agg.cancel()
        task_trade.cancel()


if __name__ == "__main__":
    asyncio.run(compare_market_latency())
