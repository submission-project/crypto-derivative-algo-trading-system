## Redis 

### Redis Stream
- XADD : 이벤트 추가
- MAXLEN : Stream에 너무 많은 데이터가 쌓이지 않도록 오래된 데이터 삭제

```bash
XADD trades:binance:perp:BTCUSDT * price 65000.1 size 0.001 trade_id 123
```
- Redis Stream에 데이터를 추가하는 명령어

```bash
XADD trades:binance:perp:BTCUSDT MAXLEN 2000 * price 65000.1 size 0.001
XADD trades:binance:perp:BTCUSDT MAXLEN ~ 2000 * field value field value ...
```
- 새 데이터를 추가하되, 이 Stream에는 최대 2000개 정도만 유지해라. 
- 근사치 삭제 (~): MAXLEN ~ 1000과 같이 사용하면 1000개 내외로 데이터를 유지하며, 이 방식은 메모리 할당 효율이 더 높음
- 정확한 삭제 (=): MAXLEN = 1000과 같이 사용하면 정확히 1000개만 유지하지만, 성능상 비용이 조금 더 듬
- Redis가 성능 좋게 관리할 수 있도록 대략적으로 자름 - >trade 저장에서는 보통 MAXLEN ~가 더 좋음
MAXLEN ~ 2000