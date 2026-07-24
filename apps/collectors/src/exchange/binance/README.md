# 각 거래소 API 정보

# Binance
## 실시간 스트림 구독용 WebSocket
- **BASE_URL**: `wss://fstream.binance.com/<stream>?<param>`

#### `<stream>`
| 구분      | 용도                     | Endpoint                            |
| ------- | ---------------------- | ----------------------------------- |
| Public  | 고빈도 public market data | `wss://fstream.binance.com/public`  |
| Market  | 일반 market data         | `wss://fstream.binance.com/market`  |
| Private | user data              | `wss://fstream.binance.com/private` |

#### `WS Mode` vs `Stream Mode`

| 구분             | WS Mode                     | Stream Mode                           |
| -------------- | --------------------------- | ------------------------------------- |
| URL 방식         | `/ws/<streamName>`          | `/stream?streams=<stream1>/<stream2>` |
| payload 형태     | raw payload                 | `{ stream, data }` wrapper            |
| 파싱 방식          | 메시지를 바로 파싱                  | `data` 안쪽을 파싱                         |
| 어떤 stream인지 식별 | payload 내부 symbol/event로 추론 | `stream` 필드로 바로 식별                    |
| 여러 stream 처리   | 가능                          | 가능                                    |
| 실무 편의성         | 단일/단순 구독에 편함                | 여러 stream 통합 처리에 편함                   |

- `wss://fstream.binance.com/public/ws/bnbusdt@depth/ethusdt@depth`, Ex) `wss://fstream.binance.com/public/ws/bnbusdt@depth/ethusdt@depth`

- `wss://fstream.binance.com/stream?streams=bnbusdt@depth/ethusdt@depth`, Ex) `wss://fstream.binance.com/stream?streams=bnbusdt@depth/ethusdt@depth/btcusdt@markPrice`

#### 연결 유효 시간
WebSocket 연결은 최대 24시간 동안만 유효합니다.
=> **24시간이 지나면 연결이 끊길 수 있으므로, 클라이언트는 자동 재연결 로직을 가져야 합니다.**

#### Ping/Pong
서버는 3분마다 ping frame을 보냅니다.
=> **클라이언트가 10분 안에 pong frame을 보내지 않으면 연결이 종료됩니다.**

#### 메시지 제한
- WebSocket 연결은 초당 최대 10개의 수신 메시지로 제한 => **반복적으로 제한을 초과해 끊기면 IP ban이 발생**

#### 스트림 개수 제한
- 하나의 WebSocket 연결은 최대 1024개 스트림까지 구독할 수 있음

### /public /market /private
| 구분      | 용도                     | Endpoint                            |
| ------- | ---------------------- | ----------------------------------- |
| Public  | 고빈도 public market data | `wss://fstream.binance.com/public`  |
| Market  | 일반 market data         | `wss://fstream.binance.com/market`  |
| Private | user data              | `wss://fstream.binance.com/private` |



--------------------

### ws-fapi VS fstream/private
- ws-fapi는 내가 요청을 보내야 응답이 오는 구조
- fstream은 구독해두면 서버가 이벤트를 계속 push하는 구조

```
[주문 실행]
wss://ws-fapi.binance.com/ws-fapi/v1
  ├─ order.place
  ├─ order.cancel
  ├─ order.modify
  └─ userDataStream.start 가능

[계정 이벤트 수신]
wss://fstream.binance.com/private/ws/<listenKey>
  ├─ ORDER_TRADE_UPDATE
  ├─ ACCOUNT_UPDATE
  └─ MARGIN_CALL
```
--------------------


## 주문/계정 조회/수정/삭제/요청-응답용 WebSocket API
- **base endpoint**: `wss://ws-fapi.binance.com/ws-fapi/v1`
- **testnet**: `wss://testnet.binancefuture.com/ws-fapi/v1`
```
주문 넣기
주문 취소
주문 조회
포지션 조회
계정 상태 조회
일부 market data request
```


#### 연결 수명
- 하나의 WebSocket API 연결은 최대 24시간만 유효 => **자동 재연결 로직 필요**

#### Ping/Pong
- 클라이언트가 10분 안에 pong frame을 보내지 않으면 연결이 종료 => **서버는 3분마다 ping frame을 보내야 됨**

```
서버 → ping
클라이언트 → 같은 payload를 복사해서 pong
```

```
ping을 받으면 가능한 빨리 pong 응답
pong payload는 ping payload와 동일해야 함
클라이언트가 먼저 보내는 unsolicited pong도 허용됨
하지만 unsolicited pong만으로는 연결 유지 보장 안 됨
```

#### User Data Stream 연결 필요
- 실제 계정 이벤트 수신을 위해 별도 WebSocket 연결 필요

-----

## User Data Stream [private] 연결 순서
- **Endpoint** : `wss://fstream.binance.com/private/ws/<listenKey>`
- 내 계정 상태 변화 이벤트를 받는 용도[계정 정보, 주문 체결/수정/삭제 이벤트 등] 수신

```
ORDER_TRADE_UPDATE
ACCOUNT_UPDATE
MARGIN_CALL
ACCOUNT_CONFIG_UPDATE
listenKeyExpired
```

#### 1. listenKey 생성
`[POST] https://fapi.binance.com/fapi/v1/listenKey`
```
{
  "listenKey": "XaEAKTsQSRLZAGH9tuIu3..."
}
```

#### 2. WebSocket 연결
`wss://fstream.binance.com/private/ws/<listenKey>`

#### 3. listenKey 유효시간
- listenKey는 생성 후 60분 동안 유효 => **계속 사용하려면 주기적으로 연장**

`[PUT] https://fapi.binance.com/fapi/v1/listenKey`
=> **실무에서는 30분마다 연장**
```
POST listenKey 생성
→ WebSocket 연결
→ 30분마다 PUT으로 연장
→ 연결 끊기면 재연결
```

#### 4. listenKey가 사라졌을 때
PUT 요청 시 아래 에러가 나오면:
`-1125 This listenKey does not exist.`
=> **기존 listenKey가 만료되었거나 유효하지 않은 상태**

=> **새로 만들어야 됨**
`[POST] /fapi/v1/listenKey`

#### 5. DELETE
User Data Stream을 닫고 싶으면: => `[DELETE] /fapi/v1/listenKey`