# Collectors

Redpanda Connect (구 Benthos) 기반의 시장 데이터 수집 파이프라인 설정 파일 모음입니다.

각 YAML 파일은 **하나의 독립적인 Redpanda Connect 프로세스**로 실행되며,
거래소 WebSocket에서 실시간 데이터를 수신하고, 정규화(Normalize)한 뒤 Redpanda 토픽으로 전송합니다.

## 디렉토리 구조

```
collectors/
├── exchange/
│   ├── binance/
│   │   ├── perp/        # Binance perpetual 운영형 collector
│   │   └── spot/        # Binance spot collector
│   ├── bybit/
│   │   └── perp/
│   ├── okx/
│   │   └── perp/
│   ├── htx/
│   │   └── perp/
│   ├── kraken/
│   │   └── perp/
│   ├── ...              # bitget, gate, mexc, kucoin, bingx, lbank, bitfinex
│   └── shared/
│       └── cex_market_data/
│           ├── python/  # Cross-CEX shared runtime
│           └── docs/
└── README.md
```

## 데이터 흐름

```
거래소 WebSocket ──▶ Redpanda Connect ──▶ Redpanda Topic
                     (정규화 처리)
```

## 토픽 네이밍 규칙

`market.{data_type}.{exchange}.{market_type}`

운영 수집기의 토픽/Redis stream key는 `common.market_naming`에서 생성합니다.
기본값은 기존 규칙을 유지하며, 환경별로 다음 값을 조정할 수 있습니다:

```bash
MARKET_TOPIC_PREFIX=market
MARKET_TOPIC_MARKET_TYPE=perp
MARKET_PIPELINE_EXCHANGES=binance,bybit,okx,bitget
MARKET_REDIS_STREAM_PREFIX=market
```

| 토픽 | 설명 |
|------|------|
| `market.trades.binance.spot` | 바이낸스 현물 체결 |
| `market.ticker.binance.spot` | 바이낸스 현물 티커 |
| `market.kline.binance.spot` | 바이낸스 현물 1분봉 |
| `market.trades.okx.spot` | OKX 현물 체결 |
| `market.ticker.okx.spot` | OKX 현물 티커 |
| `market.kline.okx.spot` | OKX 현물 1분봉 |

## Cross-CEX Market Data

`exchange/shared/cex_market_data/python`은 Binance trade collector와 별도로 둔
크로스 거래소 시장 데이터 수집기입니다.

- Snapshot mode: 여러 거래소의 BTC perpetual `orderbook`과
  `open_interest`를 1회 조회해 같은 레코드 형태로 정규화합니다.
- Operational mode: 공통 WebSocket/REST polling 런타임으로
  `trade`, `orderbook`, `open_interest`를 지속 수집합니다.
- Exchange modules: 거래소별 REST/WebSocket endpoint, subscribe payload,
  normalizer는 `apps/collectors/src/exchange/{exchange}/perp/python/src/{exchange}_perp_collector/`
  아래의 `rest.py`, `operational.py`에서 개별 관리합니다.

```bash
PYTHONPATH=apps/collectors/src/exchange/shared/cex_market_data/python/src \
uv run python -m cex_market_data_collector.main \
  --exchanges binance,bybit,okx,bitget,gate \
  --depth 20
```

```bash
PYTHONPATH=apps/collectors/src/exchange/shared/cex_market_data/python/src \
uv run python -m cex_market_data_collector.operational_main \
  --exchanges binance,bybit,okx \
  --sink stdout \
  --oi-interval-s 60
```

자세한 내용은 `apps/collectors/src/exchange/shared/cex_market_data/docs/README.md`를 참고하세요.

## 실행 방법

### 사전 요구 사항
- Redpanda 실행 중 (`make dev`)
- `rpk connect` CLI 설치 (또는 Docker 이미지)

### 환경 변수
| 변수 | 기본값 | 설명 |
|------|--------|------|
| `REDPANDA_BROKERS` | `localhost:9092` | Redpanda 브로커 주소 |

### 심볼 추가
거래소별 endpoint, subscribe payload, symbol 설정은
`apps/collectors/src/exchange/{exchange}/{market_type}/...` 아래에서 관리합니다.
공통 shared runtime에는 거래소별 심볼/endpoint를 넣지 않습니다.

## 정규화된 출력 스키마

모든 거래소의 trade 데이터는 아래 형식으로 통일됩니다:

```json
{
  "exchange": "<exchange>",
  "market_type": "<market_type>",
  "symbol": "<symbol>",
  "price": 64521.30,
  "size": 0.0012,
  "is_buyer_maker": false,
  "exchange_ts": 1714280000123,
  "local_ts": 1714280000135,
  "trade_id": "3847291"
}
```

이 형식은 `packages/schemas/src/schemas/market.py`의 `Trade` 모델과 일치합니다.
