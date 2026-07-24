# CEX Market Data Collector

Cross-exchange BTC perpetual market-data collectors for portfolio research,
downstream strategy features, and operational ingestion.

This package has two modes:

- `snapshot collector`: one-shot REST collection for notebooks and audits.
- `operational collector`: long-running WebSocket + REST polling runtime for
  production-style ingestion.

The package normalizes three market data types:

- `trade`: real-time trade prints when a venue exposes a practical public
  WebSocket stream.
- `orderbook`: bid/ask depth levels for microstructure features such as spread,
  depth imbalance, and local liquidity.
- `open_interest`: outstanding derivatives positions for leverage, crowding,
  and liquidation-risk features.

## Why This Is Separate From The Existing Binance Trade Collector

`apps/collectors/src/exchange/binance/perp/python` is an execution-grade Binance trade stream
collector with Binance-specific gap repair. This package extracts the
cross-exchange parts into adapter-driven collection:

- common WebSocket runtime
- common REST poll runtime
- common event sink abstraction
- per-exchange adapters for endpoint and payload differences

Binance gap repair remains in the Binance-specific collector because other
venues do not share Binance's trade-id semantics or historical-trade recovery
API.

## Supported Venues

The adapter list is intentionally explicit:

```text
binance, bybit, okx, bitget, gate, mexc, kucoin, bingx, htx, kraken, bitfinex, lbank
```

Each exchange module returns the original raw payload together with normalized
fields so the notebook can audit suspicious values.

## Exchange Module Layout

Exchange-specific endpoint and symbol logic does not live in this shared
package. It lives under each exchange directory:

```text
apps/collectors/src/exchange/{exchange}/perp/python/src/{exchange}_perp_collector/
  rest.py          # REST orderbook/open-interest adapter
  operational.py   # WebSocket trade/orderbook spec and normalizer
```

The shared package only provides:

- common models
- common HTTP client
- common WebSocket runtime
- common sink abstraction
- dynamic module loading

## Run Snapshot Collector

From the repository root:

```bash
PYTHONPATH=apps/collectors/src/exchange/shared/cex_market_data/python/src \
uv run python -m cex_market_data_collector.main \
  --exchanges binance,bybit,okx,bitget,gate \
  --depth 20
```

For line-delimited output:

```bash
PYTHONPATH=apps/collectors/src/exchange/shared/cex_market_data/python/src \
uv run python -m cex_market_data_collector.main \
  --exchanges binance,bybit,okx \
  --depth 20 \
  --jsonl
```

## Run Operational Collector

Local stdout validation:

```bash
PYTHONPATH=apps/collectors/src/exchange/shared/cex_market_data/python/src \
uv run python -m cex_market_data_collector.operational_main \
  --exchanges binance,bybit,okx \
  --sink stdout \
  --oi-interval-s 60
```

Redpanda output:

```bash
PYTHONPATH=apps/collectors/src/exchange/shared/cex_market_data/python/src \
uv run python -m cex_market_data_collector.operational_main \
  --exchanges binance,bybit,okx,bitget,gate,mexc,kraken,htx \
  --sink redpanda \
  --redpanda-brokers localhost:9092
```

Topic naming:

```text
market.mixed.{exchange}.perp
market.open_interest.{exchange}.perp
```

These names are built through `common.market_naming`. Defaults preserve the
existing topic shape, but deployment-specific naming can be changed with:

```bash
MARKET_TOPIC_PREFIX=market
MARKET_TOPIC_MARKET_TYPE=perp
MARKET_PIPELINE_EXCHANGES=binance,bybit,okx,bitget
MARKET_REDIS_STREAM_PREFIX=market
```

`market.mixed.*` contains normalized `trade` and `orderbook` records. The record
itself includes `data_type`.

## Normalization Notes

Open interest units differ by exchange:

- Some venues report base coin amount.
- Some report contract count and require a multiplier.
- Some report USD/notional value directly.
- Some endpoints expose only a current snapshot, not reliable historical data.

For this reason, use `open_interest_value_usd` for cross-exchange aggregation
when present, and preserve `raw` for audit. If only `open_interest` exists, treat
it as venue-local until a unit conversion is verified.

## Operational Coverage

Current operational WebSocket coverage:

| Exchange | Trade WS | Orderbook WS | Open Interest |
|---|---:|---:|---:|
| Binance | Yes | Yes | REST poll |
| Bybit | Yes | Yes | REST poll |
| OKX | Yes | Yes | REST poll |
| Bitget | Yes | Yes | REST poll |
| Gate | Yes | Yes | REST poll |
| MEXC | Yes | Yes | REST poll |
| Kraken | Yes | Yes | REST poll |
| HTX | Yes | Yes | REST poll |
| Bitfinex | Yes | Partial | REST poll |
| KuCoin | Pending tokenized WS | Pending tokenized WS | REST poll |
| BingX | Pending live WS validation | Pending live WS validation | REST poll |
| LBank | Pending live WS validation | Pending live WS validation | REST poll |

The pending venues still have operational open-interest polling and adapter
slots. Trade/depth streaming should be enabled only after live endpoint
validation because their public WebSocket handshake or region-specific endpoints
are less stable.
