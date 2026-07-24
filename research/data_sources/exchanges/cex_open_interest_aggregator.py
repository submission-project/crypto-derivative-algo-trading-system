"""Cross-exchange BTC perpetual open-interest collection.

Snapshot collection covers more venues. Historical collection is intentionally
limited to venues with verified public historical OI endpoints.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Sequence
import pandas as pd
import requests


DEFAULT_TOP_EXCHANGES: tuple[str, ...] = (
    "binance",
    "bybit",
    "okx",
    "bitget",
    "gate",
    "kucoin",
    "mexc",
    "deribit",
    "kraken",
    "bingx",
    # "htx",
)

DEFAULT_HISTORICAL_EXCHANGES: tuple[str, ...] = (
    "binance",
    "bybit",
    "gate",
    "okx",
    # "htx",
)


@dataclass(frozen=True, slots=True)
class OpenInterestSnapshot:
    """Normalized open-interest snapshot for one exchange.

    `open_interest_amount` is the base-asset amount when the exchange response
    makes that practical. `open_interest_value` is normalized to quote/USD
    notional and is the safer field to aggregate across venues.
    """

    exchange: str
    symbol: str
    open_interest_amount: float | None
    open_interest_value: float | None
    timestamp_ms: int | None
    datetime: str | None
    raw: Any
    error: str | None = None


@dataclass(frozen=True, slots=True)
class AggregatedOpenInterest:
    """Summary statistics for cross-exchange open interest."""

    total_open_interest_value: float
    covered_exchange_count: int
    requested_exchange_count: int
    shares: Mapping[str, float]
    hhi: float
    binance_share: float | None
    errors: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class OpenInterestHistoryPoint:
    """Normalized historical OI point for one exchange and timestamp."""

    exchange: str
    symbol: str
    timestamp_ms: int
    datetime: str
    open_interest_amount: float | None
    open_interest_value: float | None
    raw: Any
    note: str | None = None


def _now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def _ms_to_iso(timestamp_ms: int | None) -> str | None:
    if timestamp_ms is None:
        return None
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat()


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_float(mapping: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _to_float(mapping.get(key))
        if value is not None:
            return value
    return None


def _first_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, list) and value:
        first = value[0]
        if isinstance(first, Mapping):
            return first
    if isinstance(value, Mapping):
        return value
    return {}


def _history_point(
    *,
    exchange: str,
    symbol: str,
    timestamp_ms: int,
    amount: float | None,
    value: float | None,
    raw: Any,
    note: str | None = None,
) -> OpenInterestHistoryPoint:
    return OpenInterestHistoryPoint(
        exchange=exchange,
        symbol=symbol,
        timestamp_ms=timestamp_ms,
        datetime=_ms_to_iso(timestamp_ms) or "",
        open_interest_amount=amount,
        open_interest_value=value,
        raw=raw,
        note=note,
    )


def _lookup_reference_price(
    reference_prices: Mapping[int, float] | None,
    timestamp_ms: int,
    *,
    tolerance_ms: int = 5 * 60 * 1000,
) -> float | None:
    if not reference_prices:
        return None
    exact = reference_prices.get(timestamp_ms)
    if exact is not None:
        return float(exact)
    if not reference_prices:
        return None
    nearest_ts = min(reference_prices, key=lambda ts: abs(ts - timestamp_ms))
    if abs(nearest_ts - timestamp_ms) <= tolerance_ms:
        return float(reference_prices[nearest_ts])
    return None


def _get_json(url: str, *, timeout_s: float) -> Any:
    response = requests.get(
        url,
        timeout=timeout_s,
        headers={"User-Agent": "takora-research/0.1"},
    )
    response.raise_for_status()
    return response.json()


def _dedupe_history_points(
    points: Sequence[OpenInterestHistoryPoint],
    *,
    limit: int | None = None,
) -> list[OpenInterestHistoryPoint]:
    deduped: dict[tuple[str, int], OpenInterestHistoryPoint] = {}
    for point in points:
        deduped[(point.exchange, point.timestamp_ms)] = point
    ordered = sorted(deduped.values(), key=lambda point: point.timestamp_ms)
    if limit is not None and limit > 0:
        return ordered[-limit:]
    return ordered


def _previous_end_time(points: Sequence[OpenInterestHistoryPoint]) -> int | None:
    if not points:
        return None
    return min(point.timestamp_ms for point in points) - 1


def _fetch_binance_history(
    *,
    period: str,
    limit: int,
    start_time_ms: int | None,
    end_time_ms: int | None,
    timeout_s: float,
) -> list[OpenInterestHistoryPoint]:
    api_limit = 500
    current_end = end_time_ms
    points: list[OpenInterestHistoryPoint] = []

    while len(points) < limit:
        request_limit = min(api_limit, max(limit - len(points), 1))
        url = (
            "https://fapi.binance.com/futures/data/openInterestHist"
            f"?symbol=BTCUSDT&period={period}&limit={request_limit}"
        )
        if start_time_ms is not None:
            url += f"&startTime={start_time_ms}"
        if current_end is not None:
            url += f"&endTime={current_end}"
        data = _get_json(url, timeout_s=timeout_s)
        if not isinstance(data, list) or not data:
            break

        batch = [
            _history_point(
                exchange="binance",
                symbol="BTCUSDT",
                timestamp_ms=int(row["timestamp"]),
                amount=_to_float(row.get("sumOpenInterest")),
                value=_to_float(row.get("sumOpenInterestValue")),
                raw=row,
            )
            for row in data
        ]
        points.extend(batch)
        next_end = _previous_end_time(batch)
        if next_end is None or next_end == current_end:
            break
        if start_time_ms is not None and next_end < start_time_ms:
            break
        if len(batch) < request_limit:
            break
        current_end = next_end

    return _dedupe_history_points(points, limit=limit)


def _fetch_bybit_history(
    *,
    period: str,
    limit: int,
    start_time_ms: int | None,
    end_time_ms: int | None,
    reference_prices: Mapping[int, float] | None,
    timeout_s: float,
) -> list[OpenInterestHistoryPoint]:
    interval = "5min" if period == "5m" else period
    api_limit = 200
    current_end = end_time_ms
    points: list[OpenInterestHistoryPoint] = []

    while len(points) < limit:
        request_limit = min(api_limit, max(limit - len(points), 1))
        url = (
            "https://api.bybit.com/v5/market/open-interest"
            f"?category=linear&symbol=BTCUSDT&intervalTime={interval}&limit={request_limit}"
        )
        if start_time_ms is not None:
            url += f"&startTime={start_time_ms}"
        if current_end is not None:
            url += f"&endTime={current_end}"
        data = _get_json(url, timeout_s=timeout_s)
        rows = _first_mapping(data.get("result")).get("list", [])
        if not rows:
            break

        batch: list[OpenInterestHistoryPoint] = []
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            timestamp_ms = int(_to_float(row.get("timestamp")) or 0)
            amount = _to_float(row.get("openInterest"))
            ref_price = _lookup_reference_price(reference_prices, timestamp_ms)
            value = amount * ref_price if amount is not None and ref_price is not None else None
            batch.append(
                _history_point(
                    exchange="bybit",
                    symbol="BTCUSDT",
                    timestamp_ms=timestamp_ms,
                    amount=amount,
                    value=value,
                    raw=row,
                    note="openInterest amount multiplied by reference BTCUSDT close",
                )
            )
        points.extend(batch)
        next_end = _previous_end_time(batch)
        if next_end is None or next_end == current_end:
            break
        if start_time_ms is not None and next_end < start_time_ms:
            break
        if len(rows) < request_limit:
            break
        current_end = next_end

    return _dedupe_history_points(points, limit=limit)


def _fetch_gate_history(
    *,
    period: str,
    limit: int,
    start_time_ms: int | None,
    end_time_ms: int | None,
    timeout_s: float,
) -> list[OpenInterestHistoryPoint]:
    api_limit = 1000
    current_end = end_time_ms
    points: list[OpenInterestHistoryPoint] = []

    while len(points) < limit:
        request_limit = min(api_limit, max(limit - len(points), 1))
        url = f"https://api.gateio.ws/api/v4/futures/usdt/contract_stats?contract=BTC_USDT&limit={request_limit}"
        if start_time_ms is not None:
            url += f"&from={start_time_ms // 1000}"
        if current_end is not None:
            url += f"&to={current_end // 1000}"
        data = _get_json(url, timeout_s=timeout_s)
        if not isinstance(data, list) or not data:
            break

        batch: list[OpenInterestHistoryPoint] = []
        for row in data:
            if not isinstance(row, Mapping):
                continue
            timestamp = _to_float(row.get("time"))
            if timestamp is None:
                continue
            timestamp_ms = int(timestamp * 1000)
            batch.append(
                _history_point(
                    exchange="gate",
                    symbol="BTC_USDT",
                    timestamp_ms=timestamp_ms,
                    amount=_to_float(row.get("open_interest")),
                    value=_to_float(row.get("open_interest_usd")),
                    raw=row,
                )
            )
        points.extend(batch)
        next_end = _previous_end_time(batch)
        if next_end is None or next_end == current_end:
            break
        if start_time_ms is not None and next_end < start_time_ms:
            break
        if len(data) < request_limit:
            break
        current_end = next_end

    return _dedupe_history_points(points, limit=limit)


def _fetch_okx_history(
    *,
    period: str,
    limit: int,
    start_time_ms: int | None,
    end_time_ms: int | None,
    timeout_s: float,
) -> list[OpenInterestHistoryPoint]:
    api_limit = 100
    current_end = end_time_ms
    points: list[OpenInterestHistoryPoint] = []

    while len(points) < limit:
        url = (
            "https://www.okx.com/api/v5/rubik/stat/contracts/open-interest-volume"
            f"?ccy=BTC&period={period}"
        )
        if start_time_ms is not None:
            url += f"&begin={start_time_ms}"
        if current_end is not None:
            url += f"&end={current_end}"
        data = _get_json(url, timeout_s=timeout_s)
        rows = data.get("data", []) if isinstance(data, Mapping) else []
        if not rows:
            break

        batch: list[OpenInterestHistoryPoint] = []
        for row in rows[:api_limit]:
            if not isinstance(row, list) or len(row) < 2:
                continue
            timestamp_ms = int(_to_float(row[0]) or 0)
            batch.append(
                _history_point(
                    exchange="okx",
                    symbol="BTC_CONTRACTS",
                    timestamp_ms=timestamp_ms,
                    amount=None,
                    value=_to_float(row[1]),
                    raw=row,
                    note="OKX rubik endpoint is BTC contracts aggregate OI",
                )
            )
        points.extend(batch)
        next_end = _previous_end_time(batch)
        if next_end is None or next_end == current_end:
            break
        if start_time_ms is not None and next_end < start_time_ms:
            break
        if len(batch) < min(api_limit, max(limit - len(points) + len(batch), 1)):
            break
        current_end = next_end

    return _dedupe_history_points(points, limit=limit)


# def _fetch_htx_history(
#     *,
#     period: str,
#     limit: int,
#     start_time_ms: int | None,
#     end_time_ms: int | None,
#     timeout_s: float,
# ) -> list[OpenInterestHistoryPoint]:
#     htx_period = "5min" if period == "5m" else period
#     api_limit = 200
#     current_end = end_time_ms
#     points: list[OpenInterestHistoryPoint] = []

#     while len(points) < limit:
#         request_limit = min(api_limit, max(limit - len(points), 1))
#         url = (
#             "https://api.hbdm.com/linear-swap-api/v1/swap_his_open_interest"
#             f"?contract_code=BTC-USDT&period={htx_period}&amount_type=1&size={request_limit}"
#         )
#         data = _get_json(url, timeout_s=timeout_s)
#         rows = _first_mapping(data.get("data")).get("tick", []) if isinstance(data, Mapping) else []
#         if not rows:
#             break

#         batch: list[OpenInterestHistoryPoint] = []
#         for row in rows:
#             if not isinstance(row, Mapping):
#                 continue
#             timestamp_ms = int(_to_float(row.get("ts")) or 0)
#             if start_time_ms is not None and timestamp_ms < start_time_ms:
#                 continue
#             if current_end is not None and timestamp_ms > current_end:
#                 continue
#             batch.append(
#                 _history_point(
#                     exchange="htx",
#                     symbol="BTC-USDT",
#                     timestamp_ms=timestamp_ms,
#                     amount=_to_float(row.get("volume")),
#                     value=_to_float(row.get("value")),
#                     raw=row,
#                     note="HTX linear swap historical OI value",
#                 )
#             )
#         points.extend(batch)

#         # HTX public endpoint does not expose an end-time parameter for this
#         # route, so repeated calls would return the same latest window.
#         break

#     return _dedupe_history_points(points, limit=limit)


def _snapshot(
    *,
    exchange: str,
    symbol: str = "BTCUSDT",
    amount: float | None,
    value: float | None,
    timestamp_ms: int | None,
    raw: Any,
    error: str | None = None,
) -> OpenInterestSnapshot:
    return OpenInterestSnapshot(
        exchange=exchange,
        symbol=symbol,
        open_interest_amount=amount,
        open_interest_value=value,
        timestamp_ms=timestamp_ms,
        datetime=_ms_to_iso(timestamp_ms),
        raw=raw,
        error=error,
    )


def _error_snapshot(exchange: str, error: str) -> OpenInterestSnapshot:
    return _snapshot(
        exchange=exchange,
        amount=None,
        value=None,
        timestamp_ms=_now_ms(),
        raw={},
        error=error,
    )


def _fetch_binance(timeout_s: float) -> OpenInterestSnapshot:
    oi = _get_json(
        "https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT",
        timeout_s=timeout_s,
    )
    mark = _get_json(
        "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT",
        timeout_s=timeout_s,
    )
    amount = _to_float(oi.get("openInterest"))
    mark_price = _to_float(mark.get("markPrice"))
    value = amount * mark_price if amount is not None and mark_price is not None else None
    timestamp_ms = int(_to_float(oi.get("time")) or _to_float(mark.get("time")) or _now_ms())
    return _snapshot(
        exchange="binance",
        amount=amount,
        value=value,
        timestamp_ms=timestamp_ms,
        raw={"open_interest": oi, "mark_price": mark},
    )


def _fetch_bybit(timeout_s: float) -> OpenInterestSnapshot:
    oi = _get_json(
        "https://api.bybit.com/v5/market/open-interest"
        "?category=linear&symbol=BTCUSDT&intervalTime=5min&limit=1",
        timeout_s=timeout_s,
    )
    ticker = _get_json(
        "https://api.bybit.com/v5/market/tickers?category=linear&symbol=BTCUSDT",
        timeout_s=timeout_s,
    )
    item = _first_mapping(_first_mapping(oi.get("result")).get("list"))
    ticker_item = _first_mapping(_first_mapping(ticker.get("result")).get("list"))
    amount = _to_float(item.get("openInterest"))
    mark_price = _first_float(ticker_item, "markPrice", "lastPrice")
    value = amount * mark_price if amount is not None and mark_price is not None else None
    timestamp_ms = int(_to_float(item.get("timestamp")) or _to_float(ticker_item.get("time")) or _now_ms())
    return _snapshot(
        exchange="bybit",
        amount=amount,
        value=value,
        timestamp_ms=timestamp_ms,
        raw={"open_interest": oi, "ticker": ticker},
    )


def _fetch_okx(timeout_s: float) -> OpenInterestSnapshot:
    oi = _get_json(
        "https://www.okx.com/api/v5/public/open-interest?instType=SWAP&instId=BTC-USDT-SWAP",
        timeout_s=timeout_s,
    )
    item = _first_mapping(oi.get("data"))
    amount = _first_float(item, "oiCcy", "oi")
    value = _first_float(item, "oiUsd")
    timestamp_ms = int(_to_float(item.get("ts")) or _now_ms())
    return _snapshot(
        exchange="okx",
        amount=amount,
        value=value,
        timestamp_ms=timestamp_ms,
        raw={"open_interest": oi},
    )


def _fetch_bitget(timeout_s: float) -> OpenInterestSnapshot:
    oi = _get_json(
        "https://api.bitget.com/api/v2/mix/market/open-interest"
        "?symbol=BTCUSDT&productType=USDT-FUTURES",
        timeout_s=timeout_s,
    )
    ticker = _get_json(
        "https://api.bitget.com/api/v2/mix/market/ticker"
        "?symbol=BTCUSDT&productType=USDT-FUTURES",
        timeout_s=timeout_s,
    )
    item = _first_mapping(_first_mapping(oi.get("data")).get("openInterestList")) or _first_mapping(oi.get("data"))
    ticker_item = _first_mapping(ticker.get("data"))
    amount = _first_float(item, "size", "openInterest", "holdingAmount", "amount")
    mark_price = _first_float(ticker_item, "markPrice", "lastPr", "last", "indexPrice")
    value = _first_float(item, "openInterestValue", "openInterestUsd", "value")
    if value is None and amount is not None and mark_price is not None:
        value = amount * mark_price
    timestamp_ms = int(_first_float(item, "ts", "timestamp") or _first_float(ticker_item, "ts", "timestamp") or _now_ms())
    return _snapshot(
        exchange="bitget",
        amount=amount,
        value=value,
        timestamp_ms=timestamp_ms,
        raw={"open_interest": oi, "ticker": ticker},
    )


def _fetch_gate(timeout_s: float) -> OpenInterestSnapshot:
    stats = _get_json(
        "https://api.gateio.ws/api/v4/futures/usdt/contract_stats?contract=BTC_USDT&limit=1",
        timeout_s=timeout_s,
    )
    contract = _get_json(
        "https://api.gateio.ws/api/v4/futures/usdt/contracts/BTC_USDT",
        timeout_s=timeout_s,
    )
    item = _first_mapping(stats)
    open_interest_contracts = _first_float(item, "open_interest", "openInterest")
    multiplier = _first_float(contract, "quanto_multiplier", "multiplier") or 1.0
    amount = open_interest_contracts * multiplier if open_interest_contracts is not None else None
    mark_price = _first_float(item, "mark_price", "markPrice", "last")
    value = _first_float(item, "open_interest_usd", "openInterestUsd")
    if value is None and amount is not None and mark_price is not None:
        value = amount * mark_price
    timestamp_ms = int((_first_float(item, "time_ms") or _first_float(item, "time") or _now_ms()))
    if timestamp_ms < 10_000_000_000:
        timestamp_ms *= 1000
    return _snapshot(
        exchange="gate",
        amount=amount,
        value=value,
        timestamp_ms=timestamp_ms,
        raw={"stats": stats, "contract": contract},
    )


def _fetch_kucoin(timeout_s: float) -> OpenInterestSnapshot:
    data = _get_json(
        "https://api-futures.kucoin.com/api/v1/contracts/XBTUSDTM",
        timeout_s=timeout_s,
    )
    item = _first_mapping(data.get("data"))
    open_interest = _first_float(item, "openInterest")
    multiplier = _first_float(item, "multiplier") or 1.0
    lot_size = _first_float(item, "lotSize") or 1.0
    mark_price = _first_float(item, "markPrice", "indexPrice", "lastTradePrice")
    amount = open_interest * multiplier * lot_size if open_interest is not None else None
    value = amount * mark_price if amount is not None and mark_price is not None else None
    timestamp_ms = int(_first_float(item, "timestamp") or _now_ms())
    return _snapshot(
        exchange="kucoin",
        symbol="XBTUSDTM",
        amount=amount,
        value=value,
        timestamp_ms=timestamp_ms,
        raw={"contract": data},
    )


def _fetch_mexc(timeout_s: float) -> OpenInterestSnapshot:
    ticker = _get_json(
        "https://contract.mexc.com/api/v1/contract/ticker?symbol=BTC_USDT",
        timeout_s=timeout_s,
    )
    detail = _get_json(
        "https://contract.mexc.com/api/v1/contract/detail?symbol=BTC_USDT",
        timeout_s=timeout_s,
    )
    item = _first_mapping(ticker.get("data"))
    detail_item = _first_mapping(detail.get("data"))
    hold_vol = _first_float(item, "holdVol", "openInterest")
    contract_size = _first_float(detail_item, "contractSize") or 1.0
    mark_price = _first_float(item, "fairPrice", "indexPrice", "lastPrice")
    amount = hold_vol * contract_size if hold_vol is not None else None
    value = amount * mark_price if amount is not None and mark_price is not None else None
    timestamp_ms = int(_first_float(item, "timestamp") or _now_ms())
    return _snapshot(
        exchange="mexc",
        symbol="BTC_USDT",
        amount=amount,
        value=value,
        timestamp_ms=timestamp_ms,
        raw={"ticker": ticker, "detail": detail},
    )


def _fetch_deribit(timeout_s: float) -> OpenInterestSnapshot:
    ticker = _get_json(
        "https://www.deribit.com/api/v2/public/ticker?instrument_name=BTC-PERPETUAL",
        timeout_s=timeout_s,
    )
    item = _first_mapping(ticker.get("result"))
    value = _first_float(item, "open_interest")
    mark_price = _first_float(item, "mark_price", "index_price", "last_price")
    amount = value / mark_price if value is not None and mark_price else None
    timestamp_ms = int(_first_float(item, "timestamp") or _now_ms())
    return _snapshot(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        amount=amount,
        value=value,
        timestamp_ms=timestamp_ms,
        raw={"ticker": ticker},
    )


def _fetch_kraken(timeout_s: float) -> OpenInterestSnapshot:
    ticker = _get_json(
        "https://futures.kraken.com/derivatives/api/v3/tickers/PF_XBTUSD",
        timeout_s=timeout_s,
    )
    instruments = _get_json(
        "https://futures.kraken.com/derivatives/api/v3/instruments",
        timeout_s=timeout_s,
    )
    item = _first_mapping(ticker.get("ticker"))
    instrument = next(
        (
            row
            for row in instruments.get("instruments", [])
            if isinstance(row, Mapping) and row.get("symbol") == "PF_XBTUSD"
        ),
        {},
    )
    amount = _first_float(item, "openInterest")
    mark_price = _first_float(item, "markPrice", "last", "bid", "ask")
    contract_size = _first_float(instrument, "contractSize") or 1.0
    value = amount * mark_price * contract_size if amount is not None and mark_price is not None else None
    timestamp_ms = _now_ms()
    return _snapshot(
        exchange="kraken",
        symbol="PF_XBTUSD",
        amount=amount,
        value=value,
        timestamp_ms=timestamp_ms,
        raw={"ticker": ticker, "instrument": instrument},
    )


def _fetch_bingx(timeout_s: float) -> OpenInterestSnapshot:
    oi = _get_json(
        "https://open-api.bingx.com/openApi/swap/v2/quote/openInterest?symbol=BTC-USDT",
        timeout_s=timeout_s,
    )
    ticker = _get_json(
        "https://open-api.bingx.com/openApi/swap/v2/quote/ticker?symbol=BTC-USDT",
        timeout_s=timeout_s,
    )
    item = _first_mapping(oi.get("data"))
    ticker_item = _first_mapping(ticker.get("data"))
    mark_price = _first_float(ticker_item, "lastPrice", "markPrice", "indexPrice")
    value = _first_float(item, "openInterestValue", "value", "openInterest")
    # BingX reports BTC-USDT openInterest in notional/value terms. The original
    # collector used ccxt's openInterestValue directly; multiplying this field
    # by BTC price again overstates OI by roughly one BTC price.
    amount = value / mark_price if value is not None and mark_price else None
    timestamp_ms = int(_first_float(item, "time", "timestamp") or _first_float(ticker_item, "time", "timestamp") or _now_ms())
    return _snapshot(
        exchange="bingx",
        symbol="BTC-USDT",
        amount=amount,
        value=value,
        timestamp_ms=timestamp_ms,
        raw={"open_interest": oi, "ticker": ticker},
    )


# def _fetch_htx(timeout_s: float) -> OpenInterestSnapshot:
#     oi = _get_json(
#         "https://api.hbdm.com/linear-swap-api/v1/swap_open_interest?contract_code=BTC-USDT",
#         timeout_s=timeout_s,
#     )
#     data = _first_mapping(oi.get("data")) if isinstance(oi, Mapping) else {}
#     amount = _first_float(data, "volume", "amount")
#     value = _first_float(data, "value")
#     timestamp_ms = int(_to_float(oi.get("ts")) or _now_ms()) if isinstance(oi, Mapping) else _now_ms()
#     return _snapshot(
#         exchange="htx",
#         symbol="BTC-USDT",
#         amount=amount,
#         value=value,
#         timestamp_ms=timestamp_ms,
#         raw={"open_interest": oi},
#     )


_FETCHERS: Mapping[str, Callable[[float], OpenInterestSnapshot]] = {
    "binance": _fetch_binance,
    "bybit": _fetch_bybit,
    "okx": _fetch_okx,
    "bitget": _fetch_bitget,
    "gate": _fetch_gate,
    "kucoin": _fetch_kucoin,
    "mexc": _fetch_mexc,
    "deribit": _fetch_deribit,
    "kraken": _fetch_kraken,
    "bingx": _fetch_bingx,
    # "htx": _fetch_htx,
}


def collect_historical_open_interest(
    exchanges: Sequence[str] = DEFAULT_HISTORICAL_EXCHANGES,
    *,
    period: str = "5m",
    limit: int = 200,
    start_time_ms: int | None = None,
    end_time_ms: int | None = None,
    reference_prices: Mapping[int, float] | None = None,
    timeout_s: float = 10.0,
) -> list[OpenInterestHistoryPoint]:
    """Fetch historical OI points from venues with verified history endpoints.

    `reference_prices` is used for venues such as Bybit that return OI amount
    but not historical notional value. Use BTCUSDT close prices keyed by
    millisecond timestamp.
    """

    points: list[OpenInterestHistoryPoint] = []
    normalized = [exchange.lower() for exchange in exchanges]
    for exchange in normalized:
        if exchange == "binance":
            points.extend(
                _fetch_binance_history(
                    period=period,
                    limit=limit,
                    start_time_ms=start_time_ms,
                    end_time_ms=end_time_ms,
                    timeout_s=timeout_s,
                )
            )
        elif exchange == "bybit":
            points.extend(
                _fetch_bybit_history(
                    period=period,
                    limit=limit,
                    start_time_ms=start_time_ms,
                    end_time_ms=end_time_ms,
                    reference_prices=reference_prices,
                    timeout_s=timeout_s,
                )
            )
        elif exchange == "gate":
            points.extend(
                _fetch_gate_history(
                    period=period,
                    limit=limit,
                    start_time_ms=start_time_ms,
                    end_time_ms=end_time_ms,
                    timeout_s=timeout_s,
                )
            )
        elif exchange == "okx":
            points.extend(
                _fetch_okx_history(
                    period=period,
                    limit=limit,
                    start_time_ms=start_time_ms,
                    end_time_ms=end_time_ms,
                    timeout_s=timeout_s,
                )
            )
        # elif exchange == "htx":
        #     points.extend(
        #         _fetch_htx_history(
        #             period=period,
        #             limit=limit,
        #             start_time_ms=start_time_ms,
        #             end_time_ms=end_time_ms,
        #             timeout_s=timeout_s,
        #         )
        #     )
        else:
            continue
    return sorted(points, key=lambda point: (point.timestamp_ms, point.exchange))


def historical_open_interest_points_to_records(
    points: Sequence[OpenInterestHistoryPoint],
) -> list[dict[str, Any]]:
    """Convert historical OI points to records suitable for pandas or polars."""

    return [
        {
            "exchange": point.exchange,
            "symbol": point.symbol,
            "timestamp_ms": point.timestamp_ms,
            "datetime": point.datetime,
            "open_interest_amount": point.open_interest_amount,
            "open_interest_value": point.open_interest_value,
            "note": point.note,
        }
        for point in points
    ]


def historical_open_interest_points_to_frame(points: Sequence[OpenInterestHistoryPoint]):
    """Return a pandas DataFrame of normalized historical OI points."""

    return pd.DataFrame(historical_open_interest_points_to_records(points))


def aggregate_historical_open_interest_frame(
    points: Sequence[OpenInterestHistoryPoint],
    *,
    min_exchange_count: int = 2,
):
    """Aggregate historical OI values by timestamp across exchanges."""


    exchanges = sorted(list({p.exchange for p in points if p.exchange}))
    if "binance" not in exchanges:
        exchanges = ["binance"] + exchanges

    empty_columns = ["timestamp_ms", "time", "multi_cex_oi_value", "exchange_count"]
    for exc in exchanges:
        empty_columns.append(f"{exc}_oi_value")
        empty_columns.append(f"{exc}_share")

    frame = historical_open_interest_points_to_frame(points)
    if frame.empty:
        return pd.DataFrame(columns=empty_columns)

    value_frame = frame.dropna(subset=["open_interest_value"]).copy()
    if value_frame.empty:
        return pd.DataFrame(columns=empty_columns)

    pivot = value_frame.pivot_table(
        index="timestamp_ms",
        columns="exchange",
        values="open_interest_value",
        aggfunc="last",
    ).sort_index()
    aggregate = pivot.copy()
    aggregate["multi_cex_oi_value"] = pivot.sum(axis=1, skipna=True)
    aggregate["exchange_count"] = pivot.notna().sum(axis=1)

    for exc in pivot.columns:
        aggregate[f"{exc}_oi_value"] = pivot[exc]
        aggregate[f"{exc}_share"] = aggregate[f"{exc}_oi_value"] / aggregate["multi_cex_oi_value"]

    if "binance" not in pivot.columns:
        aggregate["binance_oi_value"] = pd.NA
        aggregate["binance_share"] = pd.NA

    aggregate = aggregate[aggregate["exchange_count"] >= min_exchange_count].reset_index()
    aggregate["time"] = pd.to_datetime(aggregate["timestamp_ms"], unit="ms", utc=True)
    return aggregate


def collect_top_cex_open_interest_snapshot(
    exchanges: Sequence[str] = DEFAULT_TOP_EXCHANGES,
    *,
    timeout_s: float = 10.0,
    max_workers: int = 8,
) -> list[OpenInterestSnapshot]:
    """Fetch current BTC perpetual open interest from selected exchanges.

    The function is network-bound and intended for notebook exploration. It
    never raises because one exchange fails; failed venues are returned with an
    `error` field so coverage remains visible.
    """

    normalized = [exchange.lower() for exchange in exchanges]
    results_by_exchange: dict[str, OpenInterestSnapshot] = {}

    def fetch(exchange: str) -> OpenInterestSnapshot:
        fetcher = _FETCHERS.get(exchange)
        if fetcher is None:
            return _error_snapshot(exchange, f"Unsupported exchange: {exchange}")
        try:
            return fetcher(timeout_s)
        except Exception as exc:  # noqa: BLE001 - notebook diagnostics need the error text.
            return _error_snapshot(exchange, str(exc))

    workers = min(max_workers, max(1, len(normalized)))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch, exchange): exchange for exchange in normalized}
        for future in as_completed(futures):
            exchange = futures[future]
            results_by_exchange[exchange] = future.result()

    return [results_by_exchange[exchange] for exchange in normalized]


def aggregate_open_interest_snapshots(
    snapshots: Sequence[OpenInterestSnapshot],
) -> AggregatedOpenInterest:
    """Aggregate exchange snapshots into total OI, shares, and concentration."""

    valid = [
        snapshot
        for snapshot in snapshots
        if snapshot.error is None and snapshot.open_interest_value is not None
    ]
    total = sum(snapshot.open_interest_value or 0.0 for snapshot in valid)
    shares = {
        snapshot.exchange: (snapshot.open_interest_value or 0.0) / total
        for snapshot in valid
        if total > 0
    }
    hhi = sum(share * share for share in shares.values())
    errors = {
        snapshot.exchange: snapshot.error
        for snapshot in snapshots
        if snapshot.error is not None
    }
    return AggregatedOpenInterest(
        total_open_interest_value=total,
        covered_exchange_count=len(valid),
        requested_exchange_count=len(snapshots),
        shares=shares,
        hhi=hhi,
        binance_share=shares.get("binance"),
        errors=errors,
    )


def open_interest_snapshots_to_records(
    snapshots: Sequence[OpenInterestSnapshot],
) -> list[dict[str, Any]]:
    """Convert snapshots to records suitable for pandas or polars."""

    aggregate = aggregate_open_interest_snapshots(snapshots)
    return [
        {
            "exchange": snapshot.exchange,
            "symbol": snapshot.symbol,
            "open_interest_amount": snapshot.open_interest_amount,
            "open_interest_value": snapshot.open_interest_value,
            "share": aggregate.shares.get(snapshot.exchange),
            "timestamp_ms": snapshot.timestamp_ms,
            "datetime": snapshot.datetime,
            "error": snapshot.error,
        }
        for snapshot in snapshots
    ]


def open_interest_snapshots_to_frame(snapshots: Sequence[OpenInterestSnapshot]):
    """Return a pandas DataFrame for notebook display."""

    return pd.DataFrame(open_interest_snapshots_to_records(snapshots))
