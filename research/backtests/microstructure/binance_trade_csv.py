from __future__ import annotations

import csv
import importlib.util
from dataclasses import dataclass, field
from pathlib import Path
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl

from .simulator import BacktestPoint


CsvEngine = Literal["auto", "polars", "pandas", "python"]
CsvPaths = Sequence[str | Path]

DEFAULT_BTCUSDT_FUTURES_TRADE_DAILY_DIR = Path(
    "research/datasets/exchange/binance/assets/btcusdt/future/trade/daily"
)


@dataclass(frozen=True, slots=True)
class TradeBar:
    bucket_start_ms: int
    close_price: float
    start_price: float = 0.0
    price_base: float = 0.0
    price_gap_list: list[float] = field(default_factory=list)
    volume: float = 0.0
    quote_volume: float = 0.0
    taker_buy_volume: float = 0.0
    taker_sell_volume: float = 0.0
    taker_buy_quote_volume: float = 0.0
    taker_sell_quote_volume: float = 0.0
    trade_count: int = 0
    first_id: int = 0
    last_id: int = 0

    @property
    def taker_imbalance(self) -> float:
        if self.quote_volume <= 0:
            return 0.0
        return (self.taker_buy_quote_volume - self.taker_sell_quote_volume) / self.quote_volume


def _parse_bool(value: str) -> bool:
    return value.strip().lower() in {"true", "1", "yes"}


def _validate_aggregation_args(bucket_ms: int, max_rows: int | None) -> None:
    if bucket_ms <= 0:
        raise ValueError("bucket_ms must be positive")
    if max_rows is not None and max_rows <= 0:
        raise ValueError("max_rows must be positive when provided")


def _is_module_available(name: str) -> bool:
    return importlib.util.find_spec(name) is not None


def _select_engine(engine: CsvEngine) -> Literal["polars", "pandas", "python"]:
    if engine == "auto":
        if _is_module_available("polars"):
            return "polars"
        if _is_module_available("pandas"):
            return "pandas"
        return "python"
    if engine in {"polars", "pandas", "python"}:
        return engine
    raise ValueError("engine must be one of: auto, polars, pandas, python")


def _coerce_csv_paths(paths: CsvPaths) -> list[Path]:
    if isinstance(paths, str | Path):
        raise TypeError("paths must be a sequence of CSV paths, e.g. [path1, path2]")

    paths = [Path(item) for item in paths]
    if not paths:
        raise ValueError("paths must contain at least one CSV file")
    return paths


def _merge_trade_bars(bars: Iterable[TradeBar]) -> list[TradeBar]:
    merged: dict[int, TradeBar] = {}
    for bar in sorted(bars, key=lambda item: item.bucket_start_ms):
        existing = merged.get(bar.bucket_start_ms)
        if existing is None:
            merged[bar.bucket_start_ms] = bar
            continue

        existing_prices = [existing.price_base + gap for gap in existing.price_gap_list] if existing.price_gap_list else [existing.close_price]
        bar_prices = [bar.price_base + gap for gap in bar.price_gap_list] if bar.price_gap_list else [bar.close_price]

        if bar.first_id < existing.first_id:
            start_price = bar.start_price
            merged_prices = bar_prices + existing_prices
        else:
            start_price = existing.start_price
            merged_prices = existing_prices + bar_prices

        if bar.last_id > existing.last_id:
            close_price = bar.close_price
        else:
            close_price = existing.close_price

        merged[bar.bucket_start_ms] = TradeBar(
            bucket_start_ms=bar.bucket_start_ms,
            start_price=start_price,
            close_price=close_price,
            price_base=start_price,
            price_gap_list=[p - start_price for p in merged_prices],
            volume=existing.volume + bar.volume,
            quote_volume=existing.quote_volume + bar.quote_volume,
            taker_buy_volume=existing.taker_buy_volume + bar.taker_buy_volume,
            taker_sell_volume=existing.taker_sell_volume + bar.taker_sell_volume,
            taker_buy_quote_volume=existing.taker_buy_quote_volume + bar.taker_buy_quote_volume,
            taker_sell_quote_volume=existing.taker_sell_quote_volume + bar.taker_sell_quote_volume,
            trade_count=existing.trade_count + bar.trade_count,
            first_id=min(existing.first_id, bar.first_id),
            last_id=max(existing.last_id, bar.last_id),
        )
    return list(merged.values())


def aggregate_trade_csv_to_bars(
    paths: CsvPaths,
    *,
    bucket_ms: int = 60_000,
    max_rows: int | None = None,
    engine: CsvEngine = "auto",
) -> list[TradeBar]:
    _validate_aggregation_args(bucket_ms, max_rows)

    csv_paths = _coerce_csv_paths(paths)
    selected_engine = _select_engine(engine)
    bars: list[TradeBar] = []
    remaining_rows = max_rows

    for csv_path in csv_paths:
        if remaining_rows is not None and remaining_rows <= 0:
            break

        if selected_engine == "polars":
            file_bars = _aggregate_trade_csv_to_bars_polars(
                csv_path,
                bucket_ms=bucket_ms,
                max_rows=remaining_rows,
            )
        elif selected_engine == "pandas":
            file_bars = _aggregate_trade_csv_to_bars_pandas(
                csv_path,
                bucket_ms=bucket_ms,
                max_rows=remaining_rows,
            )
        else:
            file_bars = _aggregate_trade_csv_to_bars_python(
                csv_path,
                bucket_ms=bucket_ms,
                max_rows=remaining_rows,
            )

        bars.extend(file_bars)
        if remaining_rows is not None:
            remaining_rows -= sum(bar.trade_count for bar in file_bars)

    return _merge_trade_bars(bars)


def aggregate_trade_csv_files_to_bars(
    paths: CsvPaths,
    *,
    bucket_ms: int = 60_000,
    max_rows: int | None = None,
    engine: CsvEngine = "auto",
) -> list[TradeBar]:
    return aggregate_trade_csv_to_bars(
        paths,
        bucket_ms=bucket_ms,
        max_rows=max_rows,
        engine=engine,
    )


def _aggregate_trade_csv_to_bars_python(
    path: Path,
    *,
    bucket_ms: int,
    max_rows: int | None,
) -> list[TradeBar]:
    bars: list[TradeBar] = []
    current_bucket: int | None = None
    start_price = 0.0
    close_price = 0.0
    price_list: list[float] = []
    volume = 0.0
    quote_volume = 0.0
    taker_buy_volume = 0.0
    taker_sell_volume = 0.0
    taker_buy_quote_volume = 0.0
    taker_sell_quote_volume = 0.0
    trade_count = 0
    first_id = -1
    last_id = -1

    def flush() -> None:
        nonlocal start_price, close_price, price_list, volume, quote_volume, taker_buy_volume, taker_sell_volume, taker_buy_quote_volume, taker_sell_quote_volume, trade_count, first_id, last_id
        if current_bucket is None or trade_count == 0:
            return
        bars.append(
            TradeBar(
                bucket_start_ms=current_bucket,
                start_price=start_price,
                close_price=close_price,
                price_base=start_price,
                price_gap_list=[p - start_price for p in price_list],
                volume=volume,
                quote_volume=quote_volume,
                taker_buy_volume=taker_buy_volume,
                taker_sell_volume=taker_sell_volume,
                taker_buy_quote_volume=taker_buy_quote_volume,
                taker_sell_quote_volume=taker_sell_quote_volume,
                trade_count=trade_count,
                first_id=first_id,
                last_id=last_id,
            )
        )
        start_price = 0.0
        close_price = 0.0
        price_list = []
        volume = 0.0
        quote_volume = 0.0
        taker_buy_volume = 0.0
        taker_sell_volume = 0.0
        taker_buy_quote_volume = 0.0
        taker_sell_quote_volume = 0.0
        trade_count = 0
        first_id = -1
        last_id = -1

    with path.open("r", newline="") as file:
        reader = csv.DictReader(file)
        for row_idx, row in enumerate(reader):
            if max_rows is not None and row_idx >= max_rows:
                break

            trade_id = int(row["id"])
            qty = float(row["qty"])
            timestamp = int(row["time"])
            price = float(row["price"])
            quote_qty = float(row["quote_qty"])
            is_buyer_maker = _parse_bool(row["is_buyer_maker"])
            bucket = timestamp - (timestamp % bucket_ms)

            if current_bucket is None:
                current_bucket = bucket
            elif bucket != current_bucket:
                flush()
                current_bucket = bucket

            if trade_count == 0:
                first_id = trade_id
                start_price = price
            last_id = trade_id

            close_price = price
            price_list.append(price)
            volume += qty
            quote_volume += quote_qty
            if is_buyer_maker:
                taker_sell_volume += qty
                taker_sell_quote_volume += quote_qty
            else:
                taker_buy_volume += qty
                taker_buy_quote_volume += quote_qty
            trade_count += 1

    flush()
    return bars


def _aggregate_trade_csv_to_bars_polars(
    path: Path,
    *,
    bucket_ms: int,
    max_rows: int | None,
) -> list[TradeBar]:
    try:
        import polars as pl
    except ImportError as exc:
        raise ImportError("polars is required when engine='polars'") from exc

    # id,price,qty,quote_qty,time,is_buyer_maker
    frame = pl.scan_csv(
        path,
        schema_overrides={
            "id": pl.Int64,
            "price": pl.Float64,
            "qty": pl.Float64,
            "quote_qty": pl.Float64,
            "time": pl.Int64,
            "is_buyer_maker": pl.Boolean,
        },
    )
    if max_rows is not None:
        frame = frame.head(max_rows)

    aggregated = (
        frame.select("id", "price", "qty", "quote_qty", "time", "is_buyer_maker")
        .with_columns(
            ((pl.col("time") // bucket_ms) * bucket_ms).alias("bucket_start_ms"),
            pl.when(pl.col("is_buyer_maker"))
            .then(0.0)
            .otherwise(pl.col("qty"))
            .alias("taker_buy_volume"),
            pl.when(pl.col("is_buyer_maker"))
            .then(pl.col("qty"))
            .otherwise(0.0)
            .alias("taker_sell_volume"),
            pl.when(pl.col("is_buyer_maker"))
            .then(0.0)
            .otherwise(pl.col("quote_qty"))
            .alias("taker_buy_quote_volume"),
            pl.when(pl.col("is_buyer_maker"))
            .then(pl.col("quote_qty"))
            .otherwise(0.0)
            .alias("taker_sell_quote_volume"),
        )
        .group_by("bucket_start_ms")
        .agg(
            pl.col("price").first().alias("start_price"),
            pl.col("price").last().alias("close_price"),
            pl.col("price").alias("price_list"),
            pl.col("qty").sum().alias("volume"),
            pl.col("quote_qty").sum().alias("quote_volume"),
            pl.col("taker_buy_volume").sum(),
            pl.col("taker_sell_volume").sum(),
            pl.col("taker_buy_quote_volume").sum(),
            pl.col("taker_sell_quote_volume").sum(),
            pl.len().alias("trade_count"),
            pl.col("id").first().alias("first_id"),
            pl.col("id").last().alias("last_id"),
        )
        .sort("bucket_start_ms")
        .collect()
    )

    return [
        TradeBar(
            bucket_start_ms=int(row["bucket_start_ms"]),
            start_price=float(row["start_price"]),
            close_price=float(row["close_price"]),
            price_base=float(row["start_price"]),
            price_gap_list=[float(p) - float(row["start_price"]) for p in row["price_list"]],
            volume=float(row["volume"]),
            quote_volume=float(row["quote_volume"]),
            taker_buy_volume=float(row["taker_buy_volume"]),
            taker_sell_volume=float(row["taker_sell_volume"]),
            taker_buy_quote_volume=float(row["taker_buy_quote_volume"]),
            taker_sell_quote_volume=float(row["taker_sell_quote_volume"]),
            trade_count=int(row["trade_count"]),
            first_id=int(row["first_id"]),
            last_id=int(row["last_id"]),
        )
        for row in aggregated.iter_rows(named=True)
    ]


def _aggregate_trade_csv_to_bars_pandas(
    path: Path,
    *,
    bucket_ms: int,
    max_rows: int | None,
) -> list[TradeBar]:
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError("pandas is required when engine='pandas'") from exc

    frame = pd.read_csv(
        path,
        usecols=["id", "price", "qty", "quote_qty", "time", "is_buyer_maker"],
        nrows=max_rows,
    )
    frame["id"] = frame["id"].astype("int64")
    frame["price"] = frame["price"].astype("float64")
    frame["qty"] = frame["qty"].astype("float64")
    frame["quote_qty"] = frame["quote_qty"].astype("float64")
    frame["time"] = frame["time"].astype("int64")
    is_buyer_maker = frame["is_buyer_maker"].astype(str).str.lower().isin({"true", "1", "yes"})

    frame["bucket_start_ms"] = (frame["time"] // bucket_ms) * bucket_ms
    frame["taker_buy_volume"] = frame["qty"].where(~is_buyer_maker, 0.0)
    frame["taker_sell_volume"] = frame["qty"].where(is_buyer_maker, 0.0)
    frame["taker_buy_quote_volume"] = frame["quote_qty"].where(~is_buyer_maker, 0.0)
    frame["taker_sell_quote_volume"] = frame["quote_qty"].where(is_buyer_maker, 0.0)

    aggregated = (
        frame.groupby("bucket_start_ms", sort=True)
        .agg(
            start_price=("price", "first"),
            close_price=("price", "last"),
            price_list=("price", list),
            volume=("qty", "sum"),
            quote_volume=("quote_qty", "sum"),
            taker_buy_volume=("taker_buy_volume", "sum"),
            taker_sell_volume=("taker_sell_volume", "sum"),
            taker_buy_quote_volume=("taker_buy_quote_volume", "sum"),
            taker_sell_quote_volume=("taker_sell_quote_volume", "sum"),
            trade_count=("price", "size"),
            first_id=("id", "first"),
            last_id=("id", "last"),
        )
        .reset_index()
    )

    return [
        TradeBar(
            bucket_start_ms=int(row.bucket_start_ms),
            start_price=float(row.start_price),
            close_price=float(row.close_price),
            price_base=float(row.start_price),
            price_gap_list=[float(p) - float(row.start_price) for p in row.price_list],
            volume=float(row.volume),
            quote_volume=float(row.quote_volume),
            taker_buy_volume=float(row.taker_buy_volume),
            taker_sell_volume=float(row.taker_sell_volume),
            taker_buy_quote_volume=float(row.taker_buy_quote_volume),
            taker_sell_quote_volume=float(row.taker_sell_quote_volume),
            trade_count=int(row.trade_count),
            first_id=int(row.first_id),
            last_id=int(row.last_id),
        )
        for row in aggregated.itertuples(index=False)
    ]


def bars_to_backtest_points(
    bars: Iterable[TradeBar],
    *,
    signals: Sequence[int] | None = None,
    default_spread_bps: float = 1.0,
    force_flat_last: bool = True,
) -> list[BacktestPoint]:
    if default_spread_bps < 0:
        raise ValueError("default_spread_bps must be non-negative")

    bars_list = list(bars)
    point_signals = list(signals) if signals is not None else [0] * len(bars_list)
    if len(point_signals) != len(bars_list):
        raise ValueError("signals length must match bars length")
    if any(signal not in {-1, 0, 1} for signal in point_signals):
        raise ValueError("signals must contain only -1, 0, or 1")
    if force_flat_last and point_signals:
        point_signals[-1] = 0

    points: list[BacktestPoint] = []
    for bar, signal in zip(bars_list, point_signals):
        half_spread = bar.close_price * default_spread_bps / 20_000.0
        points.append(
            BacktestPoint(
                timestamp=bar.bucket_start_ms,
                price=bar.close_price,
                signal=signal,
                bid=bar.close_price - half_spread,
                ask=bar.close_price + half_spread,
                bar_volume_usd=bar.quote_volume,
            )
        )
    return points


def bars_to_dataframe(
    bars: Iterable[TradeBar],
    *,
    engine: Literal["polars", "pandas"] = "polars",
) -> pl.DataFrame | pd.DataFrame:
    """Convert a sequence of TradeBars to a Polars or Pandas DataFrame."""
    bars_list = list(bars)
    if engine == "polars":
        try:
            import polars as pl
        except ImportError as exc:
            raise ImportError("polars is required when engine='polars'") from exc

        if not bars_list:
            return pl.DataFrame(
                schema={
                    "bucket_start_ms": pl.Int64,
                    "close_price": pl.Float64,
                    "start_price": pl.Float64,
                    "price_base": pl.Float64,
                    "price_gap_list": pl.List(pl.Float64),
                    "volume": pl.Float64,
                    "quote_volume": pl.Float64,
                    "taker_buy_volume": pl.Float64,
                    "taker_sell_volume": pl.Float64,
                    "taker_buy_quote_volume": pl.Float64,
                    "taker_sell_quote_volume": pl.Float64,
                    "trade_count": pl.Int64,
                    "first_id": pl.Int64,
                    "last_id": pl.Int64,
                }
            )
        return pl.DataFrame(bars_list)
    elif engine == "pandas":
        try:
            import pandas as pd
        except ImportError as exc:
            raise ImportError("pandas is required when engine='pandas'") from exc

        if not bars_list:
            return pd.DataFrame(
                columns=[
                    "bucket_start_ms",
                    "close_price",
                    "start_price",
                    "price_base",
                    "price_gap_list",
                    "volume",
                    "quote_volume",
                    "taker_buy_volume",
                    "taker_sell_volume",
                    "taker_buy_quote_volume",
                    "taker_sell_quote_volume",
                    "trade_count",
                    "first_id",
                    "last_id",
                ]
            ).astype(
                {
                    "bucket_start_ms": "int64",
                    "close_price": "float64",
                    "start_price": "float64",
                    "price_base": "float64",
                    "price_gap_list": "object",
                    "volume": "float64",
                    "quote_volume": "float64",
                    "taker_buy_volume": "float64",
                    "taker_sell_volume": "float64",
                    "taker_buy_quote_volume": "float64",
                    "taker_sell_quote_volume": "float64",
                    "trade_count": "int64",
                    "first_id": "int64",
                    "last_id": "int64",
                }
            )
        return pd.DataFrame(bars_list)
    else:
        raise ValueError("engine must be either 'polars' or 'pandas'")


def load_binance_trade_csv_points(
    paths: CsvPaths,
    *,
    bucket_ms: int = 60_000,
    max_rows: int | None = 50_000,
    signals: Sequence[int] | None = None,
    default_spread_bps: float = 1.0,
    engine: CsvEngine = "auto",
) -> list[BacktestPoint]:
    bars = aggregate_trade_csv_to_bars(
        paths,
        bucket_ms=bucket_ms,
        max_rows=max_rows,
        engine=engine,
    )
    return bars_to_backtest_points(
        bars,
        signals=signals,
        default_spread_bps=default_spread_bps,
    )


def latest_trade_csv(directory: str | Path = DEFAULT_BTCUSDT_FUTURES_TRADE_DAILY_DIR) -> Path | None:
    paths = sorted(Path(directory).glob("BTCUSDT-trades-*.csv"))
    return paths[-1] if paths else None
