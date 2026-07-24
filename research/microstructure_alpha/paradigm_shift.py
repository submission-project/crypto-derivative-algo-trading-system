from __future__ import annotations

from dataclasses import dataclass
from math import exp, log, sqrt
from typing import Iterable, Mapping

import numpy as np
import pandas as pd


@dataclass(frozen=True, slots=True)
class OnlineShiftResult:
    timestamp: pd.Timestamp
    score: float
    is_shift: bool
    z_scores: dict[str, float]


def infer_timestamp_unit(values: pd.Series | np.ndarray | Iterable[object]) -> str:
    series = pd.Series(values).dropna()
    if series.empty:
        return "ns"
    if np.issubdtype(series.dtype, np.datetime64):
        return "datetime"
    sample = pd.to_numeric(series, errors="coerce").dropna()
    if sample.empty:
        return "datetime"
    value = float(sample.abs().median())
    if value >= 1e17:
        return "ns"
    if value >= 1e14:
        return "us"
    if value >= 1e11:
        return "ms"
    return "s"


def to_datetime_series(values: pd.Series | np.ndarray | Iterable[object], *, unit: str = "infer") -> pd.Series:
    series = pd.Series(values)
    if unit == "infer":
        unit = infer_timestamp_unit(series)
    if unit == "datetime":
        return pd.to_datetime(series)
    return pd.to_datetime(pd.to_numeric(series, errors="coerce"), unit=unit)


def _regular_time_index(
    frames: list[pd.DataFrame],
    *,
    timestamp_col: str,
    freq: str,
) -> pd.DatetimeIndex:
    starts: list[pd.Timestamp] = []
    ends: list[pd.Timestamp] = []
    for frame in frames:
        if frame.empty or timestamp_col not in frame.columns:
            continue
        timestamps = pd.to_datetime(frame[timestamp_col]).dropna()
        if timestamps.empty:
            continue
        starts.append(pd.Timestamp(timestamps.min()).floor(freq))
        ends.append(pd.Timestamp(timestamps.max()).ceil(freq))
    if not starts or not ends:
        raise ValueError("no timestamp data available")
    return pd.date_range(min(starts), max(ends), freq=freq)


def _sum_columns(frame: pd.DataFrame, suffix: str) -> pd.Series:
    cols = [col for col in frame.columns if col.endswith(suffix)]
    if not cols:
        return pd.Series(0.0, index=frame.index)
    return frame[cols].apply(pd.to_numeric, errors="coerce").fillna(0.0).sum(axis=1)


def build_microstructure_shift_frame(
    *,
    price_df: pd.DataFrame,
    orderbook_df: pd.DataFrame | None = None,
    oi_df: pd.DataFrame | None = None,
    timestamp_col: str = "timestamp",
    freq: str = "10ms",
    price_col: str = "close",
    oi_col: str = "oi_total",
) -> pd.DataFrame:
    """
    가격, 미결제 약정(OI), 주문장 이벤트 흐름 데이터를 정규화된 ms/ns 그리드에 맞춰 정렬

    거래 횟수나 주문장 업데이트 횟수와 같은 이벤트 흐름 열은 해당 버킷에 이벤트가 도착하지 않을 경우
    0으로 채워짐. 종가나 미결제 약정(OI)과 같은 상태 기반 열은 미래 값으로 채워짐.
    """
    frames = [frame for frame in [price_df, orderbook_df, oi_df] if frame is not None and not frame.empty]
    grid = _regular_time_index(frames, timestamp_col=timestamp_col, freq=freq)
    out = pd.DataFrame(index=grid)
    out.index.name = timestamp_col

    if price_df is not None and not price_df.empty:
        price = price_df.copy()
        price[timestamp_col] = pd.to_datetime(price[timestamp_col])
        price = price.sort_values(timestamp_col).drop_duplicates(timestamp_col).set_index(timestamp_col)
        for col in ["open", "high", "low", "close"]:
            if col in price.columns:
                out[col] = pd.to_numeric(price[col], errors="coerce").reindex(grid).ffill()
        if price_col in out.columns:
            log_close = np.log(out[price_col].replace(0, np.nan))
            out["price_ret_bps"] = log_close.diff().replace([np.inf, -np.inf], np.nan).fillna(0.0) * 10_000.0
        if "high" in out.columns and "low" in out.columns:
            out["price_range_bps"] = (out["high"] / out["low"].replace(0, np.nan) - 1.0).fillna(0.0) * 10_000.0

        for col in ["volume", "trade_count", "taker_buy_volume", "taker_sell_volume"]:
            if col in price.columns:
                out[col] = pd.to_numeric(price[col], errors="coerce").reindex(grid).fillna(0.0)
        if {"taker_buy_volume", "taker_sell_volume"}.issubset(out.columns):
            denominator = out["taker_buy_volume"] + out["taker_sell_volume"]
            out["taker_imbalance"] = (
                (out["taker_buy_volume"] - out["taker_sell_volume"]) / denominator.replace(0, np.nan)
            ).fillna(0.0)

    if oi_df is not None and not oi_df.empty and oi_col in oi_df.columns:
        oi = oi_df.copy()
        oi[timestamp_col] = pd.to_datetime(oi[timestamp_col])
        oi = oi.sort_values(timestamp_col).drop_duplicates(timestamp_col).set_index(timestamp_col)
        out[oi_col] = pd.to_numeric(oi[oi_col], errors="coerce").reindex(grid).ffill()
        out["oi_delta"] = out[oi_col].diff().replace([np.inf, -np.inf], np.nan).fillna(0.0)
        out["oi_delta_bps"] = (out["oi_delta"] / out[oi_col].shift(1).replace(0, np.nan)).fillna(0.0) * 10_000.0

    if orderbook_df is not None and not orderbook_df.empty:
        ob = orderbook_df.copy()
        ob[timestamp_col] = pd.to_datetime(ob[timestamp_col])
        ob = ob.sort_values(timestamp_col).drop_duplicates(timestamp_col).set_index(timestamp_col)
        out["total_bid_update_count"] = _sum_columns(ob, "_bid_update_count").reindex(grid).fillna(0.0)
        out["total_ask_update_count"] = _sum_columns(ob, "_ask_update_count").reindex(grid).fillna(0.0)
        out["total_bid_size"] = _sum_columns(ob, "_bid_size").reindex(grid).fillna(0.0)
        out["total_ask_size"] = _sum_columns(ob, "_ask_size").reindex(grid).fillna(0.0)
        total_updates = out["total_bid_update_count"] + out["total_ask_update_count"]
        total_size = out["total_bid_size"] + out["total_ask_size"]
        out["update_imbalance"] = (
            (out["total_bid_update_count"] - out["total_ask_update_count"]) / total_updates.replace(0, np.nan)
        ).fillna(0.0)
        out["size_imbalance"] = (
            (out["total_bid_size"] - out["total_ask_size"]) / total_size.replace(0, np.nan)
        ).fillna(0.0)

    return out.reset_index()


def add_orderbook_update_intensity_features(
    frame: pd.DataFrame,
    *,
    bid_update_col: str = "total_bid_update_count",
    ask_update_col: str = "total_ask_update_count",
) -> pd.DataFrame:
    """
    호가 업데이트 패턴 감지를 위한 주문장 전용 특징을 추가

    이러한 특징은 가격이 완전히 재평가되기 전에 업데이트 강도의 눈에 띄는 상변화를 포착하기 위한 것
    """
    missing = [col for col in [bid_update_col, ask_update_col] if col not in frame.columns]
    if missing:
        raise ValueError(f"missing orderbook update columns: {missing}")

    data = frame.copy()
    bid = pd.to_numeric(data[bid_update_col], errors="coerce").fillna(0.0).clip(lower=0.0)
    ask = pd.to_numeric(data[ask_update_col], errors="coerce").fillna(0.0).clip(lower=0.0)
    total = bid + ask
    max_side = pd.concat([bid, ask], axis=1).max(axis=1)
    min_side = pd.concat([bid, ask], axis=1).min(axis=1)

    data["ob_bid_update_log"] = np.log1p(bid)
    data["ob_ask_update_log"] = np.log1p(ask)
    data["ob_update_intensity"] = total
    data["ob_update_intensity_log"] = np.log1p(total)
    data["ob_update_synchrony"] = (min_side / max_side.replace(0, np.nan)).fillna(0.0) * np.log1p(total)
    data["ob_update_pressure"] = ((bid - ask) / total.replace(0, np.nan)).fillna(0.0)
    return data


def detect_orderbook_update_intensity_shifts(
    frame: pd.DataFrame,
    *,
    timestamp_col: str = "timestamp",
    baseline_bars: int = 2500,
    recent_bars: int = 10,
    threshold: float = 5.5,
    cooldown_bars: int = 100,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    오더북 업데이트 강도 특징만 사용하여 시프트를 감지
    """
    enriched = add_orderbook_update_intensity_features(frame)
    feature_cols = [
        "ob_bid_update_log",
        "ob_ask_update_log",
        "ob_update_intensity_log",
        "ob_update_synchrony",
        "ob_update_pressure",
    ]
    result = detect_paradigm_shifts(
        enriched,
        feature_cols=feature_cols,
        timestamp_col=timestamp_col,
        baseline_bars=baseline_bars,
        recent_bars=recent_bars,
        threshold=threshold,
        cooldown_bars=cooldown_bars,
    )
    return enriched, result


def detect_orderbook_update_bursts(
    frame: pd.DataFrame,
    *,
    timestamp_col: str = "timestamp",
    bid_update_col: str = "total_bid_update_count",
    ask_update_col: str = "total_ask_update_count",
    baseline_bars: int = 2500,
    recent_bars: int = 10,
    baseline_quantile: float = 0.90,
    min_exceedance_ratio: float = 0.35,
    min_intensity_ratio: float = 1.50,
    cooldown_bars: int = 100,
    eps: float = 1e-9,
) -> pd.DataFrame:
    """
    가시적인 업데이트 강도 급증 양상을 탐지

    최근 orderbook 업데이트 횟수가 이전 기준선의 상위 사분위수 수준 이상에서 지속적인 시간 비율을 차지하는 특정 시각적 패턴에 대해 엄격한 기준을 적용
    """
    if not 0.0 < baseline_quantile < 1.0:
        raise ValueError("baseline_quantile must be between 0 and 1")
    if not 0.0 < min_exceedance_ratio <= 1.0:
        raise ValueError("min_exceedance_ratio must be in (0, 1]")
    if min_intensity_ratio <= 0:
        raise ValueError("min_intensity_ratio must be positive")

    data = add_orderbook_update_intensity_features(
        frame,
        bid_update_col=bid_update_col,
        ask_update_col=ask_update_col,
    )
    data[timestamp_col] = pd.to_datetime(data[timestamp_col])
    data = data.sort_values(timestamp_col).reset_index(drop=True)

    intensity = pd.to_numeric(data["ob_update_intensity"], errors="coerce").fillna(0.0)
    shifted = intensity.shift(recent_bars)
    baseline_median = shifted.rolling(baseline_bars, min_periods=baseline_bars).median()
    baseline_high = shifted.rolling(baseline_bars, min_periods=baseline_bars).quantile(baseline_quantile)
    recent_median = intensity.rolling(recent_bars, min_periods=recent_bars).median()
    recent_high = intensity.rolling(recent_bars, min_periods=recent_bars).quantile(0.90)
    exceedance = (intensity > baseline_high).astype(float)
    exceedance_ratio = exceedance.rolling(recent_bars, min_periods=recent_bars).mean()
    intensity_ratio = recent_median / baseline_median.replace(0, np.nan)
    high_ratio = recent_high / baseline_high.replace(0, np.nan)

    result = data[[timestamp_col]].copy()
    result["ob_update_intensity"] = intensity
    result["baseline_median"] = baseline_median
    result["baseline_high"] = baseline_high
    result["recent_median"] = recent_median
    result["recent_high"] = recent_high
    result["exceedance_ratio"] = exceedance_ratio.fillna(0.0)
    result["intensity_ratio"] = intensity_ratio.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    result["high_ratio"] = high_ratio.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    result["burst_score"] = (
        np.log1p(result["intensity_ratio"].clip(lower=0.0))
        + 1.5 * result["exceedance_ratio"]
        + np.log1p(result["high_ratio"].clip(lower=0.0))
    )

    raw_signal = (
        (result["exceedance_ratio"] >= min_exceedance_ratio)
        & (result["intensity_ratio"] >= min_intensity_ratio)
        & (result["recent_high"] > result["baseline_high"])
    )
    signal = np.zeros(len(result), dtype=bool)
    last_signal_idx = -10**12
    for idx, value in enumerate(raw_signal.to_numpy(dtype=bool)):
        if not value:
            continue
        if idx - last_signal_idx < cooldown_bars:
            continue
        signal[idx] = True
        last_signal_idx = idx
    result["is_burst"] = signal
    result["burst_threshold_quantile"] = baseline_quantile
    return result


def label_shift_price_lead(
    frame: pd.DataFrame,
    shift_result: pd.DataFrame,
    *,
    price_col: str = "close",
    timestamp_col: str = "timestamp",
    signal_col: str = "is_shift",
    score_col: str | None = None,
    price_move_bps: float = 5.0,
    lookahead: str | pd.Timedelta = "5s",
) -> pd.DataFrame:
    """
    감지된 orderbook 시프트별로 첫 미래 가격 변동을 찾음

    `lead_time_ms`는 orderbook 시프트가 `lookahead` 윈도우 내에서 최소 `price_move_bps`의 가격
    변동보다 먼저 도착할 때 양수
    """
    if price_col not in frame.columns:
        raise ValueError(f"missing price column: {price_col}")
    if timestamp_col not in frame.columns or timestamp_col not in shift_result.columns:
        raise ValueError(f"missing timestamp column: {timestamp_col}")
    if signal_col not in shift_result.columns:
        raise ValueError(f"missing signal column: {signal_col}")
    if score_col is None:
        score_col = "shift_score" if "shift_score" in shift_result.columns else "burst_score"
    if score_col not in shift_result.columns:
        raise ValueError(f"missing score column: {score_col}")
    if price_move_bps <= 0:
        raise ValueError("price_move_bps must be positive")

    data = frame[[timestamp_col, price_col]].copy()
    data[timestamp_col] = pd.to_datetime(data[timestamp_col])
    data[price_col] = pd.to_numeric(data[price_col], errors="coerce")
    data = data.dropna().sort_values(timestamp_col).reset_index(drop=True)
    lookahead_delta = pd.Timedelta(lookahead)

    signals = shift_result[shift_result[signal_col]].copy()
    signals[timestamp_col] = pd.to_datetime(signals[timestamp_col])
    rows: list[dict[str, object]] = []
    for signal in signals.itertuples(index=False):
        signal_time = getattr(signal, timestamp_col)
        current = data[data[timestamp_col] >= signal_time].head(1)
        if current.empty:
            continue
        base_price = float(current[price_col].iloc[0])
        future = data[
            (data[timestamp_col] >= signal_time)
            & (data[timestamp_col] <= signal_time + lookahead_delta)
        ].copy()
        if base_price <= 0 or future.empty:
            continue
        future["future_move_bps"] = np.log(future[price_col] / base_price) * 10_000.0
        hit = future[future["future_move_bps"].abs() >= price_move_bps].head(1)
        hit_time = pd.NaT
        lead_ms = np.nan
        move_bps = np.nan
        if not hit.empty:
            hit_time = pd.Timestamp(hit[timestamp_col].iloc[0])
            lead_ms = (hit_time - pd.Timestamp(signal_time)).total_seconds() * 1000.0
            move_bps = float(hit["future_move_bps"].iloc[0])
        rows.append(
                {
                    "shift_time": pd.Timestamp(signal_time),
                    "shift_score": float(getattr(signal, score_col)),
                    "strongest_feature": getattr(signal, "strongest_feature")
                    if hasattr(signal, "strongest_feature")
                    else score_col,
                "price_hit_time": hit_time,
                "lead_time_ms": lead_ms,
                "price_move_bps": move_bps,
            }
        )
    return pd.DataFrame(rows)


def _rolling_mad(values: np.ndarray) -> float:
    median = np.nanmedian(values)
    return float(np.nanmedian(np.abs(values - median)))


def _rowwise_root_mean_square(frame: pd.DataFrame) -> np.ndarray:
    values = frame.to_numpy(dtype=float)
    valid = np.isfinite(values)
    squared_sum = np.where(valid, np.square(values), 0.0).sum(axis=1)
    counts = valid.sum(axis=1)
    mean_square = np.divide(squared_sum, counts, out=np.zeros_like(squared_sum), where=counts > 0)
    return np.sqrt(mean_square)


def detect_paradigm_shifts(
    frame: pd.DataFrame,
    *,
    feature_cols: list[str],
    timestamp_col: str = "timestamp",
    baseline_bars: int = 500,
    recent_bars: int = 50,
    threshold: float = 4.0,
    cooldown_bars: int = 50,
    include_scale_shift: bool = True,
    max_abs_z: float = 12.0,
    eps: float = 1e-9,
) -> pd.DataFrame:
    """
    다변량 특징 벡터로부터 미세구조의 형태 변화를 감지

    각 막대마다, 감지기는 로버스트 중앙값/MAD z-점수를 사용하여 최근 창을 이전
    기준 창과 비교합니다. 이 방법은 입력 데이터가 이미 버킷화되거나 이벤트 정렬된 경우,
    ms/ns 단위의 데이터에서도 작동
    """
    if baseline_bars < 3:
        raise ValueError("baseline_bars must be at least 3")
    if recent_bars < 1:
        raise ValueError("recent_bars must be positive")
    if cooldown_bars < 0:
        raise ValueError("cooldown_bars must be non-negative")
    missing = [col for col in feature_cols if col not in frame.columns]
    if missing:
        raise ValueError(f"missing feature columns: {missing}")

    data = frame[[timestamp_col, *feature_cols]].copy()
    data[timestamp_col] = pd.to_datetime(data[timestamp_col])
    data = data.sort_values(timestamp_col).reset_index(drop=True)
    features = data[feature_cols].apply(pd.to_numeric, errors="coerce").replace([np.inf, -np.inf], np.nan)
    features = features.fillna(0.0)

    center_z = pd.DataFrame(index=data.index)
    scale_z = pd.DataFrame(index=data.index)
    for col in feature_cols:
        series = features[col]
        recent_center = series.rolling(recent_bars, min_periods=recent_bars).median()
        baseline = series.shift(recent_bars)
        baseline_center = baseline.rolling(baseline_bars, min_periods=baseline_bars).median()
        baseline_mad = baseline.rolling(baseline_bars, min_periods=baseline_bars).apply(_rolling_mad, raw=True)
        baseline_scale = (1.4826 * baseline_mad).clip(lower=eps)
        center_z[col] = ((recent_center - baseline_center) / baseline_scale).clip(-max_abs_z, max_abs_z)

        if include_scale_shift:
            recent_std = series.rolling(recent_bars, min_periods=recent_bars).std().clip(lower=eps)
            baseline_std = baseline.rolling(baseline_bars, min_periods=baseline_bars).std().clip(lower=eps)
            scale_z[col] = np.log(recent_std / baseline_std).replace([np.inf, -np.inf], np.nan).clip(
                -max_abs_z,
                max_abs_z,
            )

    center_score = _rowwise_root_mean_square(center_z)
    if include_scale_shift:
        scale_score = _rowwise_root_mean_square(scale_z)
        score = np.sqrt(np.square(center_score) + 0.35 * np.square(scale_score))
    else:
        scale_score = np.zeros(len(data))
        score = center_score

    result = data[[timestamp_col]].copy()
    result["shift_score"] = np.nan_to_num(score, nan=0.0, posinf=0.0, neginf=0.0)
    result["center_score"] = np.nan_to_num(center_score, nan=0.0, posinf=0.0, neginf=0.0)
    result["scale_score"] = np.nan_to_num(scale_score, nan=0.0, posinf=0.0, neginf=0.0)
    for col in feature_cols:
        result[f"{col}_z"] = center_z[col].replace([np.inf, -np.inf], np.nan).fillna(0.0)

    strongest_features: list[str] = []
    signed_scores: list[float] = []
    for idx in result.index:
        row = center_z.loc[idx].replace([np.inf, -np.inf], np.nan).fillna(0.0)
        strongest = row.abs().idxmax()
        strongest_features.append(str(strongest))
        signed_scores.append(float(row[strongest]))
    result["strongest_feature"] = strongest_features
    result["strongest_feature_z"] = signed_scores

    signal = np.zeros(len(result), dtype=bool)
    last_signal_idx = -10**12
    scores = result["shift_score"].to_numpy(dtype=float)
    for idx, value in enumerate(scores):
        if value < threshold:
            continue
        if idx - last_signal_idx < cooldown_bars:
            continue
        signal[idx] = True
        last_signal_idx = idx
    result["is_shift"] = signal
    result["shift_threshold"] = threshold
    return result


def compare_window_before_after(
    frame: pd.DataFrame,
    *,
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
    feature_cols: list[str],
    timestamp_col: str = "timestamp",
    lookback: str | pd.Timedelta = "25s",
) -> pd.DataFrame:
    data = frame.copy()
    data[timestamp_col] = pd.to_datetime(data[timestamp_col])
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    lookback_delta = pd.Timedelta(lookback)
    before = data[(data[timestamp_col] >= start_ts - lookback_delta) & (data[timestamp_col] < start_ts)]
    during = data[(data[timestamp_col] >= start_ts) & (data[timestamp_col] < end_ts)]

    rows: list[dict[str, object]] = []
    before_seconds = max(lookback_delta.total_seconds(), 1e-9)
    during_seconds = max((end_ts - start_ts).total_seconds(), 1e-9)
    for col in feature_cols:
        if col not in data.columns:
            continue
        before_values = pd.to_numeric(before[col], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
        during_values = pd.to_numeric(during[col], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
        before_mean = float(before_values.mean()) if len(before_values) else 0.0
        during_mean = float(during_values.mean()) if len(during_values) else 0.0
        before_sum = float(before_values.sum()) if len(before_values) else 0.0
        during_sum = float(during_values.sum()) if len(during_values) else 0.0
        rows.append(
            {
                "feature": col,
                "before_mean": before_mean,
                "during_mean": during_mean,
                "mean_change": during_mean - before_mean,
                "mean_ratio": during_mean / before_mean if abs(before_mean) > 1e-12 else np.nan,
                "before_per_second": before_sum / before_seconds,
                "during_per_second": during_sum / during_seconds,
                "per_second_ratio": (during_sum / during_seconds) / (before_sum / before_seconds)
                if abs(before_sum) > 1e-12
                else np.nan,
            }
        )
    return pd.DataFrame(rows)


class OnlineEWMParadigmShiftDetector:
    """
    ns/ms 단위 이벤트 스트림을 위한 저지연 온라인 검출기

    이 검출기는 이전 EWMA 평균/분산을 사용하여 새로운 특징 벡터의 점수를 매긴 다음, 상태를 업데이트
    업데이트당 런타임은 O(특징의 개수)
    """

    def __init__(
        self,
        feature_names: list[str],
        *,
        half_life_bars: float = 200.0,
        threshold: float = 4.0,
        warmup_bars: int = 200,
        cooldown_bars: int = 50,
        eps: float = 1e-9,
    ) -> None:
        if not feature_names:
            raise ValueError("feature_names must not be empty")
        if half_life_bars <= 0:
            raise ValueError("half_life_bars must be positive")
        self.feature_names = feature_names
        self.alpha = 1.0 - exp(log(0.5) / half_life_bars)
        self.threshold = threshold
        self.warmup_bars = warmup_bars
        self.cooldown_bars = cooldown_bars
        self.eps = eps
        self.count = 0
        self.last_signal_count = -10**12
        self.mean = np.zeros(len(feature_names), dtype=float)
        self.var = np.ones(len(feature_names), dtype=float)

    def update(self, timestamp: object, values: Mapping[str, float] | Iterable[float]) -> OnlineShiftResult:
        if isinstance(values, Mapping):
            vector = np.array([float(values.get(name, 0.0)) for name in self.feature_names], dtype=float)
        else:
            vector = np.array(list(values), dtype=float)
            if len(vector) != len(self.feature_names):
                raise ValueError("values length must match feature_names")
        vector = np.nan_to_num(vector, nan=0.0, posinf=0.0, neginf=0.0)

        scale = np.sqrt(np.maximum(self.var, self.eps))
        z = np.clip((vector - self.mean) / scale, -12.0, 12.0)
        score = float(sqrt(float(np.mean(np.square(z)))))
        is_ready = self.count >= self.warmup_bars
        is_shift = (
            is_ready
            and score >= self.threshold
            and self.count - self.last_signal_count >= self.cooldown_bars
        )
        if is_shift:
            self.last_signal_count = self.count

        delta = vector - self.mean
        self.mean = self.mean + self.alpha * delta
        self.var = (1.0 - self.alpha) * (self.var + self.alpha * np.square(delta))
        self.count += 1

        timestamp_series = to_datetime_series([timestamp])
        return OnlineShiftResult(
            timestamp=pd.Timestamp(timestamp_series.iloc[0]),
            score=score,
            is_shift=is_shift,
            z_scores=dict(zip(self.feature_names, z)),
        )
