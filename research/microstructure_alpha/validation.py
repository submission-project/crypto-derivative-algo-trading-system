from __future__ import annotations

from dataclasses import dataclass
from math import erf, sqrt
from statistics import mean, pstdev
from typing import Iterable, Sequence


@dataclass(frozen=True, slots=True)
class ValidationSummary:
    count: int
    mean_forward_return: float
    hit_ratio: float
    information_coefficient: float
    t_stat: float
    p_value_normal_approx: float


def _clean_pairs(
    feature_values: Sequence[float],
    forward_returns: Sequence[float | None],
) -> list[tuple[float, float]]:
    pairs: list[tuple[float, float]] = []
    for feature, ret in zip(feature_values, forward_returns):
        if ret is None:
            continue
        pairs.append((float(feature), float(ret)))
    return pairs


def pearson_corr(xs: Sequence[float], ys: Sequence[float]) -> float:
    if len(xs) != len(ys):
        raise ValueError("xs and ys must have the same length")
    if len(xs) < 2:
        return 0.0

    x_mean = mean(xs)
    y_mean = mean(ys)
    x_std = pstdev(xs)
    y_std = pstdev(ys)
    if x_std == 0 or y_std == 0:
        return 0.0

    covariance = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys)) / len(xs)
    return covariance / (x_std * y_std)


def t_stat(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 0.0
    sample_mean = mean(values)
    sample_std = pstdev(values)
    if sample_std == 0:
        return 0.0
    return sample_mean / (sample_std / sqrt(len(values)))


def normal_approx_two_sided_p_value(t_value: float) -> float:
    cdf = 0.5 * (1.0 + erf(abs(t_value) / sqrt(2.0)))
    return 2.0 * (1.0 - cdf)


def summarize_signal(
    feature_values: Sequence[float],
    returns: Sequence[float | None],
) -> ValidationSummary:
    pairs = _clean_pairs(feature_values, returns)
    if not pairs:
        return ValidationSummary(0, 0.0, 0.0, 0.0, 0.0, 1.0)

    xs = [x for x, _ in pairs]
    ys = [y for _, y in pairs]
    t_value = t_stat(ys)
    return ValidationSummary(
        count=len(pairs),
        mean_forward_return=mean(ys),
        hit_ratio=sum(1 for y in ys if y > 0) / len(ys),
        information_coefficient=pearson_corr(xs, ys),
        t_stat=t_value,
        p_value_normal_approx=normal_approx_two_sided_p_value(t_value),
    )


def decile_returns(
    feature_values: Sequence[float],
    returns: Sequence[float | None],
    buckets: int = 10,
) -> list[dict[str, float]]:
    if buckets <= 1:
        raise ValueError("buckets must be greater than 1")

    pairs = sorted(_clean_pairs(feature_values, returns), key=lambda pair: pair[0])
    if not pairs:
        return []

    bucket_size = max(1, len(pairs) // buckets)
    rows: list[dict[str, float]] = []
    for bucket_idx in range(buckets):
        start = bucket_idx * bucket_size
        end = len(pairs) if bucket_idx == buckets - 1 else (bucket_idx + 1) * bucket_size
        chunk = pairs[start:end]
        if not chunk:
            continue
        chunk_returns = [ret for _, ret in chunk]
        rows.append(
            {
                "bucket": float(bucket_idx + 1),
                "count": float(len(chunk)),
                "feature_min": chunk[0][0],
                "feature_max": chunk[-1][0],
                "mean_forward_return": mean(chunk_returns),
                "hit_ratio": sum(1 for ret in chunk_returns if ret > 0) / len(chunk),
            }
        )
    return rows


# ── Walk-Forward Validation ──


@dataclass(frozen=True, slots=True)
class WalkForwardFold:
    fold_index: int
    train_size: int
    test_size: int
    train_summary: ValidationSummary
    test_summary: ValidationSummary


def walk_forward_validate(
    feature_values: Sequence[float],
    returns: Sequence[float | None],
    *,
    n_folds: int = 5,
    train_ratio: float = 0.6,
) -> list[WalkForwardFold]:
    """
    Walk-Forward 교차 검증

    데이터를 순차적인 폴드로 분할하며, 각 폴드는 train 윈도우와 test 윈도우로 구성
    폴드별 in-sample 및 out-of-sample ValidationSummary를 보고
    """
    if n_folds < 2:
        raise ValueError("n_folds must be >= 2")
    if not 0 < train_ratio < 1:
        raise ValueError("train_ratio must be between 0 and 1")

    pairs = _clean_pairs(feature_values, returns)
    if not pairs:
        return []

    total = len(pairs)
    fold_size = total // n_folds
    if fold_size < 4:
        return []

    results: list[WalkForwardFold] = []
    for fold_idx in range(n_folds):
        start = fold_idx * fold_size
        end = total if fold_idx == n_folds - 1 else (fold_idx + 1) * fold_size
        fold_pairs = pairs[start:end]
        split = int(len(fold_pairs) * train_ratio)
        if split < 2 or len(fold_pairs) - split < 2:
            continue

        train_xs = [x for x, _ in fold_pairs[:split]]
        train_ys: list[float | None] = [y for _, y in fold_pairs[:split]]
        test_xs = [x for x, _ in fold_pairs[split:]]
        test_ys: list[float | None] = [y for _, y in fold_pairs[split:]]

        results.append(
            WalkForwardFold(
                fold_index=fold_idx,
                train_size=split,
                test_size=len(fold_pairs) - split,
                train_summary=summarize_signal(train_xs, train_ys),
                test_summary=summarize_signal(test_xs, test_ys),
            )
        )
    return results


def signal_autocorrelation(
    signals: Sequence[int],
    max_lag: int = 10,
) -> list[float]:
    """
    시그널 신호 시퀀스의 상관관계

    높은 상관관계는 신호가 "끈적끈적"하다는 것(낮은 턴오버)을 의미하며,
    낮은 상관관계는 잦은 전환을 의미
    """
    if max_lag < 1:
        raise ValueError("max_lag must be >= 1")
    n = len(signals)
    if n < max_lag + 1:
        return [0.0] * max_lag

    float_signals = [float(s) for s in signals]
    mean_s = sum(float_signals) / n
    var_s = sum((s - mean_s) ** 2 for s in float_signals) / n
    if var_s == 0:
        return [0.0] * max_lag

    autocorrs: list[float] = []
    for lag in range(1, max_lag + 1):
        cov = sum(
            (float_signals[i] - mean_s) * (float_signals[i + lag] - mean_s)
            for i in range(n - lag)
        ) / (n - lag)
        autocorrs.append(cov / var_s)
    return autocorrs


def turnover_rate(positions: Sequence[int]) -> float:
    """
    위치가 변하는 시간 단계의 비율.

    회전율이 낮을수록 → 거래 비용이 줄어듭니다.
    """
    if len(positions) < 2:
        return 0.0
    changes = sum(1 for i in range(1, len(positions)) if positions[i] != positions[i - 1])
    return changes / (len(positions) - 1)
