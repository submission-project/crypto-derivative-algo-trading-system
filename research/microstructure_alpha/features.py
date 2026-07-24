from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from typing import Iterable, Sequence


@dataclass(frozen=True, slots=True)
class BookTop:
    bid_price: float
    bid_size: float
    ask_price: float
    ask_size: float

    @property
    def mid_price(self) -> float:
        return (self.bid_price + self.ask_price) / 2.0

    @property
    def spread_bps(self) -> float:
        mid = self.mid_price
        if mid <= 0:
            return 0.0
        return (self.ask_price - self.bid_price) / mid * 10_000.0


@dataclass(frozen=True, slots=True)
class TradeBucket:
    buy_taker_qty: float
    sell_taker_qty: float

    @property
    def total_qty(self) -> float:
        return self.buy_taker_qty + self.sell_taker_qty


DensityMatrix2x2 = tuple[tuple[float, float], tuple[float, float]]


@dataclass(frozen=True, slots=True)
class MarketDensityMatrixState:
    p_box: float
    p_impulse: float
    coherence: float
    matrix: DensityMatrix2x2
    trace: float
    determinant: float
    eigenvalues: tuple[float, float]
    purity: float
    entropy: float
    is_positive_semidefinite: bool

    @property
    def dominant_state(self) -> str:
        return "box" if self.p_box >= self.p_impulse else "impulse"


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    if denominator == 0:
        return default
    value = numerator / denominator
    return value if isfinite(value) else default


def trade_imbalance(bucket: TradeBucket) -> float:
    return bucket.buy_taker_qty - bucket.sell_taker_qty


def normalized_trade_imbalance(bucket: TradeBucket) -> float:
    return safe_divide(
        bucket.buy_taker_qty - bucket.sell_taker_qty,
        bucket.total_qty,
    )


def orderbook_imbalance(levels: Iterable[tuple[float, float]]) -> float:
    """Return normalized bid/ask size imbalance from `(bid_size, ask_size)` levels."""
    bid_size = 0.0
    ask_size = 0.0
    for bid, ask in levels:
        bid_size += bid
        ask_size += ask
    return safe_divide(bid_size - ask_size, bid_size + ask_size)


def microprice(top: BookTop) -> float:
    denominator = top.bid_size + top.ask_size
    if denominator == 0:
        return top.mid_price
    return (top.bid_price * top.ask_size + top.ask_price * top.bid_size) / denominator


def forward_returns(prices: Sequence[float], horizon: int) -> list[float | None]:
    if horizon <= 0:
        raise ValueError("horizon must be positive")

    result: list[float | None] = []
    for idx, price in enumerate(prices):
        future_idx = idx + horizon
        if price <= 0 or future_idx >= len(prices):
            result.append(None)
            continue
        result.append(prices[future_idx] / price - 1.0)
    return result


def signal_from_imbalance(
    imbalance: float,
    *,
    entry_threshold: float,
    exit_threshold: float = 0.0,
) -> int:
    if entry_threshold <= 0:
        raise ValueError("entry_threshold must be positive")
    if imbalance >= entry_threshold:
        return 1
    if imbalance <= -entry_threshold:
        return -1
    if abs(imbalance) <= exit_threshold:
        return 0
    return 0


# ── Advanced Microstructure Features ──


def rolling_realized_volatility(
    prices: Sequence[float],
    window: int,
) -> list[float | None]:
    """Annualised realized volatility from log-returns over a rolling window."""
    from math import log, sqrt

    if window < 2:
        raise ValueError("window must be >= 2")

    result: list[float | None] = []
    for i in range(len(prices)):
        if i < window:
            result.append(None)
            continue
        log_rets = [
            log(prices[j] / prices[j - 1])
            for j in range(i - window + 1, i + 1)
            if prices[j - 1] > 0
        ]
        if len(log_rets) < 2:
            result.append(None)
            continue
        mean_r = sum(log_rets) / len(log_rets)
        var = sum((r - mean_r) ** 2 for r in log_rets) / (len(log_rets) - 1)
        result.append(sqrt(var) * sqrt(365.0 * 24.0 * 3600.0 / window))
    return result


def rolling_period_volatility_bps(
    prices: Sequence[float],
    window: int,
) -> list[float | None]:
    """
    Rolling standard deviation of log-returns in basis points (non-annualized).

    가격의 로그수익률 표준편차를 bps로 변환
    """

    from math import log, sqrt

    if window < 2:
        raise ValueError("window must be >= 2")

    result: list[float | None] = []
    for i in range(len(prices)):
        if i < window:
            result.append(None)
            continue
        log_rets = [
            log(prices[j] / prices[j - 1])
            for j in range(i - window + 1, i + 1)
            if prices[j - 1] > 0
        ]
        if len(log_rets) < 2:
            result.append(None)
            continue
        mean_r = sum(log_rets) / len(log_rets)
        var = sum((r - mean_r) ** 2 for r in log_rets) / (len(log_rets) - 1)
        result.append(sqrt(var) * 10_000.0)
    return result


def hurst_exponent(prices: Sequence[float], max_lag: int = 20) -> float:
    """
    R/S analysis based Hurst exponent.

    H > 0.5 → trending (momentum)
    H ≈ 0.5 → random walk
    H < 0.5 → mean-reverting

    Adapted from brand_score.ipynb Cell 1398.
    """
    from math import log, sqrt

    if len(prices) < max_lag + 1:
        return 0.5  # fallback: random walk

    log_rets = [log(prices[i] / prices[i - 1]) for i in range(1, len(prices)) if prices[i - 1] > 0]
    if len(log_rets) < max_lag:
        return 0.5  # fallback: random walk

    lags = list(range(2, max_lag))
    tau: list[float] = []
    for lag in lags:
        diffs = [log_rets[j] - log_rets[j - lag] for j in range(lag, len(log_rets))]
        if not diffs:
            continue
        mean_d = sum(diffs) / len(diffs)
        std_d = sqrt(sum((d - mean_d) ** 2 for d in diffs) / len(diffs))
        tau.append(sqrt(std_d) if std_d > 0 else 1e-10)

    if len(tau) < 2:
        return 0.5

    # log-log regression for slope
    log_lags = [log(lag) for lag in lags[: len(tau)]]
    log_tau = [log(t) for t in tau]
    n = len(log_lags)
    sum_x = sum(log_lags)
    sum_y = sum(log_tau)
    sum_xy = sum(x * y for x, y in zip(log_lags, log_tau))
    sum_x2 = sum(x * x for x in log_lags)
    denom = n * sum_x2 - sum_x * sum_x
    if denom == 0:
        return 0.5
    slope = (n * sum_xy - sum_x * sum_y) / denom
    return slope * 2.0


def vpin(
    buy_volumes: Sequence[float],
    sell_volumes: Sequence[float],
    window: int = 50,
) -> list[float | None]:
    """
    거래량 동기화 정보 기반 거래 확률(VPIN)

    VPIN = 이동 창 기간 동안 |매수_거래량 - 매도_거래량|의 평균 / (매수_거래량 + 매도_거래량)의 평균.
    VPIN이 높을수록 → 정보 기반 거래의 확률이 높아짐 → 잠재적인 변동성 사건 발생 가능성.
    """
    if len(buy_volumes) != len(sell_volumes):
        raise ValueError("buy_volumes and sell_volumes must have the same length")
    if window < 1:
        raise ValueError("window must be >= 1")

    result: list[float | None] = []
    for i in range(len(buy_volumes)):
        if i < window - 1:
            result.append(None)
            continue
        abs_imb_sum = 0.0
        total_vol_sum = 0.0
        for j in range(i - window + 1, i + 1):
            abs_imb_sum += abs(buy_volumes[j] - sell_volumes[j])
            total_vol_sum += buy_volumes[j] + sell_volumes[j]
        result.append(
            abs_imb_sum / total_vol_sum if total_vol_sum > 0 else 0.0
        )
    return result


def order_flow_imbalance_multilevel(
    bid_sizes: Sequence[Sequence[float]],
    ask_sizes: Sequence[Sequence[float]],
    prev_bid_prices: Sequence[Sequence[float]],
    curr_bid_prices: Sequence[Sequence[float]],
    prev_ask_prices: Sequence[Sequence[float]],
    curr_ask_prices: Sequence[Sequence[float]],
) -> list[float]:
    """
    Multi-level Order Flow Imbalance (OFI).

    For each time step, computes the net order-flow change across multiple
    price levels on both bid and ask sides. Positive OFI → buying pressure.
    """
    n = len(bid_sizes)
    result: list[float] = []
    for t in range(n):
        ofi = 0.0
        levels = min(len(bid_sizes[t]), len(ask_sizes[t]))
        for lv in range(levels):
            # bid side: size increase if price stays or rises
            if t > 0 and lv < len(prev_bid_prices[t]) and lv < len(curr_bid_prices[t]):
                if curr_bid_prices[t][lv] >= prev_bid_prices[t][lv]:
                    ofi += bid_sizes[t][lv] - (bid_sizes[t - 1][lv] if t > 0 and lv < len(bid_sizes[t - 1]) else 0)
                else:
                    ofi -= bid_sizes[t][lv]
            # ask side: size increase if price stays or drops → selling pressure
            if t > 0 and lv < len(prev_ask_prices[t]) and lv < len(curr_ask_prices[t]):
                if curr_ask_prices[t][lv] <= prev_ask_prices[t][lv]:
                    ofi -= ask_sizes[t][lv] - (ask_sizes[t - 1][lv] if t > 0 and lv < len(ask_sizes[t - 1]) else 0)
                else:
                    ofi += ask_sizes[t][lv]
        result.append(ofi)
    return result


def transfer_entropy(
    source: Sequence[float],
    target: Sequence[float],
    delay: int = 1,
    bins: int = 10,
) -> float:
    """
    소스에서 대상로의 전이 엔트로피.

    소스 시계열에서 대상으로의 정보 흐름을 측정하며,
    인과적 영향의 방향을 나타냅니다.

    brand_score.ipynb의 1390번 셀에서 발췌.
    """
    from math import log2

    n = len(source)
    if n != len(target):
        raise ValueError("source and target must have the same length")
    if n < delay + 2:
        return 0.0

    def _digitize(values: Sequence[float], num_bins: int) -> list[int]:
        mn = min(values)
        mx = max(values)
        rng = mx - mn if mx > mn else 1.0
        return [min(int((v - mn) / rng * num_bins), num_bins - 1) for v in values]

    src_d = _digitize(list(source), bins)
    tgt_d = _digitize(list(target), bins)

    # Build joint counts: P(y_next, y_past, x_past), P(y_next, y_past), P(y_past, x_past), P(y_past)
    joint_yyx: dict[tuple[int, int, int], int] = {}
    joint_yy: dict[tuple[int, int], int] = {}
    joint_yx: dict[tuple[int, int], int] = {}
    count_y: dict[int, int] = {}

    valid = n - delay - 1
    for i in range(delay, n - 1):
        y_next = tgt_d[i + 1]
        y_past = tgt_d[i]
        x_past = src_d[i - delay]

        key_yyx = (y_next, y_past, x_past)
        joint_yyx[key_yyx] = joint_yyx.get(key_yyx, 0) + 1

        key_yy = (y_next, y_past)
        joint_yy[key_yy] = joint_yy.get(key_yy, 0) + 1

        key_yx = (y_past, x_past)
        joint_yx[key_yx] = joint_yx.get(key_yx, 0) + 1

        count_y[y_past] = count_y.get(y_past, 0) + 1

    # TE = Σ P(y_next, y_past, x_past) * log2(P(y_next|y_past,x_past) / P(y_next|y_past))
    te = 0.0
    for (y_next, y_past, x_past), count in joint_yyx.items():
        p_yyx = count / valid
        p_ynext_given_yx = count / joint_yx.get((y_past, x_past), 1)
        p_ynext_given_y = joint_yy.get((y_next, y_past), 1) / count_y.get(y_past, 1)
        if p_ynext_given_yx > 0 and p_ynext_given_y > 0:
            te += p_yyx * log2(p_ynext_given_yx / p_ynext_given_y)

    return max(0.0, te)


# ── Open Interest & Box Range Features ──


def rolling_min_max_channel(
    values: Sequence[float],
    window: int,
    noise_percent: float = 0.005,
) -> list[tuple[float, float] | None]:
    """
    noise_percent 값에 따라 조정된 창 범위 내에서 채널의 이동 최소값/최대값을 계산

    하한: min + (range * noise_percent)
    상한: max - (range * noise_percent)
    """
    if window < 2:
        raise ValueError("window must be >= 2")

    result: list[tuple[float, float] | None] = []
    for i in range(len(values)):
        if i < window - 1:
            result.append(None)
            continue
        window_vals = values[i - window + 1 : i + 1]
        mx = max(window_vals)
        mn = min(window_vals)
        rng = mx - mn
        if rng == 0:
            result.append((mn, mx))
        else:
            result.append((mn + rng * noise_percent, mx - rng * noise_percent))
    return result


def buyer_taker_density(
    is_buyer_maker: Sequence[bool],
    window: int,
) -> list[float | None]:
    if window < 1:
        raise ValueError("window must be >= 1")

    result: list[float | None] = []
    for i in range(len(is_buyer_maker)):
        if i < window - 1:
            result.append(None)
            continue
        window_events = is_buyer_maker[i - window + 1 : i + 1]
        buyer_initiated_count = sum(1 for is_seller in window_events if not is_seller)
        result.append(buyer_initiated_count / window)
    return result


def market_quantum_density_matrix(
    realized_volatility_bps: Sequence[float | None],
    ob_imbalances: Sequence[float],
    volatility_threshold_bps: float = 5.0,
) -> list[tuple[float, float, float] | None]:
    from math import exp, sqrt

    n = len(ob_imbalances)
    if len(realized_volatility_bps) != n:
        raise ValueError("realized_volatility_bps and ob_imbalances must have the same length")

    result: list[tuple[float, float, float] | None] = []
    for i in range(n):
        vol_bps = realized_volatility_bps[i]
        if vol_bps is None:
            result.append(None)
            continue

        # p_box: probability of range-bound (low volatility)
        # Using sigmoid transition around threshold
        p_box = 1.0 / (1.0 + exp(min(max(vol_bps - volatility_threshold_bps, -50.0), 50.0)))
        p_impulse = 1.0 - p_box

        # Coherence: strength of relation between box and breakout states, scaled by depth imbalance
        coherence = sqrt(p_box * p_impulse) * ob_imbalances[i]

        result.append((p_box, p_impulse, coherence))
    return result


def density_matrix_from_components(
    p_box: float,
    p_impulse: float,
    coherence: float,
) -> DensityMatrix2x2:
    return (
        (p_box, coherence),
        (coherence, p_impulse),
    )


def density_matrix_trace(matrix: DensityMatrix2x2) -> float:
    return matrix[0][0] + matrix[1][1]


def density_matrix_determinant(matrix: DensityMatrix2x2) -> float:
    return matrix[0][0] * matrix[1][1] - matrix[0][1] * matrix[1][0]


def density_matrix_eigenvalues(matrix: DensityMatrix2x2) -> tuple[float, float]:
    from math import sqrt

    trace = density_matrix_trace(matrix)
    determinant = density_matrix_determinant(matrix)
    discriminant = max(trace * trace - 4.0 * determinant, 0.0)
    root = sqrt(discriminant)
    low = (trace - root) / 2.0
    high = (trace + root) / 2.0
    return low, high


def density_matrix_purity(matrix: DensityMatrix2x2) -> float:
    """Return Tr(rho^2) for a real 2x2 density-matrix-like state."""
    a, b = matrix[0]
    c, d = matrix[1]
    return a * a + d * d + 2.0 * b * c


def density_matrix_entropy(
    matrix: DensityMatrix2x2,
    *,
    eigenvalue_floor: float = 1e-12,
) -> float:
    """Return von Neumann entropy, -Tr(rho log rho), using eigenvalues."""
    from math import log

    entropy = 0.0
    for eigenvalue in density_matrix_eigenvalues(matrix):
        if eigenvalue <= eigenvalue_floor:
            continue
        entropy -= eigenvalue * log(eigenvalue)
    return entropy


def is_positive_semidefinite_density_matrix(
    matrix: DensityMatrix2x2,
    *,
    tolerance: float = 1e-12,
) -> bool:
    return all(eigenvalue >= -tolerance for eigenvalue in density_matrix_eigenvalues(matrix))


def market_quantum_density_matrix_states(
    realized_volatility_bps: Sequence[float | None],
    ob_imbalances: Sequence[float],
    volatility_threshold_bps: float = 5.0,
    *,
    psd_tolerance: float = 1e-12,
) -> list[MarketDensityMatrixState | None]:

    components = market_quantum_density_matrix(
        realized_volatility_bps,
        ob_imbalances,
        volatility_threshold_bps=volatility_threshold_bps,
    )

    states: list[MarketDensityMatrixState | None] = []
    for component in components:
        if component is None:
            states.append(None)
            continue

        p_box, p_impulse, coherence = component
        matrix = density_matrix_from_components(p_box, p_impulse, coherence)
        states.append(
            MarketDensityMatrixState(
                p_box=p_box,
                p_impulse=p_impulse,
                coherence=coherence,
                matrix=matrix,
                trace=density_matrix_trace(matrix),
                determinant=density_matrix_determinant(matrix),
                eigenvalues=density_matrix_eigenvalues(matrix),
                purity=density_matrix_purity(matrix),
                entropy=density_matrix_entropy(matrix),
                is_positive_semidefinite=is_positive_semidefinite_density_matrix(
                    matrix,
                    tolerance=psd_tolerance,
                ),
            )
        )
    return states
