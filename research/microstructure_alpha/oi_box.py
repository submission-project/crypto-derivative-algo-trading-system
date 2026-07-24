# 가격/OI 박스권을 자동으로 잡는 메소드

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass, replace
from typing import Literal

import numpy as np
import pandas as pd

BreakDirection = Literal["up", "down", "center_shift", "end"]

# 박스 하나를 담는 데이터 구조
@dataclass(frozen=True, slots=True)
class OIBox:
    box_id: int
    start: pd.Timestamp # 박스 시작
    end: pd.Timestamp   # 박스 끝
    low: float          # 박스 하단
    high: float         # 박스 상단
    mid: float          # 박스 중간
    width: float        # 박스 너비
    bars: int           # 박스 기간(지속된 봉 개수)
    coverage: float     # 박스 커버리지(값들이 박스 안에 있었던 비율)
    low_touches: int    # 하단 근처 접촉 횟수
    high_touches: int   # 상단 근처 접촉 횟수
    break_direction: BreakDirection # 박스가 어떻게 끝났는지
    score: float        # 박스 품질 점수

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class _StableBoxCandidate:
    run_id: int
    box: OIBox


@dataclass(frozen=True, slots=True)
class StableOIBoxDiagnostics:
    start_offsets: tuple[int, ...]
    raw_boxes: int
    after_warmup_boxes: int
    after_width_boxes: int
    rejected_by_warmup: int
    rejected_by_width: int
    clusters: int
    required_support: int
    stable_clusters: int
    stable_boxes: int
    max_raw_width_ratio: float | None
    max_candidate_width_ratio: float | None
    cluster_supports: tuple[int, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _prepare_oi_frame(
    frame: pd.DataFrame,
    *,
    value_col: str,
    timestamp_col: str,
) -> pd.DataFrame:
    """
    timestamp, value column 존재 확인
    timestamp를 datetime으로 변환
    value를 numeric으로 변환
    NaN 제거
    시간순 정렬
    중복 timestamp 제거
    """
    if value_col not in frame.columns:
        raise ValueError(f"missing value column: {value_col}")
    if timestamp_col not in frame.columns:
        raise ValueError(f"missing timestamp column: {timestamp_col}")

    data = frame[[timestamp_col, value_col]].copy()
    data[timestamp_col] = pd.to_datetime(data[timestamp_col])
    data[value_col] = pd.to_numeric(data[value_col], errors="coerce")
    data = data.dropna().sort_values(timestamp_col).drop_duplicates(timestamp_col)
    data = data.reset_index(drop=True)
    if data.empty:
        raise ValueError("OI frame has no usable rows")
    return data


def _robust_bounds(
    values: np.ndarray,
    *,
    lower_quantile: float,
    upper_quantile: float,
    margin_ratio: float,
    min_width: float,
) -> tuple[float, float]:
    low, high = np.nanquantile(values, [lower_quantile, upper_quantile])
    width = max(float(high - low), min_width)
    return float(low - width * margin_ratio), float(high + width * margin_ratio)


def _summarize_box(
    *,
    box_id: int,
    data: pd.DataFrame,
    start_idx: int,
    end_idx: int,
    value_col: str,
    timestamp_col: str,
    lower_quantile: float,
    upper_quantile: float,
    margin_ratio: float,
    min_width: float,
    touch_ratio: float,
    break_direction: BreakDirection,
) -> OIBox | None:
    if end_idx < start_idx:
        return None

    segment = data.iloc[start_idx : end_idx + 1]
    values = segment[value_col].to_numpy(dtype=float)
    if len(values) == 0:
        return None

    low, high = _robust_bounds(
        values,
        lower_quantile=lower_quantile,
        upper_quantile=upper_quantile,
        margin_ratio=margin_ratio,
        min_width=min_width,
    )
    width = high - low
    mid = (low + high) / 2.0
    touch_band = max(width * touch_ratio, min_width)
    inside = (values >= low) & (values <= high)
    low_touches = int(np.sum(values <= low + touch_band))
    high_touches = int(np.sum(values >= high - touch_band))
    coverage = float(np.mean(inside))

    touch_score = min(1.0, (low_touches + high_touches) / max(2.0, len(values) * 0.08))
    coverage_score = min(1.0, coverage)
    duration_score = min(1.0, len(values) / 96.0)
    score = 0.55 * coverage_score + 0.30 * touch_score + 0.15 * duration_score

    return OIBox(
        box_id=box_id,
        start=pd.Timestamp(segment[timestamp_col].iloc[0]),
        end=pd.Timestamp(segment[timestamp_col].iloc[-1]),
        low=low,
        high=high,
        mid=mid,
        width=width,
        bars=int(len(values)),
        coverage=coverage,
        low_touches=low_touches,
        high_touches=high_touches,
        break_direction=break_direction,
        score=float(score),
    )


"""
1. 데이터 정리
2. start_idx부터 현재 idx까지 후보 구간 생성
3. 후보 구간의 robust bounds 계산
4. 현재 값이 박스 밖으로 나갔는지 확인
5. 일정 bars 이상 지속 이탈하면 박스 종료
6. 중심값이 크게 이동해도 박스 종료
7. 마지막 남은 구간도 박스로 요약
"""
def detect_oi_box_ranges(
    frame: pd.DataFrame,
    *,
    value_col: str = "exchange_oi_sum",
    timestamp_col: str = "timestamp",
    min_bars: int = 72, #최소 6시간짜리 구간만 박스로 인정
    lower_quantile: float = 0.10,
    upper_quantile: float = 0.90,
    margin_ratio: float = 0.08,
    breakout_buffer_ratio: float = 0.12,
    breakout_confirm_bars: int = 6, #30분 이상 이탈해야 진짜 이탈로 봄
    center_shift_bars: int = 48, #최근 4시간의 중심 변화 확인
    center_shift_ratio: float = 0.90, #기존 박스 폭의 90% 이상 중심이 이동하면 새 박스로 판단
    min_width_ratio: float = 0.001,
    touch_ratio: float = 0.12,
) -> list[OIBox]:
    if min_bars < 3:
        raise ValueError("min_bars must be at least 3")
    if breakout_confirm_bars < 1:
        raise ValueError("breakout_confirm_bars must be positive")
    if not 0.0 < lower_quantile < upper_quantile < 1.0:
        raise ValueError("quantiles must satisfy 0 < lower < upper < 1")
    if margin_ratio < 0:
        raise ValueError("margin_ratio must be non-negative")
    if breakout_buffer_ratio < 0:
        raise ValueError("breakout_buffer_ratio must be non-negative")
    if center_shift_bars < 2:
        raise ValueError("center_shift_bars must be at least 2")
    if center_shift_ratio <= 0:
        raise ValueError("center_shift_ratio must be positive")
    if min_width_ratio < 0:
        raise ValueError("min_width_ratio must be non-negative")

    data = _prepare_oi_frame(frame, value_col=value_col, timestamp_col=timestamp_col)
    values = data[value_col].to_numpy(dtype=float)
    global_width = max(float(np.nanmedian(values)) * min_width_ratio, 1e-12)

    boxes: list[OIBox] = []
    start_idx = 0
    outside_count = 0
    outside_direction: BreakDirection | None = None
    idx = min_bars

    while idx < len(data):
        current_values = values[start_idx:idx]
        low, high = _robust_bounds(
            current_values,
            lower_quantile=lower_quantile,
            upper_quantile=upper_quantile,
            margin_ratio=margin_ratio,
            min_width=global_width,
        )
        width = high - low
        buffer = max(width * breakout_buffer_ratio, global_width)
        value = values[idx]

        direction: BreakDirection | None = None
        if value > high + buffer:
            direction = "up"
        elif value < low - buffer:
            direction = "down"

        if direction is None:
            outside_count = 0
            outside_direction = None
        elif direction == outside_direction:
            outside_count += 1
        else:
            outside_direction = direction
            outside_count = 1

        if outside_direction and outside_count >= breakout_confirm_bars:
            end_idx = idx - breakout_confirm_bars
            box = _summarize_box(
                box_id=len(boxes),
                data=data,
                start_idx=start_idx,
                end_idx=end_idx,
                value_col=value_col,
                timestamp_col=timestamp_col,
                lower_quantile=lower_quantile,
                upper_quantile=upper_quantile,
                margin_ratio=margin_ratio,
                min_width=global_width,
                touch_ratio=touch_ratio,
                break_direction=outside_direction,
            )
            if box is not None and box.bars >= min_bars:
                boxes.append(box)
            start_idx = max(end_idx + 1, idx - breakout_confirm_bars + 1)
            idx = start_idx + min_bars
            outside_count = 0
            outside_direction = None
            continue

        enough_history = idx - start_idx >= min_bars + center_shift_bars
        if enough_history:
            stable_values = values[start_idx : idx - center_shift_bars]
            recent_values = values[idx - center_shift_bars : idx]
            stable_low, stable_high = _robust_bounds(
                stable_values,
                lower_quantile=lower_quantile,
                upper_quantile=upper_quantile,
                margin_ratio=margin_ratio,
                min_width=global_width,
            )
            stable_width = stable_high - stable_low
            center_gap = abs(float(np.nanmedian(recent_values) - np.nanmedian(stable_values)))
            if center_gap > max(stable_width * center_shift_ratio, global_width):
                end_idx = idx - center_shift_bars - 1
                box = _summarize_box(
                    box_id=len(boxes),
                    data=data,
                    start_idx=start_idx,
                    end_idx=end_idx,
                    value_col=value_col,
                    timestamp_col=timestamp_col,
                    lower_quantile=lower_quantile,
                    upper_quantile=upper_quantile,
                    margin_ratio=margin_ratio,
                    min_width=global_width,
                    touch_ratio=touch_ratio,
                    break_direction="center_shift",
                )
                if box is not None and box.bars >= min_bars:
                    boxes.append(box)
                start_idx = idx - center_shift_bars
                idx = start_idx + min_bars
                outside_count = 0
                outside_direction = None
                continue

        idx += 1

    final_box = _summarize_box(
        box_id=len(boxes),
        data=data,
        start_idx=start_idx,
        end_idx=len(data) - 1,
        value_col=value_col,
        timestamp_col=timestamp_col,
        lower_quantile=lower_quantile,
        upper_quantile=upper_quantile,
        margin_ratio=margin_ratio,
        min_width=global_width,
        touch_ratio=touch_ratio,
        break_direction="end",
    )
    if final_box is not None and final_box.bars >= min_bars:
        boxes.append(final_box)

    return boxes


def _timestamp_overlap_ratio(
    left_start: pd.Timestamp,
    left_end: pd.Timestamp,
    right_start: pd.Timestamp,
    right_end: pd.Timestamp,
) -> float:
    latest_start = max(left_start, right_start)
    earliest_end = min(left_end, right_end)
    if earliest_end <= latest_start:
        return 0.0

    left_duration = max((left_end - left_start).total_seconds(), 1e-12)
    right_duration = max((right_end - right_start).total_seconds(), 1e-12)
    overlap = (earliest_end - latest_start).total_seconds()
    return float(overlap / min(left_duration, right_duration))


def _median_timestamp(timestamps: list[pd.Timestamp]) -> pd.Timestamp:
    if not timestamps:
        raise ValueError("timestamps must not be empty")
    return pd.Timestamp(int(np.median([timestamp.value for timestamp in timestamps])))


def _most_common_break_direction(boxes: list[OIBox]) -> BreakDirection:
    if not boxes:
        return "end"
    counts = Counter(box.break_direction for box in boxes)
    return counts.most_common(1)[0][0]


def _summarize_fixed_bounds_box(
    *,
    box_id: int,
    data: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    low: float,
    high: float,
    value_col: str,
    timestamp_col: str,
    min_width: float,
    touch_ratio: float,
    break_direction: BreakDirection,
    stability_ratio: float,
) -> OIBox | None:
    if high <= low or end <= start:
        return None

    mask = (data[timestamp_col] >= start) & (data[timestamp_col] <= end)
    segment = data.loc[mask, [timestamp_col, value_col]]
    if segment.empty:
        return None

    values = segment[value_col].to_numpy(dtype=float)
    width = float(high - low)
    mid = float((low + high) / 2.0)
    touch_band = max(width * touch_ratio, min_width)
    inside = (values >= low) & (values <= high)
    low_touches = int(np.sum(values <= low + touch_band))
    high_touches = int(np.sum(values >= high - touch_band))
    coverage = float(np.mean(inside))

    touch_score = min(1.0, (low_touches + high_touches) / max(2.0, len(values) * 0.08))
    coverage_score = min(1.0, coverage)
    duration_score = min(1.0, len(values) / 96.0)
    stability_score = min(1.0, max(0.0, stability_ratio))
    score = (
        0.42 * coverage_score
        + 0.23 * touch_score
        + 0.15 * duration_score
        + 0.20 * stability_score
    )

    return OIBox(
        box_id=box_id,
        start=pd.Timestamp(segment[timestamp_col].iloc[0]),
        end=pd.Timestamp(segment[timestamp_col].iloc[-1]),
        low=float(low),
        high=float(high),
        mid=mid,
        width=width,
        bars=int(len(values)),
        coverage=coverage,
        low_touches=low_touches,
        high_touches=high_touches,
        break_direction=break_direction,
        score=float(score),
    )


def _box_width_ratio(box: OIBox) -> float:
    return float(box.width / max(abs(box.mid), 1e-12))


def _cluster_stable_box_candidates(
    candidates: list[_StableBoxCandidate],
    *,
    global_width: float,
    level_tolerance_ratio: float,
    time_overlap_ratio: float,
) -> list[list[_StableBoxCandidate]]:
    clusters: list[list[_StableBoxCandidate]] = []
    sorted_candidates = sorted(candidates, key=lambda candidate: (candidate.box.start, candidate.box.mid))

    for candidate in sorted_candidates:
        box = candidate.box
        best_cluster_idx: int | None = None
        best_overlap = 0.0
        for cluster_idx, cluster in enumerate(clusters):
            cluster_boxes = [item.box for item in cluster]
            cluster_mid = float(np.median([item.mid for item in cluster_boxes]))
            cluster_width = float(np.median([item.width for item in cluster_boxes]))
            level_tolerance = max(
                abs(cluster_mid) * level_tolerance_ratio,
                cluster_width * 0.75,
                global_width,
            )
            if abs(box.mid - cluster_mid) > level_tolerance:
                continue

            overlap = max(
                _timestamp_overlap_ratio(box.start, box.end, other.start, other.end)
                for other in cluster_boxes
            )
            if overlap >= time_overlap_ratio and overlap > best_overlap:
                best_cluster_idx = cluster_idx
                best_overlap = overlap

        if best_cluster_idx is None:
            clusters.append([candidate])
        else:
            clusters[best_cluster_idx].append(candidate)

    return clusters


def _detect_stable_oi_box_ranges_with_diagnostics(
    frame: pd.DataFrame,
    *,
    value_col: str = "exchange_oi_sum",
    timestamp_col: str = "timestamp",
    min_bars: int = 72,
    lower_quantile: float = 0.10,
    upper_quantile: float = 0.90,
    margin_ratio: float = 0.08,
    breakout_buffer_ratio: float = 0.12,
    breakout_confirm_bars: int = 6,
    center_shift_bars: int = 48,
    center_shift_ratio: float = 0.90,
    min_width_ratio: float = 0.001,
    touch_ratio: float = 0.12,
    start_step_bars: int | None = None,
    max_start_offset_bars: int | None = None,
    warmup_bars: int | None = None,
    max_box_width_ratio: float | None = 0.035,
    min_consensus_starts: int = 2,
    min_consensus_ratio: float = 0.30,
    level_tolerance_ratio: float = 0.006,
    time_overlap_ratio: float = 0.35,
) -> tuple[list[OIBox], StableOIBoxDiagnostics]:
    if min_consensus_starts < 1:
        raise ValueError("min_consensus_starts must be positive")
    if not 0.0 < min_consensus_ratio <= 1.0:
        raise ValueError("min_consensus_ratio must be in (0, 1]")
    if level_tolerance_ratio < 0:
        raise ValueError("level_tolerance_ratio must be non-negative")
    if not 0.0 <= time_overlap_ratio <= 1.0:
        raise ValueError("time_overlap_ratio must be in [0, 1]")
    if max_box_width_ratio is not None and max_box_width_ratio <= 0:
        raise ValueError("max_box_width_ratio must be positive when provided")

    data = _prepare_oi_frame(frame, value_col=value_col, timestamp_col=timestamp_col)
    if len(data) < min_bars:
        diagnostics = StableOIBoxDiagnostics(
            start_offsets=(),
            raw_boxes=0,
            after_warmup_boxes=0,
            after_width_boxes=0,
            rejected_by_warmup=0,
            rejected_by_width=0,
            clusters=0,
            required_support=0,
            stable_clusters=0,
            stable_boxes=0,
            max_raw_width_ratio=None,
            max_candidate_width_ratio=None,
            cluster_supports=(),
        )
        return [], diagnostics

    values = data[value_col].to_numpy(dtype=float)
    global_width = max(float(np.nanmedian(values)) * min_width_ratio, 1e-12)
    step = start_step_bars if start_step_bars is not None else max(1, center_shift_bars // 2)
    if step < 1:
        raise ValueError("start_step_bars must be positive")

    max_offset = (
        max_start_offset_bars
        if max_start_offset_bars is not None
        else min(len(data) - min_bars, min_bars + center_shift_bars)
    )
    if max_offset < 0:
        diagnostics = StableOIBoxDiagnostics(
            start_offsets=(),
            raw_boxes=0,
            after_warmup_boxes=0,
            after_width_boxes=0,
            rejected_by_warmup=0,
            rejected_by_width=0,
            clusters=0,
            required_support=0,
            stable_clusters=0,
            stable_boxes=0,
            max_raw_width_ratio=None,
            max_candidate_width_ratio=None,
            cluster_supports=(),
        )
        return [], diagnostics

    warmup = warmup_bars if warmup_bars is not None else min_bars
    if warmup < 0:
        raise ValueError("warmup_bars must be non-negative")

    offsets = list(range(0, min(max_offset, len(data) - min_bars) + 1, step))
    if 0 not in offsets:
        offsets.insert(0, 0)

    required_support = max(
        min_consensus_starts,
        int(np.ceil(len(offsets) * min_consensus_ratio)),
    )
    required_support = min(required_support, len(offsets))

    candidates: list[_StableBoxCandidate] = []
    raw_width_ratios: list[float] = []
    candidate_width_ratios: list[float] = []
    raw_boxes = 0
    after_warmup_boxes = 0
    rejected_by_warmup = 0
    rejected_by_width = 0

    for run_id, offset in enumerate(offsets):
        shifted = data.iloc[offset:].reset_index(drop=True)
        shifted_boxes = detect_oi_box_ranges(
            shifted,
            value_col=value_col,
            timestamp_col=timestamp_col,
            min_bars=min_bars,
            lower_quantile=lower_quantile,
            upper_quantile=upper_quantile,
            margin_ratio=margin_ratio,
            breakout_buffer_ratio=breakout_buffer_ratio,
            breakout_confirm_bars=breakout_confirm_bars,
            center_shift_bars=center_shift_bars,
            center_shift_ratio=center_shift_ratio,
            min_width_ratio=min_width_ratio,
            touch_ratio=touch_ratio,
        )
        warmup_timestamp = data[timestamp_col].iloc[min(offset + warmup, len(data) - 1)]
        for box in shifted_boxes:
            raw_boxes += 1
            raw_width_ratios.append(_box_width_ratio(box))
            if box.start < warmup_timestamp:
                rejected_by_warmup += 1
                continue
            after_warmup_boxes += 1
            if max_box_width_ratio is not None and _box_width_ratio(box) > max_box_width_ratio:
                rejected_by_width += 1
                continue
            candidate_width_ratios.append(_box_width_ratio(box))
            candidates.append(_StableBoxCandidate(run_id=run_id, box=box))

    if not candidates:
        diagnostics = StableOIBoxDiagnostics(
            start_offsets=tuple(offsets),
            raw_boxes=raw_boxes,
            after_warmup_boxes=after_warmup_boxes,
            after_width_boxes=0,
            rejected_by_warmup=rejected_by_warmup,
            rejected_by_width=rejected_by_width,
            clusters=0,
            required_support=required_support,
            stable_clusters=0,
            stable_boxes=0,
            max_raw_width_ratio=max(raw_width_ratios) if raw_width_ratios else None,
            max_candidate_width_ratio=None,
            cluster_supports=(),
        )
        return [], diagnostics

    clusters = _cluster_stable_box_candidates(
        candidates,
        global_width=global_width,
        level_tolerance_ratio=level_tolerance_ratio,
        time_overlap_ratio=time_overlap_ratio,
    )

    stable_boxes: list[OIBox] = []
    cluster_supports: list[int] = []
    stable_clusters = 0
    for cluster in clusters:
        support = len({candidate.run_id for candidate in cluster})
        cluster_supports.append(support)
        if support < required_support:
            continue
        stable_clusters += 1

        cluster_boxes = [candidate.box for candidate in cluster]
        low = float(np.median([box.low for box in cluster_boxes]))
        high = float(np.median([box.high for box in cluster_boxes]))
        start = _median_timestamp([box.start for box in cluster_boxes])
        end = _median_timestamp([box.end for box in cluster_boxes])
        break_direction = _most_common_break_direction(cluster_boxes)
        stability_ratio = support / max(len(offsets), 1)
        stable_box = _summarize_fixed_bounds_box(
            box_id=len(stable_boxes),
            data=data,
            start=start,
            end=end,
            low=low,
            high=high,
            value_col=value_col,
            timestamp_col=timestamp_col,
            min_width=global_width,
            touch_ratio=touch_ratio,
            break_direction=break_direction,
            stability_ratio=stability_ratio,
        )
        if stable_box is not None and stable_box.bars >= min_bars:
            stable_boxes.append(stable_box)

    stable_boxes = sorted(stable_boxes, key=lambda box: (box.start, box.end, box.mid))
    stable_boxes = [replace(box, box_id=box_id) for box_id, box in enumerate(stable_boxes)]
    diagnostics = StableOIBoxDiagnostics(
        start_offsets=tuple(offsets),
        raw_boxes=raw_boxes,
        after_warmup_boxes=after_warmup_boxes,
        after_width_boxes=len(candidates),
        rejected_by_warmup=rejected_by_warmup,
        rejected_by_width=rejected_by_width,
        clusters=len(clusters),
        required_support=required_support,
        stable_clusters=stable_clusters,
        stable_boxes=len(stable_boxes),
        max_raw_width_ratio=max(raw_width_ratios) if raw_width_ratios else None,
        max_candidate_width_ratio=max(candidate_width_ratios) if candidate_width_ratios else None,
        cluster_supports=tuple(sorted(cluster_supports, reverse=True)),
    )
    return stable_boxes, diagnostics


def detect_stable_oi_box_ranges(
    frame: pd.DataFrame,
    *,
    value_col: str = "exchange_oi_sum",
    timestamp_col: str = "timestamp",
    min_bars: int = 72,
    lower_quantile: float = 0.10,
    upper_quantile: float = 0.90,
    margin_ratio: float = 0.08,
    breakout_buffer_ratio: float = 0.12,
    breakout_confirm_bars: int = 6,
    center_shift_bars: int = 48,
    center_shift_ratio: float = 0.90,
    min_width_ratio: float = 0.001,
    touch_ratio: float = 0.12,
    start_step_bars: int | None = None,
    max_start_offset_bars: int | None = None,
    warmup_bars: int | None = None,
    max_box_width_ratio: float | None = 0.035,
    min_consensus_starts: int = 2,
    min_consensus_ratio: float = 0.30,
    level_tolerance_ratio: float = 0.006,
    time_overlap_ratio: float = 0.35,
) -> list[OIBox]:
    """
     차트의 보이는 왼쪽 가장자리에서 박스를 그리는 방식을 모방하기 때문에 의도적으로 경로에 의존적
    이 래퍼는 해당 동작을 그대로 유지하지만, 다음 세 가지 추가 검사를 통과한 박스만 허용

    1. 각 이동된 시작점(`warmup_bars`)에 너무 가까운 박스는 무시
    2. 중간 레벨에 비해 너비가 너무 큰 박스는 제외
    3. 여러 개의 이동된 시작 지점에서 나타나는 박스만 유지
    """
    boxes, _ = _detect_stable_oi_box_ranges_with_diagnostics(
        frame,
        value_col=value_col,
        timestamp_col=timestamp_col,
        min_bars=min_bars,
        lower_quantile=lower_quantile,
        upper_quantile=upper_quantile,
        margin_ratio=margin_ratio,
        breakout_buffer_ratio=breakout_buffer_ratio,
        breakout_confirm_bars=breakout_confirm_bars,
        center_shift_bars=center_shift_bars,
        center_shift_ratio=center_shift_ratio,
        min_width_ratio=min_width_ratio,
        touch_ratio=touch_ratio,
        start_step_bars=start_step_bars,
        max_start_offset_bars=max_start_offset_bars,
        warmup_bars=warmup_bars,
        max_box_width_ratio=max_box_width_ratio,
        min_consensus_starts=min_consensus_starts,
        min_consensus_ratio=min_consensus_ratio,
        level_tolerance_ratio=level_tolerance_ratio,
        time_overlap_ratio=time_overlap_ratio,
    )
    return boxes


def diagnose_stable_oi_box_ranges(
    frame: pd.DataFrame,
    *,
    value_col: str = "exchange_oi_sum",
    timestamp_col: str = "timestamp",
    min_bars: int = 72,
    lower_quantile: float = 0.10,
    upper_quantile: float = 0.90,
    margin_ratio: float = 0.08,
    breakout_buffer_ratio: float = 0.12,
    breakout_confirm_bars: int = 6,
    center_shift_bars: int = 48,
    center_shift_ratio: float = 0.90,
    min_width_ratio: float = 0.001,
    touch_ratio: float = 0.12,
    start_step_bars: int | None = None,
    max_start_offset_bars: int | None = None,
    warmup_bars: int | None = None,
    max_box_width_ratio: float | None = 0.035,
    min_consensus_starts: int = 2,
    min_consensus_ratio: float = 0.30,
    level_tolerance_ratio: float = 0.006,
    time_overlap_ratio: float = 0.35,
) -> StableOIBoxDiagnostics:
    """
    stable box가 왜 안 잡혔는지 확인하는 함수
    """
    _, diagnostics = _detect_stable_oi_box_ranges_with_diagnostics(
        frame,
        value_col=value_col,
        timestamp_col=timestamp_col,
        min_bars=min_bars,
        lower_quantile=lower_quantile,
        upper_quantile=upper_quantile,
        margin_ratio=margin_ratio,
        breakout_buffer_ratio=breakout_buffer_ratio,
        breakout_confirm_bars=breakout_confirm_bars,
        center_shift_bars=center_shift_bars,
        center_shift_ratio=center_shift_ratio,
        min_width_ratio=min_width_ratio,
        touch_ratio=touch_ratio,
        start_step_bars=start_step_bars,
        max_start_offset_bars=max_start_offset_bars,
        warmup_bars=warmup_bars,
        max_box_width_ratio=max_box_width_ratio,
        min_consensus_starts=min_consensus_starts,
        min_consensus_ratio=min_consensus_ratio,
        level_tolerance_ratio=level_tolerance_ratio,
        time_overlap_ratio=time_overlap_ratio,
    )
    return diagnostics


def oi_boxes_to_frame(boxes: list[OIBox]) -> pd.DataFrame:
    return pd.DataFrame([box.to_dict() for box in boxes])


def oi_box_lines_to_frame(boxes: list[OIBox], *, include_mid: bool = True) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for box in boxes:
        rows.append(
            {
                "box_id": box.box_id,
                "line_type": "vertical",
                "line_name": "start",
                "timestamp": box.start,
                "level": np.nan,
            }
        )
        rows.append(
            {
                "box_id": box.box_id,
                "line_type": "vertical",
                "line_name": "end",
                "timestamp": box.end,
                "level": np.nan,
            }
        )
        rows.append(
            {
                "box_id": box.box_id,
                "line_type": "horizontal",
                "line_name": "low",
                "timestamp": pd.NaT,
                "level": box.low,
            }
        )
        rows.append(
            {
                "box_id": box.box_id,
                "line_type": "horizontal",
                "line_name": "high",
                "timestamp": pd.NaT,
                "level": box.high,
            }
        )
        if include_mid:
            rows.append(
                {
                    "box_id": box.box_id,
                    "line_type": "horizontal",
                    "line_name": "mid",
                    "timestamp": pd.NaT,
                    "level": box.mid,
                }
            )
    return pd.DataFrame(rows)


def assign_oi_box_features(
    frame: pd.DataFrame,
    boxes: list[OIBox],
    *,
    value_col: str = "exchange_oi_sum",
    timestamp_col: str = "timestamp",
    edge_ratio: float = 0.15,
) -> pd.DataFrame:
    if not 0.0 < edge_ratio < 0.5:
        raise ValueError("edge_ratio must be between 0 and 0.5")

    data = frame.copy()
    data[timestamp_col] = pd.to_datetime(data[timestamp_col])
    data["oi_box_id"] = pd.NA
    data["oi_box_low"] = np.nan
    data["oi_box_high"] = np.nan
    data["oi_box_mid"] = np.nan
    data["oi_box_position"] = np.nan
    data["oi_box_zone"] = "outside"

    for box in boxes:
        mask = (data[timestamp_col] >= box.start) & (data[timestamp_col] <= box.end)
        if not mask.any():
            continue
        row_index = data.index[mask]
        position = (pd.to_numeric(data.loc[row_index, value_col], errors="coerce") - box.low) / box.width
        zone = pd.Series("inside", index=row_index, dtype="object")
        zone.loc[position < 0.0] = "breakdown"
        zone.loc[position > 1.0] = "breakout"
        zone.loc[position.between(0.0, edge_ratio, inclusive="both")] = "lower_edge"
        zone.loc[position.between(edge_ratio, 1.0 - edge_ratio, inclusive="neither")] = "inside"
        zone.loc[position.between(1.0 - edge_ratio, 1.0, inclusive="both")] = "upper_edge"

        data.loc[row_index, "oi_box_id"] = box.box_id
        data.loc[row_index, "oi_box_low"] = box.low
        data.loc[row_index, "oi_box_high"] = box.high
        data.loc[row_index, "oi_box_mid"] = box.mid
        data.loc[row_index, "oi_box_position"] = position
        data.loc[row_index, "oi_box_zone"] = zone

    return data


def plot_oi_boxes(
    frame: pd.DataFrame,
    boxes: list[OIBox],
    *,
    value_col: str = "exchange_oi_sum",
    timestamp_col: str = "timestamp",
    ax=None,
    line_color: str = "black",
    box_color: str = "tab:orange",
    alpha: float = 0.18,
    show_range_lines: bool = True,
    show_midline: bool = True,
    label_lines: bool = False,
):
    import matplotlib.dates as mdates
    import matplotlib.patches as patches
    import matplotlib.pyplot as plt

    data = _prepare_oi_frame(frame, value_col=value_col, timestamp_col=timestamp_col)
    if ax is None:
        _, ax = plt.subplots(figsize=(20, 8))

    ax.plot(data[timestamp_col], data[value_col], color=line_color, linewidth=1.1)
    for box in boxes:
        start_num = mdates.date2num(box.start.to_pydatetime())
        end_num = mdates.date2num(box.end.to_pydatetime())
        rect = patches.Rectangle(
            (start_num, box.low),
            end_num - start_num,
            box.high - box.low,
            facecolor=box_color,
            edgecolor=box_color,
            linewidth=1.0,
            alpha=alpha,
        )
        ax.add_patch(rect)
        if show_range_lines:
            line_alpha = min(alpha + 0.35, 0.85)
            # ax.hlines(
            #     [box.low, box.high],
            #     xmin=box.start,
            #     xmax=box.end,
            #     color=box_color,
            #     linestyle="--",
            #     linewidth=1.0,
            #     alpha=line_alpha,
            # )
            # if show_midline:
            #     ax.hlines(
            #         box.mid,
            #         xmin=box.start,
            #         xmax=box.end,
            #         color=box_color,
            #         linestyle=":",
            #         linewidth=0.9,
            #         alpha=line_alpha * 0.75,
            #     )
            ax.axvline(box.start, color=box_color, linestyle="--", linewidth=0.9, alpha=line_alpha)
            ax.axvline(box.end, color=box_color, linestyle=":", linewidth=0.9, alpha=line_alpha)
        else:
            ax.axvline(box.start, color=box_color, linestyle="--", linewidth=0.8, alpha=0.5)
        ax.text(box.start, box.high, f"#{box.box_id}", fontsize=8, color=box_color, va="bottom")
        if label_lines:
            ax.text(box.start, box.low, f"low {box.low:,.0f}", fontsize=7, color=box_color, va="top")
            ax.text(box.start, box.high, f"high {box.high:,.0f}", fontsize=7, color=box_color, va="bottom")

    ax.set_title(f"Auto Box Ranges: {value_col}")
    ax.set_xlabel("timestamp")
    ax.set_ylabel(value_col)
    ax.xaxis_date()
    ax.grid(True, alpha=0.2)
    return ax
