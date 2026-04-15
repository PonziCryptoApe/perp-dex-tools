"""统计套利信号管理器。"""

from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from statistics import median
from typing import Optional

from ..models.signal import SignalType

logger = logging.getLogger(__name__)


@dataclass
class DirectionStats:
    """单个方向的统计套利快照。"""

    current_raw_pct: float
    current_adjusted_pct: float
    baseline_pct: float
    medium_median_pct: Optional[float]
    long_median_pct: Optional[float]
    medium_quantile_pct: Optional[float]
    long_quantile_pct: Optional[float]
    medium_mad_pct: Optional[float]
    long_mad_pct: Optional[float]
    medium_score: Optional[float]
    long_score: Optional[float]
    long_baseline_score: Optional[float]
    breakout_score: Optional[float]
    final_score: Optional[float]
    active_score: Optional[float]
    medium_samples: int
    long_samples: int
    medium_span_seconds: float
    long_span_seconds: float
    medium_time_ready: bool
    long_time_ready: bool
    ready: bool
    same_sign: bool
    regime_suspected: bool
    eligible: bool
    reject_reason: str

    def to_dict(self) -> dict:
        """转为字典，便于日志与 metadata 使用。"""
        return {
            "current_raw_pct": self.current_raw_pct,
            "current_adjusted_pct": self.current_adjusted_pct,
            "baseline_pct": self.baseline_pct,
            "medium_median_pct": self.medium_median_pct,
            "long_median_pct": self.long_median_pct,
            "medium_quantile_pct": self.medium_quantile_pct,
            "long_quantile_pct": self.long_quantile_pct,
            "medium_mad_pct": self.medium_mad_pct,
            "long_mad_pct": self.long_mad_pct,
            "medium_score": self.medium_score,
            "long_score": self.long_score,
            "long_baseline_score": self.long_baseline_score,
            "breakout_score": self.breakout_score,
            "final_score": self.final_score,
            "active_score": self.active_score,
            "medium_samples": self.medium_samples,
            "long_samples": self.long_samples,
            "medium_span_seconds": self.medium_span_seconds,
            "long_span_seconds": self.long_span_seconds,
            "medium_time_ready": self.medium_time_ready,
            "long_time_ready": self.long_time_ready,
            "ready": self.ready,
            "same_sign": self.same_sign,
            "regime_suspected": self.regime_suspected,
            "eligible": self.eligible,
            "reject_reason": self.reject_reason,
        }


class StatArbSignalManager:
    """统计套利信号管理器（30m/60m + median/MAD + 双方向 score）。"""

    def __init__(
        self,
        baseline_adjustment: bool = True,
        baseline_ratio: float = 0.5,
        medium_window_seconds: int = 1800,
        long_window_seconds: int = 3600,
        medium_min_samples: int = 120,
        long_min_samples: int = 240,
        medium_weight: float = 0.4,
        long_weight: float = 0.6,
        score_mode: str = "weighted",
        breakout_quantile: float = 0.7,
        entry_threshold: float = 2.8,
        min_score_gap: float = 0.5,
        min_mad_pct: float = 0.003,
        require_same_sign_for_medium_long: bool = True,
        block_when_regime_suspected: bool = True,
    ):
        self.baseline_adjustment = bool(baseline_adjustment)
        self.baseline_ratio = max(0.0, float(baseline_ratio))
        self.medium_window_seconds = int(medium_window_seconds)
        self.long_window_seconds = int(long_window_seconds)
        self.medium_min_samples = int(medium_min_samples)
        self.long_min_samples = int(long_min_samples)
        self.medium_weight = float(medium_weight)
        self.long_weight = float(long_weight)
        normalized_score_mode = str(score_mode).lower()
        self.score_mode = (
            normalized_score_mode
            if normalized_score_mode in {"weighted", "long_baseline", "quantile_breakout"}
            else "weighted"
        )
        self.breakout_quantile = min(max(float(breakout_quantile), 0.0), 1.0)
        self.entry_threshold = float(entry_threshold)
        self.min_score_gap = float(min_score_gap)
        self.min_mad_pct = float(min_mad_pct)
        # 给窗口跨度判定预留 2 秒容差，避免 899.3/900 这类边界值长期卡在“未就绪”。
        self.window_ready_tolerance_seconds = 2.0
        self.require_same_sign_for_medium_long = bool(require_same_sign_for_medium_long)
        self.block_when_regime_suspected = bool(block_when_regime_suspected)

        self._open_history: deque[tuple[float, float]] = deque()
        self._close_history: deque[tuple[float, float]] = deque()
        self._current_open_raw_pct: Optional[float] = None
        self._current_close_raw_pct: Optional[float] = None
        self._current_open_adjusted_pct: Optional[float] = None
        self._current_close_adjusted_pct: Optional[float] = None
        self._current_baseline_pct: float = 0.0
        self._total_samples = 0

        logger.info(
            "📊 统计套利信号管理器已启用:\n"
            f"   基线修正: {'启用' if self.baseline_adjustment else '禁用'} (ratio={self.baseline_ratio:.3f})\n"
            f"   窗口: 30m={self.medium_window_seconds}s, 60m={self.long_window_seconds}s\n"
            f"   最小样本: 30m={self.medium_min_samples}, 60m={self.long_min_samples}\n"
            f"   权重: 30m={self.medium_weight:.2f}, 60m={self.long_weight:.2f}\n"
            f"   分数模式: {self.score_mode} | 分位突破阈值={self.breakout_quantile:.2f}\n"
            f"   阈值: entry={self.entry_threshold:.3f}, gap={self.min_score_gap:.3f}, MAD下限={self.min_mad_pct:.6f}"
        )

    def add_spreads(
        self,
        open_spread: Decimal,
        close_spread: Decimal,
        baseline_pct: Decimal,
        now_ts: Optional[float] = None,
    ) -> None:
        """添加最新净价差样本。"""
        ts = time.time() if now_ts is None else float(now_ts)
        raw_open = float(open_spread)
        raw_close = float(close_spread)
        baseline_total = float(baseline_pct)
        baseline_adjustment = baseline_total * self.baseline_ratio if self.baseline_adjustment else 0.0

        adjusted_open = raw_open - baseline_adjustment
        adjusted_close = raw_close - baseline_adjustment

        self._open_history.append((ts, adjusted_open))
        self._close_history.append((ts, adjusted_close))
        self._current_open_raw_pct = raw_open
        self._current_close_raw_pct = raw_close
        self._current_open_adjusted_pct = adjusted_open
        self._current_close_adjusted_pct = adjusted_close
        self._current_baseline_pct = baseline_adjustment
        self._total_samples += 1

        self._evict_old(ts)

    def _evict_old(self, now_ts: float) -> None:
        """清理超出最长窗口的旧样本。"""
        cutoff_ts = now_ts - self.long_window_seconds
        while self._open_history and self._open_history[0][0] < cutoff_ts:
            self._open_history.popleft()
        while self._close_history and self._close_history[0][0] < cutoff_ts:
            self._close_history.popleft()

    def _window_values(self, history: deque[tuple[float, float]], now_ts: float, window_seconds: int) -> list[float]:
        """获取某个窗口内的样本值。"""
        cutoff_ts = now_ts - window_seconds
        return [value for ts, value in history if ts >= cutoff_ts]

    def _window_entries(
        self,
        history: deque[tuple[float, float]],
        now_ts: float,
        window_seconds: int,
    ) -> list[tuple[float, float]]:
        """获取某个窗口内的样本（含时间戳）。"""
        cutoff_ts = now_ts - window_seconds
        return [(ts, value) for ts, value in history if ts >= cutoff_ts]

    def _calc_mad(self, values: list[float], center: float) -> Optional[float]:
        """计算 MAD。"""
        if not values:
            return None
        deviations = [abs(value - center) for value in values]
        return float(median(deviations))

    def _calc_quantile(self, values: list[float], q: float) -> Optional[float]:
        """计算分位数，使用线性插值。"""
        if not values:
            return None
        if len(values) == 1:
            return float(values[0])
        ordered = sorted(float(value) for value in values)
        position = (len(ordered) - 1) * q
        lower_index = int(position)
        upper_index = min(lower_index + 1, len(ordered) - 1)
        lower_value = ordered[lower_index]
        upper_value = ordered[upper_index]
        weight = position - lower_index
        return float(lower_value + (upper_value - lower_value) * weight)

    def _same_sign(self, medium_value: Optional[float], long_value: Optional[float]) -> bool:
        """判断中长期中枢是否同向。"""
        if medium_value is None or long_value is None:
            return False
        return (medium_value > 0 and long_value > 0) or (medium_value < 0 and long_value < 0)

    def _build_direction_stats(
        self,
        history: deque[tuple[float, float]],
        current_raw_pct: Optional[float],
        current_adjusted_pct: Optional[float],
        now_ts: float,
    ) -> DirectionStats:
        """构建单方向统计快照。"""
        medium_entries = self._window_entries(history, now_ts, self.medium_window_seconds)
        long_entries = self._window_entries(history, now_ts, self.long_window_seconds)
        medium_values = [value for _, value in medium_entries]
        long_values = [value for _, value in long_entries]
        medium_samples = len(medium_values)
        long_samples = len(long_values)
        medium_span_seconds = (
            float(medium_entries[-1][0] - medium_entries[0][0])
            if len(medium_entries) >= 2
            else 0.0
        )
        long_span_seconds = (
            float(long_entries[-1][0] - long_entries[0][0])
            if len(long_entries) >= 2
            else 0.0
        )
        medium_time_ready = (
            medium_span_seconds + self.window_ready_tolerance_seconds
            >= float(self.medium_window_seconds)
        )
        long_time_ready = (
            long_span_seconds + self.window_ready_tolerance_seconds
            >= float(self.long_window_seconds)
        )
        ready = (
            medium_samples >= self.medium_min_samples
            and long_samples >= self.long_min_samples
            and medium_time_ready
            and long_time_ready
        )

        if current_raw_pct is None or current_adjusted_pct is None:
            return DirectionStats(
                current_raw_pct=0.0,
                current_adjusted_pct=0.0,
                baseline_pct=self._current_baseline_pct,
                medium_median_pct=None,
                long_median_pct=None,
                medium_quantile_pct=None,
                long_quantile_pct=None,
                medium_mad_pct=None,
                long_mad_pct=None,
                medium_score=None,
                long_score=None,
                long_baseline_score=None,
                breakout_score=None,
                final_score=None,
                active_score=None,
                medium_samples=medium_samples,
                long_samples=long_samples,
                medium_span_seconds=medium_span_seconds,
                long_span_seconds=long_span_seconds,
                medium_time_ready=medium_time_ready,
                long_time_ready=long_time_ready,
                ready=False,
                same_sign=False,
                regime_suspected=False,
                eligible=False,
                reject_reason="缺少当前价差",
            )

        if not ready:
            return DirectionStats(
                current_raw_pct=current_raw_pct,
                current_adjusted_pct=current_adjusted_pct,
                baseline_pct=self._current_baseline_pct,
                medium_median_pct=None,
                long_median_pct=None,
                medium_mad_pct=None,
                long_mad_pct=None,
                medium_score=None,
                long_score=None,
                long_baseline_score=None,
                final_score=None,
                active_score=None,
                medium_samples=medium_samples,
                long_samples=long_samples,
                medium_span_seconds=medium_span_seconds,
                long_span_seconds=long_span_seconds,
                medium_time_ready=medium_time_ready,
                long_time_ready=long_time_ready,
                ready=False,
                same_sign=False,
                regime_suspected=False,
                eligible=False,
                reject_reason=(
                    "窗口时间跨度不足"
                    if not (medium_time_ready and long_time_ready)
                    else "样本不足"
                ),
            )

        medium_median_pct = float(median(medium_values))
        long_median_pct = float(median(long_values))
        medium_quantile_pct = self._calc_quantile(medium_values, self.breakout_quantile)
        long_quantile_pct = self._calc_quantile(long_values, self.breakout_quantile)
        medium_mad_raw = self._calc_mad(medium_values, medium_median_pct)
        long_mad_raw = self._calc_mad(long_values, long_median_pct)
        medium_mad_pct = max(float(medium_mad_raw or 0.0), self.min_mad_pct)
        long_mad_pct = max(float(long_mad_raw or 0.0), self.min_mad_pct)
        medium_score = (medium_median_pct - current_adjusted_pct) / medium_mad_pct
        long_score = (long_median_pct - current_adjusted_pct) / long_mad_pct
        long_baseline_score = (long_median_pct - current_adjusted_pct) / medium_mad_pct
        breakout_score = (
            (current_adjusted_pct - long_quantile_pct) / medium_mad_pct
            if long_quantile_pct is not None
            else None
        )
        final_score = self.medium_weight * medium_score + self.long_weight * long_score
        if self.score_mode == "long_baseline":
            active_score = long_baseline_score
        elif self.score_mode == "quantile_breakout":
            active_score = breakout_score
        else:
            active_score = final_score

        same_sign = self._same_sign(medium_median_pct, long_median_pct)
        median_gap_scale = max(medium_mad_pct, long_mad_pct, self.min_mad_pct)
        regime_suspected = abs(medium_median_pct - long_median_pct) / median_gap_scale >= 3.0

        reject_reason = ""
        eligible = True
        if self.require_same_sign_for_medium_long and not same_sign:
            eligible = False
            reject_reason = "30m/60m 中枢不同向"
        elif self.block_when_regime_suspected and regime_suspected:
            eligible = False
            reject_reason = "怀疑发生 regime 变化"
        elif self.score_mode == "quantile_breakout" and (
            medium_quantile_pct is None
            or long_quantile_pct is None
            or current_adjusted_pct <= medium_quantile_pct
            or current_adjusted_pct <= long_quantile_pct
        ):
            eligible = False
            reject_reason = "未突破30m/60m分位阈值"
        elif active_score < self.entry_threshold:
            eligible = False
            reject_reason = (
                f"突破强度不足({active_score:.3f} < {self.entry_threshold:.3f})"
                if self.score_mode == "quantile_breakout"
                else f"分数不足({active_score:.3f} < {self.entry_threshold:.3f})"
            )

        return DirectionStats(
            current_raw_pct=current_raw_pct,
            current_adjusted_pct=current_adjusted_pct,
            baseline_pct=self._current_baseline_pct,
            medium_median_pct=medium_median_pct,
            long_median_pct=long_median_pct,
            medium_quantile_pct=medium_quantile_pct,
            long_quantile_pct=long_quantile_pct,
            medium_mad_pct=medium_mad_pct,
            long_mad_pct=long_mad_pct,
            medium_score=medium_score,
            long_score=long_score,
            long_baseline_score=long_baseline_score,
            breakout_score=breakout_score,
            final_score=final_score,
            active_score=active_score,
            medium_samples=medium_samples,
            long_samples=long_samples,
            medium_span_seconds=medium_span_seconds,
            long_span_seconds=long_span_seconds,
            medium_time_ready=medium_time_ready,
            long_time_ready=long_time_ready,
            ready=True,
            same_sign=same_sign,
            regime_suspected=regime_suspected,
            eligible=eligible,
            reject_reason=reject_reason,
        )

    def get_signal_context(self, now_ts: Optional[float] = None) -> dict:
        """返回当前双方向统计套利上下文。"""
        ts = time.time() if now_ts is None else float(now_ts)
        open_stats = self._build_direction_stats(
            history=self._open_history,
            current_raw_pct=self._current_open_raw_pct,
            current_adjusted_pct=self._current_open_adjusted_pct,
            now_ts=ts,
        )
        close_stats = self._build_direction_stats(
            history=self._close_history,
            current_raw_pct=self._current_close_raw_pct,
            current_adjusted_pct=self._current_close_adjusted_pct,
            now_ts=ts,
        )

        selected_signal_type = None
        selection_reason = "无可执行方向"
        if open_stats.eligible and not close_stats.eligible:
            selected_signal_type = SignalType.OPEN
            selection_reason = "仅 OPEN 方向满足统计套利条件"
        elif close_stats.eligible and not open_stats.eligible:
            selected_signal_type = SignalType.CLOSE
            selection_reason = "仅 CLOSE 方向满足统计套利条件"
        elif open_stats.eligible and close_stats.eligible:
            open_score = open_stats.active_score or 0.0
            close_score = close_stats.active_score or 0.0
            score_gap = abs(open_score - close_score)
            if score_gap < self.min_score_gap:
                selection_reason = (
                    f"双方向分数差不足({score_gap:.3f} < {self.min_score_gap:.3f})"
                )
            elif open_score > close_score:
                selected_signal_type = SignalType.OPEN
                selection_reason = "OPEN 方向分数更高"
            else:
                selected_signal_type = SignalType.CLOSE
                selection_reason = "CLOSE 方向分数更高"
        else:
            if not open_stats.ready or not close_stats.ready:
                selection_reason = "统计套利窗口样本尚未就绪"
            elif open_stats.reject_reason and close_stats.reject_reason:
                selection_reason = f"OPEN={open_stats.reject_reason}; CLOSE={close_stats.reject_reason}"

        return {
            "ready": open_stats.ready and close_stats.ready,
            "selected_signal_type": selected_signal_type,
            "selection_reason": selection_reason,
            "open": open_stats,
            "close": close_stats,
            "total_samples": self._total_samples,
        }
