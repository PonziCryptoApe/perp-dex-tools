"""双中位数超额信号管理器。"""

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
class MedianEdgeDirectionStats:
    """单个方向的双中位数超额快照。"""

    current_raw_pct: float
    current_adjusted_pct: float
    baseline_pct: float
    medium_median_pct: Optional[float]
    long_median_pct: Optional[float]
    medium_edge_pct: Optional[float]
    long_edge_pct: Optional[float]
    threshold_pct: Optional[float]
    active_edge_pct: Optional[float]
    medium_samples: int
    long_samples: int
    medium_span_seconds: float
    long_span_seconds: float
    medium_time_ready: bool
    long_time_ready: bool
    ready: bool
    eligible: bool
    reject_reason: str

    def to_dict(self) -> dict:
        """转为字典，便于日志和 metadata 使用。"""
        return {
            "current_raw_pct": self.current_raw_pct,
            "current_adjusted_pct": self.current_adjusted_pct,
            "baseline_pct": self.baseline_pct,
            "medium_median_pct": self.medium_median_pct,
            "long_median_pct": self.long_median_pct,
            "medium_edge_pct": self.medium_edge_pct,
            "long_edge_pct": self.long_edge_pct,
            "threshold_pct": self.threshold_pct,
            "active_edge_pct": self.active_edge_pct,
            "medium_samples": self.medium_samples,
            "long_samples": self.long_samples,
            "medium_span_seconds": self.medium_span_seconds,
            "long_span_seconds": self.long_span_seconds,
            "medium_time_ready": self.medium_time_ready,
            "long_time_ready": self.long_time_ready,
            "ready": self.ready,
            "eligible": self.eligible,
            "reject_reason": self.reject_reason,
        }


class MedianEdgeSignalManager:
    """调整后价差双中位数超额信号管理器。"""

    def __init__(
        self,
        baseline_adjustment: bool = True,
        baseline_ratio: float = 0.5,
        medium_window_seconds: int = 1800,
        long_window_seconds: int = 3600,
        medium_min_samples: int = 120,
        long_min_samples: int = 240,
        min_edge_bps: float = 2.25,
    ):
        self.baseline_adjustment = bool(baseline_adjustment)
        self.baseline_ratio = max(0.0, float(baseline_ratio))
        self.medium_window_seconds = int(medium_window_seconds)
        self.long_window_seconds = int(long_window_seconds)
        self.medium_min_samples = int(medium_min_samples)
        self.long_min_samples = int(long_min_samples)
        self.min_edge_bps = float(min_edge_bps)
        self.min_edge_pct = float(min_edge_bps) / 100.0
        self.window_ready_tolerance_seconds = 2.0

        self._open_history: deque[tuple[float, float]] = deque()
        self._close_history: deque[tuple[float, float]] = deque()
        self._current_open_raw_pct: Optional[float] = None
        self._current_close_raw_pct: Optional[float] = None
        self._current_open_adjusted_pct: Optional[float] = None
        self._current_close_adjusted_pct: Optional[float] = None
        self._current_baseline_pct: float = 0.0
        self._total_samples = 0

        logger.info(
            "📊 双中位数超额信号管理器已启用:\n"
            f"   基线修正: {'启用' if self.baseline_adjustment else '禁用'} (ratio={self.baseline_ratio:.3f})\n"
            f"   窗口: 30m={self.medium_window_seconds}s, 60m={self.long_window_seconds}s\n"
            f"   最小样本: 30m={self.medium_min_samples}, 60m={self.long_min_samples}\n"
            f"   超额门槛: {self.min_edge_bps:.2f} bps ({self.min_edge_pct:.4f}%)"
        )

    def add_spreads(
        self,
        open_spread: Decimal,
        close_spread: Decimal,
        baseline_pct: Decimal,
        now_ts: Optional[float] = None,
    ) -> None:
        """添加最新价差样本。"""
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

    def _window_entries(
        self,
        history: deque[tuple[float, float]],
        now_ts: float,
        window_seconds: int,
    ) -> list[tuple[float, float]]:
        """返回窗口内的样本。"""
        cutoff_ts = now_ts - window_seconds
        return [(ts, value) for ts, value in history if ts >= cutoff_ts]

    def _build_direction_stats(
        self,
        history: deque[tuple[float, float]],
        current_raw_pct: Optional[float],
        current_adjusted_pct: Optional[float],
        now_ts: float,
    ) -> MedianEdgeDirectionStats:
        """构建单方向快照。"""
        medium_entries = self._window_entries(history, now_ts, self.medium_window_seconds)
        long_entries = self._window_entries(history, now_ts, self.long_window_seconds)
        medium_values = [value for _, value in medium_entries]
        long_values = [value for _, value in long_entries]

        medium_samples = len(medium_values)
        long_samples = len(long_values)
        medium_span_seconds = (
            float(medium_entries[-1][0] - medium_entries[0][0]) if len(medium_entries) >= 2 else 0.0
        )
        long_span_seconds = (
            float(long_entries[-1][0] - long_entries[0][0]) if len(long_entries) >= 2 else 0.0
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
            return MedianEdgeDirectionStats(
                current_raw_pct=0.0,
                current_adjusted_pct=0.0,
                baseline_pct=self._current_baseline_pct,
                medium_median_pct=None,
                long_median_pct=None,
                medium_edge_pct=None,
                long_edge_pct=None,
                threshold_pct=None,
                active_edge_pct=None,
                medium_samples=medium_samples,
                long_samples=long_samples,
                medium_span_seconds=medium_span_seconds,
                long_span_seconds=long_span_seconds,
                medium_time_ready=medium_time_ready,
                long_time_ready=long_time_ready,
                ready=False,
                eligible=False,
                reject_reason="缺少当前价差",
            )

        if not ready:
            return MedianEdgeDirectionStats(
                current_raw_pct=current_raw_pct,
                current_adjusted_pct=current_adjusted_pct,
                baseline_pct=self._current_baseline_pct,
                medium_median_pct=None,
                long_median_pct=None,
                medium_edge_pct=None,
                long_edge_pct=None,
                threshold_pct=None,
                active_edge_pct=None,
                medium_samples=medium_samples,
                long_samples=long_samples,
                medium_span_seconds=medium_span_seconds,
                long_span_seconds=long_span_seconds,
                medium_time_ready=medium_time_ready,
                long_time_ready=long_time_ready,
                ready=False,
                eligible=False,
                reject_reason=(
                    "窗口时间跨度不足"
                    if not (medium_time_ready and long_time_ready)
                    else "样本不足"
                ),
            )

        medium_median_pct = float(median(medium_values))
        long_median_pct = float(median(long_values))
        medium_edge_pct = float(current_adjusted_pct - medium_median_pct)
        long_edge_pct = float(current_adjusted_pct - long_median_pct)
        threshold_pct = max(medium_median_pct, long_median_pct) + self.min_edge_pct
        active_edge_pct = min(medium_edge_pct, long_edge_pct)
        eligible = (
            current_adjusted_pct > medium_median_pct + self.min_edge_pct
            and current_adjusted_pct > long_median_pct + self.min_edge_pct
        )
        reject_reason = "" if eligible else "未同时突破30m/60m中位数+最小边际"

        return MedianEdgeDirectionStats(
            current_raw_pct=current_raw_pct,
            current_adjusted_pct=current_adjusted_pct,
            baseline_pct=self._current_baseline_pct,
            medium_median_pct=medium_median_pct,
            long_median_pct=long_median_pct,
            medium_edge_pct=medium_edge_pct,
            long_edge_pct=long_edge_pct,
            threshold_pct=threshold_pct,
            active_edge_pct=active_edge_pct,
            medium_samples=medium_samples,
            long_samples=long_samples,
            medium_span_seconds=medium_span_seconds,
            long_span_seconds=long_span_seconds,
            medium_time_ready=medium_time_ready,
            long_time_ready=long_time_ready,
            ready=True,
            eligible=eligible,
            reject_reason=reject_reason,
        )

    def get_signal_context(self, now_ts: Optional[float] = None) -> dict:
        """返回当前双方向上下文。"""
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
            selection_reason = "仅 OPEN 方向满足双中位数超额条件"
        elif close_stats.eligible and not open_stats.eligible:
            selected_signal_type = SignalType.CLOSE
            selection_reason = "仅 CLOSE 方向满足双中位数超额条件"
        elif open_stats.eligible and close_stats.eligible:
            if open_stats.current_adjusted_pct > close_stats.current_adjusted_pct:
                selected_signal_type = SignalType.OPEN
                selection_reason = "双方向都满足，OPEN 当前调整后价差更大"
            elif close_stats.current_adjusted_pct > open_stats.current_adjusted_pct:
                selected_signal_type = SignalType.CLOSE
                selection_reason = "双方向都满足，CLOSE 当前调整后价差更大"
            else:
                selection_reason = "双方向调整后价差相等，放弃本次信号"
        else:
            if not open_stats.ready or not close_stats.ready:
                selection_reason = "双中位数窗口样本尚未就绪"
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
