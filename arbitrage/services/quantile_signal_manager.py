"""
分位数信号管理器（固定样本数）
"""

import time
import numpy as np
from collections import deque
from decimal import Decimal
from typing import Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class QuantileSignalManager:
    """分位数信号管理器（用 P50/P60 等分位数做门槛）"""

    def __init__(
        self,
        sample_size: int = 2000,
        min_samples: Optional[int] = None,
        quantile: float = 0.6,
    ):
        """
        初始化

        Args:
            sample_size: 样本容量（固定窗口）
            min_samples: 最小样本数（未达到时不放行）
            quantile: 分位数（0~1，如 0.6 表示 P60）
        """
        if not 0 < quantile < 1:
            raise ValueError("quantile 必须在 (0, 1) 范围内")

        self.sample_size = int(sample_size)
        self.min_samples = int(min_samples) if min_samples is not None else int(sample_size)
        self.quantile = float(quantile)

        self.open_spreads = deque(maxlen=self.sample_size)
        self.close_spreads = deque(maxlen=self.sample_size)
        self.time_spreads = deque(maxlen=self.sample_size)

        self.current_open_threshold: Optional[float] = None
        self.current_close_threshold: Optional[float] = None
        self.total_samples_added = 0

        logger.info(
            f"📊 分位数信号管理器已启用:\n"
            f"   样本容量: {self.sample_size}\n"
            f"   最小样本: {self.min_samples}\n"
            f"   分位数: P{int(self.quantile * 100)}"
        )

    def add_spreads(self, open_spread: Decimal, close_spread: Decimal) -> None:
        """添加跨所价差样本。"""
        self.open_spreads.append(float(open_spread))
        self.close_spreads.append(float(close_spread))
        self.time_spreads.append(time.time())
        self.total_samples_added += 1
        self._refresh_thresholds_if_ready()

    def is_ready(self) -> bool:
        """是否已经满足最小样本数。"""
        return len(self.open_spreads) >= self.min_samples and len(self.close_spreads) >= self.min_samples

    def _refresh_thresholds_if_ready(self) -> None:
        """样本充足时更新分位数阈值。"""
        if not self.is_ready():
            return
        open_values = np.array(list(self.open_spreads))
        close_values = np.array(list(self.close_spreads))
        self.current_open_threshold = float(np.quantile(open_values, self.quantile))
        self.current_close_threshold = float(np.quantile(close_values, self.quantile))

    def get_thresholds(self) -> Tuple[Optional[float], Optional[float]]:
        """返回当前分位数阈值（开仓/平仓）。"""
        if not self.is_ready():
            return None, None
        if self.current_open_threshold is None or self.current_close_threshold is None:
            self._refresh_thresholds_if_ready()
        return self.current_open_threshold, self.current_close_threshold

    def get_stats(self) -> dict:
        """获取当前统计信息。"""
        return {
            'sample_size': self.sample_size,
            'min_samples': self.min_samples,
            'quantile': self.quantile,
            'quantile_pct': int(self.quantile * 100),
            'open_samples': len(self.open_spreads),
            'close_samples': len(self.close_spreads),
            'total_samples': self.total_samples_added,
            'current_open': self.current_open_threshold,
            'current_close': self.current_close_threshold,
            'ready': self.is_ready(),
        }

    def get_time_length(self) -> float:
        """返回样本窗口的时间跨度（秒）。"""
        if len(self.time_spreads) >= 2:
            return self.time_spreads[-1] - self.time_spreads[0]
        return 0.0
