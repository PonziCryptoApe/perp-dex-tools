"""
动态阈值管理器（标准差法 - 固定样本数）
"""

import time
import numpy as np
from collections import deque
from decimal import Decimal
from typing import Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class DynamicThresholdManager:
    """动态阈值管理器（均值 + 标准差法）"""
    
    def __init__(
        self,
        sample_size: int = 1000,
        min_samples: int = 200,
        std_multiplier: float = 2.0,
        min_total_threshold: float = 0.02,
    ):
        """
        初始化
        
        Args:
            sample_size: 样本容量（固定为 1000）
            min_samples: 最小样本数（开始计算的阈值）
            std_multiplier: 标准差倍数（阈值 = 均值 + N*标准差）
            min_total_threshold: 最小阈值和（%）
            adjustment_cooldown: 调整冷却期（秒）
        """
        self.sample_size = sample_size
        self.min_samples = min_samples
        self.std_multiplier = std_multiplier
        self.min_total_threshold = min_total_threshold
        
        # ✅ 数据存储：固定容量，自动保持最新数据
        self.open_spreads = deque(maxlen=sample_size)
        self.close_spreads = deque(maxlen=sample_size)
        
        # 当前阈值
        self.current_open_threshold = None
        self.current_close_threshold = None
        
        # 统计信息
        self.adjustment_count = 0
        self.total_samples_added = 0
        
        logger.info(
            f"📊 动态阈值已启用 (标准差法):\n"
            f"   样本容量: {sample_size} (固定)\n"
            f"   最小样本: {min_samples} | 阈值 = 均值 + {std_multiplier}σ\n"
            f"   最小阈值和: {min_total_threshold}%"
        )
    
    def add_spreads(self, open_spread: Decimal, close_spread: Decimal):
        """
        添加价差数据
        
        Args:
            open_spread: 开仓价差（%）
            close_spread: 平仓价差（%）
        """
        # ✅ deque 会自动移除最早的数据（当达到 maxlen 时）
        self.open_spreads.append(float(open_spread))
        self.close_spreads.append(float(close_spread))
        self.total_samples_added += 1
    
    def try_adjust(
        self, 
        current_position: Decimal, 
        max_position: Decimal
    ) -> Tuple[Optional[float], Optional[float]]:
        """
        尝试调整阈值
        
        Args:
            current_position: 当前持仓
            max_position: 最大持仓
        
        Returns:
            (new_open_threshold, new_close_threshold) 或 (None, None)
        """
        
        # ✅ 检查样本数（需要至少 min_samples 个样本才开始计算）
        if len(self.open_spreads) < self.min_samples and len(self.close_spreads) < self.min_samples:
            logger.debug(
                f"⏳ 样本不足: 开仓{len(self.open_spreads)}/{self.min_samples} "
                f"平仓{len(self.close_spreads)}/{self.min_samples}"
            )
            return None, None
        
        # ✅ 计算阈值（均值 + 标准差）
        open_values = np.array(list(self.open_spreads))
        close_values = np.array(list(self.close_spreads))
        
        # 开仓阈值 = 均值 + N*标准差
        open_mean = np.mean(open_values)
        open_std = np.std(open_values)
        new_open = open_mean + self.std_multiplier * open_std
        
        # 平仓阈值 = 均值 + N*标准差
        close_mean = np.mean(close_values)
        close_std = np.std(close_values)
        new_close = close_mean + self.std_multiplier * close_std
        
        # ✅ 检查阈值和
        threshold_sum = new_open + new_close
        if threshold_sum < self.min_total_threshold:
            logger.warning(
                f"⚠️ 阈值调整跳过:\n"
                f"   开仓: μ={open_mean:.4f}% + {self.std_multiplier}σ={open_std:.4f}% = {new_open:.4f}%\n"
                f"   平仓: μ={close_mean:.4f}% + {self.std_multiplier}σ={close_std:.4f}% = {new_close:.4f}%\n"
                f"   阈值和: {threshold_sum:.4f}% < 最小要求 {self.min_total_threshold:.4f}%"
            )
            return None, None
        
        # ✅ 记录调整
        old_open = self.current_open_threshold
        old_close = self.current_close_threshold
        self.current_open_threshold = new_open
        self.current_close_threshold = new_close
        self.adjustment_count += 1
        
        logger.info(
            f"✅ 阈值调整#{self.adjustment_count}:\n"
            f"   开仓: {old_open or 0:.4f}% → {new_open:.4f}% "
            f"   (μ={open_mean:.4f}% + {self.std_multiplier}σ={open_std:.4f}%)\n"
            f"   平仓: {old_close or 0:.4f}% → {new_close:.4f}% "
            f"   (μ={close_mean:.4f}% + {self.std_multiplier}σ={close_std:.4f}%)\n"
            f"   阈值和: {threshold_sum:.4f}%\n"
            f"   样本: 开仓{len(open_values)} 平仓{len(close_values)} | 总计: {self.total_samples_added}"
        )
        
        return new_open, new_close
    
    def get_stats(self) -> dict:
        """获取统计信息"""
        if len(self.open_spreads) < self.min_samples or len(self.close_spreads) < self.min_samples:
            return {
                'adjustment_count': self.adjustment_count,
                'total_samples': self.total_samples_added,
                'open_samples': len(self.open_spreads),
                'close_samples': len(self.close_spreads),
                'status': 'collecting'
            }
        
        open_values = np.array(list(self.open_spreads))
        close_values = np.array(list(self.close_spreads))
        
        return {
            'adjustment_count': self.adjustment_count,
            'total_samples': self.total_samples_added,
            'current_open': self.current_open_threshold,
            'current_close': self.current_close_threshold,
            'open_samples': len(open_values),
            'open_mean': float(np.mean(open_values)),
            'open_std': float(np.std(open_values)),
            'open_min': float(np.min(open_values)),
            'open_max': float(np.max(open_values)),
            'close_samples': len(close_values),
            'close_mean': float(np.mean(close_values)),
            'close_std': float(np.std(close_values)),
            'close_min': float(np.min(close_values)),
            'close_max': float(np.max(close_values)),
            'status': 'active'
        }