"""
动态阈值管理器（标准差法 - 固定样本数）
"""

import time
import numpy as np
import csv
from pathlib import Path
from datetime import datetime
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
        min_samples: int = 10,
        std_multiplier: float = 1.0,
        min_total_threshold: float = 0.02,
        max_std_multiplier: float = 4.0,
        min_std_multiplier: float = 0,
        enable_logging: bool = True,  # ✅ 新增：是否启用数据记录
        log_dir: str = "logs/arbitrage/dynamic_threshold"  # ✅ 新增：日志目录
    ):
        """
        初始化
        
        Args:
            sample_size: 样本容量（固定为 1000）
            min_samples: 最小样本数（开始计算的阈值）
            std_multiplier: 标准差倍数（阈值 = 均值 + N*标准差）
            min_total_threshold: 最小阈值和（%）
            max_std_multiplier: 最大标准差倍数
            min_std_multiplier: 最小标准差倍数
            enable_logging: 是否启用数据记录
            log_dir: 日志目录
        """
        self.sample_size = sample_size
        self.min_samples = min_samples
        self.std_multiplier = std_multiplier
        self.min_total_threshold = min_total_threshold
        self.max_std_multiplier = max_std_multiplier
        self.min_std_multiplier = min_std_multiplier
        self.enable_logging = enable_logging
        
        # ✅ 数据存储：固定容量，自动保持最新数据
        self.open_spreads = deque(maxlen=sample_size)
        self.close_spreads = deque(maxlen=sample_size)
        self.time_spreads = deque(maxlen=sample_size)
        self.time_sample_start = None
        self.time_sample_end = None
        
        # 当前阈值
        self.current_open_threshold = None
        self.current_close_threshold = None
        
        # 统计信息
        self.adjustment_count = 0
        self.total_samples_added = 0
        # ========== ✅ 新增：数据持久化 ==========
        if self.enable_logging:
            # 创建日志目录
            self.log_dir = Path(log_dir)
            self.log_dir.mkdir(parents=True, exist_ok=True)
            
            # 生成文件名（按日期）
            today = datetime.now().strftime("%Y%m%d")
            
            # 价差数据文件
            self.spreads_file = self.log_dir / f"spreads_{today}.csv"
            self._init_spreads_file()
            
            # 阈值调整记录文件
            self.adjustments_file = self.log_dir / f"adjustments_{today}.csv"
            self._init_adjustments_file()
            
            # 统计摘要文件
            self.stats_file = self.log_dir / f"stats_{today}.csv"
            self._init_stats_file()
            
            logger.info(f"📁 数据记录已启用，文件保存至: {self.log_dir}")
        logger.info(
            f"📊 动态阈值已启用 (标准差法):\n"
            f"   样本容量: {sample_size} (固定)\n"
            f"   最小样本: {min_samples} | 阈值 = 均值 + {std_multiplier}σ\n"
            f"   初始倍数: {std_multiplier}σ | 标准差倍数范围: [{min_std_multiplier}, {max_std_multiplier}]\n"
            f"   最小阈值和: {min_total_threshold}%"
            f"   数据记录: {'启用' if enable_logging else '禁用'}"
        )

    def _init_spreads_file(self):
        """初始化价差数据文件"""
        if not self.spreads_file.exists():
            with open(self.spreads_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'timestamp',           # 时间戳
                    'datetime',            # 日期时间
                    'open_spread',         # 开仓价差（%）
                    'close_spread',        # 平仓价差（%）
                    'total_samples',       # 累计样本数
                    'current_open_threshold',   # 当前开仓阈值
                    'current_close_threshold'   # 当前平仓阈值
                ])
    
    def _init_adjustments_file(self):
        """初始化阈值调整记录文件"""
        if not self.adjustments_file.exists():
            with open(self.adjustments_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'timestamp',           # 时间戳
                    'datetime',            # 日期时间
                    'adjustment_count',    # 调整次数
                    'open_mean',           # 开仓均值
                    'open_std',            # 开仓标准差
                    'close_mean',          # 平仓均值
                    'close_std',           # 平仓标准差
                    'std_multiplier',      # 标准差倍数
                    'old_open_threshold',  # 旧开仓阈值
                    'new_open_threshold',  # 新开仓阈值
                    'old_close_threshold', # 旧平仓阈值
                    'new_close_threshold', # 新平仓阈值
                    'threshold_sum',       # 阈值和
                    'open_samples',        # 开仓样本数
                    'close_samples',       # 平仓样本数
                    'total_samples'        # 累计样本数
                ])
    
    def _init_stats_file(self):
        """初始化统计摘要文件"""
        if not self.stats_file.exists():
            with open(self.stats_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'timestamp',
                    'datetime',
                    'total_samples',
                    'adjustment_count',
                    'current_std_multiplier',
                    'current_open_threshold',
                    'current_close_threshold',
                    'open_mean',
                    'open_std',
                    'open_min',
                    'open_max',
                    'close_mean',
                    'close_std',
                    'close_min',
                    'close_max'
                ])
    def add_spreads(self, open_spread: Decimal, close_spread: Decimal):
        """
        添加价差数据
        
        Args:
            open_spread: 开仓价差（%）
            close_spread: 平仓价差（%）
        """
        logger.info(f"🔍 价差: {open_spread:.4f}%, 反向价差: {close_spread:.4f}%")

        # ✅ deque 会自动移除最早的数据（当达到 maxlen 时）
        self.open_spreads.append(float(open_spread))
        self.close_spreads.append(float(close_spread))
        self.time_spreads.append(time.time())
        self.total_samples_added += 1
        if self.total_samples_added == 1:
            self.time_sample_start = time.time()
        if self.total_samples_added == self.sample_size -1:
            self.time_sample_end = time.time()
            logger.info(
                f"⏱️ 收集到 {self.sample_size} 个样本，"
                f"耗时 {self.time_sample_end - self.time_sample_start:.2f} 秒"
            )
        # ✅ 记录到文件
        if self.enable_logging:
            now = time.time()
            with open(self.spreads_file, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    now,
                    datetime.fromtimestamp(now).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                    float(open_spread),
                    float(close_spread),
                    self.total_samples_added,
                    self.current_open_threshold or 0,
                    self.current_close_threshold or 0
                ])
    
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
        if len(self.open_spreads) < self.min_samples or len(self.close_spreads) < self.min_samples:
            logger.info(
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
        # ========== ✅ 自适应缩放逻辑 ==========
        adjusted_multiplier = self.std_multiplier
        if threshold_sum < self.min_total_threshold:
            # ✅ 阈值和过小 → 增大倍数
            if open_std > 0 and close_std > 0:
                # 计算所需的倍数（使 threshold_sum = min_total_threshold）
                # new_open + new_close = min_total_threshold
                # (mean_open + k*std_open) + (mean_close + k*std_close) = min_total_threshold
                # k = (min_total_threshold - mean_open - mean_close) / (std_open + std_close)
                
                mean_sum = open_mean + close_mean
                std_sum = open_std + close_std
                
                if std_sum > 0:
                    required_multiplier = (self.min_total_threshold - mean_sum) / std_sum
                    
                    # ✅ 限制在合理范围
                    adjusted_multiplier = min(max(required_multiplier, self.min_std_multiplier), self.max_std_multiplier)
                    
                    # ✅ 重新计算阈值
                    new_open = open_mean + adjusted_multiplier * open_std
                    new_close = close_mean + adjusted_multiplier * close_std
                    threshold_sum = new_open + new_close
                    
                    logger.info(
                        f"📈 自适应调整（阈值和过小）:\n"
                        f"   原倍数: {self.std_multiplier:.2f}σ → 调整后: {adjusted_multiplier:.2f}σ\n"
                        f"   原阈值和: {open_mean + self.std_multiplier * open_std + close_mean + self.std_multiplier * close_std:.4f}% "
                        f"→ 调整后: {threshold_sum:.4f}%"
                    )
        
        elif threshold_sum > self.min_total_threshold * 1.2:  # ✅ 如果阈值和过大（超过 1.2 倍）
            # ✅ 可选：缩小倍数（使阈值更紧）
            if open_std > 0 and close_std > 0:
                mean_sum = open_mean + close_mean
                std_sum = open_std + close_std
                
                if std_sum > 0:
                    # ✅ 缩小到 min_total_threshold 的 1.2 倍（留一点余量）
                    target_threshold = self.min_total_threshold * 1.1
                    required_multiplier = (target_threshold - mean_sum) / std_sum
                    
                    # ✅ 只缩小，不增大
                    if required_multiplier < self.std_multiplier:
                        adjusted_multiplier = min(max(required_multiplier, self.min_std_multiplier), self.max_std_multiplier)
                        
                        new_open = open_mean + adjusted_multiplier * open_std
                        new_close = close_mean + adjusted_multiplier * close_std
                        threshold_sum = new_open + new_close
                        
                        logger.info(
                            f"📉 自适应调整（阈值和过大）:\n"
                            f"   原倍数: {self.std_multiplier:.2f}σ → 调整后: {adjusted_multiplier:.2f}σ\n"
                            f"   原阈值和: {open_mean + self.std_multiplier * open_std + close_mean + self.std_multiplier * close_std:.4f}% "
                            f"→ 调整后: {threshold_sum:.4f}%"
                        )
        self.std_multiplier = adjusted_multiplier

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
        # ✅ 记录到文件
        if self.enable_logging:
            now = time.time()
            with open(self.adjustments_file, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    now,
                    datetime.fromtimestamp(now).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                    self.adjustment_count,
                    open_mean,
                    open_std,
                    close_mean,
                    close_std,
                    adjusted_multiplier,
                    old_open or 0,
                    new_open,
                    old_close or 0,
                    new_close,
                    threshold_sum,
                    len(open_values),
                    len(close_values),
                    self.total_samples_added
                ])
        
        # ✅ 定期记录统计摘要（每 100 次调整）
        if self.enable_logging and self.adjustment_count % 100 == 0:
            self._save_stats_snapshot()
        return new_open, new_close
    def _save_stats_snapshot(self):
        """保存统计摘要快照"""
        stats = self.get_stats()
        
        if stats['status'] == 'active':
            now = time.time()
            with open(self.stats_file, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    now,
                    datetime.fromtimestamp(now).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                    stats['total_samples'],
                    stats['adjustment_count'],
                    self.std_multiplier,
                    stats['current_open'],
                    stats['current_close'],
                    stats['open_mean'],
                    stats['open_std'],
                    stats['open_min'],
                    stats['open_max'],
                    stats['close_mean'],
                    stats['close_std'],
                    stats['close_min'],
                    stats['close_max']
                ])
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
    def get_time_length(self) -> float:
        if len(self.time_spreads) >= self.sample_size:
            return self.time_spreads[-1] - self.time_spreads[0]
        return 0.0

    def export_all_data(self, output_dir: Optional[str] = None):
        """
        导出所有数据（价差 + 调整记录 + 统计摘要）
        
        Args:
            output_dir: 输出目录（默认使用配置的 log_dir）
        """
        if not self.enable_logging:
            logger.warning("⚠️ 数据记录未启用，无法导出")
            return
        
        output_path = Path(output_dir) if output_dir else self.log_dir
        output_path.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"📤 导出数据到: {output_path}")
        logger.info(f"   价差数据: {self.spreads_file}")
        logger.info(f"   阈值调整: {self.adjustments_file}")
        logger.info(f"   统计摘要: {self.stats_file}")