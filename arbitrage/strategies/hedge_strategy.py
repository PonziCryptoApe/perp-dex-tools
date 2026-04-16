"""对冲套利策略"""

import asyncio
from aiolimiter import AsyncLimiter
from datetime import datetime
import logging
import random
import time
import yaml
import os
import csv
from pathlib import Path
from decimal import Decimal, ROUND_DOWN
from typing import Optional

from helpers.util import beijing_to_timestamp
from .base_strategy import BaseStrategy
from ..models.prices import PriceSnapshot
from ..models.signal import SignalType, TradingSignal
from ..services.price_monitor import PriceMonitorService
from ..services.position_manager import PositionManagerService
from ..services.order_executor_parallel import OrderExecutor
from ..services.dynamic_threshold import DynamicThresholdManager
from ..services.median_edge_signal_manager import MedianEdgeSignalManager
from ..services.quantile_signal_manager import QuantileSignalManager
from ..services.stat_arb_signal_manager import StatArbSignalManager
from ..services.risk_control_service import RiskControlService, RiskLevel
from ..services.process_diagnostics import ProcessDiagnosticsService
from ..models.position import Position

logger = logging.getLogger(__name__)

class HedgeStrategy(BaseStrategy):
    """对冲套利策略"""
    
    def __init__(
        self,
        pair_id: Optional[str],
        symbol: str,
        symbol_a: str,
        symbol_b: str,
        quantity: Decimal,
        quantity_precision: Decimal,
        open_threshold_pct: float,
        close_threshold_pct: float,
        exchange_a,
        exchange_b,
        lark_bot=None,
        monitor_only: bool = False,
        trade_logger=None,
        max_signal_delay_ms_a: int = 200,
        max_signal_delay_ms_b: int = 200,
        min_depth_quantity: Decimal = Decimal('0.01'),
        accumulate_mode: bool = False,
        max_position: Decimal = Decimal('0.1'),
        direction_reverse: bool = False, # 默认负滑点方向才下单
        cooldown_range: tuple = (10.0, 10.0),
        cooldown_seconds: Optional[float] = 5,
        dynamic_threshold: Optional[dict] = None,
        signal_logic: Optional[dict] = None,
        end_time: Optional[str] = None,
        edge_filter_enabled: bool = False,
        min_edge_bps: float = 0.8,
        edge_base_cost_bps: float = 3.0,
        edge_fee_bps: float = 0.0,
        edge_latency_bps_per_100ms: float = 0.0,
        edge_latency_free_ms: float = 120.0,
        risk_control: Optional[dict] = None,
        local_override_path: Optional[str] = None,
        data_collection_only: bool = False,
        orderbook_collection_interval_seconds: float = 1.0,
        orderbook_collection_dir: Optional[str] = None,
    ):
        super().__init__(
            strategy_name=f"Hedge-{symbol}",
            symbol=symbol,
            quantity=quantity,
            quantity_precision=quantity_precision
        )
        self.pair_id = pair_id
        self.symbol_a = symbol_a
        self.symbol_b = symbol_b
        self.open_threshold_pct = open_threshold_pct
        self.close_threshold_pct = close_threshold_pct
        self.exchange_a = exchange_a
        self.exchange_b = exchange_b
        self.lark_bot = lark_bot
        self.monitor_only = monitor_only
        self.signal_submitter = None
        self.signal_clearer = None
        self.max_signal_delay_ms_a = max_signal_delay_ms_a
        self.max_signal_delay_ms_b = max_signal_delay_ms_b
        self.min_depth_quantity = min_depth_quantity
        self.direction_reverse = direction_reverse
        self.cooldown_seconds = cooldown_seconds
        self.cooldown_range = cooldown_range
        self.signal_total = 0
        self.signal_delay = 0
        self.edge_filter_enabled = bool(edge_filter_enabled)
        self.min_edge_bps = max(0.0, float(min_edge_bps))
        self.edge_base_cost_bps = max(0.0, float(edge_base_cost_bps))
        self.edge_fee_bps = max(0.0, float(edge_fee_bps))
        self.edge_latency_bps_per_100ms = max(0.0, float(edge_latency_bps_per_100ms))
        self.edge_latency_free_ms = max(0.0, float(edge_latency_free_ms))
        self.risk_control_config = risk_control if isinstance(risk_control, dict) else {}
        self.risk_control_enabled = bool(self.risk_control_config.get('enabled', False))
        self.risk_control_service = RiskControlService(
            exchange_a=exchange_a,
            exchange_b=exchange_b,
            symbol_a=symbol_a,
            symbol_b=symbol_b,
            config=self.risk_control_config,
            lark_bot=lark_bot,
            base_max_position=max_position,
            position_step=quantity,
        )
        self._risk_reduce_cooldown_seconds = float(self.risk_control_config.get('reduce_cooldown_seconds', 5.0))
        self._last_risk_reduce_time = 0.0

        self.start_vol_a = 0
        self.start_equity_a = 0
        self.start_vol_b = 0
        self.start_equity_b = 0
        self.local_override_path = Path(local_override_path).expanduser() if local_override_path else None
        self.data_collection_only = bool(data_collection_only)
        self.orderbook_collection_interval_seconds = float(orderbook_collection_interval_seconds)
        self.orderbook_collection_dir = (
            Path(orderbook_collection_dir).expanduser()
            if orderbook_collection_dir
            else Path("logs/arbitrage/orderbook_data")
        )
        self._orderbook_collection_path: Optional[Path] = None
        self._last_orderbook_collection_time = 0.0

        # ✅ 新增：结束时间
        self.end_time_stamp = None
        if end_time:
            self.end_time_stamp = beijing_to_timestamp(end_time)

        # ✅ 新增：下单限流器（每60秒最多35次）
        self.order_limiter_a = AsyncLimiter(35, 60)
        self.order_limiter_b = AsyncLimiter(600, 60)

        # ✅ 使用 PositionManagerService 管理持仓
        self.position_manager = PositionManagerService(
            trade_logger=trade_logger,
            accumulate_mode=accumulate_mode,
            max_position=max_position,
            position_step=quantity
        )
        self.risk_control_service.set_base_max_position(max_position)

        # 价格监控服务
        self.monitor = PriceMonitorService(
            symbol=symbol,
            exchange_a=exchange_a,
            exchange_b=exchange_b,
            trigger_exchange='exchange_b'
        )
        self.process_diagnostics = ProcessDiagnosticsService(
            symbol=symbol,
            exchange_a=exchange_a,
            exchange_b=exchange_b,
            monitor=self.monitor,
            interval_seconds=60.0,
        )
        
        # 订单执行服务
        self.executor = OrderExecutor(
            exchange_a=exchange_a,
            exchange_b=exchange_b,
            quantity=quantity,
            quantity_precision=quantity_precision,
            order_limiter_a=self.order_limiter_a,
            order_limiter_b=self.order_limiter_b,
            trade_logger=trade_logger,
            get_strategy_position_after=self.position_manager.get_current_position_qty
        )
        
        # 持仓管理
        self.open_signal_count = 0
        self.close_signal_count = 0

        # ✅ 添加执行锁
        self._executing_lock = asyncio.Lock()
        self._is_executing = False
        
        # ✅ 新增：日志节流
        self.last_log_time = 0
        self.log_interval = 5.0  # 每5秒最多输出一次日志

        # ✅ 添加冷却期
        # self._last_open_time = 0
        # self._last_close_time = 0
        self._last_execution_time = 0
        
        self.signal_stats = {
            # 开仓信号统计
            'open': {
                'total': 0,              # 总信号数（满足阈值）
                'delay_filtered': 0,     # 因延迟过滤
                'depth_insufficient': 0, # 因深度不足跳过
                'threshold_filtered': 0, # 因动态阈值和不足跳过
                'edge_filtered': 0,      # 因边际不足跳过
                'depth_adjusted': 0,     # 因深度调整数量
                'limited_a': 0,            # 因限流跳过
                'limited_b': 0,            # 因限流跳过
                'skipped': 0,            # 因仓位达到上限跳过
                'executed': 0            # 实际执行
            },
            # 平仓信号统计
            'close': {
                'total': 0,
                'delay_filtered': 0,
                'depth_insufficient': 0,
                'threshold_filtered': 0,
                'edge_filtered': 0,      # 因边际不足跳过
                'depth_adjusted': 0,
                'limited_a': 0,
                'limited_b': 0,
                'skipped': 0,
                'executed': 0
            }
        }
        # ✅ 定期输出统计（可选）
        self._last_stats_log_time = 0
        self._stats_log_interval = 60  # 每 60 秒输出一次统计
        self._equity_log_interval = 5 * 60  # 每 15 分钟输出一次权益和交易量
        self._last_equity_log_time = None
        self._last_yaml_check_time = None
        self._yaml_check_interval = 5.0  # 每 5 秒检查一次本地热加载配置
        self._local_override_last_mtime: Optional[float] = None
        self._is_executed = False
        self._last_effective_max_position: Optional[Decimal] = None
        self._last_non_zero_strategy_qty = Decimal('0')
        self._end_time_triggered = False
        self._signal_sequence = 0
        self._threshold_skip_log_interval = 30.0
        self._last_threshold_skip_logs = {}
        self._sample_snapshot_log_interval = 10.0
        self._last_sample_snapshot_log_time = 0.0
        # self._last_threshold_check_time = None
        # 信号逻辑配置（默认沿用旧逻辑）
        self.signal_logic = signal_logic if isinstance(signal_logic, dict) else {}
        self.signal_mode = str(self.signal_logic.get('mode', 'legacy')).lower()
        self.signal_quantile = float(self.signal_logic.get('quantile', 0.6))
        self.signal_sample_size = int(self.signal_logic.get('sample_size', 2000))
        self.signal_min_samples = int(self.signal_logic.get('min_samples', self.signal_sample_size))
        self.signal_min_edge_pct = Decimal(str(self.signal_logic.get('min_edge_pct', 0.01)))
        self.signal_min_abs_spread_pct = Decimal(str(self.signal_logic.get('min_abs_spread_pct', 0.03)))
        self.quantile_log_enabled = bool(self.signal_logic.get('log_samples', True))
        self.quantile_event_log_enabled = bool(self.signal_logic.get('log_events', True))
        self.quantile_log_every_n = int(self.signal_logic.get('log_every_n', 1))
        self._quantile_log_counter = 0
        self.quantile_log_dir = Path(self.signal_logic.get('log_dir', 'logs/arbitrage/quantile_signal'))
        self._quantile_samples_path: Optional[Path] = None
        self._quantile_events_path: Optional[Path] = None
        self.median_edge_logic = self.signal_logic.get('median_edge', {})
        if not isinstance(self.median_edge_logic, dict):
            self.median_edge_logic = {}
        self.median_edge_enabled = bool(self.median_edge_logic.get('enabled', False))
        self.median_edge_baseline_adjustment = bool(self.median_edge_logic.get('baseline_adjustment', True))
        self.median_edge_baseline_ratio = float(self.median_edge_logic.get('baseline_ratio', 0.5))
        self.median_edge_medium_window_seconds = int(self.median_edge_logic.get('medium_window_seconds', 1800))
        self.median_edge_long_window_seconds = int(self.median_edge_logic.get('long_window_seconds', 3600))
        self.median_edge_medium_min_samples = int(self.median_edge_logic.get('medium_min_samples', 120))
        self.median_edge_long_min_samples = int(self.median_edge_logic.get('long_min_samples', 240))
        self.median_edge_min_edge_bps = float(self.median_edge_logic.get('min_edge_bps', 2.25))
        self.median_edge_manager = None
        self._median_edge_context = None
        self.stat_arb_logic = self.signal_logic.get('stat_arb', {})
        if not isinstance(self.stat_arb_logic, dict):
            self.stat_arb_logic = {}
        self.stat_arb_enabled = bool(self.stat_arb_logic.get('enabled', False))
        self.stat_arb_baseline_adjustment = bool(self.stat_arb_logic.get('baseline_adjustment', True))
        self.stat_arb_baseline_ratio = float(self.stat_arb_logic.get('baseline_ratio', 0.5))
        self.stat_arb_medium_window_seconds = int(self.stat_arb_logic.get('medium_window_seconds', 1800))
        self.stat_arb_long_window_seconds = int(self.stat_arb_logic.get('long_window_seconds', 3600))
        self.stat_arb_medium_min_samples = int(self.stat_arb_logic.get('medium_min_samples', 120))
        self.stat_arb_long_min_samples = int(self.stat_arb_logic.get('long_min_samples', 240))
        self.stat_arb_medium_weight = float(self.stat_arb_logic.get('medium_weight', 0.4))
        self.stat_arb_long_weight = float(self.stat_arb_logic.get('long_weight', 0.6))
        self.stat_arb_score_mode = str(self.stat_arb_logic.get('score_mode', 'weighted')).lower()
        self.stat_arb_breakout_quantile = float(self.stat_arb_logic.get('breakout_quantile', 0.7))
        self.stat_arb_entry_threshold = float(self.stat_arb_logic.get('entry_threshold', 2.8))
        entry_raw_floor = self.stat_arb_logic.get('entry_raw_floor_pct')
        self.stat_arb_entry_raw_floor_pct = (
            Decimal(str(entry_raw_floor)) if entry_raw_floor is not None else None
        )
        self.stat_arb_exit_threshold = float(self.stat_arb_logic.get('exit_threshold', 0.8))
        self.stat_arb_exit_score_source = str(self.stat_arb_logic.get('exit_score_source', 'final')).lower()
        self.stat_arb_exit_spread_floor_pct = Decimal(
            str(self.stat_arb_logic.get('exit_spread_floor_pct', -0.01))
        )
        exit_take_profit = self.stat_arb_logic.get('exit_take_profit_pct')
        self.stat_arb_exit_take_profit_pct = (
            Decimal(str(exit_take_profit)) if exit_take_profit is not None else None
        )
        self.stat_arb_min_score_gap = float(self.stat_arb_logic.get('min_score_gap', 0.5))
        self.stat_arb_min_mad_pct = float(self.stat_arb_logic.get('min_mad_pct', 0.003))
        self.stat_arb_quality_log_interval_seconds = float(
            self.stat_arb_logic.get('quality_log_interval_seconds', 15.0)
        )
        self.stat_arb_require_same_sign = bool(
            self.stat_arb_logic.get('require_same_sign_for_medium_long', True)
        )
        self.stat_arb_block_regime = bool(
            self.stat_arb_logic.get('block_when_regime_suspected', True)
        )
        self.stat_arb_manager = None
        self._stat_arb_context = None
        self._stat_arb_position_context = None
        self._last_stat_arb_quality_log_time = 0.0

        # 分位数信号管理器（新逻辑）
        self.quantile_manager = None
        if self.signal_mode == 'quantile':
            self.quantile_manager = QuantileSignalManager(
                sample_size=self.signal_sample_size,
                min_samples=self.signal_min_samples,
                quantile=self.signal_quantile,
            )
            if self.quantile_log_enabled or self.quantile_event_log_enabled:
                self._init_quantile_logs()

        if self.signal_mode == 'median_edge' and self.median_edge_enabled:
            self.median_edge_manager = MedianEdgeSignalManager(
                baseline_adjustment=self.median_edge_baseline_adjustment,
                baseline_ratio=self.median_edge_baseline_ratio,
                medium_window_seconds=self.median_edge_medium_window_seconds,
                long_window_seconds=self.median_edge_long_window_seconds,
                medium_min_samples=self.median_edge_medium_min_samples,
                long_min_samples=self.median_edge_long_min_samples,
                min_edge_bps=self.median_edge_min_edge_bps,
            )

        if self.signal_mode == 'stat_arb' and self.stat_arb_enabled:
            self.stat_arb_manager = StatArbSignalManager(
                baseline_adjustment=self.stat_arb_baseline_adjustment,
                baseline_ratio=self.stat_arb_baseline_ratio,
                medium_window_seconds=self.stat_arb_medium_window_seconds,
                long_window_seconds=self.stat_arb_long_window_seconds,
                medium_min_samples=self.stat_arb_medium_min_samples,
                long_min_samples=self.stat_arb_long_min_samples,
                medium_weight=self.stat_arb_medium_weight,
                long_weight=self.stat_arb_long_weight,
                score_mode=self.stat_arb_score_mode,
                breakout_quantile=self.stat_arb_breakout_quantile,
                entry_threshold=self.stat_arb_entry_threshold,
                min_score_gap=self.stat_arb_min_score_gap,
                min_mad_pct=self.stat_arb_min_mad_pct,
                require_same_sign_for_medium_long=self.stat_arb_require_same_sign,
                block_when_regime_suspected=self.stat_arb_block_regime,
            )

        # 动态阈值管理器（旧逻辑）
        self.threshold_manager = None
        if self.signal_mode == 'legacy':
            dt_config = dynamic_threshold if isinstance(dynamic_threshold, dict) else {}
            if dt_config.get('enabled', False):
                self.threshold_manager = DynamicThresholdManager(
                    sample_size=dt_config.get('sample_size', 1000),
                    min_samples=dt_config.get('min_samples', 200),
                    std_multiplier=dt_config.get('std_multiplier', 1.0),
                    min_total_threshold=dt_config.get('min_total_threshold', 0.02),
                    max_std_multiplier=dt_config.get('max_std_multiplier', 4.0),
                    min_std_multiplier=dt_config.get('min_std_multiplier', 0.0)
                )

        # 启动时先应用一次本地覆盖配置；未提供的字段保持当前值不变。
        self.check_yaml_config_updates(force=True, init_load=True)

        logger.info(
            f"🎯 策略配置:\n"
            f"   Symbol: {symbol}\n"
            f"   Quantity: {quantity}\n"
            f"   延迟阈值(A/B): {self.max_signal_delay_ms_a}/{self.max_signal_delay_ms_b} ms\n"
            f"   Open Threshold: {self.open_threshold_pct}%\n"
            f"   Close Threshold: {self.close_threshold_pct}%\n"
            f"   Exchange A: {exchange_a.exchange_name}\n"
            f"   Exchange B: {exchange_b.exchange_name}\n"
            f"   Monitor Only: {monitor_only}\n"
            f"   累计模式: {'✅ 启用' if accumulate_mode else '❌ 禁用'}\n"
            f"   风控模块: {'✅ 启用' if self.risk_control_enabled else '❌ 禁用'}\n"
            f"   信号逻辑: {'分位数' if self.signal_mode == 'quantile' else ('双中位数超额' if self.signal_mode == 'median_edge' else ('统计套利' if self.signal_mode == 'stat_arb' else '标准差'))}\n"
            f"   分位数配置: P{int(self.signal_quantile * 100)} | 样本{self.signal_sample_size} | 最小样本{self.signal_min_samples}\n"
            f"   分位数最小边际: {self.signal_min_edge_pct:.4f}%\n"
            f"   分位数绝对底线: {self.signal_min_abs_spread_pct:.4f}%\n"
            f"   双中位数开关: {'✅ 启用' if self.median_edge_enabled else '❌ 禁用'}\n"
            f"   双中位数基线修正: {'✅ 启用' if self.median_edge_baseline_adjustment else '❌ 禁用'} | ratio={self.median_edge_baseline_ratio:.3f}\n"
            f"   双中位数窗口: 30m={self.median_edge_medium_window_seconds}s | 60m={self.median_edge_long_window_seconds}s\n"
            f"   双中位数样本: 30m={self.median_edge_medium_min_samples} | 60m={self.median_edge_long_min_samples}\n"
            f"   双中位数超额门槛: {self.median_edge_min_edge_bps:.2f} bps\n"
            f"   统计套利开关: {'✅ 启用' if self.stat_arb_enabled else '❌ 禁用'}\n"
            f"   统计套利窗口: 30m={self.stat_arb_medium_window_seconds}s | 60m={self.stat_arb_long_window_seconds}s\n"
            f"   统计套利模式: score_mode={self.stat_arb_score_mode} | breakout_q={self.stat_arb_breakout_quantile:.2f} | exit_score={self.stat_arb_exit_score_source}\n"
            f"   统计套利阈值: entry={self.stat_arb_entry_threshold:.3f} | entry_floor={self.stat_arb_entry_raw_floor_pct if self.stat_arb_entry_raw_floor_pct is not None else '--'} | exit={self.stat_arb_exit_threshold:.3f} | exit_floor={self.stat_arb_exit_spread_floor_pct:.4f}% | exit_tp={self.stat_arb_exit_take_profit_pct if self.stat_arb_exit_take_profit_pct is not None else '--'} | gap={self.stat_arb_min_score_gap:.3f} | MAD下限={self.stat_arb_min_mad_pct:.6f}\n"
            f"   统计套利质量日志间隔: {self.stat_arb_quality_log_interval_seconds:.1f}s\n"
            f"   边际二次过滤: {'✅ 启用' if self.edge_filter_enabled else '❌ 禁用'}\n"
            f"   最小安全边际: {self.min_edge_bps:.2f} bps\n"
            f"   基础成本估计: {self.edge_base_cost_bps:.2f} bps\n"
            f"   手续费估计: {self.edge_fee_bps:.2f} bps\n"
            f"   延迟风险系数: {self.edge_latency_bps_per_100ms:.2f} bps/100ms\n"
            f"   延迟免惩罚阈值: {self.edge_latency_free_ms:.0f} ms\n"
            f"   数据采集模式: {'✅ 启用' if self.data_collection_only else '❌ 禁用'}\n"
            f"   订单簿写盘间隔: {self.orderbook_collection_interval_seconds:.2f}s\n"
            f"   订单簿写盘目录: {self.orderbook_collection_dir}\n"
            f"   本地热加载配置: {self.local_override_path or '--'}"
        )
        if self.signal_mode in {'stat_arb', 'median_edge'} and not accumulate_mode:
            logger.warning(
                "⚠️ 当前使用双方向信号模式但未开启累计模式：第一版仅在累计模式下完整支持双方向建仓，"
                "传统模式下 CLOSE 方向仍沿用“有持仓才评估”的旧语义"
            )
    
    async def start(self):
        """启动策略"""
        logger.info(f"🚀 启动策略: {self.strategy_name}")
        
        # 启动价格监控
        await self.monitor.start()
        await self.process_diagnostics.start()
        # 启动后台风控（异步监控，不阻塞信号热路径）
        if self.risk_control_enabled:
            await self.risk_control_service.start()
        self._init_orderbook_collection_log()
        if self.data_collection_only:
            self.monitor.subscribe(self._on_price_update)
            self.is_running = True
            logger.info(f"✅ 订单簿采集已启动: {self.strategy_name}")
            return
        # ✅ 新增：启动时同步仓位
        if self.position_manager.accumulate_mode:
            logger.info("🔄 累计模式启动，同步交易所仓位...")
            synced_qty = await self.position_manager.sync_from_exchanges(
                exchange_a=self.exchange_a,
                exchange_b=self.exchange_b,
                symbol_a=self.symbol_a,
                symbol_b=self.symbol_b
            )
            
            if synced_qty is not None:
                logger.info(
                    f"✅ 仓位同步完成:\n"
                    f"   本地仓位: {synced_qty:+.4f}\n"
                    f"   最大仓位: ±{self.position_manager.max_position}\n"
                    f"   剩余空间: {self.position_manager.max_position - abs(synced_qty):.4f}"
                )
            else:
                logger.warning("⚠️ 仓位同步失败，使用初始值 0")
        logger.info("🔍 开始获取初始权益和交易量")
        a_exchange_volume, a_exchange_equity, b_exchange_volume, b_exchange_equity = await self.get_equity_and_volume()
        logger.info(f"A所: 交易量 {a_exchange_volume} 权益 {a_exchange_equity}, B所: 交易量 { b_exchange_volume } 权益 { b_exchange_equity }")
        self.start_vol_a = a_exchange_volume
        self.start_equity_a = a_exchange_equity
        self.start_vol_b = b_exchange_volume
        self.start_equity_b = b_exchange_equity
        # 订阅价格更新
        self.monitor.subscribe(self._on_price_update)
        
        self.is_running = True
        logger.info(f"✅ 策略已启动: {self.strategy_name}")
    
    async def stop(self):
        """停止策略"""
        logger.info(f"⏹️ 停止策略: {self.strategy_name}")
        
        self.is_running = False
        # 取消订阅价格更新
        self.monitor.unsubscribe(self._on_price_update)
        await self.process_diagnostics.stop()
        
        # 停止价格监控
        await self.monitor.stop()
        if self.risk_control_enabled:
            await self.risk_control_service.stop()
        if self.executor is not None:
            await self.executor.close()
        
        logger.info(f"✅ 策略已停止: {self.strategy_name}")

    async def get_equity_and_volume(self):
        """获取交易所的权益和交易量"""
        task_a_balance = asyncio.create_task(self.exchange_a.get_balance())
        task_a_volume = asyncio.create_task(self.exchange_a.get_trade_volume())
        task_b_balance = asyncio.create_task(self.exchange_b.get_balance())
        task_b_volume = asyncio.create_task(self.exchange_b.get_trade_volume())
        a_exchange_equity, a_exchange_volume, b_exchange_equity, b_exchange_volume = await asyncio.gather(task_a_balance, task_a_volume, task_b_balance, task_b_volume)
        
        logger.info(f"📊 A所交易量: '----', 权益: {a_exchange_equity:.2f}")
        logger.info(f"📊 B所交易量: {b_exchange_volume:.2f}, 权益: {b_exchange_equity:.2f}")
        return a_exchange_volume, a_exchange_equity, b_exchange_volume, b_exchange_equity

    def _init_orderbook_collection_log(self) -> None:
        """初始化订单簿采集 CSV 文件。"""
        self.orderbook_collection_dir.mkdir(parents=True, exist_ok=True)
        date_tag = datetime.now().strftime('%Y%m%d')
        pair_tag = self.pair_id or self.symbol.lower()
        self._orderbook_collection_path = self.orderbook_collection_dir / f"orderbook_samples_{pair_tag}_{date_tag}.csv"
        if self._orderbook_collection_path.exists():
            logger.info(f"📁 [{self.symbol}] 订单簿采集文件: {self._orderbook_collection_path}")
            return

        with self._orderbook_collection_path.open('w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'timestamp',
                'datetime',
                'pair_id',
                'symbol',
                'exchange_a',
                'exchange_b',
                'exchange_a_bid',
                'exchange_a_ask',
                'exchange_a_mark',
                'exchange_a_bid_size',
                'exchange_a_ask_size',
                'exchange_a_timestamp',
                'exchange_a_quote_id',
                'exchange_b_bid',
                'exchange_b_ask',
                'exchange_b_mark',
                'exchange_b_bid_size',
                'exchange_b_ask_size',
                'exchange_b_timestamp',
                'exchange_b_quote_id',
                'signal_delay_ms_a',
                'signal_delay_ms_b',
                'is_stale',
                'spread_pct',
                'reverse_spread_pct',
                'avg_local_spread_pct',
                'total_local_spread_pct',
                'baseline_adjustment_pct',
                'orderbook_a_updates',
                'orderbook_b_updates',
                'fetch_duration_ms_b',
            ])
        logger.info(f"📁 [{self.symbol}] 创建订单簿采集文件: {self._orderbook_collection_path}")

    def _collect_orderbook_snapshot(
        self,
        prices: PriceSnapshot,
        signal_delay_ms_a: float,
        signal_delay_ms_b: float,
        is_stale: bool,
    ) -> None:
        """按固定频率记录订单簿快照。"""
        if self._orderbook_collection_path is None:
            self._init_orderbook_collection_log()
        if self._orderbook_collection_path is None:
            return

        now = time.time()
        if now - self._last_orderbook_collection_time < self.orderbook_collection_interval_seconds:
            return
        self._last_orderbook_collection_time = now

        avg_local_spread_pct = self._calculate_avg_local_spread_pct(prices)
        total_local_spread_pct = avg_local_spread_pct * Decimal('2')
        baseline_adjustment_pct = (
            total_local_spread_pct * Decimal(str(self.stat_arb_baseline_ratio))
            if self.stat_arb_baseline_adjustment
            else Decimal('0')
        )
        orderbook_b = getattr(self.monitor, 'orderbook_b', None) or {}
        fetch_duration = orderbook_b.get('fetch_duration')

        with self._orderbook_collection_path.open('a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                f"{now:.6f}",
                datetime.fromtimestamp(now).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                self.pair_id or '',
                self.symbol,
                self.exchange_a.exchange_name,
                self.exchange_b.exchange_name,
                f"{prices.exchange_a_bid}",
                f"{prices.exchange_a_ask}",
                f"{prices.exchange_a_mark}" if prices.exchange_a_mark is not None else "",
                f"{prices.exchange_a_bid_size}" if prices.exchange_a_bid_size is not None else "",
                f"{prices.exchange_a_ask_size}" if prices.exchange_a_ask_size is not None else "",
                f"{self._normalize_timestamp(prices.exchange_a_timestamp):.6f}",
                prices.exchange_a_quote_id or '',
                f"{prices.exchange_b_bid}",
                f"{prices.exchange_b_ask}",
                f"{prices.exchange_b_mark}" if prices.exchange_b_mark is not None else "",
                f"{prices.exchange_b_bid_size}" if prices.exchange_b_bid_size is not None else "",
                f"{prices.exchange_b_ask_size}" if prices.exchange_b_ask_size is not None else "",
                f"{self._normalize_timestamp(prices.exchange_b_timestamp):.6f}",
                prices.exchange_b_quote_id or '',
                f"{signal_delay_ms_a:.3f}",
                f"{signal_delay_ms_b:.3f}",
                "1" if is_stale else "0",
                f"{prices.calculate_spread_pct():.6f}",
                f"{prices.calculate_reverse_spread_pct():.6f}",
                f"{avg_local_spread_pct:.6f}",
                f"{total_local_spread_pct:.6f}",
                f"{baseline_adjustment_pct:.6f}",
                getattr(self.monitor, 'orderbook_a_updates', 0),
                getattr(self.monitor, 'orderbook_b_updates', 0),
                f"{float(fetch_duration):.3f}" if fetch_duration is not None else "",
            ])

    async def _on_price_update(self, prices: PriceSnapshot):
        """
        处理价格更新
        
        ✅ 核心逻辑：
        - 无持仓时：只检查开仓信号
        - 有持仓时：只检查平仓信号
        """
        if not self.is_running:
            return

        price_update_time_a = self._normalize_timestamp(prices.exchange_a_timestamp)
        price_update_time_b = self._normalize_timestamp(prices.exchange_b_timestamp)
        signal_trigger_time = time.time()
        signal_delay_ms_a = (signal_trigger_time - price_update_time_a) * 1000
        signal_delay_ms_b = (signal_trigger_time - price_update_time_b) * 1000
        is_stale, stale_msg = self.monitor.is_orderbook_stale(
            max_age_a=self.max_signal_delay_ms_a / 1000,
            max_age_b=self.max_signal_delay_ms_b / 1000,
        )
        self._collect_orderbook_snapshot(
            prices=prices,
            signal_delay_ms_a=signal_delay_ms_a,
            signal_delay_ms_b=signal_delay_ms_b,
            is_stale=is_stale,
        )
        if self.data_collection_only:
            return
        if is_stale:
            if self.threshold_manager:
                self._log_threshold_skip_reason("订单簿过时", detail=stale_msg)
            await self._clear_all_signal_states(f"订单簿过时: {stale_msg}")
            return
        try:
            signal_flag = False
            self.signal_total += 1
            # ✅ 过滤延迟过大的信号
            if signal_delay_ms_a <= self.max_signal_delay_ms_a and signal_delay_ms_b <= self.max_signal_delay_ms_b:
                signal_flag = True
            else:
                self.signal_delay += 1
                if self.threshold_manager:
                    exceeded_sides = []
                    if signal_delay_ms_a > self.max_signal_delay_ms_a:
                        exceeded_sides.append("A")
                    if signal_delay_ms_b > self.max_signal_delay_ms_b:
                        exceeded_sides.append("B")
                    exceeded_label = ",".join(exceeded_sides) if exceeded_sides else "unknown"
                    self._log_threshold_skip_reason(
                        "信号延迟超过阈值",
                        detail=(
                            f"超阈值侧={exceeded_label}, "
                            f"A={signal_delay_ms_a:.2f}/{self.max_signal_delay_ms_a}ms, "
                            f"B={signal_delay_ms_b:.2f}/{self.max_signal_delay_ms_b}ms"
                        ),
                        level=logging.WARNING,
                    )
                logger.warning(
                    f"⚠️ [{self.symbol}] 信号延迟过大: "
                    f"A {signal_delay_ms_a:.2f} ms（A阈值: {self.max_signal_delay_ms_a} ms），"
                    f" B {signal_delay_ms_b:.2f} ms（B阈值: {self.max_signal_delay_ms_b} ms）"
                )
                await self._clear_all_signal_states("信号延迟超过阈值")
                return  # 丢弃该信号
            # 计算价差
            spread_pct = prices.calculate_spread_pct()
            reverse_spread_pct = prices.calculate_reverse_spread_pct()
            # ✅ 风控快照只读：热路径不做外部 IO
            risk_block_open = False
            risk_decision = None
            if self.risk_control_enabled:
                risk_decision = self.risk_control_service.get_latest_decision()
                risk_block_open = risk_decision.block_open
                effective_max_position = self._apply_risk_position_cap(risk_decision)
                reduced, reduced_target_abs = await self._try_apply_dynamic_position_cap(
                    prices,
                    risk_decision,
                    effective_max_position,
                )
                if reduced:
                    if self.threshold_manager:
                        self._log_threshold_skip_reason("风控已执行动态减仓")
                    await self._clear_all_signal_states("风控已执行动态减仓")
                    return
                if risk_decision.need_reduce:
                    reduced = await self._try_apply_risk_reduction(
                        prices,
                        risk_decision.target_position_ratio,
                        target_abs_override=reduced_target_abs,
                    )
                    if reduced:
                        if self.threshold_manager:
                            self._log_threshold_skip_reason("风控主动减仓进行中")
                        await self._clear_all_signal_states("风控主动减仓进行中")
                        return
            # if self._last_threshold_check_time is None:
                # self._last_threshold_check_time = time.time()
            # now = time.time()
            # ✅ 不同信号模式分别维护自己的统计上下文
            if signal_flag:
                if self.signal_mode == 'quantile' and self.quantile_manager:
                    avg_local_spread_pct = self._calculate_avg_local_spread_pct(prices)
                    adjusted_open_spread = spread_pct - avg_local_spread_pct
                    adjusted_close_spread = reverse_spread_pct - avg_local_spread_pct
                    self.quantile_manager.add_spreads(adjusted_open_spread, adjusted_close_spread)
                    self._log_quantile_sample(
                        spread_pct=spread_pct,
                        reverse_spread_pct=reverse_spread_pct,
                        avg_local_spread_pct=avg_local_spread_pct,
                    )
                elif self.signal_mode == 'median_edge' and self.median_edge_manager:
                    self._update_median_edge_context(
                        spread_pct=spread_pct,
                        reverse_spread_pct=reverse_spread_pct,
                        prices=prices,
                    )
                    median_edge_ready = bool(self._median_edge_context and self._median_edge_context.get('ready'))
                    if not median_edge_ready:
                        total_samples = self._median_edge_context.get('total_samples', 0) if self._median_edge_context else 0
                        open_stats = self._median_edge_context.get('open') if self._median_edge_context else None
                        close_stats = self._median_edge_context.get('close') if self._median_edge_context else None
                        open_medium_samples = open_stats.medium_samples if open_stats else 0
                        open_long_samples = open_stats.long_samples if open_stats else 0
                        close_medium_samples = close_stats.medium_samples if close_stats else 0
                        close_long_samples = close_stats.long_samples if close_stats else 0
                        open_medium_span = open_stats.medium_span_seconds if open_stats else 0.0
                        open_long_span = open_stats.long_span_seconds if open_stats else 0.0
                        close_medium_span = close_stats.medium_span_seconds if close_stats else 0.0
                        close_long_span = close_stats.long_span_seconds if close_stats else 0.0
                        self._log_threshold_skip_reason(
                            "双中位数窗口尚未就绪",
                            detail=(
                                f"总样本={total_samples}, "
                                f"OPEN(30m/60m)={open_medium_samples}/{open_long_samples}, "
                                f"CLOSE(30m/60m)={close_medium_samples}/{close_long_samples}, "
                                f"OPEN跨度={open_medium_span:.1f}/{open_long_span:.1f}s, "
                                f"CLOSE跨度={close_medium_span:.1f}/{close_long_span:.1f}s"
                            ),
                        )
                        await self._clear_all_signal_states("双中位数窗口样本尚未就绪")
                        return
                elif self.signal_mode == 'stat_arb' and self.stat_arb_manager:
                    self._update_stat_arb_context(
                        spread_pct=spread_pct,
                        reverse_spread_pct=reverse_spread_pct,
                        prices=prices,
                    )
                    stat_arb_ready = bool(self._stat_arb_context and self._stat_arb_context.get('ready'))
                    if not stat_arb_ready:
                        total_samples = self._stat_arb_context.get('total_samples', 0) if self._stat_arb_context else 0
                        open_stats = self._stat_arb_context.get('open') if self._stat_arb_context else None
                        close_stats = self._stat_arb_context.get('close') if self._stat_arb_context else None
                        open_medium_samples = open_stats.medium_samples if open_stats else 0
                        open_long_samples = open_stats.long_samples if open_stats else 0
                        close_medium_samples = close_stats.medium_samples if close_stats else 0
                        close_long_samples = close_stats.long_samples if close_stats else 0
                        open_medium_span = open_stats.medium_span_seconds if open_stats else 0.0
                        open_long_span = open_stats.long_span_seconds if open_stats else 0.0
                        close_medium_span = close_stats.medium_span_seconds if close_stats else 0.0
                        close_long_span = close_stats.long_span_seconds if close_stats else 0.0
                        self._log_threshold_skip_reason(
                            "统计套利窗口尚未就绪",
                            detail=(
                                f"总样本={total_samples}, "
                                f"OPEN(30m/60m)={open_medium_samples}/{open_long_samples}, "
                                f"CLOSE(30m/60m)={close_medium_samples}/{close_long_samples}, "
                                f"OPEN跨度={open_medium_span:.1f}/{open_long_span:.1f}s, "
                                f"CLOSE跨度={close_medium_span:.1f}/{close_long_span:.1f}s"
                            ),
                        )
                        await self._clear_all_signal_states("统计套利窗口样本尚未就绪")
                        return
                    self._log_stat_arb_quality_if_needed()
                elif self.threshold_manager:
                    self._log_sample_snapshot(prices, signal_delay_ms_a, signal_delay_ms_b)
                    self.threshold_manager.add_spreads(spread_pct, reverse_spread_pct)

                    current_qty = self.position_manager.get_current_position_qty()
                    new_open, new_close = self.threshold_manager.try_adjust(
                        current_qty,
                        self.position_manager.max_position
                    )

                    if new_open is not None:
                        self.open_threshold_pct = new_open
                        self.close_threshold_pct = new_close
                    else:
                        stats = self.threshold_manager.get_stats()
                        self._log_threshold_skip_reason(
                            "动态阈值尚未就绪",
                            detail=(
                                f"状态={stats.get('status')}, "
                                f"开仓样本={stats.get('open_samples', 0)}, "
                                f"平仓样本={stats.get('close_samples', 0)}"
                            ),
                        )
                        await self._clear_all_signal_states("动态阈值尚未就绪")
                        return
                elif self.signal_mode == 'legacy':
                    self._log_threshold_skip_reason(
                        "动态阈值未启用",
                        detail="signal_mode=legacy 且 dynamic_threshold.enabled=false",
                    )

            if self.position_manager.accumulate_mode:
                current_qty = self.position_manager.get_current_position_qty()
                self._remember_last_non_zero_strategy_qty(current_qty)
                # logger.debug(f"🔍 当前strategy仓位: {current_qty:+.4f} {self.symbol}")
                self._is_executed = False
                if current_qty < 0:
                    if self.signal_mode == 'stat_arb':
                        should_exit, exit_reason = self._should_trigger_stat_arb_exit(
                            execute_signal_type=SignalType.CLOSE,
                            reference_direction=SignalType.OPEN,
                            prices=prices,
                        )
                        close_signal = None
                        if should_exit:
                            close_signal = self._build_stat_arb_exit_signal(
                                execute_signal_type=SignalType.CLOSE,
                                reference_direction=SignalType.OPEN,
                                prices=prices,
                                spread_pct=reverse_spread_pct,
                                signal_delay_ms_a=signal_delay_ms_a,
                                signal_delay_ms_b=signal_delay_ms_b,
                                reason=exit_reason,
                            )
                    else:
                        close_signal = await self._check_close_signal(
                            prices,
                            reverse_spread_pct,
                            signal_delay_ms_a,
                            signal_delay_ms_b,
                        )
                    await self._sync_signal_state(
                        SignalType.CLOSE,
                        close_signal,
                        "当前价格下 CLOSE 条件不成立" if self.signal_mode != 'stat_arb' else "统计套利回归平仓条件未满足",
                    )

                    if self._executing_lock.locked():
                        await self._clear_signal(SignalType.OPEN, "执行锁占用，暂不评估 OPEN")
                        return
                    if self._is_executed is True:
                        logger.info('开仓信号已经执行过了，直接返回')
                        await self._clear_signal(SignalType.OPEN, "本轮已执行完成，等待下一次价格更新")
                        return
                    if risk_block_open:
                        await self._clear_signal(SignalType.OPEN, "风控阻断新增风险开仓")
                        return
                    open_signal = await self._check_open_signal(
                        prices,
                        spread_pct,
                        signal_delay_ms_a,
                        signal_delay_ms_b,
                    )
                    await self._sync_signal_state(
                        SignalType.OPEN,
                        open_signal,
                        "当前价格下 OPEN 条件不成立",
                    )
                else:
                    open_signal = None
                    if current_qty > 0:
                        if self.signal_mode == 'stat_arb':
                            should_exit, exit_reason = self._should_trigger_stat_arb_exit(
                                execute_signal_type=SignalType.OPEN,
                                reference_direction=SignalType.CLOSE,
                                prices=prices,
                            )
                            if should_exit:
                                open_signal = self._build_stat_arb_exit_signal(
                                    execute_signal_type=SignalType.OPEN,
                                    reference_direction=SignalType.CLOSE,
                                    prices=prices,
                                    spread_pct=spread_pct,
                                    signal_delay_ms_a=signal_delay_ms_a,
                                    signal_delay_ms_b=signal_delay_ms_b,
                                    reason=exit_reason,
                                )
                        else:
                            open_signal = await self._check_open_signal(
                                prices,
                                spread_pct,
                                signal_delay_ms_a,
                                signal_delay_ms_b,
                            )
                    elif not risk_block_open:
                        open_signal = await self._check_open_signal(
                            prices,
                            spread_pct,
                            signal_delay_ms_a,
                            signal_delay_ms_b,
                        )
                    else:
                        await self._clear_signal(SignalType.OPEN, "风控阻断新增风险开仓")
                        await self._clear_signal(SignalType.CLOSE, "风控阻断当前方向的反向开仓")
                        return

                    await self._sync_signal_state(
                        SignalType.OPEN,
                        open_signal,
                        "当前价格下 OPEN 条件不成立" if self.signal_mode != 'stat_arb' else "统计套利回归平仓条件未满足",
                    )

                    if self._executing_lock.locked():
                        await self._clear_signal(SignalType.CLOSE, "执行锁占用，暂不评估 CLOSE")
                        return
                    if self._is_executed is True:
                        logger.info('平仓已经执行过了，直接返回')
                        await self._clear_signal(SignalType.CLOSE, "本轮已执行完成，等待下一次价格更新")
                        return 
                    if risk_block_open:
                        await self._clear_signal(SignalType.CLOSE, "风控阻断新增风险反向开仓")
                        return
                    close_signal = await self._check_close_signal(
                        prices,
                        reverse_spread_pct,
                        signal_delay_ms_a,
                        signal_delay_ms_b,
                    )
                    await self._sync_signal_state(
                        SignalType.CLOSE,
                        close_signal,
                        "当前价格下 CLOSE 条件不成立",
                    )
                
            else:
                if not self.position_manager.has_position():
                    await self._clear_signal(SignalType.CLOSE, "当前无持仓，不保留 CLOSE 信号")
                    if risk_block_open:
                        await self._clear_signal(SignalType.OPEN, "风控阻断新增风险开仓")
                        return
                    open_signal = await self._check_open_signal(
                        prices,
                        spread_pct,
                        signal_delay_ms_a,
                        signal_delay_ms_b,
                    )
                    await self._sync_signal_state(
                        SignalType.OPEN,
                        open_signal,
                        "当前价格下 OPEN 条件不成立",
                    )
                else:
                    await self._clear_signal(SignalType.OPEN, "当前已有持仓，不保留 OPEN 信号")
                    if self.signal_mode == 'stat_arb':
                        should_exit, exit_reason = self._should_trigger_stat_arb_exit(
                            execute_signal_type=SignalType.CLOSE,
                            reference_direction=SignalType.OPEN,
                            prices=prices,
                        )
                        close_signal = None
                        if should_exit:
                            close_signal = self._build_stat_arb_exit_signal(
                                execute_signal_type=SignalType.CLOSE,
                                reference_direction=SignalType.OPEN,
                                prices=prices,
                                spread_pct=reverse_spread_pct,
                                signal_delay_ms_a=signal_delay_ms_a,
                                signal_delay_ms_b=signal_delay_ms_b,
                                reason=exit_reason,
                            )
                    else:
                        close_signal = await self._check_close_signal(
                            prices,
                            reverse_spread_pct,
                            signal_delay_ms_a,
                            signal_delay_ms_b,
                        )
                    await self._sync_signal_state(
                        SignalType.CLOSE,
                        close_signal,
                        "当前价格下 CLOSE 条件不成立" if self.signal_mode != 'stat_arb' else "统计套利回归平仓条件未满足",
                    )
            
            self.check_yaml_config_updates()
            if self.end_time_stamp:
                current_timestamp = time.time()
                if current_timestamp >= self.end_time_stamp:
                    if not self._end_time_triggered:
                        logger.info("⏰ 达到策略结束时间，开始减仓到0")
                        self._end_time_triggered = True
                    # 如果仓位不为0，设置最大仓位为0
                    if self.position_manager.get_current_position_qty() != 0:
                        self.position_manager.max_position = 0
                        self.risk_control_service.set_base_max_position(Decimal("0"))
                    # 如果最大仓位不为0，设置最大仓位为0
                    if self.position_manager.max_position != 0:
                        self.position_manager.max_position = 0
                        self.risk_control_service.set_base_max_position(Decimal("0"))
                    # 最大仓位为0，并且当前仓位为0，停止策略
                    if self.position_manager.max_position == 0 and self.position_manager.get_current_position_qty() == 0:
                        logger.info(f"⏰ 达到策略结束时间，仓位减为0，等待5min后拉取B所交易量和权益并停止策略")
                        # 停止期间不再输出订单簿健康告警
                        if hasattr(self, 'monitor') and self.monitor:
                            self.monitor.suppress_health_logs = True
                        await asyncio.sleep(300)  # 等待5分钟
                        logger.info(f"⏰ 5分钟等待结束，开始获取B所交易量和权益")

                        volume_a, equity_a, volume_b, equity_b = await self.get_equity_and_volume()
                        volume_delta = volume_b - self.start_vol_b
                        logger.info(
                            self._build_equity_loss_summary(
                                equity_a=equity_a,
                                equity_b=equity_b,
                                volume_delta=volume_delta,
                                volume_label="B所交易增量",
                            )
                        )
                        
                        await self.stop()
                else:
                    await self._log_equity_and_volume_if_needed()


        except Exception as e:
            logger.exception(f"❌ 价格更新处理失败: {e}")

    def _normalize_timestamp(self, ts) -> float:
        """统一时间戳为秒"""
        if ts is None:
            return time.time()
        try:
            ts_val = float(ts)
        except Exception:
            return time.time()
        # 大于 1e10 认为是毫秒
        if ts_val > 1e10:
            ts_val = ts_val / 1000.0
        return ts_val

    def _log_threshold_skip_reason(
        self,
        reason: str,
        *,
        detail: str = "",
        level: int = logging.INFO,
    ) -> None:
        """节流输出当前信号模式未放行的原因。"""
        now = time.time()
        last_time = self._last_threshold_skip_logs.get(reason, 0.0)
        if now - last_time < self._threshold_skip_log_interval:
            return
        self._last_threshold_skip_logs[reason] = now
        prefix = "未触发动态阈值调整"
        if self.signal_mode == 'quantile':
            prefix = "未触发分位数信号"
        elif self.signal_mode == 'median_edge':
            prefix = "未触发双中位数超额信号"
        elif self.signal_mode == 'stat_arb':
            prefix = "未触发统计套利信号"
        message = f"🧭 [{self.symbol}] {prefix}: {reason}"
        if detail:
            message = f"{message} | {detail}"
        logger.log(level, message)

    def _log_sample_snapshot(
        self,
        prices: PriceSnapshot,
        signal_delay_ms_a: float,
        signal_delay_ms_b: float,
    ) -> None:
        """节流输出当前参与采样的双边订单簿摘要。"""
        now = time.time()
        if now - self._last_sample_snapshot_log_time < self._sample_snapshot_log_interval:
            return
        self._last_sample_snapshot_log_time = now

        update_count_a = getattr(self.monitor, 'orderbook_a_updates', 0)
        update_count_b = getattr(self.monitor, 'orderbook_b_updates', 0)
        orderbook_b = getattr(self.monitor, 'orderbook_b', None) or {}
        fetch_duration = orderbook_b.get('fetch_duration')
        quote_id = orderbook_b.get('quote_id')
        fetch_duration_text = f"{float(fetch_duration):.2f}" if fetch_duration is not None else "--"
        quote_id_text = quote_id[:8] if isinstance(quote_id, str) and quote_id else "--"

        logger.info(
            f"🧪 [{self.symbol}] 当前采样快照:\n"
            f"   A {self.exchange_a.exchange_name}: "
            f"bid={prices.exchange_a_bid}, ask={prices.exchange_a_ask}, "
            f"delay_ms={signal_delay_ms_a:.2f}, updates={update_count_a}\n"
            f"   B {self.exchange_b.exchange_name}: "
            f"bid={prices.exchange_b_bid}, ask={prices.exchange_b_ask}, "
            f"delay_ms={signal_delay_ms_b:.2f}, updates={update_count_b}, "
            f"fetch_duration_ms={fetch_duration_text}, quote_id={quote_id_text}"
        )

    def _init_quantile_logs(self) -> None:
        """初始化分位数日志文件。"""
        self.quantile_log_dir.mkdir(parents=True, exist_ok=True)
        date_tag = datetime.now().strftime('%Y%m%d')
        self._quantile_samples_path = self.quantile_log_dir / f"quantile_samples_{self.symbol}_{date_tag}.csv"
        self._quantile_events_path = self.quantile_log_dir / f"quantile_events_{self.symbol}_{date_tag}.csv"

        if self.quantile_log_enabled and self._quantile_samples_path and not self._quantile_samples_path.exists():
            with self._quantile_samples_path.open('w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'timestamp',
                    'datetime',
                    'symbol',
                    'open_spread_pct',
                    'close_spread_pct',
                    'avg_local_spread_pct',
                    'open_adjusted_pct',
                    'close_adjusted_pct',
                    'open_threshold_pct',
                    'close_threshold_pct',
                    'quantile',
                    'sample_size',
                    'ready',
                    'open_condition',
                    'close_condition',
                ])

        if self.quantile_event_log_enabled and self._quantile_events_path and not self._quantile_events_path.exists():
            with self._quantile_events_path.open('w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'timestamp',
                    'datetime',
                    'symbol',
                    'signal_type',
                    'stage',
                    'open_spread_pct',
                    'close_spread_pct',
                    'avg_local_spread_pct',
                    'adjusted_spread_pct',
                    'threshold_pct',
                    'quantile',
                    'note',
                ])

    def _log_quantile_sample(
        self,
        spread_pct: Decimal,
        reverse_spread_pct: Decimal,
        avg_local_spread_pct: Decimal,
    ) -> None:
        """记录分位数样本快照。"""
        if not (self.quantile_manager and self.quantile_log_enabled and self._quantile_samples_path):
            return
        self._quantile_log_counter += 1
        if self.quantile_log_every_n > 1 and self._quantile_log_counter % self.quantile_log_every_n != 0:
            return

        stats = self.quantile_manager.get_stats()
        open_threshold = stats.get('current_open')
        close_threshold = stats.get('current_close')
        ready = stats.get('ready')

        open_adjusted = spread_pct - avg_local_spread_pct
        close_adjusted = reverse_spread_pct - avg_local_spread_pct

        open_condition = ""
        close_condition = ""
        if open_threshold is not None:
            open_condition = int(open_adjusted >= Decimal(str(open_threshold)))
        if close_threshold is not None:
            close_condition = int(close_adjusted >= Decimal(str(close_threshold)))

        now = time.time()
        with self._quantile_samples_path.open('a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                f"{now:.6f}",
                datetime.fromtimestamp(now).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                self.symbol,
                f"{spread_pct:.6f}",
                f"{reverse_spread_pct:.6f}",
                f"{avg_local_spread_pct:.6f}",
                f"{open_adjusted:.6f}",
                f"{close_adjusted:.6f}",
                f"{open_threshold:.6f}" if open_threshold is not None else "",
                f"{close_threshold:.6f}" if close_threshold is not None else "",
                f"{stats.get('quantile', 0):.4f}",
                stats.get('open_samples', 0),
                "1" if ready else "0",
                open_condition,
                close_condition,
            ])

    def _log_quantile_event(
        self,
        signal_type: str,
        stage: str,
        spread_pct: Decimal,
        reverse_spread_pct: Decimal,
        avg_local_spread_pct: Decimal,
        adjusted_spread_pct: Decimal,
        threshold_pct: Decimal,
        note: str = "",
    ) -> None:
        """记录分位数触发事件。"""
        if not (self.quantile_manager and self.quantile_event_log_enabled and self._quantile_events_path):
            return
        now = time.time()
        with self._quantile_events_path.open('a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                f"{now:.6f}",
                datetime.fromtimestamp(now).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                self.symbol,
                signal_type,
                stage,
                f"{spread_pct:.6f}",
                f"{reverse_spread_pct:.6f}",
                f"{avg_local_spread_pct:.6f}",
                f"{adjusted_spread_pct:.6f}",
                f"{threshold_pct:.6f}",
                f"{self.signal_quantile:.4f}",
                note,
            ])

    def _calculate_avg_local_spread_pct(self, prices: PriceSnapshot) -> Decimal:
        """计算两所平均点差（百分比）。"""
        mid_a = (prices.exchange_a_bid + prices.exchange_a_ask) / 2
        mid_b = (prices.exchange_b_bid + prices.exchange_b_ask) / 2
        if mid_a <= 0 or mid_b <= 0:
            return Decimal('0')
        spread_a = (prices.exchange_a_ask - prices.exchange_a_bid) / mid_a * Decimal('100')
        spread_b = (prices.exchange_b_ask - prices.exchange_b_bid) / mid_b * Decimal('100')
        return (spread_a + spread_b) / 2

    def _get_quantile_thresholds(self) -> tuple[Optional[Decimal], Optional[Decimal]]:
        """获取分位数阈值（开仓/平仓）。"""
        if not self.quantile_manager:
            return None, None
        open_q, close_q = self.quantile_manager.get_thresholds()
        if open_q is None or close_q is None:
            return None, None
        return Decimal(str(open_q)), Decimal(str(close_q))

    def _update_median_edge_context(
        self,
        spread_pct: Decimal,
        reverse_spread_pct: Decimal,
        prices: PriceSnapshot,
    ) -> None:
        """刷新双中位数超额上下文。"""
        if not self.median_edge_manager:
            self._median_edge_context = None
            return

        total_local_spread_pct = self._calculate_avg_local_spread_pct(prices) * Decimal('2')
        self.median_edge_manager.add_spreads(
            open_spread=spread_pct,
            close_spread=reverse_spread_pct,
            baseline_pct=total_local_spread_pct,
        )
        self._median_edge_context = self.median_edge_manager.get_signal_context()

    def _get_median_edge_direction_stats(self, signal_type: SignalType):
        """获取某个方向的双中位数超额快照。"""
        if not self._median_edge_context:
            return None
        if signal_type == SignalType.OPEN:
            return self._median_edge_context.get('open')
        return self._median_edge_context.get('close')

    def _is_median_edge_selected(self, signal_type: SignalType) -> bool:
        """当前双中位数上下文是否选择了该方向。"""
        if not self._median_edge_context:
            return False
        return self._median_edge_context.get('selected_signal_type') == signal_type

    def _format_median_edge_extra_info(self, signal_type: SignalType) -> str:
        """构建双中位数超额信号附加日志。"""
        direction_stats = self._get_median_edge_direction_stats(signal_type)
        if direction_stats is None:
            return ""
        return (
            f"   调整后价差: {direction_stats.current_adjusted_pct:.4f}%\n"
            f"   基线修正值: {direction_stats.baseline_pct:.4f}%\n"
            f"   30m/60m 中位数: {direction_stats.medium_median_pct:.4f}% / {direction_stats.long_median_pct:.4f}%\n"
            f"   30m/60m 超额: {direction_stats.medium_edge_pct:.4f}% / {direction_stats.long_edge_pct:.4f}%\n"
            f"   生效门槛: {direction_stats.threshold_pct:.4f}%\n"
            f"   选择原因: {self._median_edge_context.get('selection_reason', '--')}\n"
        )

    def _update_stat_arb_context(
        self,
        spread_pct: Decimal,
        reverse_spread_pct: Decimal,
        prices: PriceSnapshot,
    ) -> None:
        """刷新统计套利上下文。"""
        if not self.stat_arb_manager:
            self._stat_arb_context = None
            return

        total_local_spread_pct = self._calculate_avg_local_spread_pct(prices) * Decimal('2')
        self.stat_arb_manager.add_spreads(
            open_spread=spread_pct,
            close_spread=reverse_spread_pct,
            baseline_pct=total_local_spread_pct,
        )
        self._stat_arb_context = self.stat_arb_manager.get_signal_context()

    def _get_stat_arb_direction_stats(self, signal_type: SignalType):
        """获取某个方向的统计套利快照。"""
        if not self._stat_arb_context:
            return None
        if signal_type == SignalType.OPEN:
            return self._stat_arb_context.get('open')
        return self._stat_arb_context.get('close')

    def _is_stat_arb_selected(self, signal_type: SignalType) -> bool:
        """当前统计套利上下文是否选择了该方向。"""
        if not self._stat_arb_context:
            return False
        return self._stat_arb_context.get('selected_signal_type') == signal_type

    def _format_stat_arb_extra_info(self, signal_type: SignalType) -> str:
        """构建统计套利信号的附加日志。"""
        direction_stats = self._get_stat_arb_direction_stats(signal_type)
        if direction_stats is None:
            return ""

        return (
            f"   基线修正后价差: {direction_stats.current_adjusted_pct:.4f}%\n"
            f"   基线修正值: {direction_stats.baseline_pct:.4f}%\n"
            f"   30m 中位数/MAD: {direction_stats.medium_median_pct:.4f}% / {direction_stats.medium_mad_pct:.4f}%\n"
            f"   60m 中位数/MAD: {direction_stats.long_median_pct:.4f}% / {direction_stats.long_mad_pct:.4f}%\n"
            f"   30m/60m 分位阈值: {direction_stats.medium_quantile_pct:.4f}% / {direction_stats.long_quantile_pct:.4f}%\n"
            f"   30m/60m 分数: {direction_stats.medium_score:.3f} / {direction_stats.long_score:.3f}\n"
            f"   1h基准/30m偏离分数: {direction_stats.long_baseline_score:.3f}\n"
            f"   分位突破强度: {direction_stats.breakout_score:.3f}\n"
            f"   最终分数: {direction_stats.final_score:.3f}\n"
            f"   当前生效分数: {direction_stats.active_score:.3f}\n"
            f"   选择原因: {self._stat_arb_context.get('selection_reason', '--')}\n"
        )

    def _get_stat_arb_entry_score(self, direction_stats) -> Optional[float]:
        """返回当前统计套利入场使用的分数。"""
        if direction_stats is None:
            return None
        if getattr(direction_stats, 'active_score', None) is not None:
            return float(direction_stats.active_score)
        return float(direction_stats.final_score) if direction_stats.final_score is not None else None

    def _get_stat_arb_exit_score(self, direction_stats) -> Optional[float]:
        """返回当前统计套利退出使用的分数。"""
        if direction_stats is None:
            return None
        if self.stat_arb_exit_score_source == 'medium':
            return float(direction_stats.medium_score) if direction_stats.medium_score is not None else None
        return float(direction_stats.final_score) if direction_stats.final_score is not None else None

    def _get_stat_arb_entry_raw_spread_pct(
        self,
        signal_type: SignalType,
        prices: PriceSnapshot,
    ) -> Decimal:
        """返回统计套利开仓方向的真实可执行价差。"""
        if signal_type == SignalType.OPEN:
            return prices.calculate_spread_pct()
        return prices.calculate_reverse_spread_pct()

    def _set_stat_arb_position_context_from_signal(self, signal: TradingSignal) -> None:
        """记录统计套利当前持仓的入场上下文。"""
        if self.signal_mode != 'stat_arb':
            return

        stat_arb_meta = signal.metadata.get('stat_arb', {}) if isinstance(signal.metadata, dict) else {}
        current_qty = self.position_manager.get_current_position_qty()

        if not self.position_manager.accumulate_mode:
            if signal.signal_type == SignalType.OPEN and self.position_manager.has_position():
                active_direction = SignalType.OPEN
            else:
                self._clear_stat_arb_position_context("传统模式平仓完成")
                return
        else:
            if current_qty == 0:
                self._clear_stat_arb_position_context("累计模式仓位归零")
                return
            active_direction = SignalType.OPEN if current_qty < 0 else SignalType.CLOSE

        self._stat_arb_position_context = {
            "active_direction": active_direction,
            "entry_signal_id": signal.signal_id,
            "entry_time": time.time(),
            "entry_score": float(
                stat_arb_meta.get("active_score", stat_arb_meta.get("final_score", 0.0)) or 0.0
            ),
            "entry_adjusted_pct": float(stat_arb_meta.get("current_adjusted_pct", 0.0) or 0.0),
        }

    def _clear_stat_arb_position_context(self, reason: str = "") -> None:
        """清空统计套利持仓上下文。"""
        if self._stat_arb_position_context is None:
            return
        if reason:
            logger.info(f"🧹 [{self.symbol}] 清空统计套利持仓上下文: {reason}")
        self._stat_arb_position_context = None

    def _get_active_stat_arb_direction(self) -> Optional[SignalType]:
        """返回当前统计套利持仓对应的方向。"""
        if self.signal_mode != 'stat_arb' or not self._stat_arb_position_context:
            return None
        return self._stat_arb_position_context.get("active_direction")

    def _get_stat_arb_exit_executable_spread_pct(
        self,
        execute_signal_type: SignalType,
        prices: PriceSnapshot,
    ) -> Decimal:
        """返回统计套利退出方向的真实可执行价差。"""
        if execute_signal_type == SignalType.OPEN:
            return prices.calculate_spread_pct()
        return prices.calculate_reverse_spread_pct()

    def _should_trigger_stat_arb_exit(
        self,
        execute_signal_type: SignalType,
        reference_direction: SignalType,
        prices: PriceSnapshot,
    ) -> tuple[bool, str]:
        """判断统计套利当前持仓是否满足回归平仓条件。"""
        direction_stats = self._get_stat_arb_direction_stats(reference_direction)
        current_score = self._get_stat_arb_exit_score(direction_stats)
        if direction_stats is None or current_score is None:
            return False, "缺少当前方向统计快照"

        exit_spread_pct = self._get_stat_arb_exit_executable_spread_pct(execute_signal_type, prices)
        if (
            self.stat_arb_exit_take_profit_pct is not None
            and exit_spread_pct >= self.stat_arb_exit_take_profit_pct
        ):
            return True, (
                f"退出可执行价差触发止盈({exit_spread_pct:.4f}% >= "
                f"{self.stat_arb_exit_take_profit_pct:.4f}%)"
            )

        if current_score > self.stat_arb_exit_threshold:
            return False, f"分数仍高于退出阈值({current_score:.3f} > {self.stat_arb_exit_threshold:.3f})"

        if exit_spread_pct < self.stat_arb_exit_spread_floor_pct:
            logger.info(
                f"⏸️ [{self.symbol}] 统计套利退出被抑制: "
                f"执行方向={execute_signal_type.value}, 参考方向={reference_direction.value}, "
                f"current_score={current_score:.3f}, exit_threshold={self.stat_arb_exit_threshold:.3f}, "
                f"exit_spread={exit_spread_pct:.4f}%, floor={self.stat_arb_exit_spread_floor_pct:.4f}%"
            )
            return (
                False,
                f"退出可执行价差低于底线({exit_spread_pct:.4f}% < {self.stat_arb_exit_spread_floor_pct:.4f}%)",
            )

        return True, (
            f"分数回落至退出阈值以下({current_score:.3f} <= {self.stat_arb_exit_threshold:.3f})，"
            f"且退出可执行价差满足底线({exit_spread_pct:.4f}% >= {self.stat_arb_exit_spread_floor_pct:.4f}%)"
        )

    def _build_stat_arb_exit_signal(
        self,
        execute_signal_type: SignalType,
        reference_direction: SignalType,
        prices: PriceSnapshot,
        spread_pct: Decimal,
        signal_delay_ms_a: float,
        signal_delay_ms_b: float,
        reason: str,
    ) -> Optional[TradingSignal]:
        """基于统计套利回归条件构建最小平仓信号。"""
        direction_stats = self._get_stat_arb_direction_stats(reference_direction)
        if direction_stats is None:
            return None

        signal_trigger_time = time.time()
        quantity = min(abs(self.position_manager.get_current_position_qty()), self.position_manager.position_step)
        if quantity <= 0:
            return None

        metadata = {
            "signal_mode": self.signal_mode,
            "stat_arb": direction_stats.to_dict(),
            "exit_reason": reason,
            "exit_threshold": self.stat_arb_exit_threshold,
            "exit_score_source": self.stat_arb_exit_score_source,
            "exit_spread_floor_pct": str(self.stat_arb_exit_spread_floor_pct),
            "exit_take_profit_pct": (
                str(self.stat_arb_exit_take_profit_pct)
                if self.stat_arb_exit_take_profit_pct is not None
                else None
            ),
            "exit_executable_spread_pct": str(
                self._get_stat_arb_exit_executable_spread_pct(execute_signal_type, prices)
            ),
            "reference_direction": reference_direction.value,
        }

        current_score = self._get_stat_arb_exit_score(direction_stats)
        logger.info(
            f"🔁 [{self.symbol}] 统计套利回归平仓触发: "
            f"执行方向={execute_signal_type.value}, 参考方向={reference_direction.value}, "
            f"current_score={current_score:.3f}, exit_threshold={self.stat_arb_exit_threshold:.3f}, "
            f"reason={reason}"
        )

        return self._build_trading_signal(
            signal_type=execute_signal_type,
            prices=prices,
            spread_pct=spread_pct,
            signal_trigger_time=signal_trigger_time,
            signal_delay_ms_a=signal_delay_ms_a,
            signal_delay_ms_b=signal_delay_ms_b,
            quantity=quantity,
            metadata=metadata,
        )

    def _log_stat_arb_quality_if_needed(self) -> None:
        """节流输出统计套利回归质量日志。"""
        if self.signal_mode != 'stat_arb' or not self._stat_arb_position_context:
            return

        now = time.time()
        if now - self._last_stat_arb_quality_log_time < self.stat_arb_quality_log_interval_seconds:
            return

        active_direction = self._get_active_stat_arb_direction()
        if active_direction is None:
            return

        direction_stats = self._get_stat_arb_direction_stats(active_direction)
        current_score = self._get_stat_arb_exit_score(direction_stats)
        if direction_stats is None or current_score is None:
            return

        entry_score = float(self._stat_arb_position_context.get("entry_score", 0.0))
        score_revert = entry_score - current_score
        entry_adjusted_pct = float(self._stat_arb_position_context.get("entry_adjusted_pct", 0.0))
        current_adjusted_pct = float(direction_stats.current_adjusted_pct)
        adjusted_delta = current_adjusted_pct - entry_adjusted_pct
        self._last_stat_arb_quality_log_time = now

        logger.info(
            f"📈 [{self.symbol}] 统计套利回归质量: "
            f"方向={active_direction.value}, "
            f"entry_score={entry_score:.3f}, current_score={current_score:.3f}, "
            f"score回落={score_revert:.3f}, exit_threshold={self.stat_arb_exit_threshold:.3f}, "
            f"entry_adj={entry_adjusted_pct:.4f}%, current_adj={current_adjusted_pct:.4f}%, "
            f"净价差变化={adjusted_delta:+.4f}%"
        )

    def _apply_risk_position_cap(self, risk_decision) -> Decimal:
        """根据风控决策刷新当前有效最大仓位。"""
        base_max_position = self.position_manager.max_position
        level_name = getattr(risk_decision, "level", None)
        level_text = level_name.name if level_name is not None else "--"
        weak_exchange = getattr(risk_decision, "weak_exchange", "") if risk_decision else ""
        if not self.risk_control_enabled:
            applied_ratio = Decimal("1")
            effective_max_position = base_max_position
        else:
            applied_ratio = max(
                Decimal("0"),
                min(Decimal("1"), Decimal(str(getattr(risk_decision, "max_position_ratio", Decimal("1"))))),
            )
            effective_max_position = self._align_position_limit_to_step(
                base_max_position * applied_ratio
            )

        self.position_manager.set_effective_max_position(effective_max_position)

        current_effective = self.position_manager.get_effective_max_position()
        if self._last_effective_max_position != current_effective:
            weak_exchange_text = f", 弱腿交易所={weak_exchange}" if weak_exchange else ""
            ratio_text = applied_ratio if self.risk_control_enabled else Decimal("1")
            previous_effective = (
                self._last_effective_max_position
                if self._last_effective_max_position is not None
                else base_max_position
            )
            logger.warning(
                f"🛡️ 更新有效最大仓位: 等级={level_text}, "
                f"基础最大仓位={base_max_position}, 当前仓位上限比例={ratio_text}, "
                f"有效最大仓位={previous_effective}->{current_effective}"
                f"{weak_exchange_text}"
            )
            self._last_effective_max_position = current_effective

        return current_effective

    async def _try_apply_dynamic_position_cap(
        self,
        prices: PriceSnapshot,
        risk_decision,
        effective_max_position: Decimal,
    ) -> tuple[bool, Optional[Decimal]]:
        """若当前仓位已超过动态上限，立即减仓到上限以下。"""
        current_qty = self.position_manager.get_current_position_qty()
        current_abs = abs(current_qty)
        if current_abs <= Decimal("0"):
            return False, None

        base_max_position = abs(self.position_manager.max_position)
        dynamic_target_abs = max(Decimal("0"), effective_max_position)
        target_abs = dynamic_target_abs
        target_abs = self._align_position_limit_to_step(target_abs)

        if not getattr(risk_decision, "need_reduce", False):
            return False, target_abs

        if current_abs <= target_abs:
            return False, target_abs

        weak_exchange = getattr(risk_decision, "weak_exchange", "")
        level_name = getattr(risk_decision, "level", None)
        level_text = level_name.name if level_name is not None else "--"
        reason_text = f"动态仓位上限({level_text})"
        if weak_exchange:
            reason_text = f"{reason_text}({weak_exchange})"
        reduced = await self._try_apply_risk_reduction(
            prices,
            target_ratio=Decimal("1"),
            target_abs_override=target_abs,
            bypass_cooldown=True,
            reason_text=reason_text,
        )
        return reduced, target_abs

    async def _try_apply_risk_reduction(
        self,
        prices: PriceSnapshot,
        target_ratio: Decimal,
        target_abs_override: Optional[Decimal] = None,
        bypass_cooldown: bool = False,
        reason_text: str = "风控触发",
    ) -> bool:
        """
        根据风控目标仓位比例执行减仓。

        返回:
            True: 本次执行了减仓（或监控模式下完成模拟减仓）
            False: 本次未执行减仓
        """
        now = time.time()
        if (
            not bypass_cooldown
            and now - self._last_risk_reduce_time < self._risk_reduce_cooldown_seconds
        ):
            logger.info(
                f"🛡️ 跳过风控减仓: 仍在冷却中 "
                f"({now - self._last_risk_reduce_time:.2f}s < {self._risk_reduce_cooldown_seconds:.2f}s)"
            )
            return False

        if not self.position_manager.accumulate_mode:
            logger.warning("⚠️ 风控减仓当前仅支持累计模式，传统模式仅执行开仓阻断")
            return False

        # 风控减仓只在有仓位时生效
        current_qty = self.position_manager.get_current_position_qty()
        current_abs = abs(current_qty)
        if current_abs <= Decimal("0"):
            logger.info("🛡️ 跳过风控减仓: 当前无持仓")
            return False

        # 目标绝对仓位按 max_position 比例计算，避免重复按“当前仓位”连环折半
        if target_abs_override is not None:
            target_abs = max(Decimal("0"), Decimal(str(target_abs_override)))
        else:
            max_position_abs = abs(self.position_manager.max_position)
            if max_position_abs <= Decimal("0"):
                target_abs = Decimal("0")
            else:
                ratio = max(Decimal("0"), min(Decimal("1"), Decimal(str(target_ratio))))
                target_abs = max_position_abs * ratio
        target_abs = self._align_position_limit_to_step(target_abs)

        if current_abs <= target_abs:
            logger.info(
                f"🛡️ 跳过风控减仓: 当前仓位={current_qty:+.4f}, "
                f"目标绝对仓位<={target_abs:.4f}"
            )
            return False

        reduce_qty = current_abs - target_abs
        if self.quantity_precision > 0:
            reduce_qty = (reduce_qty / self.quantity_precision).to_integral_value(rounding=ROUND_DOWN) * self.quantity_precision

        # 精度截断后无可执行数量
        if reduce_qty <= Decimal("0"):
            logger.info(
                f"🛡️ 跳过风控减仓: 目标减仓量经精度截断后为 {reduce_qty:.4f}"
            )
            return False

        # 保护：不可超过当前仓位
        if reduce_qty > current_abs:
            reduce_qty = current_abs

        logger.warning(
            f"🛡️ {reason_text}: 当前仓位={current_qty:+.4f}, 当前绝对仓位={current_abs:.4f}, "
            f"目标绝对仓位<={target_abs:.4f}, "
            f"本次减仓={reduce_qty:.4f}"
        )

        # 监控模式不下单，直接模拟仓位变化
        if self.monitor_only:
            if current_qty < 0:
                self.position_manager.current_position_qty += reduce_qty
            else:
                self.position_manager.current_position_qty -= reduce_qty
            self._last_risk_reduce_time = now
            logger.warning(
                f"🛡️ 风控减仓(监控模式): 调整后仓位={self.position_manager.get_current_position_qty():+.4f}"
            )
            return True

        # 避免与正常开平仓并发
        if self._executing_lock.locked():
            logger.info("🛡️ 跳过风控减仓: 当前已有执行中的订单流程")
            return False

        async with self._executing_lock:
            self._is_executing = True
            try:
                signal_trigger_time = time.time()
                success = False

                if current_qty < 0:
                    # 负仓位（A空/B多）减仓：A 买入，B 卖出
                    success, position = await self.executor.execute_close(
                        position=self.position_manager.get_position() or self._create_dummy_position(),
                        exchange_a_price=prices.exchange_a_ask,
                        exchange_b_price=prices.exchange_b_bid,
                        exchange_a_quote_id=prices.exchange_a_quote_id,
                        exchange_b_quote_id=prices.exchange_b_quote_id,
                        signal_trigger_time=signal_trigger_time,
                        close_quantity=reduce_qty,
                        execution_context='risk_reduce',
                    )
                    if success and position:
                        self.position_manager.reduce_position(position, 'long', 0, 0)

                else:
                    # 正仓位（A多/B空）减仓：A 卖出，B 买入
                    success, position = await self.executor.execute_open(
                        exchange_a_price=prices.exchange_a_bid,
                        exchange_b_price=prices.exchange_b_ask,
                        spread_pct=prices.calculate_spread_pct(),
                        exchange_a_quote_id=prices.exchange_a_quote_id,
                        exchange_b_quote_id=prices.exchange_b_quote_id,
                        signal_trigger_time=signal_trigger_time,
                        actual_quantity=reduce_qty,
                        execution_context='risk_reduce',
                    )
                    if success and position:
                        # 这里用 add_position('short') 使净仓位向 0 收敛
                        self.position_manager.add_position(position, 'short', 0, 0)

                if not success:
                    logger.error("❌ 风控减仓下单失败，保持当前仓位")
                    return False

                self._last_risk_reduce_time = time.time()
                logger.warning(
                    f"🛡️ 风控减仓执行完成: 新仓位={self.position_manager.get_current_position_qty():+.4f}"
                )

                await asyncio.sleep(1.0)
                await self.executor.check_position_balance()
                return True
            finally:
                self._is_executing = False

    def _align_position_limit_to_step(self, value: Decimal) -> Decimal:
        """将风控计算出的目标仓位按单次成交量向下取整。"""
        aligned_value = max(Decimal("0"), Decimal(str(value)))
        step = abs(Decimal(str(self.position_manager.position_step)))
        if step <= Decimal("0"):
            return aligned_value
        step_count = (aligned_value / step).to_integral_value(rounding=ROUND_DOWN)
        return step_count * step

    def _remember_last_non_zero_strategy_qty(self, current_qty: Decimal) -> None:
        """记录最近一次非零策略仓位，用于空仓时补充说明刚刚归零的方向。"""
        if current_qty != 0:
            self._last_non_zero_strategy_qty = Decimal(str(current_qty))

    def _get_flatten_direction_text(self) -> str:
        """返回最近一次从哪一侧仓位归零。"""
        if self._last_non_zero_strategy_qty > 0:
            return "多->0"
        if self._last_non_zero_strategy_qty < 0:
            return "空->0"
        return "--"

    def _build_equity_loss_summary(
        self,
        equity_a: Decimal,
        equity_b: Decimal,
        volume_delta: Decimal,
        volume_label: str,
    ) -> str:
        """构建统一的权益损耗摘要日志。"""
        total_equity_loss = (self.start_equity_a + self.start_equity_b) - (equity_a + equity_b)
        equity_diff = equity_a - equity_b
        flatten_direction = self._get_flatten_direction_text()
        if volume_delta > 0:
            estimated_loss = total_equity_loss / (volume_delta * 2) * 100
        else:
            estimated_loss = Decimal("0")
        return (
            f"💰 当前权益损耗: ${total_equity_loss:.2f}, "
            f"A-B权益差值: {equity_diff:.2f}, "
            f"本次策略仓位归零方向: {flatten_direction}, "
            f"{volume_label}: {volume_delta:.2f}, "
            f"预估损耗(权益减量/交易增量 * 100%): {estimated_loss:.4f}%"
        )

    def _estimate_cost_bps(self, signal_delay_ms_a: float, signal_delay_ms_b: float) -> dict:
        """估算执行成本（bps）"""
        latency_est_ms = max(float(signal_delay_ms_a), float(signal_delay_ms_b))
        latency_over_ms = max(0.0, latency_est_ms - self.edge_latency_free_ms)
        latency_risk_bps = (latency_over_ms / 100.0) * self.edge_latency_bps_per_100ms
        cost_est_bps = self.edge_base_cost_bps + self.edge_fee_bps + latency_risk_bps
        required_spread_bps = cost_est_bps + self.min_edge_bps
        return {
            'latency_est_ms': latency_est_ms,
            'latency_over_ms': latency_over_ms,
            'latency_risk_bps': latency_risk_bps,
            'cost_est_bps': cost_est_bps,
            'required_spread_bps': required_spread_bps,
        }

    def _passes_edge_filter(self, spread_pct: Decimal, signal_delay_ms_a: float, signal_delay_ms_b: float) -> tuple[bool, dict]:
        """边际二次过滤：abs(spread) >= cost_est + min_edge"""
        spread_abs_bps = float(abs(spread_pct)) * 100.0
        estimate = self._estimate_cost_bps(signal_delay_ms_a, signal_delay_ms_b)
        edge_est_bps = spread_abs_bps - estimate['cost_est_bps']
        passed = spread_abs_bps >= estimate['required_spread_bps']
        estimate.update({
            'spread_abs_bps': spread_abs_bps,
            'edge_est_bps': edge_est_bps,
        })
        return passed, estimate

    def _should_apply_edge_filter(self, signal_type: str) -> tuple[bool, dict]:
        """
        仅当信号会增加风险（|仓位|变大）时才应用边际过滤。
        signal_type: 'open' 或 'close'
        """
        if signal_type not in {'open', 'close'}:
            return False, {'reason': 'invalid_signal_type'}

        if not self.position_manager.accumulate_mode:
            # 传统模式：开仓增风险，平仓减风险
            return signal_type == 'open', {
                'current_qty': Decimal('0'),
                'projected_qty': self.quantity if signal_type == 'open' else Decimal('0'),
                'risk_increasing': signal_type == 'open',
                'reason': 'traditional_mode'
            }

        current_qty = self.position_manager.get_current_position_qty()
        step = self.position_manager.position_step
        delta = -step if signal_type == 'open' else step
        projected_qty = current_qty + delta
        risk_increasing = abs(projected_qty) > abs(current_qty)

        return risk_increasing, {
            'current_qty': current_qty,
            'projected_qty': projected_qty,
            'risk_increasing': risk_increasing,
            'reason': 'accumulate_mode'
        }

    def _should_block_due_to_threshold_quality(self, signal_type: str) -> tuple[bool, dict]:
        """
        当动态阈值管理器已经判定“标准差系数打满且阈值和仍不足”时，
        统一阻断当前信号，不再继续下单。
        """
        if not self.threshold_manager or not self.threshold_manager.is_trade_blocked():
            return False, {
                'reason': 'threshold_ok',
                'current_qty': self.position_manager.get_current_position_qty(),
                'projected_qty': self.position_manager.get_current_position_qty(),
                'risk_increasing': False,
                'block_reason': '',
            }

        risk_increasing, ctx = self._should_apply_edge_filter(signal_type)
        ctx = dict(ctx)
        ctx['block_reason'] = self.threshold_manager.get_trade_block_reason()
        return True, ctx

    def _next_signal_id(self, signal_type: SignalType, signal_trigger_time: float) -> str:
        """生成单进程内唯一信号 ID。"""
        self._signal_sequence += 1
        return f"{self.symbol}:{signal_type.value}:{int(signal_trigger_time * 1000)}:{self._signal_sequence}"

    def _signal_mailbox_key(self, signal_type: SignalType) -> str:
        """返回某类信号在 mailbox 中的槽位 key。"""
        return f"{self.symbol}:{signal_type.value}"

    def _build_trading_signal(
        self,
        signal_type: SignalType,
        prices: PriceSnapshot,
        spread_pct: Decimal,
        signal_trigger_time: float,
        signal_delay_ms_a: float,
        signal_delay_ms_b: float,
        quantity: Decimal,
        metadata: Optional[dict] = None,
    ) -> TradingSignal:
        """根据当前价格快照构建交易信号。"""
        if signal_type == SignalType.OPEN:
            exchange_a_price = prices.exchange_a_bid
            exchange_b_price = prices.exchange_b_ask
            exchange_a_depth = getattr(prices, 'exchange_a_bid_size', None)
            exchange_b_depth = getattr(prices, 'exchange_b_ask_size', None)
        else:
            exchange_a_price = prices.exchange_a_ask
            exchange_b_price = prices.exchange_b_bid
            exchange_a_depth = getattr(prices, 'exchange_a_ask_size', None)
            exchange_b_depth = getattr(prices, 'exchange_b_bid_size', None)

        return TradingSignal(
            signal_id=self._next_signal_id(signal_type, signal_trigger_time),
            signal_type=signal_type,
            symbol=self.symbol,
            spread_pct=spread_pct,
            exchange_a_price=exchange_a_price,
            exchange_b_price=exchange_b_price,
            quantity=quantity,
            created_at=signal_trigger_time,
            exchange_a_quote_id=prices.exchange_a_quote_id,
            exchange_b_quote_id=prices.exchange_b_quote_id,
            exchange_a_depth=exchange_a_depth,
            exchange_b_depth=exchange_b_depth,
            signal_delay_ms_a=signal_delay_ms_a,
            signal_delay_ms_b=signal_delay_ms_b,
            prices=prices,
            reason='threshold_met',
            metadata=metadata or {},
        )

    async def _publish_signal(self, signal: TradingSignal) -> None:
        """发布信号；执行链路由外部注入。"""
        if self.signal_submitter is None:
            logger.warning(f"⚠️ [{self.symbol}] 未配置 signal_submitter，跳过信号: {signal.signal_id}")
            return

        await self.signal_submitter(signal)
        logger.info(
            f"📨 [{self.symbol}] 发布信号: id={signal.signal_id}, "
            f"type={signal.signal_type.value}, key={signal.mailbox_key}"
        )

    async def _clear_signal(self, signal_type: SignalType, reason: str = "") -> None:
        """显式清空某个方向的最新信号。"""
        if self.signal_clearer is None:
            return

        mailbox_key = self._signal_mailbox_key(signal_type)
        removed = await self.signal_clearer(mailbox_key)
        if removed and reason:
            logger.debug(
                f"🧹 [{self.symbol}] 清空信号槽位: key={mailbox_key}, reason={reason}"
            )

    async def _sync_signal_state(
        self,
        signal_type: SignalType,
        signal: Optional[TradingSignal],
        clear_reason: str,
    ) -> None:
        """同步某个方向的最新信号状态。"""
        if signal is None:
            await self._clear_signal(signal_type, clear_reason)
            return
        await self._publish_signal(signal)

    async def _clear_all_signal_states(self, reason: str) -> None:
        """一次性清空当前 symbol 的交易信号槽位。"""
        await self._clear_signal(SignalType.OPEN, reason)
        await self._clear_signal(SignalType.CLOSE, reason)

    def set_signal_submitter(self, submitter, clearer=None) -> None:
        """注入信号刷新/清空函数。"""
        self.signal_submitter = submitter
        self.signal_clearer = clearer

    async def _check_open_signal(
        self,
        prices: PriceSnapshot,
        spread_pct: Decimal,
        signal_delay_ms_a: float,
        signal_delay_ms_b: float,
    ) -> Optional[TradingSignal]:
        """
        检查开仓信号
        
        ✅ 监控模式下，会创建虚拟持仓（不实际下单）
        """
        # ✅ 累计模式：检查是否可以开空
        if not self.position_manager.accumulate_mode:
            # ✅ 传统模式：检查是否有持仓
            if self.position_manager.has_position():
                return
        
        # ✅ 检查冷却期
        current_time = time.time()
        # cooldown_seconds = random.uniform(self.cooldown_range[0], self.cooldown_range[1])

        cooldown_remaining = self.cooldown_seconds - (current_time - self._last_execution_time)
        if cooldown_remaining > 0:
            return
        
        # ✅ 如果正在执行开仓，跳过
        if self._executing_lock.locked():
            return
        
        base_direction = prices.calculate_direction_b('long')
        direction_ok = base_direction if not self.direction_reverse else not base_direction
        # 判断是否满足开仓阈值
        compare_spread_pct = spread_pct
        threshold_pct = Decimal(str(self.open_threshold_pct))
        threshold_label = "阈值"
        spread_label = "价差"
        extra_spread_info = ""
        median_edge_stats = None
        stat_arb_stats = None
        if self.signal_mode == 'median_edge' and self.median_edge_manager:
            if not self._is_median_edge_selected(SignalType.OPEN):
                return None
            median_edge_stats = self._get_median_edge_direction_stats(SignalType.OPEN)
            if median_edge_stats is None or median_edge_stats.threshold_pct is None:
                return None
            compare_spread_pct = Decimal(str(median_edge_stats.current_adjusted_pct))
            threshold_pct = Decimal(str(median_edge_stats.threshold_pct))
            threshold_label = "双中位数+边际"
            spread_label = "调整后价差"
            extra_spread_info = self._format_median_edge_extra_info(SignalType.OPEN)
        elif self.signal_mode == 'stat_arb' and self.stat_arb_manager:
            if not self._is_stat_arb_selected(SignalType.OPEN):
                return None
            stat_arb_stats = self._get_stat_arb_direction_stats(SignalType.OPEN)
            entry_score = self._get_stat_arb_entry_score(stat_arb_stats)
            if stat_arb_stats is None or entry_score is None:
                return None
            compare_spread_pct = Decimal(str(entry_score))
            threshold_pct = Decimal(str(self.stat_arb_entry_threshold))
            threshold_label = "score阈值"
            spread_label = "统计分数"
            extra_spread_info = self._format_stat_arb_extra_info(SignalType.OPEN)
        elif self.signal_mode == 'quantile' and self.quantile_manager:
            open_q, _ = self._get_quantile_thresholds()
            if open_q is None:
                return
            avg_local_spread_pct = self._calculate_avg_local_spread_pct(prices)
            compare_spread_pct = spread_pct - avg_local_spread_pct
            threshold_pct = max(open_q, Decimal('0'))
            threshold_label = f"P{int(self.signal_quantile * 100)}"
            spread_label = "修正价差"
            extra_spread_info = (
                f"   平均点差: {avg_local_spread_pct:.4f}%\n"
                f"   原始价差: {spread_pct:.4f}%\n"
            )

        if self.signal_mode == 'quantile' and self.quantile_manager:
            if compare_spread_pct <= 0:
                return
            if compare_spread_pct < self.signal_min_abs_spread_pct:
                return
            if compare_spread_pct < threshold_pct + self.signal_min_edge_pct:
                return

        if compare_spread_pct >= threshold_pct:
            if self.signal_mode == 'stat_arb' and self.stat_arb_entry_raw_floor_pct is not None:
                entry_raw_spread_pct = self._get_stat_arb_entry_raw_spread_pct(SignalType.OPEN, prices)
                if entry_raw_spread_pct < self.stat_arb_entry_raw_floor_pct:
                    logger.info(
                        f"⏭️ [{self.symbol}] 开仓信号因原始价差不足被拦截:\n"
                        f"{extra_spread_info}"
                        f"   entry_raw: {entry_raw_spread_pct:.4f}% < floor: {self.stat_arb_entry_raw_floor_pct:.4f}%\n"
                        f"   {spread_label}: {compare_spread_pct:.4f}% ({threshold_label}: {threshold_pct:.4f}%)"
                    )
                    return
            self.signal_stats['open']['total'] += 1
            # 记录信号触发时间
            signal_trigger_time = time.time()
            if self.signal_mode == 'quantile' and self.quantile_manager:
                self._log_quantile_event(
                    signal_type='open',
                    stage='condition_met',
                    spread_pct=spread_pct,
                    reverse_spread_pct=prices.calculate_reverse_spread_pct(),
                    avg_local_spread_pct=avg_local_spread_pct if self.signal_mode == 'quantile' else Decimal('0'),
                    adjusted_spread_pct=compare_spread_pct,
                    threshold_pct=threshold_pct,
                    note='',
                )

            blocked_by_threshold, threshold_ctx = self._should_block_due_to_threshold_quality('open')
            if blocked_by_threshold:
                self.signal_stats['open']['threshold_filtered'] += 1
                logger.info(
                    f"⏭️ [{self.symbol}] 开仓信号因动态阈值和不足被拦截:\n"
                    f"   当前仓位: {threshold_ctx['current_qty']:+.4f} -> "
                    f"预测仓位: {threshold_ctx['projected_qty']:+.4f}\n"
                    f"   当前阈值和: {getattr(self.threshold_manager, 'current_threshold_sum', None) or 0:.4f}%\n"
                    f"   拦截原因: {threshold_ctx['block_reason']}\n"
                    f"   价差: {spread_pct:.4f}% (阈值: {self.open_threshold_pct}%)"
                )
                return

            # ========== ✅ 新增：检查深度 ==========
            # Exchange A: 卖出（使用买一深度）
            depth_a = prices.exchange_a_bid_size
            # Exchange B: 买入（使用卖一深度）
            depth_b = prices.exchange_b_ask_size
            
            # ✅ 取最小深度
            min_depth = min(depth_a, depth_b)

            # ✅ 检查最小深度阈值
            if min_depth < self.min_depth_quantity:
                self.signal_stats['open']['depth_insufficient'] += 1

                logger.warning(
                    f"⚠️ [{self.symbol}] 开仓深度不足，跳过:\n"
                    f"   {self.exchange_a.exchange_name} 买一深度: {depth_a}\n"
                    f"   {self.exchange_b.exchange_name} 卖一深度: {depth_b}\n"
                    f"   最小深度: {min_depth} < 阈值: {self.min_depth_quantity}\n"
                    f"{extra_spread_info}"
                    f"   {spread_label}: {compare_spread_pct:.4f}% ({threshold_label}: {threshold_pct:.4f}%)"
                )
                return

            edge_estimate = None
            edge_apply, edge_ctx = self._should_apply_edge_filter('open')
            if self.edge_filter_enabled and self.signal_mode not in {'stat_arb', 'median_edge'}:
                if edge_apply:
                    passed_edge, edge_estimate = self._passes_edge_filter(
                        spread_pct=spread_pct,
                        signal_delay_ms_a=signal_delay_ms_a,
                        signal_delay_ms_b=signal_delay_ms_b
                    )
                    if not passed_edge:
                        self.signal_stats['open']['edge_filtered'] += 1
                        logger.info(
                            f"⏭️ [{self.symbol}] 开仓边际不足，跳过:\n"
                            f"   当前仓位: {edge_ctx['current_qty']:+.4f} -> 预测仓位: {edge_ctx['projected_qty']:+.4f}\n"
                            f"   绝对价差: {edge_estimate['spread_abs_bps']:.2f} bps\n"
                            f"   成本估计: {edge_estimate['cost_est_bps']:.2f} bps "
                            f"(base={self.edge_base_cost_bps:.2f}, fee={self.edge_fee_bps:.2f}, latency={edge_estimate['latency_risk_bps']:.2f})\n"
                            f"   安全边际: {self.min_edge_bps:.2f} bps\n"
                            f"   最低要求: {edge_estimate['required_spread_bps']:.2f} bps\n"
                            f"   估算边际: {edge_estimate['edge_est_bps']:.2f} bps"
                        )
                        return
            
            self.open_signal_count += 1

            edge_text = ""
            if self.edge_filter_enabled:
                if edge_apply and edge_estimate is not None:
                    edge_text = (
                        f"   边际过滤: {edge_estimate['spread_abs_bps']:.2f} bps >= "
                        f"{edge_estimate['required_spread_bps']:.2f} bps "
                        f"(成本: {edge_estimate['cost_est_bps']:.2f} bps, 估算边际: {edge_estimate['edge_est_bps']:.2f} bps)\n"
                    )
                elif not edge_apply:
                    edge_text = (
                        f"   边际过滤: 减风险路径放行 "
                        f"(仓位 {edge_ctx['current_qty']:+.4f} -> {edge_ctx['projected_qty']:+.4f})\n"
                    )

            logger.info(
                f"🔔 [{self.symbol}] 检测到开仓信号 #{self.open_signal_count}:\n"
                f"   延迟_a: {signal_delay_ms_a:.2f} ms (阈值: {self.max_signal_delay_ms_a} ms)\n"
                f"   延迟_b: {signal_delay_ms_b:.2f} ms (阈值: {self.max_signal_delay_ms_b} ms)\n"
                f"   {self.exchange_a.exchange_name}_bid: ${prices.exchange_a_bid}\n"
                f"   {self.exchange_a.exchange_name}_bid_size: {prices.exchange_a_bid_size}\n"
                f"   {self.exchange_b.exchange_name}_ask: ${prices.exchange_b_ask}\n"
                f"   {self.exchange_b.exchange_name}_ask_size: {prices.exchange_b_ask_size}\n"
                f"   {edge_text}"
                f"{extra_spread_info}"
                f"   {spread_label}: {compare_spread_pct:.4f}% ({threshold_label}: {threshold_pct:.4f}%)"
            )

            signal = self._build_trading_signal(
                signal_type=SignalType.OPEN,
                prices=prices,
                spread_pct=spread_pct,
                signal_trigger_time=signal_trigger_time,
                signal_delay_ms_a=signal_delay_ms_a,
                signal_delay_ms_b=signal_delay_ms_b,
                quantity=self.position_manager.position_step,
                metadata={
                    'compare_spread_pct': str(compare_spread_pct),
                    'threshold_pct': str(threshold_pct),
                    'threshold_label': threshold_label,
                    'spread_label': spread_label,
                    'signal_mode': self.signal_mode,
                    'median_edge': median_edge_stats.to_dict() if median_edge_stats else {},
                    'stat_arb': stat_arb_stats.to_dict() if stat_arb_stats else {},
                },
            )
            return signal

        return None

    async def _check_close_signal(
        self,
        prices: PriceSnapshot,
        spread_pct: Decimal,
        signal_delay_ms_a: float,
        signal_delay_ms_b: float,
    ) -> Optional[TradingSignal]:
        """
        检查平仓信号
        
        ✅ 监控模式下，会清除虚拟持仓（不实际下单）
        """
        # ✅ 累计模式：检查是否可以平仓（或反向开仓）
        if not self.position_manager.accumulate_mode:
            # ✅ 传统模式：检查是否有持仓
            if not self.position_manager.has_position():
                return
        
        current_position = self.position_manager.get_position()
        if not self.position_manager.accumulate_mode and current_position is None:
            return

        # ✅ 检查冷却期
        current_time = time.time()
        # cooldown_seconds = random.uniform(self.cooldown_range[0], self.cooldown_range[1])

        cooldown_remaining = self.cooldown_seconds - (current_time - self._last_execution_time)
        if cooldown_remaining > 0:
            return
        
        # ✅ 如果正在执行平仓，跳过
        if self._executing_lock.locked():
            return
        
        base_direction = prices.calculate_direction_b('short')
        direction_ok = base_direction if not self.direction_reverse else not base_direction
        # 判断是否满足平仓阈值
        compare_spread_pct = spread_pct
        threshold_pct = Decimal(str(self.close_threshold_pct))
        threshold_label = "阈值"
        spread_label = "价差"
        extra_spread_info = ""
        median_edge_stats = None
        stat_arb_stats = None
        if self.signal_mode == 'median_edge' and self.median_edge_manager:
            if not self._is_median_edge_selected(SignalType.CLOSE):
                return None
            median_edge_stats = self._get_median_edge_direction_stats(SignalType.CLOSE)
            if median_edge_stats is None or median_edge_stats.threshold_pct is None:
                return None
            compare_spread_pct = Decimal(str(median_edge_stats.current_adjusted_pct))
            threshold_pct = Decimal(str(median_edge_stats.threshold_pct))
            threshold_label = "双中位数+边际"
            spread_label = "调整后价差"
            extra_spread_info = self._format_median_edge_extra_info(SignalType.CLOSE)
        elif self.signal_mode == 'stat_arb' and self.stat_arb_manager:
            if not self._is_stat_arb_selected(SignalType.CLOSE):
                return None
            stat_arb_stats = self._get_stat_arb_direction_stats(SignalType.CLOSE)
            entry_score = self._get_stat_arb_entry_score(stat_arb_stats)
            if stat_arb_stats is None or entry_score is None:
                return None
            compare_spread_pct = Decimal(str(entry_score))
            threshold_pct = Decimal(str(self.stat_arb_entry_threshold))
            threshold_label = "score阈值"
            spread_label = "统计分数"
            extra_spread_info = self._format_stat_arb_extra_info(SignalType.CLOSE)
        elif self.signal_mode == 'quantile' and self.quantile_manager:
            _, close_q = self._get_quantile_thresholds()
            if close_q is None:
                return
            avg_local_spread_pct = self._calculate_avg_local_spread_pct(prices)
            compare_spread_pct = spread_pct - avg_local_spread_pct
            threshold_pct = max(close_q, Decimal('0'))
            threshold_label = f"P{int(self.signal_quantile * 100)}"
            spread_label = "修正价差"
            extra_spread_info = (
                f"   平均点差: {avg_local_spread_pct:.4f}%\n"
                f"   原始价差: {spread_pct:.4f}%\n"
            )

        if self.signal_mode == 'quantile' and self.quantile_manager:
            if compare_spread_pct <= 0:
                return
            if compare_spread_pct < self.signal_min_abs_spread_pct:
                return
            if compare_spread_pct < threshold_pct + self.signal_min_edge_pct:
                return

        if compare_spread_pct >= threshold_pct:
            if self.signal_mode == 'stat_arb' and self.stat_arb_entry_raw_floor_pct is not None:
                entry_raw_spread_pct = self._get_stat_arb_entry_raw_spread_pct(SignalType.CLOSE, prices)
                if entry_raw_spread_pct < self.stat_arb_entry_raw_floor_pct:
                    logger.info(
                        f"⏭️ [{self.symbol}] 反向开仓信号因原始价差不足被拦截:\n"
                        f"{extra_spread_info}"
                        f"   entry_raw: {entry_raw_spread_pct:.4f}% < floor: {self.stat_arb_entry_raw_floor_pct:.4f}%\n"
                        f"   {spread_label}: {compare_spread_pct:.4f}% ({threshold_label}: {threshold_pct:.4f}%)"
                    )
                    return
            self.signal_stats['close']['total'] += 1

            # 记录信号触发时间
            signal_trigger_time = time.time()
            if self.signal_mode == 'quantile' and self.quantile_manager:
                self._log_quantile_event(
                    signal_type='close',
                    stage='condition_met',
                    spread_pct=prices.calculate_spread_pct(),
                    reverse_spread_pct=spread_pct,
                    avg_local_spread_pct=avg_local_spread_pct if self.signal_mode == 'quantile' else Decimal('0'),
                    adjusted_spread_pct=compare_spread_pct,
                    threshold_pct=threshold_pct,
                    note='',
                )

            blocked_by_threshold, threshold_ctx = self._should_block_due_to_threshold_quality('close')
            if blocked_by_threshold:
                self.signal_stats['close']['threshold_filtered'] += 1
                logger.info(
                    f"⏭️ [{self.symbol}] 反向开仓信号因动态阈值和不足被拦截:\n"
                    f"   当前仓位: {threshold_ctx['current_qty']:+.4f} -> "
                    f"预测仓位: {threshold_ctx['projected_qty']:+.4f}\n"
                    f"   当前阈值和: {getattr(self.threshold_manager, 'current_threshold_sum', None) or 0:.4f}%\n"
                    f"   拦截原因: {threshold_ctx['block_reason']}\n"
                    f"   价差: {spread_pct:.4f}% (阈值: {self.close_threshold_pct}%)"
                )
                return

            # ========== ✅ 新增：检查平仓深度 ==========
            # Exchange A: 买入平空（使用卖一深度）
            depth_a = prices.exchange_a_ask_size
            # Exchange B: 卖出平多（使用买一深度）
            depth_b = prices.exchange_b_bid_size
            
            # ✅ 取最小深度
            min_depth = min(depth_a, depth_b)

            # ✅ 检查最小深度阈值
            if min_depth < self.min_depth_quantity:
                self.signal_stats['close']['depth_insufficient'] += 1

                logger.warning(
                    f"⚠️ [{self.symbol}] 反向开仓深度不足，跳过:\n"
                    f"   {self.exchange_a.exchange_name} 卖一深度: {depth_a}\n"
                    f"   {self.exchange_b.exchange_name} 买一深度: {depth_b}\n"
                    f"   最小深度: {min_depth} < 阈值: {self.min_depth_quantity}\n"
                    f"{extra_spread_info}"
                    f"   {spread_label}: {compare_spread_pct:.4f}% ({threshold_label}: {threshold_pct:.4f}%)"
                )
                return

            edge_estimate = None
            edge_apply, edge_ctx = self._should_apply_edge_filter('close')
            if self.edge_filter_enabled and self.signal_mode not in {'stat_arb', 'median_edge'}:
                if edge_apply:
                    passed_edge, edge_estimate = self._passes_edge_filter(
                        spread_pct=spread_pct,
                        signal_delay_ms_a=signal_delay_ms_a,
                        signal_delay_ms_b=signal_delay_ms_b
                    )
                    if not passed_edge:
                        self.signal_stats['close']['edge_filtered'] += 1
                        logger.info(
                            f"⏭️ [{self.symbol}] 反向开仓边际不足，跳过:\n"
                            f"   当前仓位: {edge_ctx['current_qty']:+.4f} -> 预测仓位: {edge_ctx['projected_qty']:+.4f}\n"
                            f"   绝对价差: {edge_estimate['spread_abs_bps']:.2f} bps\n"
                            f"   成本估计: {edge_estimate['cost_est_bps']:.2f} bps "
                            f"(base={self.edge_base_cost_bps:.2f}, fee={self.edge_fee_bps:.2f}, latency={edge_estimate['latency_risk_bps']:.2f})\n"
                            f"   安全边际: {self.min_edge_bps:.2f} bps\n"
                            f"   最低要求: {edge_estimate['required_spread_bps']:.2f} bps\n"
                            f"   估算边际: {edge_estimate['edge_est_bps']:.2f} bps"
                        )
                        return
        
            self.close_signal_count += 1

            edge_text = ""
            if self.edge_filter_enabled:
                if edge_apply and edge_estimate is not None:
                    edge_text = (
                        f"   边际过滤: {edge_estimate['spread_abs_bps']:.2f} bps >= "
                        f"{edge_estimate['required_spread_bps']:.2f} bps "
                        f"(成本: {edge_estimate['cost_est_bps']:.2f} bps, 估算边际: {edge_estimate['edge_est_bps']:.2f} bps)\n"
                    )
                elif not edge_apply:
                    edge_text = (
                        f"   边际过滤: 减风险路径放行 "
                        f"(仓位 {edge_ctx['current_qty']:+.4f} -> {edge_ctx['projected_qty']:+.4f})\n"
                    )

            logger.info(
                f"🔔 [{self.symbol}] 检测到反向开仓信号 #{self.close_signal_count}:\n"
                f"   延迟_a: {signal_delay_ms_a:.2f} ms (阈值: {self.max_signal_delay_ms_a} ms)\n"
                f"   延迟_b: {signal_delay_ms_b:.2f} ms (阈值: {self.max_signal_delay_ms_b} ms)\n"
                f"   {self.exchange_a.exchange_name}_ask: ${prices.exchange_a_ask}\n"
                f"   {self.exchange_a.exchange_name}_ask_size: {prices.exchange_a_ask_size}\n"
                f"   {self.exchange_b.exchange_name}_bid: ${prices.exchange_b_bid}\n"
                f"   {self.exchange_b.exchange_name}_bid_size: {prices.exchange_b_bid_size}\n"
                f"   {edge_text}"
                f"{extra_spread_info}"
                f"   {spread_label}: {compare_spread_pct:.4f}%({threshold_label}: {threshold_pct:.4f}%)"
            )
            
            close_quantity = self.position_manager.position_step
            if not self.position_manager.accumulate_mode and current_position is not None:
                close_quantity = current_position.quantity

            signal = self._build_trading_signal(
                signal_type=SignalType.CLOSE,
                prices=prices,
                spread_pct=spread_pct,
                signal_trigger_time=signal_trigger_time,
                signal_delay_ms_a=signal_delay_ms_a,
                signal_delay_ms_b=signal_delay_ms_b,
                quantity=close_quantity,
                metadata={
                    'compare_spread_pct': str(compare_spread_pct),
                    'threshold_pct': str(threshold_pct),
                    'threshold_label': threshold_label,
                    'spread_label': spread_label,
                    'signal_mode': self.signal_mode,
                    'median_edge': median_edge_stats.to_dict() if median_edge_stats else {},
                    'stat_arb': stat_arb_stats.to_dict() if stat_arb_stats else {},
                },
            )
            return signal

        return None

    @staticmethod
    def _deep_merge_dict(base: dict, override: dict) -> dict:
        """递归合并字典。"""
        result = dict(base or {})
        for key, value in (override or {}).items():
            if isinstance(value, dict) and isinstance(result.get(key), dict):
                result[key] = HedgeStrategy._deep_merge_dict(result[key], value)
            else:
                result[key] = value
        return result

    def _load_local_override_data(self) -> tuple[Optional[dict], Optional[float]]:
        """读取当前 pair 对应的本地覆盖配置。"""
        if not self.local_override_path or not self.pair_id:
            return None, None
        if not self.local_override_path.exists():
            return None, None

        mtime = self.local_override_path.stat().st_mtime
        with self.local_override_path.open('r', encoding='utf-8') as f:
            raw = yaml.safe_load(f) or {}

        if not isinstance(raw, dict):
            raise ValueError("本地覆盖配置格式错误：顶层必须是字典")

        common_override = raw.get('common', {})
        pair_override = {}
        if isinstance(raw.get('pairs'), dict):
            pair_override = raw['pairs'].get(self.pair_id, {}) or {}

        if common_override and not isinstance(common_override, dict):
            raise ValueError("本地覆盖配置格式错误：common 必须是字典")
        if pair_override and not isinstance(pair_override, dict):
            raise ValueError("本地覆盖配置格式错误：pairs.<pair_id> 必须是字典")

        merged = self._deep_merge_dict(common_override or {}, pair_override or {})
        return merged, mtime

    def _apply_local_override_updates(self, overrides: dict, *, init_load: bool = False) -> None:
        """按字段应用本地热加载覆盖；缺失字段保持当前值。"""
        if not overrides:
            return

        def update_attr(attr_name: str, new_value, cast, *, label: Optional[str] = None) -> None:
            current_value = getattr(self, attr_name)
            try:
                converted = cast(new_value)
            except Exception as exc:
                logger.warning(f"⚠️ [{self.symbol}] 跳过本地覆盖字段 {label or attr_name}: {exc}")
                return
            if converted == current_value:
                return
            setattr(self, attr_name, converted)
            logger.info(
                f"🔄 [{self.symbol}] 本地覆盖更新 {label or attr_name}: "
                f"{current_value} -> {converted}"
            )

        def update_decimal_attr(attr_name: str, new_value, *, label: Optional[str] = None) -> None:
            update_attr(attr_name, new_value, lambda value: Decimal(str(value)), label=label)

        if 'max_position' in overrides:
            current_max_position = self.position_manager.max_position
            new_max_position = Decimal(str(overrides['max_position']))
            if new_max_position != current_max_position:
                logger.info(
                    f"🔄 [{self.symbol}] 本地覆盖更新 max_position: "
                    f"{current_max_position} -> {new_max_position}"
                )
                self.position_manager.max_position = new_max_position
                self.risk_control_service.set_base_max_position(new_max_position)

        if 'open_threshold' in overrides:
            update_attr('open_threshold_pct', overrides['open_threshold'], float, label='open_threshold')
        if 'close_threshold' in overrides:
            update_attr('close_threshold_pct', overrides['close_threshold'], float, label='close_threshold')
        if 'cooldown_seconds' in overrides:
            update_attr('cooldown_seconds', overrides['cooldown_seconds'], float, label='cooldown_seconds')
        if 'min_depth_quantity' in overrides:
            update_decimal_attr('min_depth_quantity', overrides['min_depth_quantity'], label='min_depth_quantity')
        if 'max_signal_delay_ms_a' in overrides:
            update_attr('max_signal_delay_ms_a', overrides['max_signal_delay_ms_a'], int, label='max_signal_delay_ms_a')
        if 'max_signal_delay_ms_b' in overrides:
            update_attr('max_signal_delay_ms_b', overrides['max_signal_delay_ms_b'], int, label='max_signal_delay_ms_b')
        if 'direction_reverse' in overrides:
            update_attr('direction_reverse', overrides['direction_reverse'], bool, label='direction_reverse')

        edge_filter = overrides.get('edge_filter', {})
        if isinstance(edge_filter, dict):
            if 'enabled' in edge_filter:
                update_attr('edge_filter_enabled', edge_filter['enabled'], bool, label='edge_filter.enabled')
            if 'min_edge_bps' in edge_filter:
                update_attr('min_edge_bps', edge_filter['min_edge_bps'], float, label='edge_filter.min_edge_bps')
            if 'base_cost_bps' in edge_filter:
                update_attr('edge_base_cost_bps', edge_filter['base_cost_bps'], float, label='edge_filter.base_cost_bps')
            if 'fee_bps' in edge_filter:
                update_attr('edge_fee_bps', edge_filter['fee_bps'], float, label='edge_filter.fee_bps')
            if 'latency_bps_per_100ms' in edge_filter:
                update_attr(
                    'edge_latency_bps_per_100ms',
                    edge_filter['latency_bps_per_100ms'],
                    float,
                    label='edge_filter.latency_bps_per_100ms',
                )
            if 'latency_free_ms' in edge_filter:
                update_attr('edge_latency_free_ms', edge_filter['latency_free_ms'], float, label='edge_filter.latency_free_ms')

        signal_logic = overrides.get('signal_logic', {})
        if isinstance(signal_logic, dict):
            median_edge = signal_logic.get('median_edge', {})
            if isinstance(median_edge, dict):
                if 'baseline_adjustment' in median_edge:
                    update_attr(
                        'median_edge_baseline_adjustment',
                        median_edge['baseline_adjustment'],
                        bool,
                        label='signal_logic.median_edge.baseline_adjustment',
                    )
                    if self.median_edge_manager is not None:
                        self.median_edge_manager.baseline_adjustment = bool(self.median_edge_baseline_adjustment)
                if 'baseline_ratio' in median_edge:
                    update_attr(
                        'median_edge_baseline_ratio',
                        median_edge['baseline_ratio'],
                        float,
                        label='signal_logic.median_edge.baseline_ratio',
                    )
                    if self.median_edge_manager is not None:
                        self.median_edge_manager.baseline_ratio = float(self.median_edge_baseline_ratio)
                if 'medium_window_seconds' in median_edge:
                    update_attr(
                        'median_edge_medium_window_seconds',
                        median_edge['medium_window_seconds'],
                        int,
                        label='signal_logic.median_edge.medium_window_seconds',
                    )
                    if self.median_edge_manager is not None:
                        self.median_edge_manager.medium_window_seconds = int(self.median_edge_medium_window_seconds)
                if 'long_window_seconds' in median_edge:
                    update_attr(
                        'median_edge_long_window_seconds',
                        median_edge['long_window_seconds'],
                        int,
                        label='signal_logic.median_edge.long_window_seconds',
                    )
                    if self.median_edge_manager is not None:
                        self.median_edge_manager.long_window_seconds = int(self.median_edge_long_window_seconds)
                if 'medium_min_samples' in median_edge:
                    update_attr(
                        'median_edge_medium_min_samples',
                        median_edge['medium_min_samples'],
                        int,
                        label='signal_logic.median_edge.medium_min_samples',
                    )
                    if self.median_edge_manager is not None:
                        self.median_edge_manager.medium_min_samples = int(self.median_edge_medium_min_samples)
                if 'long_min_samples' in median_edge:
                    update_attr(
                        'median_edge_long_min_samples',
                        median_edge['long_min_samples'],
                        int,
                        label='signal_logic.median_edge.long_min_samples',
                    )
                    if self.median_edge_manager is not None:
                        self.median_edge_manager.long_min_samples = int(self.median_edge_long_min_samples)
                if 'min_edge_bps' in median_edge:
                    update_attr(
                        'median_edge_min_edge_bps',
                        median_edge['min_edge_bps'],
                        float,
                        label='signal_logic.median_edge.min_edge_bps',
                    )
                    if self.median_edge_manager is not None:
                        self.median_edge_manager.min_edge_bps = float(self.median_edge_min_edge_bps)
                        self.median_edge_manager.min_edge_pct = float(self.median_edge_min_edge_bps) / 100.0

            stat_arb = signal_logic.get('stat_arb', {})
            if isinstance(stat_arb, dict):
                if 'score_mode' in stat_arb:
                    update_attr('stat_arb_score_mode', stat_arb['score_mode'], lambda value: str(value).lower(), label='signal_logic.stat_arb.score_mode')
                    if self.stat_arb_manager is not None:
                        self.stat_arb_manager.score_mode = str(self.stat_arb_score_mode).lower()
                if 'breakout_quantile' in stat_arb:
                    update_attr('stat_arb_breakout_quantile', stat_arb['breakout_quantile'], float, label='signal_logic.stat_arb.breakout_quantile')
                    if self.stat_arb_manager is not None:
                        self.stat_arb_manager.breakout_quantile = float(self.stat_arb_breakout_quantile)
                if 'entry_threshold' in stat_arb:
                    update_attr('stat_arb_entry_threshold', stat_arb['entry_threshold'], float, label='signal_logic.stat_arb.entry_threshold')
                    if self.stat_arb_manager is not None:
                        self.stat_arb_manager.entry_threshold = float(self.stat_arb_entry_threshold)
                if 'entry_raw_floor_pct' in stat_arb:
                    if stat_arb['entry_raw_floor_pct'] is None:
                        current_value = self.stat_arb_entry_raw_floor_pct
                        if current_value is not None:
                            self.stat_arb_entry_raw_floor_pct = None
                            logger.info(
                                f"🔄 [{self.symbol}] 本地覆盖更新 signal_logic.stat_arb.entry_raw_floor_pct: "
                                f"{current_value} -> None"
                            )
                    else:
                        update_decimal_attr(
                            'stat_arb_entry_raw_floor_pct',
                            stat_arb['entry_raw_floor_pct'],
                            label='signal_logic.stat_arb.entry_raw_floor_pct',
                        )
                if 'exit_threshold' in stat_arb:
                    update_attr('stat_arb_exit_threshold', stat_arb['exit_threshold'], float, label='signal_logic.stat_arb.exit_threshold')
                if 'exit_score_source' in stat_arb:
                    update_attr('stat_arb_exit_score_source', stat_arb['exit_score_source'], lambda value: str(value).lower(), label='signal_logic.stat_arb.exit_score_source')
                if 'exit_spread_floor_pct' in stat_arb:
                    update_decimal_attr(
                        'stat_arb_exit_spread_floor_pct',
                        stat_arb['exit_spread_floor_pct'],
                        label='signal_logic.stat_arb.exit_spread_floor_pct',
                    )
                if 'exit_take_profit_pct' in stat_arb:
                    if stat_arb['exit_take_profit_pct'] is None:
                        current_value = self.stat_arb_exit_take_profit_pct
                        if current_value is not None:
                            self.stat_arb_exit_take_profit_pct = None
                            logger.info(
                                f"🔄 [{self.symbol}] 本地覆盖更新 signal_logic.stat_arb.exit_take_profit_pct: "
                                f"{current_value} -> None"
                            )
                    else:
                        update_decimal_attr(
                            'stat_arb_exit_take_profit_pct',
                            stat_arb['exit_take_profit_pct'],
                            label='signal_logic.stat_arb.exit_take_profit_pct',
                        )
                if 'min_score_gap' in stat_arb:
                    update_attr('stat_arb_min_score_gap', stat_arb['min_score_gap'], float, label='signal_logic.stat_arb.min_score_gap')
                    if self.stat_arb_manager is not None:
                        self.stat_arb_manager.min_score_gap = float(self.stat_arb_min_score_gap)
                if 'min_mad_pct' in stat_arb:
                    update_attr('stat_arb_min_mad_pct', stat_arb['min_mad_pct'], float, label='signal_logic.stat_arb.min_mad_pct')
                    if self.stat_arb_manager is not None:
                        self.stat_arb_manager.min_mad_pct = float(self.stat_arb_min_mad_pct)
                if 'quality_log_interval_seconds' in stat_arb:
                    update_attr(
                        'stat_arb_quality_log_interval_seconds',
                        stat_arb['quality_log_interval_seconds'],
                        float,
                        label='signal_logic.stat_arb.quality_log_interval_seconds',
                    )
                if 'require_same_sign_for_medium_long' in stat_arb:
                    update_attr(
                        'stat_arb_require_same_sign',
                        stat_arb['require_same_sign_for_medium_long'],
                        bool,
                        label='signal_logic.stat_arb.require_same_sign_for_medium_long',
                    )
                    if self.stat_arb_manager is not None:
                        self.stat_arb_manager.require_same_sign_for_medium_long = bool(self.stat_arb_require_same_sign)
                if 'block_when_regime_suspected' in stat_arb:
                    update_attr(
                        'stat_arb_block_regime',
                        stat_arb['block_when_regime_suspected'],
                        bool,
                        label='signal_logic.stat_arb.block_when_regime_suspected',
                    )
                    if self.stat_arb_manager is not None:
                        self.stat_arb_manager.block_when_regime_suspected = bool(self.stat_arb_block_regime)

        if init_load:
            logger.info(
                f"📥 [{self.symbol}] 已加载本地热加载配置: "
                f"{self.local_override_path}"
            )

    def check_yaml_config_updates(self, *, force: bool = False, init_load: bool = False):
        """检查并热加载本地覆盖配置。"""
        if not self.local_override_path or not self.pair_id:
            return

        now = time.time()
        if not force:
            if self._last_yaml_check_time is None:
                self._last_yaml_check_time = now
            elif now - self._last_yaml_check_time < self._yaml_check_interval:
                return
            else:
                self._last_yaml_check_time = now
        else:
            self._last_yaml_check_time = now

        try:
            overrides, mtime = self._load_local_override_data()
            if overrides is None or mtime is None:
                return
            if not force and self._local_override_last_mtime is not None and mtime <= self._local_override_last_mtime:
                return
            self._apply_local_override_updates(overrides, init_load=init_load or self._local_override_last_mtime is None)
            self._local_override_last_mtime = mtime
        except Exception as e:
            logger.exception(f"⚠️ 检查本地热加载配置时出错: {e}")

    def _create_dummy_position(self) -> Position:
        """创建虚拟 Position（累计模式用）"""
        try:
            latest_prices = self.monitor.latest_prices
            
            if latest_prices:
                signal_entry_price_a = latest_prices.exchange_a_bid
                signal_entry_price_b = latest_prices.exchange_b_ask
            else:
                # ✅ 如果没有价格，使用占位值（避免除零）
                signal_entry_price_a = Decimal('1.0')
                signal_entry_price_b = Decimal('1.0')
        except:
            signal_entry_price_a = Decimal('1.0')
            signal_entry_price_b = Decimal('1.0')
        return Position(
            symbol=self.symbol,
            quantity=self.position_manager.position_step,
            exchange_a_name=self.exchange_a.exchange_name,
            exchange_b_name=self.exchange_b.exchange_name,
            exchange_a_signal_entry_price=signal_entry_price_a,
            exchange_b_signal_entry_price=signal_entry_price_b,
            exchange_a_entry_price=signal_entry_price_a,
            exchange_b_entry_price=signal_entry_price_b,
            exchange_a_order_id='DUMMY',
            exchange_b_order_id='DUMMY',
            spread_pct=Decimal('0')
        )
    
    async def _send_multi_notification(self, direction: str, position: Position, spread_pct: Decimal):
        mode_text = "虚拟" if self.monitor_only else "实际"
        actual_slippage = position.calculate_slippage()
        logger.info(f'----------actual-------------{actual_slippage}')
        spread_label = ""
        time_label = ""
        if direction == 'long':
            title = f'对冲开多通知（{mode_text}）'
            a_slippage = actual_slippage['exit_a_slippage_pct'].quantize(Decimal('0.0001'))
            b_slippage = actual_slippage['exit_b_slippage_pct'].quantize(Decimal('0.0001'))
            total_slippage = actual_slippage['total_exit_slippage_pct'].quantize(Decimal('0.0001'))
            trigger_time = position.exit_time.strftime('%Y-%m-%d %H:%M:%S')
            threshold = self.close_threshold_pct
            qty = -self.quantity
            spread_label = "反向开仓信号价差"
            time_label = "反向开仓时间"
        else: 
            title = f'对冲开空通知（{mode_text}）'
            a_slippage = actual_slippage['entry_a_slippage_pct'].quantize(Decimal('0.0001'))
            b_slippage = actual_slippage['entry_b_slippage_pct'].quantize(Decimal('0.0001'))
            total_slippage = actual_slippage['total_entry_slippage_pct'].quantize(Decimal('0.0001'))
            trigger_time = position.entry_time.strftime('%Y-%m-%d %H:%M:%S')
            threshold = self.open_threshold_pct
            qty = self.quantity
            spread_label = "开仓信号价差"
            time_label = "开仓时间"
        current_position_qty = self.position_manager.get_current_position_qty().quantize(Decimal('0.0001'))
        message = (
            f"🔔 {title}\n\n"
            f"交易对: {self.symbol}\n"
            f"数量: {self.quantity}\n"
            f"当前仓位: {current_position_qty + qty} --> {current_position_qty}\n"
            f"{spread_label}: {spread_pct.quantize(Decimal('0.0001'))}%（阈值: {threshold}%）\n"
            f"总滑点: {total_slippage}%（A: {a_slippage}% B: {b_slippage}%）\n"
            f"{time_label}: {trigger_time}"
        )
        await self.lark_bot.send_text(message)

    async def _send_open_notification(self, position: Position, prices: PriceSnapshot):
        """发送开仓通知"""
        try:
            # ✅ 根据模式调整通知内容
            mode_text = "虚拟" if self.monitor_only else "实际"
            
            message = (
                f"🔔 对冲开仓通知 ({mode_text})\n\n"
                f"交易对: {self.symbol}\n"
                f"价差: {position.spread_pct:.4f}%\n"
                f"数量: {self.quantity}\n\n"
                f"{self.exchange_a.exchange_name} 开空:\n"
                f"  价格: ${position.exchange_a_entry_price}\n"
                f"  订单ID: {position.exchange_a_order_id}\n\n"
                f"{self.exchange_b.exchange_name} 开多:\n"
                f"  价格: ${position.exchange_b_entry_price}\n"
                f"  订单ID: {position.exchange_b_order_id}\n\n"
                f"开仓时间: {position.entry_time.strftime('%Y-%m-%d %H:%M:%S')}"  # ✅ 修复
            )
            await self.lark_bot.send_text(message)
        except Exception as e:
            logger.exception(f"❌ 发送飞书通知失败: {e}")
    
    async def _send_close_notification(self, position: Position, pnl_pct: Decimal, prices: PriceSnapshot):
        """发送平仓通知"""
        try:
            # ✅ 根据模式调整通知内容
            mode_text = "虚拟" if self.monitor_only else "实际"
            is_accumulate_mode = self.position_manager.accumulate_mode
            
            # ✅ 检查 position 是否为 None
            if position is None:
                # ✅ 反向开仓：没有原始持仓信息
                message = (
                    f"🔔 {'对冲反向开仓通知' if is_accumulate_mode else '对冲平仓通知'} ({mode_text}) - 反向开仓\n\n"
                    f"交易对: {self.symbol}\n"
                    f"盈亏: {pnl_pct:.4f}%\n"
                    f"数量: {self.position_manager.position_step}\n\n"
                    f"当前价格:\n"
                    f"  {self.exchange_a.exchange_name}: ${prices.exchange_a_ask}\n"
                    f"  {self.exchange_b.exchange_name}: ${prices.exchange_b_bid}\n\n"
                    f"备注: 仓位为 0 时执行反向开仓"
                )
            else:
                # ✅ 正常平仓：有原始持仓信息
                # ✅ 计算实际成交价差
                actual_entry_spread_pct = (
                    (position.exchange_a_entry_price - position.exchange_b_entry_price)
                    / position.exchange_b_entry_price * 100
                )
                
                # ✅ 计算实际平仓价差
                actual_exit_spread_pct = (
                    (prices.exchange_a_ask - prices.exchange_b_bid)
                    / prices.exchange_b_bid * 100
                )
                message = (
                    f"🔔 {'对冲反向开仓通知' if is_accumulate_mode else '对冲平仓通知'} ({mode_text})\n\n"
                    f"交易对: {self.symbol}\n"
                    f"盈亏: {pnl_pct:.4f}%\n"
                    f"数量: {position.quantity}\n\n"
                    f"开仓信息:\n"
                    f"  {self.exchange_a.exchange_name}: ${position.exchange_a_entry_price}\n"
                    f"  {self.exchange_b.exchange_name}: ${position.exchange_b_entry_price}\n"
                    f"  信号价差: {position.spread_pct:.4f}%\n\n"
                    f"  实际价差: {actual_entry_spread_pct:.4f}%\n"  # ✅ 新增
                    f"  价差损失: {(position.spread_pct - actual_entry_spread_pct):.4f}%\n\n"  # ✅ 新增
                    f"{'反向开仓信息' if is_accumulate_mode else '平仓信息'}:\n"
                    f"  {self.exchange_a.exchange_name}: ${prices.exchange_a_ask}\n"
                    f"  {self.exchange_b.exchange_name}: ${prices.exchange_b_bid}\n\n"
                    f"  实际价差: {actual_exit_spread_pct:.4f}%\n"  # ✅ 新增
                    f"持仓时长: {position.get_holding_duration()}"
                )
            
            await self.lark_bot.send_text(message)
        except Exception as e:
            logger.exception(f"❌ 发送飞书通知失败: {e}")
    def _format_open_stats(self) -> str:
        """格式化开仓统计信息"""
        stats = self.signal_stats['open']
        total = stats['total']
        
        if total == 0:
            return "无数据"
        
        # 计算比例
        delay_pct = (stats['delay_filtered'] / total * 100) if total > 0 else 0
        depth_pct = (stats['depth_insufficient'] / total * 100) if total > 0 else 0
        threshold_pct = (stats['threshold_filtered'] / total * 100) if total > 0 else 0
        edge_pct = (stats['edge_filtered'] / total * 100) if total > 0 else 0
        adjusted_pct = (stats['depth_adjusted'] / total * 100) if total > 0 else 0
        exec_pct = (stats['executed'] / total * 100) if total > 0 else 0
        limited_a_pct = (stats['limited_a'] / total * 100) if total > 0 else 0
        limited_b_pct = (stats['limited_b'] / total * 100) if total > 0 else 0
        skipped_pct = (stats['skipped'] / total * 100) if total > 0 else 0

        return (
            f"总信号 {total} | "
            f"延迟过滤 {stats['delay_filtered']} ({delay_pct:.1f}%) | "
            f"深度不足 {stats['depth_insufficient']} ({depth_pct:.1f}%) | "
            f"阈值拦截 {stats['threshold_filtered']} ({threshold_pct:.1f}%) | "
            f"边际不足 {stats['edge_filtered']} ({edge_pct:.1f}%) | "
            # f"数量调整 {stats['depth_adjusted']} ({adjusted_pct:.1f}%) | "
            f"执行 {stats['executed']} ({exec_pct:.1f}%) | "
            f"限流A {stats['limited_a']} ({limited_a_pct:.1f}%) | "
            f"限流B {stats['limited_b']} ({limited_b_pct:.1f}%) | "
            f"跳过 {stats['skipped']} ({skipped_pct:.1f}%)"
        )
    
    def _format_close_stats(self) -> str:
        """格式化平仓统计信息"""
        stats = self.signal_stats['close']
        total = stats['total']
        
        if total == 0:
            return "无数据"
        
        delay_pct = (stats['delay_filtered'] / total * 100) if total > 0 else 0
        depth_pct = (stats['depth_insufficient'] / total * 100) if total > 0 else 0
        threshold_pct = (stats['threshold_filtered'] / total * 100) if total > 0 else 0
        edge_pct = (stats['edge_filtered'] / total * 100) if total > 0 else 0
        exec_pct = (stats['executed'] / total * 100) if total > 0 else 0
        limited_a_pct = (stats['limited_a'] / total * 100) if total > 0 else 0
        limited_b_pct = (stats['limited_b'] / total * 100) if total > 0 else 0
        skipped_pct = (stats['skipped'] / total * 100) if total > 0 else 0

        return (
            f"总信号 {total} | "
            f"延迟过滤 {stats['delay_filtered']} ({delay_pct:.1f}%) | "
            f"深度不足 {stats['depth_insufficient']} ({depth_pct:.1f}%) | "
            f"阈值拦截 {stats['threshold_filtered']} ({threshold_pct:.1f}%) | "
            f"边际不足 {stats['edge_filtered']} ({edge_pct:.1f}%) | "
            f"执行 {stats['executed']} ({exec_pct:.1f}%) | "
            f"限流A {stats['limited_a']} ({limited_a_pct:.1f}%) | "
            f"限流B {stats['limited_b']} ({limited_b_pct:.1f}%) | "
            f"跳过 {stats['skipped']} ({skipped_pct:.1f}%)"
        )
    
    def _log_stats_if_needed(self):
        """定期输出统计信息"""
        current_time = time.time()
        
        if current_time - self._last_stats_log_time >= self._stats_log_interval:
            threshold_info = ""
            sample_time_length = 0.0
            if self.quantile_manager:
                stats = self.quantile_manager.get_stats()
                threshold_info = (
                    f"\n"
                    f"📊 分位数阈值:\n"
                    f"   模式: P{stats['quantile_pct']} | 样本{stats['sample_size']} | 最小样本{stats['min_samples']}\n"
                    f"   当前: 开仓{(stats.get('current_open') or 0):.4f}% "
                    f"        平仓{(stats.get('current_close') or 0):.4f}%\n"
                    f"   样本: 开仓{stats['open_samples']} 平仓{stats['close_samples']} | 就绪: {'是' if stats.get('ready') else '否'}\n"
                )
                sample_time_length = self.quantile_manager.get_time_length()
            elif self.threshold_manager:
                stats = self.threshold_manager.get_stats()
                threshold_info = (
                    f"\n"
                    f"📊 动态阈值:\n"
                    f"   当前: 开仓{stats.get('current_open', 0):.4f}% "
                    f"        平仓{stats.get('current_close', 0):.4f}% "
                    f"(调整{stats['adjustment_count']}次)\n"
                    f"   阈值和: {stats.get('current_threshold_sum', 0) or 0:.4f}% | "
                    f"交易拦截: {'是' if stats.get('trade_blocked') else '否'}\n"
                    f"   样本: 开仓{stats['open_samples']} 平仓{stats['close_samples']}\n"
                )
                sample_time_length = self.threshold_manager.get_time_length()

            logger.info(
                f"\n"
                f"{'='*60}\n"
                f"📊 策略统计报告\n"
                f"{'='*60}\n"
                f"🟢 开仓信号:\n"
                f"   {self._format_open_stats()}\n"
                f"\n"
                f"🔴 平仓信号:\n"
                f"   {self._format_close_stats()}\n"
                f"{threshold_info}"
                f" 总信号个数: {self.signal_total}\n"
                f" 延迟信号个数: {self.signal_delay}\n"
                f" 样本时间长度 {sample_time_length:.2f} 秒\n"
                f"{'='*60}"
            )
            self._last_stats_log_time = current_time
    
    def get_stats_summary(self) -> dict:
        """获取统计摘要（用于外部调用）"""
        return {
            'open': {
                **self.signal_stats['open'],
                'success_rate': (
                    self.signal_stats['open']['executed'] / self.signal_stats['open']['total'] * 100
                    if self.signal_stats['open']['total'] > 0 else 0
                )
            },
            'close': {
                **self.signal_stats['close'],
                'success_rate': (
                    self.signal_stats['close']['executed'] / self.signal_stats['close']['total'] * 100
                    if self.signal_stats['close']['total'] > 0 else 0
                )
            }
        }
    
    async def _log_equity_and_volume_if_needed(self):
        """定期记录账户权益和交易量"""
        current_time = time.time()
        if self._last_equity_log_time is None:
            self._last_equity_log_time = current_time
        current_qty = self.position_manager.get_current_position_qty()

        if current_time - self._last_equity_log_time >= self._equity_log_interval and current_qty == 0:
            position_a = await self.exchange_a.get_position(self.symbol_a)
            position_b = await self.exchange_b.get_position(self.symbol_b)

            # 解析仓位数量
            qty_a = Decimal(str(position_a.get('size', 0))) if position_a else Decimal('0')
            qty_b = Decimal(str(position_b.get('size', 0))) if position_b else Decimal('0')
            side_a = position_a.get('side') if position_a else 'neutral'
            side_b = position_b.get('side') if position_b else 'neutral'

            if qty_a ==0 and qty_b == 0:
                logger.info(f"当前A所仓位: {qty_a}({side_a}), B所仓位: {qty_b}({side_b})")
                try:
                    volume_a, equity_a, volume_b, equity_b = await self.get_equity_and_volume()
                    if volume_b > self.start_vol_b:
                        volume_delta = volume_b - self.start_vol_b
                        logger.info(
                            self._build_equity_loss_summary(
                                equity_a=equity_a,
                                equity_b=equity_b,
                                volume_delta=volume_delta,
                                volume_label="B所交易增量",
                            )
                        )
                    else:
                        logger.info(
                            self._build_equity_loss_summary(
                                equity_a=equity_a,
                                equity_b=equity_b,
                                volume_delta=Decimal("0"),
                                volume_label="B所交易增量",
                            )
                        )
                except Exception as e:
                    logger.exception(f"❌ 获取账户权益或交易量失败: {e}")
                self._last_equity_log_time = current_time
