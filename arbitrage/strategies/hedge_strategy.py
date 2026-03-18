"""对冲套利策略"""

import asyncio
from aiolimiter import AsyncLimiter
from datetime import datetime
import logging
import random
import time
import yaml
import os
from decimal import Decimal, ROUND_DOWN
from typing import Optional

from helpers.util import beijing_to_timestamp
from .base_strategy import BaseStrategy
from ..models.prices import PriceSnapshot
from ..services.price_monitor import PriceMonitorService
from ..services.position_manager import PositionManagerService
from ..services.order_executor_parallel import OrderExecutor
from ..services.dynamic_threshold import DynamicThresholdManager
from ..services.quantile_signal_manager import QuantileSignalManager
from ..services.risk_control_service import RiskControlService, RiskLevel
from ..models.position import Position

logger = logging.getLogger(__name__)

class HedgeStrategy(BaseStrategy):
    """对冲套利策略"""
    
    def __init__(
        self,
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
        risk_control: Optional[dict] = None
    ):
        super().__init__(
            strategy_name=f"Hedge-{symbol}",
            symbol=symbol,
            quantity=quantity,
            quantity_precision=quantity_precision
        )
        self.symbol_a = symbol_a
        self.symbol_b = symbol_b
        self.open_threshold_pct = open_threshold_pct
        self.close_threshold_pct = close_threshold_pct
        self.exchange_a = exchange_a
        self.exchange_b = exchange_b
        self.lark_bot = lark_bot
        self.monitor_only = monitor_only
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
        self.config_yaml_path = "./arbitrage/config/config.yaml"

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
        self._yaml_check_interval = 60  # 每 60 秒检查一次 YAML 配置文件
        self._is_executed = False
        self._last_effective_max_position: Optional[Decimal] = None
        self._last_non_zero_strategy_qty = Decimal('0')
        # self._last_threshold_check_time = None
        # 信号逻辑配置（默认沿用旧逻辑）
        self.signal_logic = signal_logic if isinstance(signal_logic, dict) else {}
        self.signal_mode = str(self.signal_logic.get('mode', 'legacy')).lower()
        self.signal_quantile = float(self.signal_logic.get('quantile', 0.6))
        self.signal_sample_size = int(self.signal_logic.get('sample_size', 2000))
        self.signal_min_samples = int(self.signal_logic.get('min_samples', self.signal_sample_size))

        # 分位数信号管理器（新逻辑）
        self.quantile_manager = None
        if self.signal_mode == 'quantile':
            self.quantile_manager = QuantileSignalManager(
                sample_size=self.signal_sample_size,
                min_samples=self.signal_min_samples,
                quantile=self.signal_quantile,
            )

        # 动态阈值管理器（旧逻辑）
        self.threshold_manager = None
        if self.signal_mode != 'quantile':
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
        
        
        logger.info(
            f"🎯 策略配置:\n"
            f"   Symbol: {symbol}\n"
            f"   Quantity: {quantity}\n"
            f"   延迟阈值(A/B): {self.max_signal_delay_ms_a}/{self.max_signal_delay_ms_b} ms\n"
            f"   Open Threshold: {open_threshold_pct}%\n"
            f"   Close Threshold: {close_threshold_pct}%\n"
            f"   Exchange A: {exchange_a.exchange_name}\n"
            f"   Exchange B: {exchange_b.exchange_name}\n"
            f"   Monitor Only: {monitor_only}\n"
            f"   累计模式: {'✅ 启用' if accumulate_mode else '❌ 禁用'}\n"
            f"   风控模块: {'✅ 启用' if self.risk_control_enabled else '❌ 禁用'}\n"
            f"   信号逻辑: {'分位数' if self.signal_mode == 'quantile' else '标准差'}\n"
            f"   分位数配置: P{int(self.signal_quantile * 100)} | 样本{self.signal_sample_size} | 最小样本{self.signal_min_samples}\n"
            f"   边际二次过滤: {'✅ 启用' if self.edge_filter_enabled else '❌ 禁用'}\n"
            f"   最小安全边际: {self.min_edge_bps:.2f} bps\n"
            f"   基础成本估计: {self.edge_base_cost_bps:.2f} bps\n"
            f"   手续费估计: {self.edge_fee_bps:.2f} bps\n"
            f"   延迟风险系数: {self.edge_latency_bps_per_100ms:.2f} bps/100ms\n"
            f"   延迟免惩罚阈值: {self.edge_latency_free_ms:.0f} ms"
        )
    
    async def start(self):
        """启动策略"""
        logger.info(f"🚀 启动策略: {self.strategy_name}")
        
        # 启动价格监控
        await self.monitor.start()
        # 启动后台风控（异步监控，不阻塞信号热路径）
        if self.risk_control_enabled:
            await self.risk_control_service.start()
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

    async def _on_price_update(self, prices: PriceSnapshot):
        """
        处理价格更新
        
        ✅ 核心逻辑：
        - 无持仓时：只检查开仓信号
        - 有持仓时：只检查平仓信号
        """
        if not self.is_running:
            return
        is_stale, stale_msg = self.monitor.is_orderbook_stale(
            max_age_a=self.max_signal_delay_ms_a / 1000,
            max_age_b=self.max_signal_delay_ms_b / 1000,
        )
        if is_stale:
            # logger.warning(f"⚠️ 订单簿过时，丢弃信号: {stale_msg}")
            return
        try:
            # ✅ 记录价格更新的时间
            price_update_time_a = self._normalize_timestamp(prices.exchange_a_timestamp)
            price_update_time_b = self._normalize_timestamp(prices.exchange_b_timestamp)

            # 记录信号触发时间
            signal_trigger_time = time.time()
            signal_delay_ms_a = (signal_trigger_time - price_update_time_a) * 1000
            signal_delay_ms_b = (signal_trigger_time - price_update_time_b) * 1000
        
            signal_flag = False
            self.signal_total += 1
            # ✅ 过滤延迟过大的信号
            if signal_delay_ms_a <= self.max_signal_delay_ms_a and signal_delay_ms_b <= self.max_signal_delay_ms_b:
                signal_flag = True
            else:
                self.signal_delay += 1
                logger.warning(
                    f"⚠️ [{self.symbol}] 信号延迟过大: "
                    f"A {signal_delay_ms_a:.2f} ms（A阈值: {self.max_signal_delay_ms_a} ms），"
                    f" B {signal_delay_ms_b:.2f} ms（B阈值: {self.max_signal_delay_ms_b} ms）"
                )
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
                    return
                if risk_decision.need_reduce:
                    reduced = await self._try_apply_risk_reduction(
                        prices,
                        risk_decision.target_position_ratio,
                        target_abs_override=reduced_target_abs,
                    )
                    if reduced:
                        return
            # if self._last_threshold_check_time is None:
                # self._last_threshold_check_time = time.time()
            # now = time.time()
            # ✅ 新增：记录价差并尝试调整阈值
            if self.quantile_manager and signal_flag:
                self.quantile_manager.add_spreads(spread_pct, reverse_spread_pct)

            if self.threshold_manager and signal_flag:
                # 添加数据
                self.threshold_manager.add_spreads(spread_pct, reverse_spread_pct)
                
                # 尝试调整
                current_qty = self.position_manager.get_current_position_qty()
                new_open, new_close = self.threshold_manager.try_adjust(
                    current_qty, 
                    self.position_manager.max_position
                )
                
                # 更新阈值
                if new_open is not None:
                    self.open_threshold_pct = new_open
                    self.close_threshold_pct = new_close
                else:
                    return

            if self.position_manager.accumulate_mode:
                current_qty = self.position_manager.get_current_position_qty()
                self._remember_last_non_zero_strategy_qty(current_qty)
                # logger.debug(f"🔍 当前strategy仓位: {current_qty:+.4f} {self.symbol}")
                self._is_executed = False
                if current_qty < 0:
                    # ✅ 优先检查平仓信号（如果可以平仓）
                    await self._check_close_signal(prices, reverse_spread_pct, signal_delay_ms_a, signal_delay_ms_b)

                    # ✅ 如果正在执行，跳过开仓检查
                    if self._executing_lock.locked():
                        return
                    if self._is_executed is True:
                        logger.info('开仓信号已经执行过了，直接返回')
                        return
                    if risk_block_open:
                        return
                    # ✅ 检查开仓信号（如果可以开仓）
                    await self._check_open_signal(prices, spread_pct, signal_delay_ms_a, signal_delay_ms_b)
                else:
                    # ✅ 正仓位时，open 方向是减风险；0 仓位时 open 属于增风险
                    if current_qty > 0:
                        await self._check_open_signal(prices, spread_pct, signal_delay_ms_a, signal_delay_ms_b)
                    elif not risk_block_open:
                        await self._check_open_signal(prices, spread_pct, signal_delay_ms_a, signal_delay_ms_b)
                    else:
                        return

                        # ✅ 如果正在执行，跳过开仓检查
                    if self._executing_lock.locked():
                        return
                    if self._is_executed is True:
                        logger.info('平仓已经执行过了，直接返回')
                        return 
                    if risk_block_open:
                        return
                    # ✅ 检查开仓信号（如果可以开仓）
                    await self._check_close_signal(prices, reverse_spread_pct, signal_delay_ms_a, signal_delay_ms_b)
                
            else:
                # ✅ 根据持仓状态决定检查哪种信号
                if not self.position_manager.has_position():
                    # 无持仓，检查开仓信号
                    if risk_block_open:
                        return
                    await self._check_open_signal(prices, spread_pct, signal_delay_ms_a, signal_delay_ms_b)
                else:
                    # 有持仓，检查平仓信号
                    await self._check_close_signal(prices, reverse_spread_pct, signal_delay_ms_a, signal_delay_ms_b)
            
            self.check_yaml_config_updates()
            if self.end_time_stamp:
                current_timestamp = time.time()
                if current_timestamp >= self.end_time_stamp:
                    logger.info(f"⏰ 达到策略结束时间，开始减仓到0")
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
                        logger.info(
                            self._build_equity_loss_summary(
                                equity_a=equity_a,
                                equity_b=equity_b,
                                volume_delta=volume_b,
                                volume_label="B所交易量",
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

    async def _check_open_signal(self, prices: PriceSnapshot, spread_pct: Decimal, signal_delay_ms_a: float, signal_delay_ms_b: float):
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
        if self.signal_mode == 'quantile' and self.quantile_manager:
            open_q, _ = self._get_quantile_thresholds()
            if open_q is None:
                return
            avg_local_spread_pct = self._calculate_avg_local_spread_pct(prices)
            compare_spread_pct = spread_pct - avg_local_spread_pct
            threshold_pct = open_q
            threshold_label = f"P{int(self.signal_quantile * 100)}"
            spread_label = "修正价差"
            extra_spread_info = (
                f"   平均点差: {avg_local_spread_pct:.4f}%\n"
                f"   原始价差: {spread_pct:.4f}%\n"
            )

        if compare_spread_pct >= threshold_pct:
            self.signal_stats['open']['total'] += 1
            # 记录信号触发时间
            signal_trigger_time = time.time()

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
            if self.edge_filter_enabled:
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

            # ✅ 检查是否为监控模式
            if self.monitor_only:
                # logger.info("📊 监控模式：不执行开仓，创建虚拟持仓以监控平仓信号")
                self.signal_stats['open']['executed'] += 1

                # ✅ 创建虚拟持仓（用于模拟）
                virtual_position = Position(
                    symbol=self.symbol,
                    quantity=self.position_manager.position_step,
                    exchange_a_name=self.exchange_a.exchange_name,
                    exchange_b_name=self.exchange_b.exchange_name,
                    exchange_a_signal_entry_price=prices.exchange_a_bid,
                    exchange_b_signal_entry_price=prices.exchange_b_ask,
                    exchange_a_entry_price=prices.exchange_a_bid,
                    exchange_b_entry_price=prices.exchange_b_ask,
                    exchange_a_order_id='MONITOR_A',
                    exchange_b_order_id='MONITOR_B',
                    spread_pct=spread_pct,
                    signal_entry_time=signal_trigger_time
                )

                self.position_manager.set_position(virtual_position)
                self._last_execution_time = time.time()
                await asyncio.sleep(0.06)  # 模拟异步行为
                
                # 发送飞书通知（可选）
                if self.lark_bot:
                    if self.position_manager.accumulate_mode:
                        await self._send_multi_notification('short', position, spread_pct)
                    else:
                        await self._send_open_notification(position, prices)

                return
            
            async with self._executing_lock:
                if self.position_manager.accumulate_mode:
                    if not self.position_manager.can_open('short'):
                        logger.warning("⏳ 开仓操作期间仓位已达阈值，跳过本次开仓")
                        # 统计次数
                        self.signal_stats['open']['skipped'] += 1
                        return
                else:
                    if self.position_manager.has_position():
                        logger.warning("⏳ 开仓操作期间已有持仓，跳过本次开仓")
                        return
                    
                if self.order_limiter_a:
                    if self.order_limiter_a.has_capacity():
                        await self.order_limiter_a.acquire()
                    else:
                        logger.info(f"⏳ 开仓操作限流器限流中，直接返回（Exchange A）")
                        self.signal_stats['open']['limited_a'] += 1
                        return
                if self.order_limiter_b:
                    if self.order_limiter_b.has_capacity():
                        await self.order_limiter_b.acquire()
                    else:
                        logger.info(f"⏳ 开仓操作限流器限流中，直接返回（Exchange B）")
                        self.signal_stats['open']['limited_b'] += 1
                        return
                    
                self._is_executing = True

                try:
                    # 实际交易模式：执行开仓
                    success, position = await self.executor.execute_open(
                        exchange_a_price=prices.exchange_a_bid,
                        exchange_b_price=prices.exchange_b_ask,
                        spread_pct=spread_pct,
                        exchange_a_quote_id=prices.exchange_a_quote_id,
                        exchange_b_quote_id=prices.exchange_b_quote_id,
                        signal_trigger_time=signal_trigger_time,
                        actual_quantity=self.position_manager.position_step
                    )
                    
                    if success:
                        self.signal_stats['open']['executed'] += 1
                        self._is_executed = True

                        self._last_execution_time = time.time()

                        # ✅ 累计模式：添加仓位
                        if self.position_manager.accumulate_mode:
                            self.position_manager.add_position(position, 'short', signal_delay_ms_a, signal_delay_ms_b)
                        else:
                            self.position_manager.set_position(position)

                        # summary = self.position_manager.get_position_summary()
                        # logger.info(
                        #     f"✅ 开仓成功: {position}\n"
                        #     f"📊 仓位状态: {summary['direction']} {summary['current_qty']:+} / ±{summary['max_position']} ({summary['utilization']}%)\n"
                        #     f"📊 统计: {self._format_open_stats()}"
                        # )

                        await asyncio.sleep(2)
                        logger.info(f"🔍 开仓后校验仓位...")
                        expected_qty = self.position_manager.get_current_position_qty()

                        is_consistent = await self.position_manager.verify_and_sync(
                            exchange_a=self.exchange_a,
                            exchange_b=self.exchange_b,
                            symbol_a=self.symbol_a,
                            symbol_b=self.symbol_b,
                            expected_qty=expected_qty,
                            tolerance=self.quantity_precision * 10
                        )
                        
                        if not is_consistent:
                            logger.warning(f"⚠️ 开仓后仓位校验不一致，已自动修正")           
                        logger.info("🔍 开仓后检查仓位平衡...")
                        await self.executor.check_position_balance()

                        # 发送飞书通知
                        if self.lark_bot:
                            if self.position_manager.accumulate_mode:
                                await self._send_multi_notification('short', position, spread_pct)
                            else:
                                await self._send_open_notification(position, prices)

                    else:
                        await asyncio.sleep(2)
                        await self.executor.check_position_balance()

                        # ✅ 节流日志：每5秒最多输出一次
                        if current_time - self.last_log_time >= self.log_interval:
                            logger.debug(
                                f"📊 当前价差: {spread_pct:.4f}% "
                                f"({spread_label}: {compare_spread_pct:.4f}%, {threshold_label}: {threshold_pct:.4f}%) - 监控开仓中..."
                            )
                            self.last_log_time = current_time

                finally:
                    self._is_executing = False
            self._log_stats_if_needed()

    async def _check_close_signal(self, prices: PriceSnapshot, spread_pct: Decimal, signal_delay_ms_a: float, signal_delay_ms_b: float):
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
        if self.signal_mode == 'quantile' and self.quantile_manager:
            _, close_q = self._get_quantile_thresholds()
            if close_q is None:
                return
            avg_local_spread_pct = self._calculate_avg_local_spread_pct(prices)
            compare_spread_pct = spread_pct - avg_local_spread_pct
            threshold_pct = close_q
            threshold_label = f"P{int(self.signal_quantile * 100)}"
            spread_label = "修正价差"
            extra_spread_info = (
                f"   平均点差: {avg_local_spread_pct:.4f}%\n"
                f"   原始价差: {spread_pct:.4f}%\n"
            )

        if compare_spread_pct >= threshold_pct:
            self.signal_stats['close']['total'] += 1

            # 记录信号触发时间
            signal_trigger_time = time.time()

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
            if self.edge_filter_enabled:
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
            
            # ✅ 检查是否为监控模式
            if self.monitor_only:
                self.signal_stats['close']['executed'] += 1

                # ✅ 累计模式：减少仓位
                if self.position_manager.accumulate_mode:
                    # ✅ 创建临时 Position 用于记录
                    temp_position = Position(
                        symbol=self.symbol,
                        quantity=self.position_manager.position_step,
                        exchange_a_name=self.exchange_a.exchange_name,
                        exchange_b_name=self.exchange_b.exchange_name,
                        exchange_a_signal_entry_price=current_position.exchange_a_entry_price if current_position else Decimal('0'),
                        exchange_b_signal_entry_price=current_position.exchange_b_entry_price if current_position else Decimal('0'),
                        exchange_a_entry_price=current_position.exchange_a_entry_price if current_position else Decimal('0'),
                        exchange_b_entry_price=current_position.exchange_b_entry_price if current_position else Decimal('0'),
                        exchange_a_order_id='MONITOR_CLOSE_A',
                        exchange_b_order_id='MONITOR_CLOSE_B',
                        spread_pct=spread_pct,
                        signal_entry_time=signal_trigger_time
                    )
                    
                    # 设置平仓价格
                    temp_position.exchange_a_signal_exit_price = prices.exchange_a_ask
                    temp_position.exchange_b_signal_exit_price = prices.exchange_b_bid
                    temp_position.exchange_a_exit_price = prices.exchange_a_ask
                    temp_position.exchange_b_exit_price = prices.exchange_b_bid
                    temp_position.exit_time = datetime.now()
                    
                    pnl_pct = self.position_manager.reduce_position(
                        temp_position,
                        'long',
                        signal_delay_ms_a,
                        signal_delay_ms_b
                    )
                    if self.position_manager.accumulate_mode:
                       await self._send_multi_notification('long', temp_position, spread_pct)
                else:
                    # ✅ 传统模式：先设置平仓价格，再平仓
                    current_position.exchange_a_signal_exit_price = prices.exchange_a_ask
                    current_position.exchange_b_signal_exit_price = prices.exchange_b_bid
                    current_position.exchange_a_exit_price = prices.exchange_a_ask
                    current_position.exchange_b_exit_price = prices.exchange_b_bid
                    current_position.exit_time = datetime.now()
                    
                    pnl_pct = self.position_manager.close_position(signal_delay_ms_a, signal_delay_ms_b)

                self._last_execution_time = time.time()
                
                # 发送飞书通知（可选）
                if self.lark_bot:
                    if self.position_manager.accumulate_mode:
                        await self._send_multi_notification('long', current_position, spread_pct)
                    else:
                        await self._send_close_notification(current_position, pnl_pct, prices)
                return
            
            async with self._executing_lock:
                if self.position_manager.accumulate_mode:
                    if not self.position_manager.can_open('long'):
                        logger.warning("⏳ 反向开仓操作期间仓位已达阈值，跳过本次反向开仓")
                        # 统计次数
                        self.signal_stats['close']['skipped'] += 1
                        return
                else:
                    if not self.position_manager.has_position():
                        logger.warning("⏳ 获取锁后发现持仓已清空，取消平仓")
                        return
                    
                if self.order_limiter_a:
                    if self.order_limiter_a.has_capacity():
                        await self.order_limiter_a.acquire()
                    else:
                        logger.info(f"⏳ 反向开仓操作限流器限流中，直接返回（Exchange A）")
                        self.signal_stats['close']['limited_a'] += 1
                        return
                if self.order_limiter_b:
                    if self.order_limiter_b.has_capacity():
                        await self.order_limiter_b.acquire()
                    else:
                        logger.info(f"⏳ 反向开仓操作限流器限流中，直接返回（Exchange B）")
                        self.signal_stats['close']['limited_b'] += 1
                        return
                
                self._is_executing = True

                try:
                    # 实际交易模式：执行平仓
                    if self.position_manager.accumulate_mode:
                        close_quantity = self.position_manager.position_step
                    else:
                        close_quantity = current_position.quantity if current_position else self.quantity
                    
                    success, position = await self.executor.execute_close(
                        position=current_position or self._create_dummy_position(),
                        exchange_a_price=prices.exchange_a_ask,
                        exchange_b_price=prices.exchange_b_bid,
                        exchange_a_quote_id=prices.exchange_a_quote_id,
                        exchange_b_quote_id=prices.exchange_b_quote_id,
                        signal_trigger_time=signal_trigger_time,
                        close_quantity=close_quantity,
                        execution_context='reverse_open' if self.position_manager.accumulate_mode else 'strategy',
                    )
                    
                    if success:
                        self.signal_stats['close']['executed'] += 1
                        self._is_executed = True

                        self._last_execution_time = time.time()

                        # ✅ 累计模式：减少仓位
                        if self.position_manager.accumulate_mode:
                            pnl_pct = self.position_manager.reduce_position(
                                position,
                                'long',
                                signal_delay_ms_a,
                                signal_delay_ms_b
                            )
                        else:
                            self.position_manager.position = position
                            pnl_pct = self.position_manager.close_position(
                                signal_delay_ms_a,
                                signal_delay_ms_b
                            )
                        
                        summary = self.position_manager.get_position_summary()
                        # logger.info(
                        #     f"✅ 反向开仓成功: {position}\n"
                        #     f"📊 仓位状态: {summary['direction']} {summary['current_qty']:+} / ±{summary['max_position']} ({summary['utilization']}%)\n"
                        #     f"📊 统计: {self._format_close_stats()}"
                        # )
                        await asyncio.sleep(2)

                        logger.info(f"🔍 反向开仓后校验仓位...")
                        expected_qty = self.position_manager.get_current_position_qty()
                        is_consistent = await self.position_manager.verify_and_sync(
                            exchange_a=self.exchange_a,
                            exchange_b=self.exchange_b,
                            symbol_a=self.symbol_a,
                            symbol_b=self.symbol_b,
                            expected_qty=expected_qty,
                            tolerance=self.quantity_precision * 10
                        )
                        
                        if not is_consistent:
                            logger.warning("⚠️ 反向开仓后仓位不一致，已自动修正") 
                        logger.info("🔍 反向开仓后检查仓位平衡...")
                        await self.executor.check_position_balance()
                        
                        # 发送飞书通知
                        if self.lark_bot:
                            if self.position_manager.accumulate_mode:
                                await self._send_multi_notification('long', position, spread_pct)
                            else:
                                await self._send_close_notification(position, pnl_pct, prices)

                    else:
                        await asyncio.sleep(2)
                        await self.executor.check_position_balance()

                        if current_time - self.last_log_time >= self.log_interval:
                            # ✅ 节流日志：每5秒最多输出一次
                            logger.info(
                                f"📊 当前价差: {spread_pct:.4f}% "
                                f"({spread_label}: {compare_spread_pct:.4f}%, {threshold_label}: {threshold_pct:.4f}%) - 监控反向开仓中..."
                            )
                            self.last_log_time = current_time
                finally:
                    self._is_executing = False
            self._log_stats_if_needed()

    def check_yaml_config_updates(self):
        """检查 YAML 配置文件更新"""
        current_path = os.getcwd()
        config_path = self.config_yaml_path
        if not os.path.exists(config_path):
            return
        # logger.debug(f"🔍 检查 YAML 配置文件更新: {config_path}")
         # 检查间隔
        if self._last_yaml_check_time is None:
            self._last_yaml_check_time = time.time()
        elif time.time() - self._last_yaml_check_time >= self._yaml_check_interval:
            self._last_yaml_check_time = time.time()

            try:
                with open(config_path, 'r') as f:
                    new_config = yaml.safe_load(f)
                enabled = new_config.get('enabled', False)
                if enabled:
                    new_max_position = Decimal(str(new_config.get('max_position', self.position_manager.max_position)))
                    if new_max_position != self.position_manager.max_position:
                        logger.info(f"🔄 从 YAML 配置更新 max_position: {self.position_manager.max_position} --> {new_max_position}")
                        self.position_manager.max_position = new_max_position
                        self.risk_control_service.set_base_max_position(new_max_position)
                    
            except Exception as e:
                logger.exception(f"⚠️ 检查 YAML 配置文件时出错: {e}")

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
