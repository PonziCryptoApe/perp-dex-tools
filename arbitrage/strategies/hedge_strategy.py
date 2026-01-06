"""对冲套利策略"""

import asyncio
from datetime import datetime
import logging
import time
from decimal import Decimal
from typing import Optional
from .base_strategy import BaseStrategy
from ..models.prices import PriceSnapshot
from ..services.price_monitor import PriceMonitorService
from ..services.position_manager import PositionManagerService
from ..services.order_executor_parallel import OrderExecutor
from ..services.dynamic_threshold import DynamicThresholdManager
from ..models.position import Position

logger = logging.getLogger(__name__)

class HedgeStrategy(BaseStrategy):
    """对冲套利策略"""
    
    def __init__(
        self,
        symbol: str,
        quantity: Decimal,
        quantity_precision: Decimal,
        open_threshold_pct: float,
        close_threshold_pct: float,
        exchange_a,
        exchange_b,
        lark_bot=None,
        monitor_only: bool = False,
        trade_logger=None,
        max_signal_delay_ms: int = 200,
        min_depth_quantity: Decimal = Decimal('0.01'),
        accumulate_mode: bool = False,
        max_position: Decimal = Decimal('0.1'),
        direction_reverse: bool = False, # 默认负滑点方向才下单
        dynamic_threshold: Optional[dict] = None,
    ):
        super().__init__(
            strategy_name=f"Hedge-{symbol}",
            symbol=symbol,
            quantity=quantity,
            quantity_precision=quantity_precision
        )
        
        self.open_threshold_pct = open_threshold_pct
        self.close_threshold_pct = close_threshold_pct
        self.exchange_a = exchange_a
        self.exchange_b = exchange_b
        self.lark_bot = lark_bot
        self.monitor_only = monitor_only
        self.max_signal_delay_ms = max_signal_delay_ms
        self.max_signal_delay_ms_a = 200
        self.max_signal_delay_ms_b = 60
        self.min_depth_quantity = min_depth_quantity
        self.direction_reverse = direction_reverse

        # ✅ 使用 PositionManagerService 管理持仓
        self.position_manager = PositionManagerService(
            trade_logger=trade_logger,
            accumulate_mode=accumulate_mode,
            max_position=max_position,
            position_step=quantity
        )

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
            quantity_precision=quantity_precision
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
        self._last_open_time = 0
        self._last_close_time = 0
        if accumulate_mode:
            self._cooldown_seconds = 0.5  # 累计模式：0.5秒（快速反应）
        else:
            self._cooldown_seconds = 5  # 传统模式：5秒
        
        self.signal_stats = {
            # 开仓信号统计
            'open': {
                'total': 0,              # 总信号数（满足阈值）
                'delay_filtered': 0,     # 因延迟过滤
                'depth_insufficient': 0, # 因深度不足跳过
                'depth_adjusted': 0,     # 因深度调整数量
                'executed': 0            # 实际执行
            },
            # 平仓信号统计
            'close': {
                'total': 0,
                'delay_filtered': 0,
                'depth_insufficient': 0,
                'executed': 0
            }
        }
        # ✅ 定期输出统计（可选）
        self._last_stats_log_time = 0
        self._stats_log_interval = 60  # 每 60 秒输出一次统计
        # self._last_threshold_check_time = None
        # 动态阈值管理器
        dt_config = dynamic_threshold
        if dt_config.get('enabled', False):
            self.threshold_manager = DynamicThresholdManager(
                sample_size=dt_config.get('sample_size', 1000),
                min_samples=dt_config.get('min_samples', 200),
                std_multiplier=dt_config.get('std_multiplier', 1.0),
                min_total_threshold=dt_config.get('min_total_threshold', 0.02),
            )
        else:
            self.threshold_manager = None
        
        
        logger.info(
            f"🎯 策略配置:\n"
            f"   Symbol: {symbol}\n"
            f"   Quantity: {quantity}\n"
            f"   Open Threshold: {open_threshold_pct}%\n"
            f"   Close Threshold: {close_threshold_pct}%\n"
            f"   Exchange A: {exchange_a.exchange_name}\n"
            f"   Exchange B: {exchange_b.exchange_name}\n"
            f"   Monitor Only: {monitor_only}\n"
            f"   累计模式: {'✅ 启用' if accumulate_mode else '❌ 禁用'}"
        )
    
    async def start(self):
        """启动策略"""
        logger.info(f"🚀 启动策略: {self.strategy_name}")
        
        # 启动价格监控
        await self.monitor.start()
        # ✅ 新增：启动时同步仓位
        if self.position_manager.accumulate_mode:
            logger.info("🔄 累计模式启动，同步交易所仓位...")
            synced_qty = await self.position_manager.sync_from_exchanges(
                exchange_a=self.exchange_a,
                exchange_b=self.exchange_b,
                symbol=self.symbol
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
        
        # 订阅价格更新
        self.monitor.subscribe(self._on_price_update)
        
        self.is_running = True
        logger.info(f"✅ 策略已启动: {self.strategy_name}")
    
    async def stop(self):
        """停止策略"""
        logger.info(f"⏹️ 停止策略: {self.strategy_name}")
        
        self.is_running = False
        
        # 停止价格监控
        await self.monitor.stop()
        
        logger.info(f"✅ 策略已停止: {self.strategy_name}")
    
    async def _on_price_update(self, prices: PriceSnapshot):
        """
        处理价格更新
        
        ✅ 核心逻辑：
        - 无持仓时：只检查开仓信号
        - 有持仓时：只检查平仓信号
        """
        if not self.is_running:
            return
        is_stale, stale_msg = self.monitor.is_orderbook_stale(self.max_signal_delay_ms / 1000)
        if is_stale:
            # logger.warning(f"⚠️ 订单簿过时，丢弃信号: {stale_msg}")
            return
        try:
            # ✅ 记录价格更新的时间
            price_update_time_a = prices.exchange_a_timestamp
            price_update_time_b = prices.exchange_b_timestamp

            # 记录信号触发时间
            signal_trigger_time = time.time()
            signal_delay_ms_a = (signal_trigger_time - price_update_time_a) * 1000
            signal_delay_ms_b = (signal_trigger_time - price_update_time_b) * 1000
        
            signal_flag = False
            # ✅ 过滤延迟过大的信号
            if signal_delay_ms_a <= self.max_signal_delay_ms and signal_delay_ms_b <= self.max_signal_delay_ms:
                signal_flag = True
            else:
                logger.warning(f"⚠️ 信号延迟过大: A {signal_delay_ms_a:.2f} ms（阈值: {self.max_signal_delay_ms} ms），"
                               f" B {signal_delay_ms_b:.2f} ms（阈值: {self.max_signal_delay_ms} ms）")
                return  # 丢弃该信号
            # 计算价差
            spread_pct = prices.calculate_spread_pct()
            reverse_spread_pct = prices.calculate_reverse_spread_pct()
            # if self._last_threshold_check_time is None:
                # self._last_threshold_check_time = time.time()
            # now = time.time()
            # ✅ 新增：记录价差并尝试调整阈值
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

            if self.position_manager.accumulate_mode:
                current_qty = self.position_manager.get_current_position_qty()
                logger.info(f"🔍 当前strategy仓位: {current_qty:+.4f}")
                if current_qty < 0:
                    # ✅ 优先检查平仓信号（如果可以平仓）
                    if self.position_manager.can_open('long'):
                        await self._check_close_signal(prices, reverse_spread_pct, signal_delay_ms_a, signal_delay_ms_b)

                        # ✅ 如果正在执行，跳过开仓检查
                        if self._executing_lock.locked():
                            return
                
                    # ✅ 检查开仓信号（如果可以开仓）
                    if self.position_manager.can_open('short'):
                        await self._check_open_signal(prices, spread_pct, signal_delay_ms_a, signal_delay_ms_b)
                else:
                    # ✅ 优先检查平仓信号（如果可以平仓）
                    if self.position_manager.can_open('short'):
                        await self._check_open_signal(prices, spread_pct, signal_delay_ms_a, signal_delay_ms_b)

                        # ✅ 如果正在执行，跳过开仓检查
                        if self._executing_lock.locked():
                            return
                
                    # ✅ 检查开仓信号（如果可以开仓）
                    if self.position_manager.can_open('long'):
                        await self._check_close_signal(prices, reverse_spread_pct, signal_delay_ms_a, signal_delay_ms_b)
                
            else:
                # ✅ 根据持仓状态决定检查哪种信号
                if not self.position_manager.has_position():
                    # 无持仓，检查开仓信号
                    await self._check_open_signal(prices, spread_pct, signal_delay_ms_a, signal_delay_ms_b)
                else:
                    # 有持仓，检查平仓信号
                    await self._check_close_signal(prices, reverse_spread_pct, signal_delay_ms_a, signal_delay_ms_b)

        except Exception as e:
            logger.error(f"❌ 价格更新处理失败: {e}")
            import traceback
            traceback.print_exc()

    async def _check_open_signal(self, prices: PriceSnapshot, spread_pct: Decimal, signal_delay_ms_a: float, signal_delay_ms_b: float):
        """
        检查开仓信号
        
        ✅ 监控模式下，会创建虚拟持仓（不实际下单）
        """
        # ✅ 累计模式：检查是否可以开空
        if not self.position_manager.accumulate_mode:
            # 开仓信号 = Extended 开空（卖出），Variational 开多（买入）
            # ✅ 传统模式：检查是否有持仓
            if self.position_manager.has_position():
                return
        
        # ✅ 检查冷却期
        current_time = time.time()
        cooldown_remaining = self._cooldown_seconds - (current_time - self._last_open_time)
        if cooldown_remaining > 0:
            return
        
        # ✅ 如果正在执行开仓，跳过
        if self._executing_lock.locked():
            # logger.debug("⏳ 另一个操作正在执行，跳过本次信号")
            return
        
        current_time = time.time()
        base_direction = prices.calculate_direction_b('long')
        direction_ok = base_direction if not self.direction_reverse else not base_direction
        # 判断是否满足开仓阈值
        if spread_pct >= Decimal(str(self.open_threshold_pct)) and direction_ok:
            self.signal_stats['open']['total'] += 1
            # 记录信号触发时间
            signal_trigger_time = time.time()
            # signal_delay_ms_a = (signal_trigger_time - price_update_time_a) * 1000
            # signal_delay_ms_b = (signal_trigger_time - price_update_time_b) * 1000
        
            # ✅ 过滤延迟过大的信号
            if signal_delay_ms_a > self.max_signal_delay_ms or signal_delay_ms_b > self.max_signal_delay_ms:
                self.signal_stats['open']['delay_filtered'] += 1

                # logger.warning(
                #     f"⚠️ 开仓信号延迟过大，已过滤:\n"
                #     f"   延迟_a: {signal_delay_ms_a:.2f} ms (阈值: {self.max_signal_delay_ms} ms)\n"
                #     f"   延迟_b: {signal_delay_ms_b:.2f} ms (阈值: {self.max_signal_delay_ms} ms)\n"
                #     f"   {self.exchange_a.exchange_name}_bid: ${prices.exchange_a_bid}\n"
                #     f"   {self.exchange_a.exchange_name}_bid_size: {prices.exchange_a_bid_size}\n"
                #     f"   {self.exchange_b.exchange_name}_ask: ${prices.exchange_b_ask}\n"
                #     f"   {self.exchange_b.exchange_name}_ask_size: {prices.exchange_b_ask_size}\n"
                #     f"   价差: {spread_pct:.4f}% (阈值: {self.open_threshold_pct}%)\n"
                #     f"   数量: {self.quantity}"
                # )
                # return  # ✅ 丢弃该信号
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
                    f"⚠️ 深度不足，跳过开仓:\n"
                    f"   {self.exchange_a.exchange_name} 买一深度: {depth_a}\n"
                    f"   {self.exchange_b.exchange_name} 卖一深度: {depth_b}\n"
                    f"   最小深度: {min_depth} < 阈值: {self.min_depth_quantity}\n"
                    f"   价差: {spread_pct:.4f}% (阈值: {self.open_threshold_pct}%)"
                )
                return
            
            # ✅ 动态调整数量（取配置数量和深度的最小值）
            actual_quantity = min(self.quantity, depth_a, depth_b)
            
            # ✅ 如果调整后数量小于最小阈值，跳过
            if actual_quantity < self.min_depth_quantity:
                self.signal_stats['open']['depth_insufficient'] += 1

                logger.warning(
                    f"⚠️ 调整后数量不足，跳过开仓:\n"
                    f"   配置数量: {self.quantity}\n"
                    f"   {self.exchange_a.exchange_name} 买一深度: {depth_a}\n"
                    f"   {self.exchange_b.exchange_name} 卖一深度: {depth_b}\n"
                    f"   调整后数量: {actual_quantity} < 阈值: {self.min_depth_quantity}"
                )
                return
            
            # ✅ 如果数量被调整，记录日志
            if actual_quantity < self.quantity:
                self.signal_stats['open']['depth_adjusted'] += 1

                # logger.info(
                #     f"💡 根据深度调整下单数量:\n"
                #     f"   配置数量: {self.quantity}\n"
                #     f"   {self.exchange_a.exchange_name} 买一深度: {depth_a}\n"
                #     f"   {self.exchange_b.exchange_name} 卖一深度: {depth_b}\n"
                #     f"   实际数量: {actual_quantity} (调整: {((actual_quantity - self.quantity) / self.quantity * 100):+.2f}%)"
                # )
                return
            
            logger.info(
                f"🔔 检测到开仓信号:\n"
                f"   延迟_a: {signal_delay_ms_a:.2f} ms (阈值: {self.max_signal_delay_ms} ms)\n"
                f"   延迟_b: {signal_delay_ms_b:.2f} ms (阈值: {self.max_signal_delay_ms} ms)\n"
                f"   {self.exchange_a.exchange_name}_bid: ${prices.exchange_a_bid}\n"
                f"   {self.exchange_a.exchange_name}_bid_size: {prices.exchange_a_bid_size}\n"
                f"   {self.exchange_b.exchange_name}_ask: ${prices.exchange_b_ask}\n"
                f"   {self.exchange_b.exchange_name}_ask_size: {prices.exchange_b_ask_size}\n"
                f"   价差: {spread_pct:.4f}% (阈值: {self.open_threshold_pct}%)\n"
                f"   数量: {actual_quantity}"
            )

            self.open_signal_count += 1

            # ✅ 检查是否为监控模式
            if self.monitor_only:
                # logger.info("📊 监控模式：不执行开仓，创建虚拟持仓以监控平仓信号")
                self.signal_stats['open']['executed'] += 1

                # ✅ 创建虚拟持仓（用于模拟）
                virtual_position = Position(
                    symbol=self.symbol,
                    quantity=actual_quantity,
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
                self._last_open_time = time.time()
                await asyncio.sleep(0.06)  # 模拟异步行为
                
                # 发送飞书通知（可选）
                if self.lark_bot:
                    if self.position_manager.accumulate_mode:
                        await self._send_multi_notification('short', position, spread_pct)
                    else:
                        await self._send_open_notification(position, prices)

                return
            async with self._executing_lock:
                # ✅ 累计模式：再次检查（防止并发）
                if self.position_manager.accumulate_mode:
                    if not self.position_manager.can_open('short'):
                        logger.warning("⏳ 开仓操作期间仓位已达阈值，跳过本次开仓")
                        return
                else:
                    if self.position_manager.has_position():
                        logger.warning("⏳ 开仓操作期间已有持仓，跳过本次开仓")
                        return
                
                self._is_executing = True
                # if self.position_manager.has_position():
                #     logger.warning("⏳ 开仓操作期间已有持仓，跳过本次开仓")
                #     return
                # self._is_executing = True
                try:
                    # 实际交易模式：执行开仓
                    success, position = await self.executor.execute_open(
                        exchange_a_price=prices.exchange_a_bid,
                        exchange_b_price=prices.exchange_b_ask,
                        spread_pct=spread_pct,
                        exchange_a_quote_id=prices.exchange_a_quote_id,
                        exchange_b_quote_id=prices.exchange_b_quote_id,
                        signal_trigger_time=signal_trigger_time,
                        actual_quantity=actual_quantity
                    )
                    
                    if success:
                        self.signal_stats['open']['executed'] += 1

                        # ✅ 累计模式：添加仓位
                        if self.position_manager.accumulate_mode:
                            self.position_manager.add_position(position, 'short', signal_delay_ms_a, signal_delay_ms_b)
                        else:
                            self.position_manager.set_position(position)

                        self._last_open_time = time.time()

                        summary = self.position_manager.get_position_summary()

                        logger.info(
                            f"✅ 开仓成功: {position}\n"
                            f"📊 仓位状态: {summary['direction']} {summary['current_qty']:+} / ±{summary['max_position']} ({summary['utilization']}%)\n"
                            f"📊 统计: {self._format_open_stats()}"  # ✅ 新增
                        )
                        # ========== ✅ 新增：校验仓位 ==========
                        # logger.info(f"🔍 开仓后校验仓位...")
                        # expected_qty = self.position_manager.get_current_position_qty()
                        
                        # is_consistent = await self.position_manager.verify_and_sync(
                        #     exchange_a=self.exchange_a,
                        #     exchange_b=self.exchange_b,
                        #     symbol=self.symbol,
                        #     expected_qty=expected_qty,
                        #     tolerance=self.quantity_precision
                        # )
                        
                        # if not is_consistent:
                        #     logger.warning(f"⚠️ 开仓后仓位校验不一致，已自动修正为交易所实际值")
                        # ========== 新增部分结束 ==========
                        # logger.info(f"✅ 开仓成功: {position}，等待平仓...")
                        # logger.info("🔍 开仓后检查仓位平衡...")
                        await self.executor.check_position_balance()

                        # 发送飞书通知
                        if self.lark_bot:
                            if self.position_manager.accumulate_mode:
                                await self._send_multi_notification('short', position, spread_pct)
                            else:
                                await self._send_open_notification(position, prices)

                    else:
                        # ✅ 节流日志：每5秒最多输出一次
                        if current_time - self.last_log_time >= self.log_interval:
                            logger.debug(
                                f"📊 当前价差: {spread_pct:.4f}% "
                                f"(开仓阈值: {self.open_threshold_pct}%) - 监控开仓中..."
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
        position = self.position_manager.get_position()

        if not self.position_manager.accumulate_mode and position is None:
            return
        
        # ✅ 如果正在执行平仓，跳过
        if self._executing_lock.locked():
            return
        
        current_time = time.time()
        base_direction = prices.calculate_direction_b('short')
        direction_ok = base_direction if not self.direction_reverse else not base_direction

        # 判断是否满足平仓阈值
        if spread_pct >= Decimal(str(self.close_threshold_pct)) and direction_ok:
            self.signal_stats['close']['total'] += 1

            # 记录信号触发时间
            signal_trigger_time = time.time()

            # ✅ 计算延迟（价格更新 → 信号触发）
            # signal_delay_ms_a = (signal_trigger_time - price_update_time_a) * 1000
            # signal_delay_ms_b = (signal_trigger_time - price_update_time_b) * 1000

            # ✅ 过滤延迟过大的信号
            # if signal_delay_ms_a > self.max_signal_delay_ms or signal_delay_ms_b > self.max_signal_delay_ms:
            #     self.signal_stats['close']['delay_filtered'] += 1

            #     # 计算当前盈亏（仅用于日志）
            #     if position:
            #         pnl_pct = position.calculate_pnl_pct(
            #             exchange_a_price=prices.exchange_a_ask,
            #             exchange_b_price=prices.exchange_b_bid
            #         )
            #     else:
            #         pnl_pct = Decimal('0')
                
            #     logger.warning(
            #         f"⚠️ 反向开仓信号延迟过大，已过滤:\n"
            #         f"   延迟_a: {signal_delay_ms_a:.2f} ms (阈值: {self.max_signal_delay_ms} ms)\n"
            #         f"   延迟_b: {signal_delay_ms_b:.2f} ms (阈值: {self.max_signal_delay_ms} ms)\n"
            #         f"   {self.exchange_a.exchange_name}_ask: ${prices.exchange_a_ask}\n"
            #         f"   {self.exchange_a.exchange_name}_ask_size: {prices.exchange_a_ask_size}\n"
            #         f"   {self.exchange_b.exchange_name}_bid: ${prices.exchange_b_bid}\n"
            #         f"   {self.exchange_b.exchange_name}_bid_size: {prices.exchange_b_bid_size}\n"
            #         f"   价差: {spread_pct:.4f}% (阈值: {self.close_threshold_pct}%)\n"
            #         f"   数量: {self.quantity}\n"
            #         f"   当前盈亏: {pnl_pct:.4f}%\n"
            #         f"   持仓时长: {position.get_holding_duration() if position else 'N/A'}\n"
            #     )
            #     return  # ✅ 丢弃该信号
            # ========== ✅ 新增：检查平仓深度 ==========
            # Exchange A: 买入平空（使用卖一深度）
            depth_a = prices.exchange_a_ask_size
            # Exchange B: 卖出平多（使用买一深度）
            depth_b = prices.exchange_b_bid_size
            
            # ✅ 取最小深度
            min_depth = min(depth_a, depth_b)
            check_quantity = self.position_manager.position_step if self.position_manager.accumulate_mode else (position.quantity if position else self.quantity)

            # ✅ 检查深度是否足够（必须 >= 持仓数量）
            if min_depth < check_quantity:
                self.signal_stats['close']['depth_insufficient'] += 1

                if position:
                    pnl_pct = position.calculate_pnl_pct(
                        exchange_a_price=prices.exchange_a_ask,
                        exchange_b_price=prices.exchange_b_bid
                    )
                else:
                    pnl_pct = Decimal('0')
                
                logger.warning(
                    f"⚠️ 反向开仓深度不足，跳过:\n"
                    f"   {self.exchange_a.exchange_name} 卖一深度: {depth_a}\n"
                    f"   {self.exchange_b.exchange_name} 买一深度: {depth_b}\n"
                    f"   最小深度: {min_depth} < 持仓数量: {check_quantity}\n"
                    f"   价差: {spread_pct:.4f}% (阈值: {self.close_threshold_pct}%)\n"
                    f"   当前盈亏: {pnl_pct:.4f}%\n"
                    f"   持仓时长: {position.get_holding_duration() if position else 'N/A'}\n"
                    f"   💡 等待更好的流动性..."
                )
                return
            
            # ✅ 如果深度远大于持仓数量，记录日志
            if min_depth >= check_quantity * Decimal('2.0'):
                logger.info(
                    f"💡 反向开仓深度充足:\n"
                    f"   {self.exchange_a.exchange_name} 卖一深度: {depth_a}\n"
                    f"   {self.exchange_b.exchange_name} 买一深度: {depth_b}\n"
                    f"   持仓数量: {check_quantity}\n"
                    f"   深度/持仓: {(min_depth / check_quantity):.2f}x"
                )
        
            self.close_signal_count += 1

            # 计算当前盈亏
            if position:
                pnl_pct = position.calculate_pnl_pct(
                    exchange_a_price=prices.exchange_a_ask,
                    exchange_b_price=prices.exchange_b_bid
                )
            else:
                pnl_pct = Decimal('0')

            # 计算最大延迟
            max_delay_ms = max(signal_delay_ms_a, signal_delay_ms_b)
            operation_type = "平仓" if position else "反向开仓"

            logger.info(
                f"🔔 {operation_type}信号 #{self.close_signal_count}:\n"
                f"   {self.exchange_a.exchange_name}_ask: ${prices.exchange_a_ask}\n"
                f"   {self.exchange_a.exchange_name}_ask_size: {prices.exchange_a_ask_size}\n"
                f"   {self.exchange_b.exchange_name}_bid: ${prices.exchange_b_bid}\n"
                f"   {self.exchange_b.exchange_name}_bid_size: {prices.exchange_b_bid_size}\n"
                f"   价差: {spread_pct:.4f}%(阈值: {self.close_threshold_pct}%)\n"
                f"   数量: {check_quantity}\n"
                f"   盈亏: {pnl_pct:.4f}%\n"
                f"   持仓时长: {position.get_holding_duration() if position else 'N/A'}\n"
                f"   ⏱️ 延迟分析:\n"
                f"      Exchange A: {signal_delay_ms_a:.2f} ms\n"
                f"      Exchange B: {signal_delay_ms_b:.2f} ms\n"
                f"      最大延迟: {max_delay_ms:.2f} ms"
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
                        exchange_a_signal_entry_price=position.exchange_a_entry_price if position else Decimal('0'),
                        exchange_b_signal_entry_price=position.exchange_b_entry_price if position else Decimal('0'),
                        exchange_a_entry_price=position.exchange_a_entry_price if position else Decimal('0'),
                        exchange_b_entry_price=position.exchange_b_entry_price if position else Decimal('0'),
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
                    
                    pnl_pct = self.position_manager.reduce_position(temp_position, 'long')
                    if self.position_manager.accumulate_mode:
                       await self._send_multi_notification('long', temp_position, spread_pct)
                else:
                    # ✅ 传统模式：先设置平仓价格，再平仓
                    position.exchange_a_signal_exit_price = prices.exchange_a_ask
                    position.exchange_b_signal_exit_price = prices.exchange_b_bid
                    position.exchange_a_exit_price = prices.exchange_a_ask
                    position.exchange_b_exit_price = prices.exchange_b_bid
                    position.exit_time = datetime.now()
                    
                    pnl_pct = self.position_manager.close_position(signal_delay_ms_a, signal_delay_ms_b)

                self._last_close_time = time.time()
                
                # 发送飞书通知（可选）
                if self.lark_bot:
                    if self.position_manager.accumulate_mode:
                        await self._send_multi_notification('long', position, spread_pct)
                    else:
                        await self._send_close_notification(position, pnl_pct, prices)
                return
            
            async with self._executing_lock:
                if self.position_manager.accumulate_mode:
                    if not self.position_manager.can_open('long'):
                        logger.warning("⏳ 反向开仓操作期间超过阈值，跳过本次反向开仓")
                        return
                else:
                    if not self.position_manager.has_position():
                        logger.warning("⏳ 获取锁后发现持仓已清空，取消平仓")
                        return
                self._is_executing = True

                try:
                    # 实际交易模式：执行平仓
                    if self.position_manager.accumulate_mode:
                        close_quantity = self.position_manager.position_step
                    else:
                        close_quantity = position.quantity if position else self.quantity
                    
                    success, updated_position = await self.executor.execute_close(
                        position=position or self._create_dummy_position(),
                        exchange_a_price=prices.exchange_a_ask,
                        exchange_b_price=prices.exchange_b_bid,
                        exchange_a_quote_id=prices.exchange_a_quote_id,
                        exchange_b_quote_id=prices.exchange_b_quote_id,
                        signal_trigger_time=signal_trigger_time,
                        close_quantity=close_quantity
                    )
                    
                    if success:
                        self.signal_stats['close']['executed'] += 1

                        # ✅ 累计模式：减少仓位
                        if self.position_manager.accumulate_mode:
                            pnl_pct = self.position_manager.reduce_position(updated_position, 'long')
                        else:
                            self.position_manager.position = updated_position
                            pnl_pct = self.position_manager.close_position(
                                signal_delay_ms_a,
                                signal_delay_ms_b
                            )
                        
                        # ✅ 显示仓位摘要
                        summary = self.position_manager.get_position_summary()
                        logger.info(
                            f"✅ 反向开仓成功，盈亏: {pnl_pct:+.4f}%\n"
                            f"📊 仓位状态: {summary['direction']} {summary['current_qty']:+} / ±{summary['max_position']} ({summary['utilization']}%)\n"
                            f"📊 统计: {self._format_close_stats()}"
                        )
                        # logger.info(f"🔍 平仓后校验仓位...")
                        # expected_qty = self.position_manager.get_current_position_qty()
                        
                        # is_consistent = await self.position_manager.verify_and_sync(
                        #     exchange_a=self.exchange_a,
                        #     exchange_b=self.exchange_b,
                        #     symbol=self.symbol,
                        #     expected_qty=expected_qty,
                        #     tolerance=self.quantity_precision
                        # )
                        
                        # if not is_consistent:
                        #     logger.warning("⚠️ 平仓后仓位不一致，已自动修正") 
                        logger.info("🔍 反向开仓后检查仓位平衡...")

                        await self.executor.check_position_balance()
                               
                        # 发送飞书通知
                        if self.lark_bot:
                            if self.position_manager.accumulate_mode:
                                await self._send_multi_notification('long', updated_position, spread_pct)
                            else:
                                await self._send_close_notification(updated_position, pnl_pct, prices)

                        # 清除持仓
                        # self.position = None

                        self._last_close_time = time.time()
                    else:
                        if current_time - self.last_log_time >= self.log_interval:
                            # ✅ 节流日志：每5秒最多输出一次
                            logger.info(
                                f"📊 当前价差: {spread_pct:.4f}% "
                                f"(反向开仓阈值: {self.close_threshold_pct}%) - 监控反向开仓中..."
                            )
                            self.last_log_time = current_time
                finally:
                    self._is_executing = False
            self._log_stats_if_needed()

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
        if direction == 'long':
            title = f'对冲开多通知（{mode_text}）'
            a_slippage = actual_slippage['exit_a_slippage_pct'].quantize(Decimal('0.0001'))
            b_slippage = actual_slippage['exit_b_slippage_pct'].quantize(Decimal('0.0001'))
            total_slippage = actual_slippage['total_exit_slippage_pct'].quantize(Decimal('0.0001'))
            trigger_time = position.exit_time.strftime('%Y-%m-%d %H:%M:%S')
            threshold = self.close_threshold_pct
            qty = -self.quantity
        else: 
            title = f'对冲开空通知（{mode_text}）'
            a_slippage = actual_slippage['entry_a_slippage_pct'].quantize(Decimal('0.0001'))
            b_slippage = actual_slippage['entry_b_slippage_pct'].quantize(Decimal('0.0001'))
            total_slippage = actual_slippage['total_entry_slippage_pct'].quantize(Decimal('0.0001'))
            trigger_time = position.entry_time.strftime('%Y-%m-%d %H:%M:%S')
            threshold = self.open_threshold_pct
            qty = self.quantity
        current_position_qty = self.position_manager.get_current_position_qty().quantize(Decimal('0.0001'))
        message = (
            f"🔔 {title}\n\n"
            f"交易对: {self.symbol}\n"
            f"数量: {self.quantity}\n"
            f"当前仓位: {current_position_qty + qty} --> {current_position_qty}\n"
            f"信号价差: {spread_pct.quantize(Decimal('0.0001'))}%（阈值: {threshold}%）\n"
            f"总滑点: {total_slippage}%（A: {a_slippage}% B: {b_slippage}%）\n"
            f"开仓时间: {trigger_time}"
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
            logger.error(f"❌ 发送飞书通知失败: {e}")
    
    async def _send_close_notification(self, position: Position, pnl_pct: Decimal, prices: PriceSnapshot):
        """发送平仓通知"""
        try:
            # ✅ 根据模式调整通知内容
            mode_text = "虚拟" if self.monitor_only else "实际"
            
            # ✅ 检查 position 是否为 None
            if position is None:
                # ✅ 反向开仓：没有原始持仓信息
                message = (
                    f"🔔 对冲平仓通知 ({mode_text}) - 反向开仓\n\n"
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
                    f"🔔 对冲平仓通知 ({mode_text})\n\n"
                    f"交易对: {self.symbol}\n"
                    f"盈亏: {pnl_pct:.4f}%\n"
                    f"数量: {position.quantity}\n\n"
                    f"开仓信息:\n"
                    f"  {self.exchange_a.exchange_name}: ${position.exchange_a_entry_price}\n"
                    f"  {self.exchange_b.exchange_name}: ${position.exchange_b_entry_price}\n"
                    f"  信号价差: {position.spread_pct:.4f}%\n\n"
                    f"  实际价差: {actual_entry_spread_pct:.4f}%\n"  # ✅ 新增
                    f"  价差损失: {(position.spread_pct - actual_entry_spread_pct):.4f}%\n\n"  # ✅ 新增
                    f"平仓信息:\n"
                    f"  {self.exchange_a.exchange_name}: ${prices.exchange_a_ask}\n"
                    f"  {self.exchange_b.exchange_name}: ${prices.exchange_b_bid}\n\n"
                    f"  实际价差: {actual_exit_spread_pct:.4f}%\n"  # ✅ 新增
                    f"持仓时长: {position.get_holding_duration()}"
                )
            
            await self.lark_bot.send_text(message)
        except Exception as e:
            logger.error(f"❌ 发送飞书通知失败: {e}")
    def _format_open_stats(self) -> str:
        """格式化开仓统计信息"""
        stats = self.signal_stats['open']
        total = stats['total']
        
        if total == 0:
            return "无数据"
        
        # 计算比例
        delay_pct = (stats['delay_filtered'] / total * 100) if total > 0 else 0
        depth_pct = (stats['depth_insufficient'] / total * 100) if total > 0 else 0
        adjusted_pct = (stats['depth_adjusted'] / total * 100) if total > 0 else 0
        exec_pct = (stats['executed'] / total * 100) if total > 0 else 0
        
        return (
            f"总信号 {total} | "
            f"延迟过滤 {stats['delay_filtered']} ({delay_pct:.1f}%) | "
            f"深度不足 {stats['depth_insufficient']} ({depth_pct:.1f}%) | "
            f"数量调整 {stats['depth_adjusted']} ({adjusted_pct:.1f}%) | "
            f"执行 {stats['executed']} ({exec_pct:.1f}%)"
        )
    
    def _format_close_stats(self) -> str:
        """格式化平仓统计信息"""
        stats = self.signal_stats['close']
        total = stats['total']
        
        if total == 0:
            return "无数据"
        
        delay_pct = (stats['delay_filtered'] / total * 100) if total > 0 else 0
        depth_pct = (stats['depth_insufficient'] / total * 100) if total > 0 else 0
        exec_pct = (stats['executed'] / total * 100) if total > 0 else 0
        
        return (
            f"总信号 {total} | "
            f"延迟过滤 {stats['delay_filtered']} ({delay_pct:.1f}%) | "
            f"深度不足 {stats['depth_insufficient']} ({depth_pct:.1f}%) | "
            f"执行 {stats['executed']} ({exec_pct:.1f}%)"
        )
    
    def _log_stats_if_needed(self):
        """定期输出统计信息"""
        current_time = time.time()
        
        if current_time - self._last_stats_log_time >= self._stats_log_interval:
            threshold_info = ""
            if self.threshold_manager:
                stats = self.threshold_manager.get_stats()
                threshold_info = (
                    f"\n"
                    f"📊 动态阈值:\n"
                    f"   当前: 开仓{stats.get('current_open', 0):.4f}% "
                    f"平仓{stats.get('current_close', 0):.4f}% "
                    f"(调整{stats['adjustment_count']}次)\n"
                    f"   样本: 开仓{stats['open_samples']} 平仓{stats['close_samples']}\n"
                )
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