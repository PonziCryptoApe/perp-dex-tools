"""对冲套利策略"""

import asyncio
import logging
import time
from decimal import Decimal
from typing import Optional
from .base_strategy import BaseStrategy
from ..models.prices import PriceSnapshot
from ..services.price_monitor import PriceMonitorService
from ..services.position_manager import PositionManagerService
from ..services.order_executor_parallel import OrderExecutor
from ..models.position import Position

logger = logging.getLogger(__name__)

class HedgeStrategy(BaseStrategy):
    """对冲套利策略"""
    
    def __init__(
        self,
        symbol: str,
        quantity: Decimal,
        open_threshold_pct: float,
        close_threshold_pct: float,
        exchange_a,
        exchange_b,
        lark_bot=None,
        monitor_only: bool = False,
        trade_logger=None,
        max_signal_delay_ms: int = 150,
    ):
        super().__init__(
            strategy_name=f"Hedge-{symbol}",
            symbol=symbol,
            quantity=quantity
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

        # ✅ 使用 PositionManagerService 管理持仓
        self.position_manager = PositionManagerService(trade_logger=trade_logger)

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
            quantity=quantity
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
        self._cooldown_seconds = 5  # 开仓/平仓后冷却 5 秒
        
        logger.info(
            f"🎯 策略配置:\n"
            f"   Symbol: {symbol}\n"
            f"   Quantity: {quantity}\n"
            f"   Open Threshold: {open_threshold_pct}%\n"
            f"   Close Threshold: {close_threshold_pct}%\n"
            f"   Exchange A: {exchange_a.exchange_name}\n"
            f"   Exchange B: {exchange_b.exchange_name}\n"
            f"   Monitor Only: {monitor_only}"
        )
    
    async def start(self):
        """启动策略"""
        logger.info(f"🚀 启动策略: {self.strategy_name}")
        
        # 启动价格监控
        await self.monitor.start()
        
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
        
        try:
            # ✅ 记录价格更新的时间
            price_update_time_a = prices.exchange_a_timestamp
            price_update_time_b = prices.exchange_b_timestamp

            # 计算价差
            spread_pct = prices.calculate_spread_pct()
            reverse_spread_pct = prices.calculate_reverse_spread_pct()
            
            # ✅ 根据持仓状态决定检查哪种信号
            if not self.position_manager.has_position():
                # 无持仓，检查开仓信号
                await self._check_open_signal(prices, spread_pct, price_update_time_a, price_update_time_b)
            else:
                # 有持仓，检查平仓信号
                await self._check_close_signal(prices, reverse_spread_pct, price_update_time_a, price_update_time_b)

        except Exception as e:
            logger.error(f"❌ 价格更新处理失败: {e}")
            import traceback
            traceback.print_exc()

    async def _check_open_signal(self, prices: PriceSnapshot, spread_pct: Decimal, price_update_time_a: float, price_update_time_b: float):
        """
        检查开仓信号
        
        ✅ 监控模式下，会创建虚拟持仓（不实际下单）
        """
        # ✅ 如果已有持仓，不再开仓
        if self.position_manager.has_position():
            return
        
        # ✅ 检查冷却期
        current_time = time.time()
        cooldown_remaining = self._cooldown_seconds - (current_time - self._last_open_time)
        if cooldown_remaining > 0:
            return
        
        # ✅ 如果正在执行开仓，跳过
        if self._is_executing:
            logger.debug("⏳ 正在执行开仓操作，跳过本次信号")
            return
        
        current_time = time.time()
        
        # 判断是否满足开仓阈值
        if spread_pct >= Decimal(str(self.open_threshold_pct)):
            # 记录信号触发时间
            signal_trigger_time = time.time()
            signal_delay_ms_a = (signal_trigger_time - price_update_time_a) * 1000
            signal_delay_ms_b = (signal_trigger_time - price_update_time_b) * 1000
        
            # ✅ 过滤延迟过大的信号
            if signal_delay_ms_a > self.max_signal_delay_ms or signal_delay_ms_b > self.max_signal_delay_ms:
                logger.warning(
                    f"⚠️ 开仓信号延迟过大，已过滤:\n"
                    f"   延迟_a: {signal_delay_ms_a:.2f} ms (阈值: {self.max_signal_delay_ms} ms)\n"
                    f"   延迟_b: {signal_delay_ms_b:.2f} ms (阈值: {self.max_signal_delay_ms} ms)\n"
                    f"   价差: {spread_pct:.4f}% (阈值: {self.open_threshold_pct}%)\n"
                    f"   {self.exchange_a.exchange_name}_bid: ${prices.exchange_a_bid}\n"
                    f"   {self.exchange_b.exchange_name}_ask: ${prices.exchange_b_ask}"
                )
                return  # ✅ 丢弃该信号
            else:
                logger.info(
                    f"   延迟_a: {signal_delay_ms_a:.2f} ms (阈值: {self.max_signal_delay_ms} ms)\n"
                    f"   延迟_b: {signal_delay_ms_b:.2f} ms (阈值: {self.max_signal_delay_ms} ms)\n"
                    f"   价差: {spread_pct:.4f}% (阈值: {self.open_threshold_pct}%)\n"
                    f"   {self.exchange_a.exchange_name}_bid: ${prices.exchange_a_bid}\n"
                    f"   {self.exchange_b.exchange_name}_ask: ${prices.exchange_b_ask}"
                )

            self.open_signal_count += 1

            # ✅ 检查是否为监控模式
            if self.monitor_only:
                # logger.info("📊 监控模式：不执行开仓，创建虚拟持仓以监控平仓信号")
                
                # ✅ 创建虚拟持仓（用于模拟）
                virtual_position = Position(
                    symbol=self.symbol,
                    quantity=self.quantity,
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
                    await self._send_open_notification(virtual_position, prices)

                return
            async with self._executing_lock:
                if self.position_manager.has_position():
                    logger.warning("⏳ 开仓操作期间已有持仓，跳过本次开仓")
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
                        signal_trigger_time=signal_trigger_time
                    )
                    
                    if success:
                        self.position_manager.set_position(position)
                        self._last_open_time = time.time()
                        logger.info(f"✅ 开仓成功: {position}，等待平仓...")
                        # 发送飞书通知
                        if self.lark_bot:
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

    async def _check_close_signal(self, prices: PriceSnapshot, spread_pct: Decimal, price_update_time_a: float, price_update_time_b: float):
        """
        检查平仓信号
        
        ✅ 监控模式下，会清除虚拟持仓（不实际下单）
        """
        position = self.position_manager.get_position()

        if position is None:
            return
        
        # ✅ 如果正在执行平仓，跳过
        if self._is_executing:
            logger.info("⏳ 正在执行平仓操作，跳过本次信号")
            return
        
        current_time = time.time()

        # 判断是否满足平仓阈值
        if spread_pct >= Decimal(str(self.close_threshold_pct)):
            # 记录信号触发时间
            signal_trigger_time = time.time()

            # ✅ 计算延迟（价格更新 → 信号触发）
            signal_delay_ms_a = (signal_trigger_time - price_update_time_a) * 1000
            signal_delay_ms_b = (signal_trigger_time - price_update_time_b) * 1000

            # ✅ 过滤延迟过大的信号
            if signal_delay_ms_a > self.max_signal_delay_ms or signal_delay_ms_b > self.max_signal_delay_ms:
                # 计算当前盈亏（仅用于日志）
                pnl_pct = position.calculate_pnl_pct(
                    exchange_a_price=prices.exchange_a_ask,
                    exchange_b_price=prices.exchange_b_bid
                )
                
                logger.warning(
                    f"⚠️ 平仓信号延迟过大，已过滤:\n"
                    f"   延迟_a: {signal_delay_ms_a:.2f} ms (阈值: {self.max_signal_delay_ms} ms)\n"
                    f"   延迟_b: {signal_delay_ms_b:.2f} ms (阈值: {self.max_signal_delay_ms} ms)\n"
                    f"   {self.exchange_a.exchange_name}_ask: ${prices.exchange_a_ask}\n"
                    f"   {self.exchange_b.exchange_name}_bid: ${prices.exchange_b_bid}\n"
                    f"   价差: {spread_pct:.4f}% (阈值: {self.close_threshold_pct}%)\n"
                    f"   当前盈亏: {pnl_pct:.4f}%\n"
                    f"   持仓时长: {position.get_holding_duration()}"
                )
                return  # ✅ 丢弃该信号
            self.close_signal_count += 1

            # 计算当前盈亏
            pnl_pct = position.calculate_pnl_pct(
                exchange_a_price=prices.exchange_a_ask,
                exchange_b_price=prices.exchange_b_bid
            )
            # 计算最大延迟
            max_delay_ms = max(signal_delay_ms_a, signal_delay_ms_b)

            logger.info(
                f"🔔 平仓信号 #{self.close_signal_count}:\n"
                f"   {self.exchange_a.exchange_name}_ask: ${prices.exchange_a_ask}\n"
                f"   {self.exchange_b.exchange_name}_bid: ${prices.exchange_b_bid}\n"
                f"   价差: {spread_pct:.4f}%(阈值: {self.close_threshold_pct}%)\n"
                f"   盈亏: {pnl_pct:.4f}%\n"
                f"   持仓时长: {position.get_holding_duration()}\n"
                f"   ⏱️ 延迟分析:\n"
                f"      Exchange A: {signal_delay_ms_a:.2f} ms\n"
                f"      Exchange B: {signal_delay_ms_b:.2f} ms\n"
                f"      最大延迟: {max_delay_ms:.2f} ms"
            )
            
            # ✅ 检查是否为监控模式
            if self.monitor_only:
                self.position_manager.close_position(
                    exchange_a_exit_price=prices.exchange_a_ask,
                    exchange_b_exit_price=prices.exchange_b_bid
                )

                self._last_close_time = time.time()
                # 发送飞书通知（可选）
                if self.lark_bot:
                    await self._send_close_notification(position, pnl_pct, prices)

                # logger.info("✅ 虚拟持仓已清除，切换到开仓监控模式")
                return
            
            async with self._executing_lock:
                if not self.position_manager.has_position():
                    logger.warning(" 获取锁后发现持仓已清空，取消平仓")
                    return
                self._is_executing = True

                try:
                    # 实际交易模式：执行平仓
                    success, updated_position = await self.executor.execute_close(
                        position=position,
                        exchange_a_price=prices.exchange_a_ask,
                        exchange_b_price=prices.exchange_b_bid,
                        exchange_a_quote_id=prices.exchange_a_quote_id,
                        exchange_b_quote_id=prices.exchange_b_quote_id,
                        signal_trigger_time=signal_trigger_time
                    )
                    
                    if success:
                        logger.info(f"✅ 平仓成功，切换到开仓监控模式")

                        self.position_manager.position = updated_position

                        # ✅ 记录实际平仓到 CSV
                        pnl_pct = self.position_manager.close_position()
                        # 发送飞书通知
                        if self.lark_bot:
                            await self._send_close_notification(position, pnl_pct, prices)

                        # 清除持仓
                        # self.position = None

                        self._last_close_time = time.time()
                    else:
                        if current_time - self.last_log_time >= self.log_interval:
                            # ✅ 节流日志：每5秒最多输出一次
                            logger.info(
                                f"📊 当前价差: {spread_pct:.4f}% "
                                f"(平仓阈值: {self.close_threshold_pct}%) - 监控平仓中..."
                            )
                            self.last_log_time = current_time
                finally:
                    self._is_executing = False
    
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
            logger.error(f"发送飞书通知失败: {e}")
    
    async def _send_close_notification(self, position: Position, pnl_pct: Decimal, prices: PriceSnapshot):
        """发送平仓通知"""
        try:
            # ✅ 根据模式调整通知内容
            mode_text = "虚拟" if self.monitor_only else "实际"
            
            message = (
                f"🔔 对冲平仓通知 ({mode_text})\n\n"
                f"交易对: {self.symbol}\n"
                f"盈亏: {pnl_pct:.4f}%\n"
                f"数量: {self.quantity}\n\n"
                f"开仓信息:\n"
                f"  {self.exchange_a.exchange_name}: ${position.exchange_a_entry_price}\n"
                f"  {self.exchange_b.exchange_name}: ${position.exchange_b_entry_price}\n"
                f"  价差: {position.spread_pct:.4f}%\n\n"
                f"平仓信息:\n"
                f"  {self.exchange_a.exchange_name}: ${prices.exchange_a_ask}\n"
                f"  {self.exchange_b.exchange_name}: ${prices.exchange_b_bid}\n\n"
                f"持仓时长: {position.get_holding_duration()}"
            )
            await self.lark_bot.send_text(message)
        except Exception as e:
            logger.error(f"发送飞书通知失败: {e}")