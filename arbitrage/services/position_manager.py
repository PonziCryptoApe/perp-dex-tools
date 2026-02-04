"""持仓管理服务（支持仓位累计模式）"""

import logging
from typing import Optional, List
from decimal import Decimal
from datetime import datetime
from ..models.position import Position
from ..models.prices import PriceSnapshot
from ..utils.trade_logger import TradeLogger

logger = logging.getLogger(__name__)

class PositionManagerService:
    """
    持仓管理服务（支持累计模式和传统模式）
    
    职责:
    1. 记录开仓/平仓
    2. 计算盈亏
    3. 仓位查询
    4. 累计仓位管理（新增）
    """
    
    def __init__(
        self, 
        trade_logger: Optional[TradeLogger] = None,
        accumulate_mode: bool = False,
        max_position: Decimal = Decimal('1.0'),
        position_step: Decimal = Decimal('0.1')
    ):
        """
        初始化持仓管理服务
        
        Args:
            trade_logger: 交易日志记录器（可选）
            accumulate_mode: 是否启用累计模式（默认 False）
            max_position: 最大仓位阈值（默认 1.0）
            position_step: 单次交易量（默认 0.1）
        """
        # ✅ 传统模式属性
        self.position: Optional[Position] = None
        self.trade_logger = trade_logger
        
        # ✅ 累计模式配置
        self.accumulate_mode = accumulate_mode
        self.max_position = max_position
        self.position_step = position_step
        
        # ✅ 当前累计仓位（正数=多头，负数=空头）
        self.current_position_qty = Decimal('0')
        
        # ✅ 历史仓位记录（用于追踪）
        self.position_history: List[Position] = []
        
        logger.info(
            f"📦 PositionManager 初始化:\n"
            f"   模式: {'✅ 累计仓位模式' if accumulate_mode else '✅ 传统仓位管理模式'}\n"
            f"   最大仓位: ±{max_position}\n"
            f"   单次交易量: {position_step}"
        )
    
    def has_position(self) -> bool:
        """是否有持仓"""
        if self.accumulate_mode:
            # ✅ 累计模式：检查累计仓位是否为 0
            return self.current_position_qty != 0
        else:
            # ✅ 传统模式：检查是否有 Position 对象
            return self.position is not None
    
    def get_position(self) -> Optional[Position]:
        """获取当前持仓（传统模式）或最新持仓（累计模式）"""
        if self.accumulate_mode:
            # ✅ 累计模式：返回最新的 Position
            return self.position_history[-1] if self.position_history else None
        else:
            # ✅ 传统模式
            return self.position
    
    def can_open(self, direction: str) -> bool:
        """
        检查是否可以开仓
        
        Args:
            direction: 'short' (Exchange A 开空, Exchange B 开多) 
                      或 'long' (Exchange A 开多, Exchange B 开空)
        
        Returns:
            是否可以开仓
        """
        if not self.accumulate_mode:
            # ✅ 传统模式：没有持仓才能开仓
            return not self.has_position()
        # ✅ 累计模式：检查是否超过阈值
        if direction == 'short':
            # 开空：Exchange A 卖出，Exchange B 买入
            # current_position_qty 会变得更负
            new_position = self.current_position_qty - self.position_step
            can_open = new_position >= -self.max_position
            
            if not can_open:
                logger.warning(
                    f"🚫 空头仓位已达阈值，当前{self.current_position_qty}, 禁止开空"
                )
            return can_open
        
        else:  # 'long'
            # 开多：Exchange A 买入，Exchange B 卖出
            # current_position_qty 会变得更正
            new_position = self.current_position_qty + self.position_step
            can_open = new_position <= self.max_position
            
            if not can_open:
                logger.warning(
                    f"🚫 多头仓位已达阈值，当前 {self.current_position_qty}, 禁止开多"
                )
            return can_open
    
    def can_close(self, direction: str) -> bool:
        """
        检查是否可以平仓（或反向开仓）
        
        Args:
            direction: 'long' (Exchange A 买入, Exchange B 卖出，平空/开多)
                    或 'short' (Exchange A 卖出, Exchange B 买入，平多/开空)
        
        Returns:
            是否可以平仓
        """
        if not self.accumulate_mode:
            # ✅ 传统模式：有持仓才能平仓
            return self.has_position()
        
        # ✅ 累计模式：检查是否超过阈值
        if direction == 'long':
            # Exchange A 买入（平空），Exchange B 卖出（平多）
            # 效果：current_position_qty += position_step
            new_position = self.current_position_qty + self.position_step
            can_close = new_position <= self.max_position
            if not can_close:
                logger.warning(
                    f"🚫 反向开仓后达到阈值，当前 {self.current_position_qty}, 禁止开多"
                )
            return can_close
        
        else:  # 'short'
            # Exchange A 卖出（平多），Exchange B 买入（平空）
            # 效果：current_position_qty -= position_step（向空头方向移动）
            new_position = self.current_position_qty - self.position_step
            can_close = new_position >= -self.max_position
            if not can_close:
                logger.warning(
                    f"🚫 反向开仓后达到阈值，当前 {self.current_position_qty}，禁止开空"
                )
            return can_close
    
    def set_position(self, position: Position):
        """
        设置持仓（开仓后调用）
        
        Args:
            position: Position 对象
        """
        if not self.accumulate_mode:
            # ✅ 传统模式：直接设置
            self.position = position
        else:
            # ✅ 累计模式：记录到历史
            self.position_history.append(position)
            
            # ✅ 注意：这里不更新 current_position_qty
            # 因为 add_position() 会专门处理累计逻辑
        
        # ✅ 记录开仓交易到 CSV
        self._log_open_trade(position)
        
        logger.info(
            f"📝 记录开仓:\n"
            f"   模式: {'累计' if self.accumulate_mode else '传统'}\n"
            f"   Symbol: {position.symbol}\n"
            f"   Quantity: {position.quantity}\n"
            f"   {position.exchange_a_name}: 信号价 ${position.exchange_a_signal_entry_price} → 成交价 ${position.exchange_a_entry_price}\n"
            f"   {position.exchange_b_name}: 信号价 ${position.exchange_b_signal_entry_price} → 成交价 ${position.exchange_b_entry_price}\n"
            f"   Spread: {position.spread_pct:.4f}%"
        )
    
    def add_position(self, position: Position, direction: str, signal_delay_ms_a: int = 0, signal_delay_ms_b: int = 0):
        """
        添加仓位（累计模式专用）
        
        Args:
            position: Position 对象
            direction: 'short' 或 'long'
            signal_delay_ms_a: 交易所 A 信号延迟（毫秒）
            signal_delay_ms_b: 交易所 B 信号延迟（毫秒）
        """
        if not self.accumulate_mode:
            logger.warning("⚠️ 传统模式下不支持 add_position()，请使用 set_position()")
            self.set_position(position)
            return
        
        # ✅ 累计仓位
        if direction == 'short':
            # Exchange A 开空 → current_position_qty 变负
            self.current_position_qty -= position.quantity
        else:  # 'long'
            # Exchange A 开多 → current_position_qty 变正
            self.current_position_qty += position.quantity
        
        # ✅ 记录到历史
        self.position_history.append(position)
        
        # ✅ 记录开仓交易到 CSV
        self._log_open_trade(position, signal_delay_ms_a, signal_delay_ms_b)
        
        # ✅ 计算仓位利用率
        utilization = abs(self.current_position_qty / self.max_position * 100) if self.max_position > 0 else 0
        
        logger.info(
            f"📝 累计仓位更新（开仓）:\n"
            f"   方向: {'空头' if direction == 'short' else '多头'}\n"
            f"   数量: {position.quantity}\n"
            f"   当前累计: {self.current_position_qty:+} / ±{self.max_position}\n"
            f"   利用率: {utilization:.1f}%\n"
            f"   历史笔数: {len(self.position_history)}"
        )
    
    def reduce_position(self, position: Position, direction: str) -> Decimal:
        """
        减少仓位（累计模式专用）
        
        Args:
            position: Position 对象（包含平仓信息）
            direction: 'long' (Exchange A 买入, Exchange B 卖出，平空/开多)
                    或 'short' (Exchange A 卖出, Exchange B 买入，平多/开空)
    
        Returns:
            盈亏百分比
        """
        if not self.accumulate_mode:
            logger.warning("⚠️ 传统模式下不支持 reduce_position()，请使用 close_position()")
            return self.close_position()
        # ✅ 断言检查
        if position.quantity == 0:
            raise ValueError(
                f"❌ reduce_position() 收到 quantity=0 的 position！\n"
                f"   direction: {direction}\n"
                f"   position.symbol: {position.symbol}\n"
                f"   position.exchange_a_name: {position.exchange_a_name}\n"
                f"   position.exchange_b_name: {position.exchange_b_name}"
            )
        
        # ✅ 详细调试日志
        old_qty = self.current_position_qty
        
        # logger.info(
        #     f"🔍 [DEBUG] reduce_position() 调用:\n"
        #     f"   direction: {direction}\n"
        #     f"   position.quantity: {position.quantity}\n"
        #     f"   当前 current_position_qty: {old_qty}\n"
        #     f"   position_step: {self.position_step}\n"
        #     f"   max_position: {self.max_position}"
        # )
        # ✅ 累计仓位
        if direction == 'long':
            # Exchange A 买入平空 → current_position_qty 变正
            self.current_position_qty += position.quantity
            operation = f"+{position.quantity}"
        else:  # 'short'
            # Exchange A 卖出平多 → current_position_qty 变负
            self.current_position_qty -= position.quantity
            operation = f"-{position.quantity}"

        # ✅ 计算盈亏
        pnl_pct = position.calculate_pnl_pct(
            exchange_a_price=position.exchange_a_exit_price,
            exchange_b_price=position.exchange_b_exit_price
        )
        
        # ✅ 记录平仓交易到 CSV
        self._log_close_trade(position, pnl_pct)
        
        # ✅ 计算仓位利用率
        utilization = abs(self.current_position_qty / self.max_position * 100) if self.max_position > 0 else 0
        
        # logger.info(
        #     f"📝 累计仓位更新（平仓）:\n"
        #     f"   方向: {direction}\n"
        #     f"   操作: {old_qty} {operation} = {self.current_position_qty}\n"
        #     f"   数量: {position.quantity}\n"
        #     f"   当前累计: {self.current_position_qty:+} / ±{self.max_position}\n"
        #     f"   利用率: {utilization:.1f}%\n"
        #     # f"   盈亏: {pnl_pct:+.4f}%\n"
        #     f"   历史笔数: {len(self.position_history)}"
        # )
        
        return pnl_pct

    def close_position(self, signal_delay_ms_a: float, signal_delay_ms_b: float) -> Decimal:
        """
        平仓并计算盈亏（传统模式）
        
        Returns:
            盈亏百分比
        
        Raises:
            ValueError: 没有持仓记录
        """
        if not self.position:
            raise ValueError("没有持仓记录")
        
        # ✅ 计算盈亏
        pnl_pct = self.position.calculate_pnl_pct(
            exchange_a_price=self.position.exchange_a_exit_price,
            exchange_b_price=self.position.exchange_b_exit_price
        )
        
        # ✅ 记录平仓交易到 CSV
        self._log_close_trade(self.position, pnl_pct, signal_delay_ms_a, signal_delay_ms_b)
        
        duration = self.position.get_holding_duration()
        
        logger.info(
            f"📝 记录平仓:\n"
            f"   {self.position.exchange_a_name}: 信号价 ${self.position.exchange_a_signal_exit_price} → 成交价 ${self.position.exchange_a_exit_price}\n"
            f"   {self.position.exchange_b_name}: 信号价 ${self.position.exchange_b_signal_exit_price} → 成交价 ${self.position.exchange_b_exit_price}\n"
            f"   Duration: {duration}\n"
            f"   💰 PnL: {pnl_pct:+.4f}%"
        )
        
        # ✅ 清空持仓
        self.position = None
        
        return pnl_pct
    
    def get_current_position_qty(self) -> Decimal:
        """获取当前累计仓位数量"""
        return self.current_position_qty
    
    def get_position_summary(self) -> dict:
        """获取仓位摘要"""
        utilization = abs(self.current_position_qty / self.max_position * 100) if self.max_position > 0 else 0
        
        return {
            'mode': 'accumulate' if self.accumulate_mode else 'traditional',
            'current_qty': float(self.current_position_qty),
            'max_position': float(self.max_position),
            'position_step': float(self.position_step),
            'history_count': len(self.position_history),
            'utilization': round(utilization, 2),
            'direction': 'short' if self.current_position_qty < 0 else ('long' if self.current_position_qty > 0 else 'flat')
        }
    # ========== 在类中添加新方法 ==========

    async def sync_from_exchanges(
        self,
        exchange_a,
        exchange_b,
        symbol_a: str,
        symbol_b: str,
    ):
        """
        从交易所同步仓位
        
        Args:
            exchange_a: 交易所 A 适配器
            exchange_b: 交易所 B 适配器  
            symbol: 交易对符号
        
        Returns:
            同步后的净仓位数量
        """
        try:
            # 获取交易所仓位
            position_a = await exchange_a.get_position(symbol_a)
            position_b = await exchange_b.get_position(symbol_b)

            # 解析仓位数量
            qty_a = Decimal(str(position_a.get('size', 0))) if position_a else Decimal('0')
            qty_b = Decimal(str(position_b.get('size', 0))) if position_b else Decimal('0')
            logger.info(
                f"🔍 获取交易所仓位:"
                f"   {exchange_a.exchange_name}: {qty_a:.4f} ({'空头' if position_a and position_a.get('side') == 'short' else '多头' if position_a and position_a.get('side') == 'long' else '无仓位'}) | "
                f"   {exchange_b.exchange_name}: {qty_b:.4f} ({'空头' if position_b and position_b.get('side') == 'short' else '多头' if position_b and position_b.get('side') == 'long' else '无仓位'})"
            )
            # Exchange A 做空 → 仓位为负
            qty = qty_b
            if position_b and position_b.get('side') == 'long':
                qty_a = -qty_a
                qty = -qty_b
            if position_b and position_b.get('side') == 'short':
                qty_b = -qty_b

            # Exchange B 做多 → 仓位为正（已经是正数）
            
            # 净仓位 = qty_a（以 B 的多头数量为基准）
            synced_qty = qty
            
            logger.info(
                f"🔄 同步仓位:\n"
                f"   {exchange_a.exchange_name}: {qty_a:+.4f}\n"
                f"   {exchange_b.exchange_name}: {qty_b:+.4f}\n"
                f"   本地净仓位: {synced_qty:+.4f}"
            )
            
            # 检查对冲状态
            hedge_diff = qty_a + qty_b
            if abs(hedge_diff) > self.position_step * Decimal('0.1'):  # 允许 10% 误差
                logger.warning(
                    f"⚠️ 仓位不对冲:\n"
                    f"   {exchange_a.exchange_name}: {qty_a:+.4f}\n"
                    f"   {exchange_b.exchange_name}: {qty_b:+.4f}\n"
                    f"   差额: {hedge_diff:+.4f}\n"
                    f"   建议检查订单状态"
                )
            
            # 更新本地仓位
            self.current_position_qty = synced_qty
            
            return synced_qty
        
        except Exception as e:
            logger.error(f"❌ 同步仓位失败: {e}", exc_info=True)
            return None


    async def verify_and_sync(
        self,
        exchange_a,
        exchange_b,
        symbol_a: str,
        symbol_b: str,
        expected_qty: Decimal,
        tolerance: Decimal = Decimal('0.01')
    ) -> bool:
        """
        校验并同步仓位（交易后调用）
        
        Args:
            exchange_a: 交易所 A
            exchange_b: 交易所 B
            symbol: 交易对
            expected_qty: 预期的本地仓位
            tolerance: 允许误差
        
        Returns:
            是否一致
        """
        try:
            # 获取实际仓位
            actual_qty = await self.sync_from_exchanges(exchange_a, exchange_b, symbol_a, symbol_b)
            
            if actual_qty is None:
                logger.error("❌ 无法获取交易所仓位，跳过校验")
                return False
            
            # 计算差异
            diff = abs(actual_qty - expected_qty)
            
            if diff <= tolerance:
                logger.debug(f"✅ 仓位校验通过: 预期 {expected_qty:+.4f} = 实际 {actual_qty:+.4f}")
                return True
            
            else:
                # 差异超过阈值 → 修正本地
                logger.warning(
                    f"⚠️ 仓位不一致:\n"
                    f"   本地预期: {expected_qty:+.4f}\n"
                    f"   交易所实际: {actual_qty:+.4f}\n"
                    f"   差异: {diff:.4f}\n"
                    f"   → 已修正为交易所实际值"
                )
                
                # 强制修正
                self.current_position_qty = actual_qty
                
                return False
        
        except Exception as e:
            logger.error(f"❌ 仓位校验失败: {e}", exc_info=True)
            return False

    def _log_open_trade(self, position: Position, signal_delay_ms_a: float = 0, signal_delay_ms_b: float = 0):
        """记录开仓交易到 CSV"""
        if self.trade_logger:
            self.trade_logger.log_open_position(
                exchange_a_name=position.exchange_a_name,
                exchange_a_side='sell',
                exchange_a_signal_price=position.exchange_a_signal_entry_price,
                exchange_a_filled_price=position.exchange_a_entry_price,
                exchange_a_order_id=position.exchange_a_order_id,
                exchange_b_name=position.exchange_b_name,
                exchange_b_side='buy',
                exchange_b_signal_price=position.exchange_b_signal_entry_price,
                exchange_b_filled_price=position.exchange_b_entry_price,
                exchange_b_order_id=position.exchange_b_order_id,
                quantity=position.quantity,
                spread_pct=position.spread_pct,
                signal_delay_ms_a=signal_delay_ms_a,
                signal_delay_ms_b=signal_delay_ms_b,
                place_duration_a_ms=position.place_duration_a_ms,
                place_duration_b_ms=position.place_duration_b_ms,
                execution_duration_a_ms=position.execution_duration_a_ms,
                execution_duration_b_ms=position.execution_duration_b_ms,
                attempt_a=position.attempt_a,
                attempt_b=position.attempt_b
            )

    def _log_close_trade(self, position: Position, pnl_pct: Decimal, signal_delay_ms_a: float = 0, signal_delay_ms_b: float = 0):
        """记录平仓交易到 CSV"""
        if self.trade_logger:
            # ✅ 计算平仓时的价差
            if position.exchange_a_exit_price and position.exchange_b_exit_price:
                close_spread_pct = (
                    (position.exchange_b_exit_price - position.exchange_a_exit_price) 
                    / position.exchange_a_exit_price * 100
                )
            else:
                close_spread_pct = Decimal('0')
            
            self.trade_logger.log_close_position(
                exchange_a_name=position.exchange_a_name,
                exchange_a_side='buy',  # 平空
                exchange_a_signal_price=position.exchange_a_signal_exit_price,
                exchange_a_filled_price=position.exchange_a_exit_price,
                exchange_a_order_id=position.exchange_a_exit_order_id or '',
                exchange_b_name=position.exchange_b_name,
                exchange_b_side='sell',  # 平多
                exchange_b_signal_price=position.exchange_b_signal_exit_price,
                exchange_b_filled_price=position.exchange_b_exit_price,
                exchange_b_order_id=position.exchange_b_exit_order_id or '',
                quantity=position.quantity,
                spread_pct=close_spread_pct,
                pnl_pct=pnl_pct,
                signal_delay_ms_a=signal_delay_ms_a,
                signal_delay_ms_b=signal_delay_ms_b,
                place_duration_a_ms=position.place_duration_a_ms,
                place_duration_b_ms=position.place_duration_b_ms,
                execution_duration_a_ms=position.execution_duration_a_ms,
                execution_duration_b_ms=position.execution_duration_b_ms,
                attempt_a=position.attempt_a,
                attempt_b=position.attempt_b
            )