"""持仓管理服务"""

import logging
from typing import Optional
from decimal import Decimal
from datetime import datetime
from ..models.position import Position
from ..models.prices import PriceSnapshot
from ..utils.trade_logger import TradeLogger  # ✅ 导入 TradeLogger

logger = logging.getLogger(__name__)

class PositionManagerService:
    """
    持仓管理服务
    
    职责:
    1. 记录开仓
    2. 记录平仓
    3. 计算盈亏
    4. 持仓查询
    """
    
    def __init__(self, trade_logger: Optional[TradeLogger] = None):  # ✅ 添加参数
        """
        初始化持仓管理服务
        
        Args:
            trade_logger: 交易日志记录器（可选）
        """
        self.position: Optional[Position] = None
        self.trade_logger = trade_logger  # ✅ 保存引用
    
    def has_position(self) -> bool:
        """是否有持仓"""
        return self.position is not None
    
    def get_position(self) -> Optional[Position]:
        """获取当前持仓"""
        return self.position
    
    def set_position(self, position: Position):
        """
        设置持仓（开仓后调用）
        
        Args:
            position: Position 对象
        """
        self.position = position
        
        # ✅ 记录开仓交易到 CSV
        self._log_open_trade(position)
        
        logger.info(
            f"📝 记录开仓:\n"
            f"   Symbol: {position.symbol}\n"
            f"   Quantity: {position.quantity}\n"
            f"   {position.exchange_a_name}: 信号价 ${position.exchange_a_signal_entry_price} → 成交价 ${position.exchange_a_entry_price}\n"
            f"   {position.exchange_b_name}: 信号价 ${position.exchange_b_signal_entry_price} → 成交价 ${position.exchange_b_entry_price}\n"
            f"   Spread: {position.spread_pct:.4f}%"
        )
    
    def close_position(
        self
    ) -> Decimal:
        """
        平仓并计算盈亏
        
        Args:
            exchange_a_signal_exit_price: Exchange A 平仓信号价格
            exchange_b_signal_exit_price: Exchange B 平仓信号价格
            exchange_a_filled_exit_price: Exchange A 实际成交价格
            exchange_b_filled_exit_price: Exchange B 实际成交价格

        Returns:
            盈亏百分比
        
        Raises:
            ValueError: 没有持仓记录
        """
        if not self.position:
            raise ValueError("没有持仓记录")
        
        # if exchange_a_exit_price:
        #     self.position.exchange_a_exit_price = exchange_a_exit_price
        # if exchange_b_exit_price:
        #     self.position.exchange_b_exit_price = exchange_b_exit_price
        # if exchange_a_filled_exit_price:
        #     self.position.exchange_a_filled_exit_price = exchange_a_filled_exit_price
        # if exchange_b_filled_exit_price:
        #     self.position.exchange_b_filled_exit_price = exchange_b_filled_exit_price
        # ✅ 计算盈亏
        pnl_pct = self.position.calculate_pnl_pct(
            exchange_a_price=self.position.exchange_a_exit_price,
            exchange_b_price=self.position.exchange_b_exit_price
        )
        
        # ✅ 记录平仓交易到 CSV
        self._log_close_trade(self.position, pnl_pct)
        
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
    
    def _log_open_trade(self, position: Position):
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
                spread_pct=position.spread_pct
            )
    
    def _log_close_trade(self, position: Position, pnl_pct: Decimal):
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
                pnl_pct=pnl_pct
            )