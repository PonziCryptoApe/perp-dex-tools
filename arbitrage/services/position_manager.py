"""持仓管理服务"""

import logging
from typing import Optional
from decimal import Decimal
from datetime import datetime
from ..models.position import Position
from ..models.prices import PriceSnapshot

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
    
    def __init__(self):
        self.position: Optional[Position] = None
    
    def has_position(self) -> bool:
        """是否有持仓"""
        return self.position is not None and self.position.is_open
    
    def record_open(
        self,
        symbol: str,
        quantity: Decimal,
        prices: PriceSnapshot,
        spread_pct: float
    ):
        """
        记录开仓
        
        Args:
            symbol: 币种
            quantity: 数量
            prices: 开仓价格
            spread_pct: 开仓价差
        """
        self.position = Position(
            symbol=symbol,
            quantity=quantity,
            exchange_a_entry_price=prices.exchange_a_bid,
            exchange_b_entry_price=prices.exchange_b_ask,
            open_time=datetime.now(),
            open_spread_pct=spread_pct,
            exchange_a_name=prices.exchange_a_name,
            exchange_b_name=prices.exchange_b_name
        )
        
        logger.info(
            f"📝 记录开仓:\n"
            f"   Symbol: {symbol}\n"
            f"   Quantity: {quantity}\n"
            f"   {prices.exchange_a_name}: ${prices.exchange_a_bid}\n"
            f"   {prices.exchange_b_name}: ${prices.exchange_b_ask}\n"
            f"   Spread: {spread_pct:.4f}%"
        )
    
    def record_close(self, prices: PriceSnapshot, spread_pct: float) -> Decimal:
        """
        记录平仓并计算盈亏
        
        Args:
            prices: 平仓价格
            spread_pct: 平仓价差
        
        Returns:
            盈亏金额
        
        Raises:
            ValueError: 没有持仓记录
        """
        if not self.position:
            raise ValueError("没有持仓记录")
        
        self.position.exchange_a_exit_price = prices.exchange_a_ask
        self.position.exchange_b_exit_price = prices.exchange_b_bid
        self.position.close_time = datetime.now()
        self.position.close_spread_pct = spread_pct
        
        pnl = self.position.pnl
        pnl_pct = self.position.pnl_pct
        duration = self.position.duration_str
        
        logger.info(
            f"📝 记录平仓:\n"
            f"   {prices.exchange_a_name}: ${prices.exchange_a_ask}\n"
            f"   {prices.exchange_b_name}: ${prices.exchange_b_bid}\n"
            f"   Spread: {spread_pct:.4f}%\n"
            f"   Duration: {duration}\n"
            f"   💰 PnL: ${pnl:.4f} ({pnl_pct:+.2f}%)"
        )
        
        # 清空持仓
        closed_position = self.position
        self.position = None
        
        return pnl
    
    def get_position(self) -> Optional[Position]:
        """获取当前持仓"""
        return self.position