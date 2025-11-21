"""持仓数据模型"""

import time
from decimal import Decimal
from dataclasses import dataclass
from typing import Optional

@dataclass
class Position:
    """
    持仓信息
    
    记录对冲套利的持仓详情
    """
    symbol: str
    quantity: Decimal
    exchange_a_name: str
    exchange_b_name: str
    
    # ✅ 修正参数名（去掉 _price 后缀，改为 _entry_price）
    exchange_a_entry_price: Decimal  # Exchange A 开仓价格
    exchange_b_entry_price: Decimal  # Exchange B 开仓价格
    
    exchange_a_order_id: str
    exchange_b_order_id: str
    
    spread_pct: Decimal  # 开仓时的价差
    
    open_time: float = None
    close_time: Optional[float] = None
    
    def __post_init__(self):
        """初始化后处理"""
        if self.open_time is None:
            self.open_time = time.time()
    
    def calculate_pnl_pct(
        self,
        exchange_a_price: Decimal,
        exchange_b_price: Decimal
    ) -> Decimal:
        """
        计算盈亏百分比
        
        对冲策略的盈亏计算：
        - 开仓时：Exchange A 开空 @ exchange_a_entry_price
                Exchange B 开多 @ exchange_b_entry_price
        - 平仓时：Exchange A 平空（买入）@ exchange_a_price
                Exchange B 平多（卖出）@ exchange_b_price
        
        盈亏 = (开仓价差 - 平仓价差) / 平仓价差 * 100
        
        Args:
            exchange_a_price: Exchange A 当前价格（平仓价）
            exchange_b_price: Exchange B 当前价格（平仓价）
        
        Returns:
            盈亏百分比
        """
        # 开仓价差（Exchange A 卖出价 - Exchange B 买入价）
        open_spread = self.exchange_a_entry_price - self.exchange_b_entry_price
        
        # 平仓价差（Exchange A 买入价 - Exchange B 卖出价）
        close_spread = exchange_a_price - exchange_b_price
        
        # 盈亏 = 开仓价差 - 平仓价差
        pnl = open_spread - close_spread
        
        # 盈亏率 = 盈亏 / 开仓成本 * 100
        # 这里用 exchange_b_entry_price 作为基准（因为是开多的成本）
        if self.exchange_b_entry_price > 0:
            pnl_pct = (pnl / self.exchange_b_entry_price) * 100
        else:
            pnl_pct = Decimal('0')
        
        return pnl_pct
    
    def get_holding_duration(self) -> str:
        """
        获取持仓时长（格式化字符串）
        
        Returns:
            如 "5m 30s" 或 "1h 23m 45s"
        """
        if self.close_time:
            duration = self.close_time - self.open_time
        else:
            duration = time.time() - self.open_time
        
        hours = int(duration // 3600)
        minutes = int((duration % 3600) // 60)
        seconds = int(duration % 60)
        
        if hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"
    
    def mark_closed(self):
        """标记为已平仓"""
        self.close_time = time.time()
    
    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            'symbol': self.symbol,
            'quantity': float(self.quantity),
            'exchange_a': {
                'name': self.exchange_a_name,
                'entry_price': float(self.exchange_a_entry_price),
                'order_id': self.exchange_a_order_id
            },
            'exchange_b': {
                'name': self.exchange_b_name,
                'entry_price': float(self.exchange_b_entry_price),
                'order_id': self.exchange_b_order_id
            },
            'spread_pct': float(self.spread_pct),
            'open_time': self.open_time,
            'close_time': self.close_time,
            'holding_duration': self.get_holding_duration()
        }
    
    def __str__(self) -> str:
        """字符串表示"""
        return (
            f"Position({self.symbol}):\n"
            f"  {self.exchange_a_name} 空头 @ ${self.exchange_a_entry_price}\n"
            f"  {self.exchange_b_name} 多头 @ ${self.exchange_b_entry_price}\n"
            f"  Spread: {self.spread_pct:.4f}%\n"
            f"  Duration: {self.get_holding_duration()}"
        )