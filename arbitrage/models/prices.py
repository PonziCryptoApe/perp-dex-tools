"""价格数据模型"""

import time
from decimal import Decimal
from dataclasses import dataclass
from typing import Optional

@dataclass
class PriceSnapshot:
    """
    价格快照
    
    记录两个交易所的最新价格，用于套利计算
    """
    symbol: str
    exchange_a_bid: Decimal  # Exchange A 买一价
    exchange_a_ask: Decimal  # Exchange A 卖一价
    exchange_b_bid: Decimal  # Exchange B 买一价
    exchange_b_ask: Decimal  # Exchange B 卖一价
    exchange_a_name: str     # Exchange A 名称
    exchange_b_name: str     # Exchange B 名称
    exchange_a_timestamp: float = None # Exchange A 时间戳（extended 拿到订单簿的时间戳）
    exchange_b_timestamp: float = None # Exchange B 时间戳(VAR 包含拉取时间)
    timestamp: float = None
    
    # ✅ 深度信息（可选）
    exchange_a_bid_size: Optional[Decimal] = None
    exchange_a_ask_size: Optional[Decimal] = None
    exchange_b_bid_size: Optional[Decimal] = None
    exchange_b_ask_size: Optional[Decimal] = None
    # ✅ 新增：存储 quote_id（用于 Variational 下单）
    exchange_a_quote_id: Optional[str] = None
    exchange_b_quote_id: Optional[str] = None
    
    def __post_init__(self):
        """初始化后处理"""
        if self.timestamp is None:
            self.timestamp = time.time()
    
    def calculate_spread_pct(self) -> Decimal:
        """
        计算开仓套利空间（价差百分比）
        
        对冲策略的开仓操作：
        1. Exchange A 开空（卖出）@ exchange_a_bid
        2. Exchange B 开多（买入）@ exchange_b_ask
        
        套利收益 = exchange_a_bid - exchange_b_ask
        收益率 = (exchange_a_bid - exchange_b_ask) / exchange_b_ask * 100
        
        Returns:
            价差百分比（正数表示有套利空间，负数表示会亏损）
        """
        if self.exchange_a_bid <= 0 or self.exchange_b_ask <= 0:
            return Decimal('0')
        
        # ✅ 开仓套利空间
        spread = (self.exchange_a_bid - self.exchange_b_ask) / self.exchange_b_ask * 100
        return spread
    
    def calculate_reverse_spread_pct(self) -> Decimal:
        """
        计算反向套利空间
        
        反向操作（如果需要）：
        1. Exchange B 开空（卖出）@ exchange_b_bid
        2. Exchange A 开多（买入）@ exchange_a_ask
        
        Returns:
            反向价差百分比
        """
        if self.exchange_b_bid <= 0 or self.exchange_a_ask <= 0:
            return Decimal('0')
        
        spread = (self.exchange_b_bid - self.exchange_a_ask) / self.exchange_a_ask * 100
        return spread
    
    def get_mid_prices(self) -> tuple[Decimal, Decimal]:
        """
        获取中间价
        
        Returns:
            (exchange_a_mid, exchange_b_mid)
        """
        exchange_a_mid = (self.exchange_a_bid + self.exchange_a_ask) / 2
        exchange_b_mid = (self.exchange_b_bid + self.exchange_b_ask) / 2
        return exchange_a_mid, exchange_b_mid
    
    def get_spreads(self) -> tuple[Decimal, Decimal]:
        """
        获取买卖价差
        
        Returns:
            (exchange_a_spread, exchange_b_spread)
        """
        exchange_a_spread = self.exchange_a_ask - self.exchange_a_bid
        exchange_b_spread = self.exchange_b_ask - self.exchange_b_bid
        return exchange_a_spread, exchange_b_spread
    
    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            'symbol': self.symbol,
            'timestamp': self.timestamp,
            'exchange_a': {
                'name': self.exchange_a_name,
                'bid': float(self.exchange_a_bid),
                'ask': float(self.exchange_a_ask),
                'bid_size': float(self.exchange_a_bid_size) if self.exchange_a_bid_size else None,
                'ask_size': float(self.exchange_a_ask_size) if self.exchange_a_ask_size else None,
            },
            'exchange_b': {
                'name': self.exchange_b_name,
                'bid': float(self.exchange_b_bid),
                'ask': float(self.exchange_b_ask),
                'bid_size': float(self.exchange_b_bid_size) if self.exchange_b_bid_size else None,
                'ask_size': float(self.exchange_b_ask_size) if self.exchange_b_ask_size else None,
            },
            'spread_pct': float(self.calculate_spread_pct()),
            'reverse_spread_pct': float(self.calculate_reverse_spread_pct())
        }
    
    def __str__(self) -> str:
        """字符串表示"""
        spread_pct = self.calculate_spread_pct()
        return (
            f"PriceSnapshot({self.symbol}):\n"
            f"  {self.exchange_a_name}: Bid ${self.exchange_a_bid:.2f}, Ask ${self.exchange_a_ask:.2f}\n"
            f"  {self.exchange_b_name}: Bid ${self.exchange_b_bid:.2f}, Ask ${self.exchange_b_ask:.2f}\n"
            f"  Spread: {spread_pct:.4f}%"
        )