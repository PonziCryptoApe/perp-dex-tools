"""策略基类"""

import logging
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Optional

logger = logging.getLogger(__name__)

class BaseStrategy(ABC):
    """策略基类"""
    
    def __init__(
        self,
        strategy_name: str,
        symbol: str,
        quantity: Decimal,
        quantity_precision: Decimal
    ):
        """
        初始化策略
        
        Args:
            strategy_name: 策略名称
            symbol: 交易币种
            quantity: 交易数量
        """
        self.strategy_name = strategy_name
        self.symbol = symbol
        self.quantity = quantity
        self.quantity_precision = quantity_precision
        self.is_running = False
        
        logger.info(f"📋 初始化策略: {strategy_name}")
    
    @abstractmethod
    async def start(self):
        """启动策略"""
        pass
    
    @abstractmethod
    async def stop(self):
        """停止策略"""
        pass
    
    def get_status(self) -> dict:
        """获取策略状态"""
        return {
            'strategy_name': self.strategy_name,
            'symbol': self.symbol,
            'quantity': float(self.quantity),
            'is_running': self.is_running
        }