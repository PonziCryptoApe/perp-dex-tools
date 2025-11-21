"""交易信号模型"""

from dataclasses import dataclass
from enum import Enum
from .prices import PriceSnapshot

class SignalType(Enum):
    """信号类型"""
    OPEN = 'OPEN'      # 开仓信号
    CLOSE = 'CLOSE'    # 平仓信号

@dataclass
class TradingSignal:
    """
    交易信号
    
    注意：当前架构（进程内回调）暂时用不到此类
    未来如果改为事件驱动架构，可以使用
    """
    signal_type: SignalType
    symbol: str
    spread_pct: float
    prices: PriceSnapshot
    reason: str = ''  # 触发原因
    
    def to_dict(self):
        """转为字典"""
        return {
            'signal_type': self.signal_type.value,
            'symbol': self.symbol,
            'spread_pct': self.spread_pct,
            'reason': self.reason,
            'prices': self.prices.to_dict()
        }
    
    def __str__(self):
        return (
            f"TradingSignal(\n"
            f"  type={self.signal_type.value},\n"
            f"  symbol={self.symbol},\n"
            f"  spread={self.spread_pct:.4f}%,\n"
            f"  reason={self.reason}\n"
            f")"
        )