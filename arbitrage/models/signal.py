"""交易信号模型"""

import time
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Optional

from .prices import PriceSnapshot

class SignalType(Enum):
    """信号类型"""
    OPEN = 'OPEN'      # 开仓信号
    CLOSE = 'CLOSE'    # 平仓信号

@dataclass
class TradingSignal:
    """交易信号。"""
    signal_id: str
    signal_type: SignalType
    symbol: str
    spread_pct: Decimal
    exchange_a_price: Decimal
    exchange_b_price: Decimal
    quantity: Decimal
    created_at: float = field(default_factory=time.time)
    expire_at: Optional[float] = None
    exchange_a_quote_id: Optional[str] = None
    exchange_b_quote_id: Optional[str] = None
    exchange_a_depth: Optional[Decimal] = None
    exchange_b_depth: Optional[Decimal] = None
    signal_delay_ms_a: float = 0.0
    signal_delay_ms_b: float = 0.0
    prices: Optional[PriceSnapshot] = None
    reason: str = ''  # 触发原因
    metadata: dict[str, Any] = field(default_factory=dict)

    def is_expired(self, now: Optional[float] = None) -> bool:
        """判断信号是否已过期。"""
        if self.expire_at is None:
            return False
        check_time = time.time() if now is None else now
        return check_time >= self.expire_at

    @property
    def mailbox_key(self) -> str:
        """返回 mailbox 分流 key。"""
        return f"{self.symbol}:{self.signal_type.value}"

    def to_dict(self):
        """转为字典"""
        return {
            'signal_id': self.signal_id,
            'signal_type': self.signal_type.value,
            'symbol': self.symbol,
            'spread_pct': str(self.spread_pct),
            'exchange_a_price': str(self.exchange_a_price),
            'exchange_b_price': str(self.exchange_b_price),
            'quantity': str(self.quantity),
            'created_at': self.created_at,
            'expire_at': self.expire_at,
            'exchange_a_quote_id': self.exchange_a_quote_id,
            'exchange_b_quote_id': self.exchange_b_quote_id,
            'exchange_a_depth': str(self.exchange_a_depth) if self.exchange_a_depth is not None else None,
            'exchange_b_depth': str(self.exchange_b_depth) if self.exchange_b_depth is not None else None,
            'signal_delay_ms_a': self.signal_delay_ms_a,
            'signal_delay_ms_b': self.signal_delay_ms_b,
            'reason': self.reason,
            'metadata': self.metadata,
            'prices': self.prices.to_dict() if self.prices else None,
        }

    def __str__(self):
        return (
            f"TradingSignal(\n"
            f"  id={self.signal_id},\n"
            f"  type={self.signal_type.value},\n"
            f"  symbol={self.symbol},\n"
            f"  spread={self.spread_pct:.4f}%,\n"
            f"  a_price={self.exchange_a_price},\n"
            f"  b_price={self.exchange_b_price},\n"
            f"  quantity={self.quantity},\n"
            f"  reason={self.reason}\n"
            f")"
        )
