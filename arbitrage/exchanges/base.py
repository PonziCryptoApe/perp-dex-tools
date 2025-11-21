"""交易所适配器基类"""

from abc import ABC, abstractmethod
from typing import Optional, Callable, Dict
from decimal import Decimal

class ExchangeAdapter(ABC):
    """
    交易所适配器基类
    
    所有交易所适配器都必须实现此接口
    """
    
    def __init__(self, symbol: str, client, config: dict = None):
        """
        Args:
            symbol: 交易币种（如 BTC, ETH）
            client: 原始交易所客户端
            config: 额外配置参数
        """
        self.symbol = symbol
        self.client = client
        self.config = config or {}
        self.exchange_name = self.__class__.__name__.replace('Adapter', '')
        
        # 订单簿缓存
        self._orderbook: Optional[Dict] = None
        self._orderbook_callback: Optional[Callable] = None
    
    @abstractmethod
    async def connect(self):
        """
        连接交易所
        
        Raises:
            Exception: 连接失败
        """
        pass
    
    @abstractmethod
    async def disconnect(self):
        """断开连接"""
        pass
    
    @abstractmethod
    async def subscribe_orderbook(self, callback: Callable):
        """
        订阅订单簿
        
        Args:
            callback: 订单簿更新回调
                     callback(orderbook: dict) -> None
                     
                     orderbook 格式:
                     {
                         'bids': [[price, size], ...],
                         'asks': [[price, size], ...],
                         'timestamp': float
                     }
        """
        pass
    
    @abstractmethod
    async def place_open_order(
        self,
        side: str,  # 'BUY' 或 'SELL'
        quantity: Decimal,
        price: Optional[Decimal] = None,  # 参考价格（某些交易所需要）
        retry_mode: str = 'opportunistic',
        quote_id: Optional[str] = None
    ) -> dict:
        """
        下开仓单
        """
        pass

    @abstractmethod
    async def place_close_order(
        self,
        side: str,  # 'BUY' 或 'SELL'
        quantity: Decimal,
        price: Optional[Decimal] = None,  # 参考价格（某些交易所需要）
        retry_mode: str = 'aggressive',
        quote_id: Optional[str] = None
    ) -> dict:
        """
        下平仓单
        """
        pass

    @abstractmethod
    async def place_market_order(
        self,
        side: str,  # 'BUY' 或 'SELL'
        quantity: Decimal,
        price: Optional[Decimal] = None,  # 参考价格（某些交易所需要）
        retry_mode: str = 'opportunistic'
    ) -> dict:
        """
        下市价单
        
        Args:
            side: 买卖方向 ('BUY' 或 'SELL')
            quantity: 数量
            price: 参考价格（可选）
        
        Returns:
            {
                'success': bool,
                'order_id': str or None,
                'error': str or None
            }
        """
        pass
    
    @abstractmethod
    def get_latest_orderbook(self) -> Optional[Dict]:
        """
        获取最新订单簿（同步方法，从缓存读取）
        
        Returns:
            订单簿字典或 None
        """
        pass
    
    def format_orderbook(self, raw_orderbook) -> Dict:
        """
        格式化订单簿为标准格式（子类可覆盖）
        
        Args:
            raw_orderbook: 原始订单簿数据
        
        Returns:
            标准格式订单簿
        """
        return raw_orderbook
    
    def __str__(self):
        return f"{self.exchange_name}Adapter({self.symbol})"