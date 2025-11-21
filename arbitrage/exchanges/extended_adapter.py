"""Extended 交易所适配器"""

import asyncio
import logging
from decimal import Decimal
from typing import Optional, Callable, Dict
from .base import ExchangeAdapter
from x10.perpetual.orders import OrderSide, TimeInForce

logger = logging.getLogger(__name__)

class ExtendedAdapter(ExchangeAdapter):
    """Extended 交易所适配器"""
    
    def __init__(self, symbol: str, client, config: dict = None):
        super().__init__(symbol, client, config)
        self.contract_id = f"{symbol}-USD"
        self._orderbook_update_task = None
        # ✅ 订单状态事件
        self._order_status_events: Dict[str, asyncio.Event] = {}
        self._order_status_data: Dict[str, str] = {}  # ← 添加这一行！

    
    async def connect(self):
        """连接 Extended（使用 ExtendedClient 自带的 WebSocket）"""
        try:
            # ✅ ExtendedClient 已经在 main.py 中调用了 connect()
            # 这里只需要验证连接状态
            if not hasattr(self.client, 'ws') or self.client.ws is None:
                logger.warning(f"⚠️ {self.exchange_name} WebSocket 未连接，尝试连接...")
                await self.client.connect()
            # ✅ 双重保险：确保 tick_size 已设置
            if not hasattr(self.client.config, 'tick_size') or self.client.config.tick_size is None:
                logger.warning("⚠️ tick_size 未设置，立即获取...")
                await self.client.get_contract_attributes()
             # ✅ 确保必需的配置属性存在（防止 AttributeError）
            if not hasattr(self.client.config, 'close_order_side'):
                self.client.config.close_order_side = None
                logger.debug("设置默认 close_order_side = None（套利模式）")
            
            if not hasattr(self.client.config, 'contract_id'):
                self.client.config.contract_id = self.contract_id
                logger.debug(f"设置 contract_id = {self.contract_id}")
            
            if not hasattr(self.client.config, 'take_profit_percentage'):
                self.client.config.take_profit_percentage = None
            
            if not hasattr(self.client.config, 'stop_loss_percentage'):
                self.client.config.stop_loss_percentage = None

            logger.info(f"✅ {self.exchange_name} 已连接: {self.contract_id}")
            self.client.setup_order_update_handler(self._on_order_update)

        
        except Exception as e:
            logger.error(f"❌ {self.exchange_name} 连接失败: {e}")
            raise
    
    async def disconnect(self):
        """断开连接"""
        if self._orderbook_update_task:
            self._orderbook_update_task.cancel()
            try:
                await self._orderbook_update_task
            except asyncio.CancelledError:
                pass
        
        # ✅ 不主动断开 ExtendedClient 的连接（由调用方管理）
        logger.info(f"⏹️ {self.exchange_name} 已断开: {self.contract_id}")
    
    async def subscribe_orderbook(self, callback: Callable):
        """订阅订单簿（通过轮询 Extended API）"""
        self._orderbook_callback = callback
        
        # ✅ Extended 没有订单簿 WebSocket，使用轮询方式
        self._orderbook_update_task = asyncio.create_task(
            self._poll_orderbook()
        )
        
        logger.info(f"📡 {self.exchange_name} 订阅订单簿: {self.contract_id}")
    
    async def _poll_orderbook(self):
        """轮询订单簿数据"""
        try:
            while True:
                try:
                    # ✅ 调用 ExtendedClient 的 fetch_bbo_prices_extended 方法
                    bid, ask, _, bid_size, ask_size = \
                        await self.client.fetch_bbo_prices_extended(
                            self.contract_id
                        )
                    
                    # ✅ 转换为浮点数
                    bid = float(bid) if bid is not None else 0.0
                    ask = float(ask) if ask is not None else 0.0
                    bid_size = float(bid_size) if bid_size is not None else 0.0
                    ask_size = float(ask_size) if ask_size is not None else 0.0
                    
                    if bid > 0 and ask > 0:
                        # ✅ 格式化为标准订单簿格式
                        self._orderbook = {
                            'bids': [[bid, bid_size]],
                            'asks': [[ask, ask_size]],
                            'timestamp': asyncio.get_event_loop().time()
                        }
                        
                        # 触发回调
                        if self._orderbook_callback:
                            await self._orderbook_callback(self._orderbook)
                
                except Exception as e:
                    logger.debug(f"轮询订单簿失败: {e}")
                
                # ✅ 每 0.5 秒轮询一次
                await asyncio.sleep(0.5)
        
        except asyncio.CancelledError:
            logger.debug(f"{self.exchange_name} 订单簿轮询已停止")
        except Exception as e:
            logger.error(f"❌ {self.exchange_name} 订单簿轮询异常: {e}")

    def _on_order_update(self, order_data: dict):
        """处理 WebSocket 订单更新"""
        order_id = order_data.get('order_id')
        status = order_data.get('status')
        
        if order_id:
            # ✅ 保存最终状态
            self._order_status_data[order_id] = status
            
            # ✅ 通知等待者
            if order_id in self._order_status_events:
                self._order_status_events[order_id].set()

    async def _wait_for_order_status(
        self,
        order_id: str,
        timeout: float = 1.0
    ):
        event = asyncio.Event()
        self._order_status_events[order_id] = event

        try:
            await asyncio.wait_for(event.wait(), timeout)

            status = self._order_status_data.get(order_id, None)
            return status
        except TimeoutError:
            logger.warning(f"⚠️ 等待订单状态超时: {order_id}")
            return None
        finally:
            # 清理
            self._order_status_events.pop(order_id, None)
            self._order_status_data.pop(order_id, None)

    async def place_open_order(self,
        side: str,
        quantity: Decimal,
        price: Optional[Decimal] = None,
        retry_mode: str = 'opportunistic',
        quote_id: Optional[str] = None
    ) -> dict:
        """
        下开仓单
    
        Args:
            retry_mode: 
                - 'opportunistic': 机会主义（失败就放弃）
                - 'aggressive': 激进模式（重试直到成功）
        
        注意：Extended 使用 IOC 订单，天然就是"激进"的，
            retry_mode 参数主要用于日志记录和未来扩展
        """
        return await self.place_market_order(side, quantity, price, retry_mode)

    async def place_close_order(self,
        side: str,
        quantity: Decimal,
        price: Optional[Decimal] = None,
        retry_mode: str = 'opportunistic',
        quote_id: Optional[str] = None
    ) -> dict:
        """
        下平仓单

        Args:
            retry_mode: 
                - 'opportunistic': 机会主义（失败就放弃）
                - 'aggressive': 激进模式（重试直到成功）
        
        注意：Extended 使用 IOC 订单，天然就是"激进"的，
            retry_mode 参数主要用于日志记录和未来扩展
        """
        return await self.place_market_order(side, quantity, price, retry_mode)
    
    async def place_market_order(
        self,
        side: str,
        quantity: Decimal,
        price: Optional[Decimal] = None,
        retry_mode: str = 'opportunistic'
    ) -> dict:
        """
        下市价单
    
        Args:
            retry_mode: 
                - 'opportunistic': 机会主义（失败就放弃）
                - 'aggressive': 激进模式（重试直到成功）
        
        注意：Extended 使用 IOC 订单，天然就是"激进"的，
            retry_mode 参数主要用于日志记录和未来扩展
        """
        try:
            order_side = OrderSide.BUY if side.upper() == 'BUY' else OrderSide.SELL
            if retry_mode == 'aggressive':
                if side.upper() == 'BUY':
                    order_price = price * Decimal('0.9998')  # 确保买入
                else:
                    order_price = price * Decimal('1.0002')  # 确保卖出
                order_price = self.client.round_to_tick(order_price)
                print(f"Adjusted order price for aggressive mode: {order_price}")
            else:
                order_price = price
            
            logger.info(
                f"📤 {self.exchange_name} 下单: {side} {quantity} @ ${price} ({retry_mode})"
            )
            
            # ✅ 调用 ExtendedClient 的下单方法
            order_result = await self.client.perpetual_trading_client.place_order(
                market_name=self.contract_id,
                amount_of_synthetic=quantity,
                price=price,
                side=order_side,
                time_in_force=TimeInForce.IOC,
                post_only=False,
            )
            
            if not order_result or not hasattr(order_result, 'data') or not order_result.data:
                error_msg = getattr(order_result, 'message', 'Unknown error')
                logger.error(f"❌ 下单失败: {error_msg}")

                return {
                    'success': False,
                    'order_id': None,
                    'error': error_msg
                }
            
            order_id = order_result.data.id
            if not order_id:
                return {
                    'success': False,
                    'order_id': None,
                    'error': 'No order ID returned'
                }
            
            # 等待订单执行
            # await asyncio.sleep(0.1)
            
            # ✅ 获取订单状态
            # order_info = await self.client.get_order_info(order_id)
            # if not order_info:
            #     logger.warning(f"⚠️ 无法获取订单状态，假设已成交")
            #     return {
            #         'success': True,
            #         'order_id': order_id,
            #         'error': None
            #     }
            
            # # ✅ 检查订单状态
            # status = str(order_info.status).upper()
            status = await self._wait_for_order_status(order_id, timeout=1.0)
            logger.info(f"订单状态: {order_id} -> {status}")

            if status == 'NEW':
                status = 'OPEN'
            elif status == 'CANCELLED':
                status = 'CANCELED'

            if status in ['CANCELED', 'REJECTED']:
                # ✅ 激进模式：重试
                # if retry_mode == 'aggressive':
                #     logger.info("🔄 激进模式：订单被拒绝，重试...")
                #     # await asyncio.sleep(0.5)
                #     return await self.place_market_order(side, quantity, price, retry_mode='aggressive')
                # A 所直接返回，等待下一次机会
                return {
                    'success': False,
                    'order_id': order_id,
                    'error': f'Order {status}'
                }
            
            if status in ['NEW', 'OPEN', 'PARTIALLY_FILLED', 'FILLED']:
                logger.info(f"✅ {self.exchange_name} 下单成功: {order_id} ({status})")
                return {
                    'success': True,
                    'order_id': order_id,
                    'error': None
                }
            
            return {
                'success': False,
                'order_id': order_id,
                'error': f'Unknown status: {status}'
            }
        
        except Exception as e:
            logger.error(f"❌ {self.exchange_name} 下单失败: {e}")
            import traceback
            traceback.print_exc()

            if retry_mode == 'aggressive':
                logger.info("🔄 激进模式：重试下单...")
                await asyncio.sleep(0.5)
                return await self.place_market_order(side, quantity, price, retry_mode='opportunistic')

            return {
                'success': False,
                'order_id': None,
                'error': str(e)
            }
    
    def get_latest_orderbook(self) -> Optional[Dict]:
        """获取最新订单簿"""
        return self._orderbook