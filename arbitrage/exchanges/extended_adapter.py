"""Extended 交易所适配器"""

import asyncio
import logging
from decimal import Decimal
import time
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
        self._order_status_data: Dict[str, dict] = {}  # ← 添加这一行！
        self._order_status_futures: Dict[str, asyncio.Future] = {}
        self._extended_orderbook_handler = None
    
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
            # ✅ 注册订单更新回调
            self.client.setup_order_update_handler(self._on_order_update)
            
            # ✅ 新增：注册订单簿更新回调（如果支持）
            if hasattr(self.client, 'setup_orderbook_handler'):
                logger.info(f"📡 {self.exchange_name} 使用 WebSocket 订单簿回调")
                self.client.setup_orderbook_handler(self._on_extended_orderbook_update)
            else:
                logger.info(f"📡 {self.exchange_name} 将使用轮询订单簿（WebSocket 回调不支持）")
                self._orderbook_update_task = asyncio.create_task(
                    self._poll_orderbook()
                )

        except Exception as e:
            logger.exception(f"❌ {self.exchange_name} 连接失败: {e}")
            raise
    
    async def disconnect(self):
        """断开连接"""
        # ✅ 1. 取消订单簿轮询任务（如果在运行）
        if self._orderbook_update_task:
            logger.info(f"⏹️ 停止 {self.exchange_name} 订单簿轮询...")
            self._orderbook_update_task.cancel()
            try:
                await self._orderbook_update_task
            except asyncio.CancelledError:
                logger.debug(f"✅ {self.exchange_name} 订单簿轮询已取消")
        
        # ✅ 2. 移除订单簿回调（如果在使用 WebSocket）
        if self._extended_orderbook_handler and hasattr(self.client, 'setup_orderbook_handler'):
            logger.info(f"⏹️ 移除 {self.exchange_name} 订单簿回调...")
            self.client.setup_orderbook_handler(None)  # 移除回调
            self._extended_orderbook_handler = None
        
        # ✅ 3. 移除订单更新回调
        if hasattr(self.client, 'setup_order_update_handler'):
            self.client.setup_order_update_handler(None)
        
        # ✅ 4. 清理订单状态缓存
        self._order_status_futures.clear()
        self._order_status_data.clear()
        self._orderbook = None
        self._orderbook_callback = None
        
        # ✅ 5. 不主动断开 ExtendedClient 的连接（由调用方管理）
        # 但可以添加一个标志位
        logger.info(f"✅ {self.exchange_name} 适配器已断开: {self.contract_id}")
    
    
    async def subscribe_orderbook(self, callback: Callable):
        """订阅订单簿（通过轮询 Extended API）"""
        self._orderbook_callback = callback

        self._extended_orderbook_handler = self._on_extended_orderbook_update

        if hasattr(self.client, 'setup_orderbook_handler'):
            self.client.setup_orderbook_handler(self._extended_orderbook_handler)
        else:
            # ✅ 回退到轮询（如果 ExtendedClient 不支持自定义回调）
            logger.warning(f"⚠️ {self.exchange_name} 不支持 WebSocket 回调，使用轮询")
            self._orderbook_update_task = asyncio.create_task(
                self._poll_orderbook()
            )
        logger.info(f"📡 {self.exchange_name} 订阅订单簿: {self.contract_id}")

    async def _on_extended_orderbook_update(self, orderbook_data: dict):
        """
        处理 Extended WebSocket 订单簿更新
        
        Args:
            orderbook_data: Extended WebSocket 推送的订单簿数据
                {
                    'ts_server': 1732611639000,  # 时间戳（待定，暂不使用）
                    'timestamp': 1732611639000,  # 时间戳（秒）
                    'market': 'ETH-USD',
                    'bid': [{"p": 2945.9, "q": 10.0}],
                    'ask': [{"p": 2946.1, "q": 10.0}]
                }
        """
        try:
            # ✅ 提取数据
            bids = orderbook_data.get('bid', [])
            asks = orderbook_data.get('ask', [])
            ts = orderbook_data.get('timestamp')

            if not bids or not asks:
                return
            
            # ✅ 转换为标准格式
            bid_price = float(bids[0]['p'])
            bid_size = float(bids[0]['q'])
            ask_price = float(asks[0]['p'])
            ask_size = float(asks[0]['q'])
            
            # ✅ 创建订单簿（使用 Extended 原始时间戳）
            self._orderbook = {
                'bids': [[bid_price, bid_size]],
                'asks': [[ask_price, ask_size]],
                'timestamp': ts,  # ← 使用接收到ws数据的时间戳（毫秒转秒）
                'poll_duration_ms': 0,  # WebSocket 无 API 调用延迟
                'mark_price': None  # extended 订单簿没有该字段
            }
            
            # ✅ 触发回调
            if self._orderbook_callback:
                await self._orderbook_callback(self._orderbook)
            
            logger.debug(
                f"📊 Extended WebSocket 订单簿更新:\n"
                f"   买一: ${bid_price}\n"
                f"   卖一: ${ask_price}\n"
                f"   时间戳: {ts:.6f}"
            )
        
        except Exception as e:
            logger.exception(f"❌ 处理 Extended 订单簿更新失败: {e}")

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
                            'timestamp': time.time()
                        }
                        
                        # 触发回调
                        if self._orderbook_callback:
                            await self._orderbook_callback(self._orderbook)
                
                except Exception as e:
                    logger.exception(f"轮询订单簿失败: {e}")
                
                # ✅ 每 0.5 秒轮询一次
                await asyncio.sleep(0.5)
        
        except asyncio.CancelledError:
            logger.exception(f"{self.exchange_name} 订单簿轮询已停止")
        except Exception as e:
            logger.exception(f"❌ {self.exchange_name} 订单簿轮询异常: {e}")

    def _on_order_update(self, order_data: dict):
        """处理 WebSocket 订单更新"""
        order_id = order_data.get('order_id')
        status = order_data.get('status')
        filled_size = order_data.get('filled_size', 0)
        price = order_data.get('price', 0)
        logger.info(f"📨 收到订单更新: order_id={order_id}, status={status}, "
                    f"filled_size={filled_size}, price={price}")

        if order_id in self._order_status_futures:
            future = self._order_status_futures[order_id]
            if not future.done():
                logger.info(f"✅ 设置 Future 结果: {order_id} -> {status}")
                future.set_result({
                    'status': status,
                    'filled_size': filled_size,
                    'price': price
                })
        else:
            # ✅ 没有等待者，缓存状态
            logger.debug(f"📦 缓存订单状态: {order_id} -> {status}")
            self._order_status_data[order_id] = {
                'status': status,
                'filled_size': filled_size,
                'price': price
            }

    async def _wait_for_order_status(
        self,
        order_id: str,
        timeout: float = 1.0
    ):
        """
        等待订单状态（包含 filled_size）
        
        Returns:
            {
                'status': str,
                'filled_size': Decimal,
                'price': Decimal
            }
        """
        # ✅ 1. 先检查是否已经有状态（可能在下单时就已推送）
        if order_id in self._order_status_data:
            data = self._order_status_data.pop(order_id)
            logger.info(f"✅ 订单状态已存在（无需等待）: {order_id} -> "
                        f"{data['status']}, {data['filled_size']}, {data['price']}")
            return data

        # ✅ 2. 创建 Future 并等待
        loop = asyncio.get_event_loop()
        future = loop.create_future()
        self._order_status_futures[order_id] = future
        
        logger.info(f"⏳ 开始等待订单状态: {order_id}, 超时={timeout}s")
        
        wait_start = time.time()
    
        try:
            # ✅ 直接等待 Future（不循环！）
            data = await asyncio.wait_for(future, timeout=timeout)

            wait_duration = (time.time() - wait_start) * 1000
            logger.info(
                f"✅ 收到状态: {order_id} -> {data['status']},"
                f" filled_size={data['filled_size']}, price={data['price']} (耗时 {wait_duration:.2f} ms)"
            )
            return data

        except asyncio.TimeoutError:
            wait_duration = (time.time() - wait_start) * 1000
            logger.warning(f"⚠️ 等待订单状态超时: {order_id} ({wait_duration:.2f} ms)")
            return {
                'status': None,
                'filled_size': Decimal('0'),
                'price': Decimal('0')
            }

        except Exception as e:
            logger.exception(f"❌ 等待订单状态异常: {e}")
            return {
                'status': None,
                'filled_size': Decimal('0'),
                'price': Decimal('0')
            }
        
        finally:
            # ✅ 清理
            self._order_status_futures.pop(order_id, None)
            self._order_status_data.pop(order_id, None)

    async def place_open_order(self,
        side: str,
        quantity: Decimal,
        price: Optional[Decimal] = None,
        retry_mode: str = 'opportunistic',
        quote_id: Optional[str] = None,
        slippage: Optional[Decimal] = None
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
        return await self.place_market_order(side, quantity, price, retry_mode, slippage)

    async def place_close_order(self,
        side: str,
        quantity: Decimal,
        price: Optional[Decimal] = None,
        retry_mode: str = 'opportunistic',
        quote_id: Optional[str] = None,
        slippage: Optional[Decimal] = None
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
        return await self.place_market_order(side, quantity, price, retry_mode, slippage)
    
    async def place_market_order(
        self,
        side: str,
        quantity: Decimal,
        price: Optional[Decimal] = None,
        retry_mode: str = 'opportunistic',
        slippage: Optional[Decimal] = None
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
            slippage = slippage if slippage is not None else self.slippage
            logger.info(f"Placing market order with slippage: {slippage}")
            if retry_mode == 'aggressive':
                if side.upper() == 'BUY':
                    order_price = price * Decimal(str(1 - (slippage or Decimal('0'))))  # 确保买入
                else:
                    order_price = price * Decimal(str(1 + (slippage or Decimal('0'))))  # 确保卖出
                order_price = self.client.round_to_tick(order_price)
                print(f"Adjusted order price for aggressive mode: {order_price}")
            else:
                order_price = price
            
            logger.info(
                f"📤 {self.exchange_name} 下单: {side} {quantity} @ ${price} ({retry_mode})"
            )
            order_start_time = time.time()
            # ✅ 调用 ExtendedClient 的下单方法
            order_result = await self.client.perpetual_trading_client.place_order(
                market_name=self.contract_id,
                amount_of_synthetic=quantity,
                price=price,
                side=order_side,
                time_in_force=TimeInForce.IOC,
                post_only=False,
            )
            order_place_time = time.time()
            place_duration = (order_place_time - order_start_time) * 1000
            logger.info(f"⏱️ 下单 API 耗时: {place_duration:.2f} ms")
            logger.info(f"下单结果: {order_result}")
            if not order_result or not hasattr(order_result, 'data') or not order_result.data:
                error_msg = getattr(order_result, 'message', 'Unknown error')
                logger.error(f"❌ 下单失败: {error_msg}")

                return {
                    'success': False,
                    'order_id': None,
                    'error': error_msg,
                    'filled_price': Decimal('0'),
                    'filled_quantity': Decimal('0'),
                    'timestamp': time.time(),
                    'place_duration_ms': place_duration,
                    'execution_duration_ms': 0,
                }
            
            order_id = order_result.data.id
            if not order_id:
                return {
                    'success': False,
                    'order_id': None,
                    'error': 'No order ID returned',
                    'filled_price': Decimal('0'),
                    'filled_quantity': Decimal('0'),
                    'timestamp': time.time(),
                    'place_duration_ms': place_duration,
                    'execution_duration_ms': 0,
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
            # ✅ 等待状态
            wait_start_time = time.time()
            logger.info(f"⏳ 开始等待订单状态: {order_id}")
            
            status_data = await self._wait_for_order_status(order_id, timeout=1.0)
            status = status_data.get('status')
            filled_size_from_ws = Decimal(str(status_data.get('filled_size', 0)))
            price_from_ws = Decimal(str(status_data.get('price', 0)))
        
            logger.info(f"订单状态: {order_id} -> {status}")
            wait_end_time = time.time()
            wait_duration = (wait_end_time - wait_start_time) * 1000
            logger.info(f"⏱️ 等待状态耗时: {wait_duration:.2f} ms, 状态: {status}")  # ← 瓶颈在这里！
            
            total_duration = (wait_end_time - order_start_time) * 1000
            logger.info(f"⏱️ 下单总耗时: {total_duration:.2f} ms")
            
            if status == 'NEW':
                status = 'OPEN'
            elif status == 'CANCELLED':
                status = 'CANCELED'
            if status in ['CANCELED']:
                # ✅ 使用 WebSocket 数据
                if filled_size_from_ws > 0:
                    logger.warning(
                        f"⚠️ 部分成交后取消（WebSocket 数据）:\n"
                        f"   订单 ID: {order_id}\n"
                        f"   已成交: {filled_size_from_ws} / {quantity}\n"
                        f"   成交价: ${price_from_ws}"
                    )
                    
                    return {
                        'success': False,  # ✅ 部分成交也标记为失败（需要上层处理）
                        'order_id': order_id,
                        'error': 'Order CANCELED (partial fill)',
                        'filled_price': price_from_ws,
                        'filled_quantity': filled_size_from_ws,
                        'partial_fill': True,  # ✅ 标记为部分成交
                        'timestamp': time.time(),
                        'place_duration_ms': place_duration,
                        'execution_duration_ms': wait_duration,
                    }
                else:
                    logger.info(
                        f"✅ 订单已取消，未成交:\n"
                        f"   订单 ID: {order_id}"
                    )
                    return {
                        'success': False,
                        'order_id': order_id,
                        'error': 'Order CANCELED (no fill)',
                        'filled_price': Decimal('0'),
                        'filled_quantity': Decimal('0'),
                        'timestamp': time.time(),
                        'place_duration_ms': place_duration,
                        'execution_duration_ms': wait_duration,
                    }
            if status in ['REJECTED']:
                # ✅ 激进模式：重试
                # if retry_mode == 'aggressive':
                #     logger.info("🔄 激进模式：订单被拒绝，重试...")
                #     # await asyncio.sleep(0.5)
                #     return await self.place_market_order(side, quantity, price, retry_mode='aggressive')
                # A 所直接返回，等待下一次机会
                return {
                    'success': False,
                    'order_id': order_id,
                    'error': f'Order {status}',
                    'filled_price': Decimal('0'),
                    'filled_quantity': Decimal('0'),
                    'timestamp': time.time(),
                    'place_duration_ms': place_duration,
                    'execution_duration_ms': wait_duration,
                }
            
            if status in ['NEW', 'OPEN', 'PARTIALLY_FILLED', 'FILLED']:
                # ✅ 调用 get_order_info 获取实际成交价
                order_info = await self.client.get_order_info(order_id)
                
                if order_info:
                    filled_price = order_info.price  # ✅ 实际成交价
                    filled_quantity = order_info.filled_size
                    
                    logger.info(
                        f"✅ Extended 市价单成交:\n"
                        f"   订单 ID: {order_id}\n"
                        f"   成交价: ${filled_price}\n"
                        f"   成交量: {filled_quantity}"
                    )
                else:
                    # ✅ 后备：使用信号价格
                    filled_price = price or Decimal('0')
                    filled_quantity = quantity
                    
                    logger.warning(
                        f"⚠️ 无法获取订单详情，使用信号价格:\n"
                        f"   订单 ID: {order_id}\n"
                        f"   成交价（信号）: ${filled_price}"
                    )
                
                return {
                    'success': True,
                    'order_id': order_id,
                    'filled_price': filled_price,
                    'filled_quantity': filled_quantity,
                    'error': None,
                    'timestamp': time.time(),
                    'place_duration_ms': place_duration,
                    'execution_duration_ms': wait_duration,
                }
            
            return {
                'success': False,
                'order_id': order_id,
                'error': f'Unknown status: {status}',
                'filled_price': Decimal('0'),
                'filled_quantity': Decimal('0'),
                'timestamp': time.time(),
                'place_duration_ms': place_duration,
                'execution_duration_ms': wait_duration,
            }
        
        except Exception as e:
            logger.exception(f"❌ {self.exchange_name} 下单失败: {e}")

            if retry_mode == 'aggressive':
                logger.info("🔄 激进模式：重试下单...")
                # await asyncio.sleep(0.5)
                return await self.place_market_order(side, quantity, price, retry_mode='opportunistic')

            return {
                'success': False,
                'order_id': None,
                'error': str(e),
                'filled_price': Decimal('0'),
                'filled_quantity': Decimal('0'),
            }
    
    def get_latest_orderbook(self) -> Optional[Dict]:
        """获取最新订单簿"""
        return self._orderbook
    
    async def get_position(self, symbol: str) -> Optional[dict]:
        """
        获取 Extended 持仓信息
        
        Args:
            symbol: 币种符号（如 'HYPE'）
        
        Returns:
            {
                'symbol': 'HYPE',
                'side': 'short',
                'size': 2.5,
                'entry_price': 28.5,
                'unrealized_pnl': -0.05
            }
        """
        try:
            # ✅ 调用 ExtendedClient 的 get_position 方法
            # 注意：你需要先在 extended.py 中添加 get_position() 方法
            position = await self.client.get_position(symbol)
            
            if position:
                logger.debug(
                    f"📊 {self.exchange_name} 持仓:\n"
                    f"   Symbol: {position['symbol']}\n"
                    f"   Side: {position['side']}\n"
                    f"   Size: {position['size']}\n"
                    f"   Entry Price: ${position['entry_price']}"
                )
            else:
                logger.debug(f"📊 {self.exchange_name} 无持仓: {symbol}")
            
            return position
        
        except Exception as e:
            logger.exception(f"❌ {self.exchange_name} 获取持仓失败: {e}", exc_info=True)
            return None