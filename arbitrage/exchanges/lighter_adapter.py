"""Lighter 交易所适配器"""

import asyncio
import logging
import json
import os
import random
import time
import websockets
from decimal import Decimal
from typing import Optional, Callable, Dict
from .base import ExchangeAdapter

logger = logging.getLogger(__name__)

class LighterAdapter(ExchangeAdapter):
    """Lighter 交易所适配器"""
    
    def __init__(self, symbol: str, client, config: dict = None):
        super().__init__(symbol, client, config)

        # ✅ 调试：打印客户端的所有方法
        logger.info(f"🔍 LighterClient 可用方法:")
        for attr in dir(self.client):
            if not attr.startswith('_') and callable(getattr(self.client, attr)):
                logger.info(f"   - {attr}")

        self.market_index = None
        self.ws_task = None
        self.ws = None
        self.account_index = int(os.getenv('LIGHTER_ACCOUNT_INDEX'))

        
        # ✅ Lighter 订单簿数据
        self.lighter_order_book = {
            "bids": {},  # {Decimal(price): Decimal(size)}
            "asks": {}
        }
        self.lighter_order_book_lock = asyncio.Lock()
        self.lighter_best_bid = None
        self.lighter_best_ask = None
        self.lighter_snapshot_loaded = False
        
        # 消息计数器
        self.message_count = 0

        self._order_status_data: Dict[int, dict] = {}  # key: client_order_index
        self._order_status_futures: Dict[int, asyncio.Future] = {}
    
    async def connect(self):
        """连接 Lighter"""
        try:
            if self.client.config.contract_id is not None and self.client.config.contract_id != '':
                logger.info(
                    f"✅ {self.exchange_name} 已连接: "
                    f"contract_id={self.client.config.contract_id}"
                )
            else:
                logger.warning(
                    f"⚠️ {self.exchange_name} contract_id 未设置，"
                    f"将在订阅订单簿时获取"
                )

            if hasattr(self.client, 'setup_order_update_handler'):
                self.client.setup_order_update_handler(self._on_order_update)
                logger.info(f"📡 {self.exchange_name} 订单更新回调已注册")
            else:
                logger.warning(f"⚠️ {self.exchange_name} 不支持订单更新回调")

        except Exception as e:
            logger.error(f"❌ {self.exchange_name} 连接失败: {e}")
            raise
    
    async def _get_market_index(self) -> int:
        """获取 Lighter market index"""
        try:
            if self.client.config.contract_id is None or self.client.config.contract_id == '':
                raise ValueError(
                    "contract_id 未设置，请确保在 main.py 中调用了 "
                    "get_contract_attributes() 并设置了 contract_id"
                )
            
            market_index = int(self.client.config.contract_id)
            logger.info(f"✅ Lighter market_index: {market_index}")
            return market_index
        except Exception as e:
            logger.error(f"获取 market_index 失败: {e}")
            raise
    
    async def disconnect(self):
        self._order_status_futures.clear()
        self._order_status_data.clear()
        """断开连接"""
        if self.ws_task:
            self.ws_task.cancel()
            try:
                await self.ws_task
            except asyncio.CancelledError:
                pass
        
        if self.ws:
            await self.ws.close()
        if hasattr(self.client, 'setup_order_update_handler'):
            self.client.setup_order_update_handler(None)
        logger.info(f"⏹️ {self.exchange_name} 已断开: market {self.market_index}")
    
    async def subscribe_orderbook(self, callback: Callable):
        """订阅订单簿（使用 Lighter WebSocket）"""
        self._orderbook_callback = callback
        
        # 获取 market_index
        self.market_index = await self._get_market_index()
        
        # 启动 WebSocket 任务
        self.ws_task = asyncio.create_task(self._handle_lighter_ws())
        
        logger.info(f"📡 {self.exchange_name} 订阅订单簿: market {self.market_index}")
    
    async def _handle_lighter_ws(self):
        """处理 Lighter WebSocket"""
        url = "wss://mainnet.zklighter.elliot.ai/stream"
        reconnect_count = 0
        
        while True:
            try:
                logger.info(f"🔌 连接 Lighter WebSocket: {url}")
                
                async with websockets.connect(url) as ws:
                    self.ws = ws
                    reconnect_count = 0
                    
                    # ✅ 订阅订单簿
                    subscribe_msg = {
                        "type": "subscribe",
                        "channel": f"order_book/{self.market_index}"
                    }
                    await ws.send(json.dumps(subscribe_msg))
                    logger.info(f"📡 已订阅 Lighter 订单簿: market {self.market_index}")
                    try:

                        # ✅ 新增：订阅订单更新流
                        ten_minutes_deadline = int(time.time() + 10 * 60)

                        auth_token, err = self.client.lighter_client.create_auth_token_with_expiry(ten_minutes_deadline)

                        if err is not None:
                            logger.warning(f"⚠️ Failed to create auth token for account orders subscription: {err}")
                        else: 

                            subscribe_orders_msg = {
                                "type": "subscribe",
                                "channel": f"account_orders/{self.market_index}/{self.account_index}",
                                "auth": auth_token
                            }
                            await ws.send(json.dumps(subscribe_orders_msg))
                            logger.info("✅ Subscribed to account orders with auth token (expires in 10 minutes)")
                    except Exception as e:
                        logger.warning(f"⚠️ Error creating auth token for account orders subscription: {e}")

                    # 接收消息
                    while True:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=1.0)  # 1s 超时
                            data = json.loads(msg)
                            
                            if data.get("type") == "ping":
                                await ws.send(json.dumps({"type": "pong"}))
                                continue
                            
                            await self._process_lighter_message(data)  # 处理消息
                            
                        except asyncio.TimeoutError:
                            logger.debug("⏳ Lighter WS 1s 无消息，继续监听...")  # 心跳检查
                            continue
                        except websockets.exceptions.ConnectionClosed:
                            logger.warning("⚠️ Lighter WS 连接关闭，重连...")
                            break  # 跳出内循环，重连外层
            
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"⚠️ Lighter WebSocket 连接关闭: {e}")
            except Exception as e:
                logger.error(f"❌ Lighter WebSocket 异常: {e}")
                import traceback
                traceback.print_exc()
            
            # 重连逻辑
            reconnect_count += 1
            wait_time = min(10, reconnect_count)
            logger.info(f"⏳ {wait_time}秒后重连 Lighter WebSocket...")
            await asyncio.sleep(wait_time)
    
    async def _process_lighter_message(self, data: dict):
        """
        处理 Lighter WebSocket 消息
        
        消息格式：
        {
          "type": "update/order_book",
          "channel": "order_book:0",
          "order_book": {
            "bids": [{"price": "3075.85", "size": "3.2078"}],
            "asks": [{"price": "3076.10", "size": "3.0000"}]
          }
        }
        """
        msg_type = data.get("type")
        channel = data.get("channel", "")        
        # ✅ Lighter 使用 "update/order_book" 类型
        if msg_type == "update/order_book":
            # ✅ 如果是第一次收到，当作快照处理
            if not self.lighter_snapshot_loaded:
                logger.info("📸 收到 Lighter 初始订单簿（当作快照）")
                await self._handle_lighter_snapshot(data)
            else:
                # ✅ 后续消息当作增量更新
                await self._handle_lighter_update(data)
        
        elif msg_type == "snapshot":
            # ✅ 如果有专门的 snapshot 类型
            logger.info("📸 收到 Lighter 快照消息")
            await self._handle_lighter_snapshot(data)

        elif msg_type in ["update/account_orders"]:
            logger.debug(f"📨 收到订单更新消息: {data}")
            orders = data.get("orders", {}).get(str(self.market_index), [])
            for order_data in orders:
                logger.info(f"---------order-data---------{order_data}")
                if order_data.get('status') == "filled":
                # 调用订单更新 handler
                    self._on_order_update(order_data)
        else:
            # 未知消息类型
            if self.message_count <= 5:
                logger.debug(f"⏭️ 跳过消息类型: {msg_type}")
    
    async def _handle_lighter_snapshot(self, data: dict):
        """处理 Lighter 快照消息"""
        try:
            async with self.lighter_order_book_lock:
                # ✅ 清空订单簿
                self.lighter_order_book = {"bids": {}, "asks": {}}
                
                # ✅ 数据在 order_book 字段内
                order_book = data.get("order_book", {})
                
                bids = order_book.get("bids", [])
                asks = order_book.get("asks", [])
                
                logger.info(
                    f"📸 Lighter 快照数据:\n"
                    f"   bids: {len(bids)} 条\n"
                    f"   asks: {len(asks)} 条"
                )
                
                # ✅ 处理 bids
                for bid in bids:
                    price = Decimal(str(bid["price"]))
                    size = Decimal(str(bid["size"]))
                    
                    # ✅ 跳过 size=0 的档位
                    if size > 0:
                        self.lighter_order_book["bids"][price] = size
                
                # ✅ 处理 asks
                for ask in asks:
                    price = Decimal(str(ask["price"]))
                    size = Decimal(str(ask["size"]))
                    
                    if size > 0:
                        self.lighter_order_book["asks"][price] = size
                
                # 更新最佳价格
                self._update_lighter_best_prices()
                
                self.lighter_snapshot_loaded = True
                
                logger.info(
                    f"✅ Lighter 快照加载完成:\n"
                    f"   {len(self.lighter_order_book['bids'])} bids\n"
                    f"   {len(self.lighter_order_book['asks'])} asks\n"
                    f"   Best Bid: ${self.lighter_best_bid}\n"
                    f"   Best Ask: ${self.lighter_best_ask}"
                )
                
                # 通知回调
                await self._notify_orderbook_update()
        
        except Exception as e:
            logger.error(f"❌ 处理 Lighter 快照失败: {e}")
            import traceback
            traceback.print_exc()
    
    async def _handle_lighter_update(self, data: dict):
        """处理 Lighter 增量更新消息"""
        if not self.lighter_snapshot_loaded:
            return
        
        try:
            async with self.lighter_order_book_lock:
                # ✅ 数据在 order_book 字段内
                order_book = data.get("order_book", {})
                
                # ✅ 处理 bids 更新
                for bid in order_book.get("bids", []):
                    price = Decimal(str(bid["price"]))
                    size = Decimal(str(bid["size"]))
                    
                    if size == 0:
                        # 删除该价格档位
                        self.lighter_order_book["bids"].pop(price, None)
                    else:
                        # 更新该价格档位
                        self.lighter_order_book["bids"][price] = size
                
                # ✅ 处理 asks 更新
                for ask in order_book.get("asks", []):
                    price = Decimal(str(ask["price"]))
                    size = Decimal(str(ask["size"]))
                    
                    if size == 0:
                        self.lighter_order_book["asks"].pop(price, None)
                    else:
                        self.lighter_order_book["asks"][price] = size
                
                # 更新最佳价格
                self._update_lighter_best_prices()
                
                # 通知回调
                await self._notify_orderbook_update()
        
        except Exception as e:
            logger.error(f"❌ 处理 Lighter 更新失败: {e}")
            import traceback
            traceback.print_exc()
    
    def _update_lighter_best_prices(self):
        """更新 Lighter 最佳买卖价"""
        if self.lighter_order_book["bids"]:
            self.lighter_best_bid = max(self.lighter_order_book["bids"].keys())
        else:
            self.lighter_best_bid = None
        
        if self.lighter_order_book["asks"]:
            self.lighter_best_ask = min(self.lighter_order_book["asks"].keys())
        else:
            self.lighter_best_ask = None
    
    async def _notify_orderbook_update(self):
        """通知订单簿更新"""
        if self._orderbook_callback and not self.lighter_best_bid or not self.lighter_best_ask:
            logger.warning(
                f"⚠️ 订单簿数据不完整:\n"
                f"   Best Bid: {self.lighter_best_bid}\n"
                f"   Best Ask: {self.lighter_best_ask}"
            )
            return
        
        # 格式化为标准订单簿格式
        bid_size = float(self.lighter_order_book["bids"].get(self.lighter_best_bid, 0))
        ask_size = float(self.lighter_order_book["asks"].get(self.lighter_best_ask, 0))
        
        ts = time.time()
        self._orderbook = {
            'bids': [[float(self.lighter_best_bid), bid_size]],
            'asks': [[float(self.lighter_best_ask), ask_size]],
            'timestamp': ts,
            'poll_duration_ms': 0,  # WebSocket 无延迟
            'mark_price': None  # Lighter 无该字段
        }
        self.client.order_book = {
                    'bids': dict(self.lighter_order_book['bids']),
                    'asks': dict(self.lighter_order_book['asks'])
                }
        self.client.best_bid = self.lighter_best_bid
        self.client.best_ask = self.lighter_best_ask
        logger.debug("✅ Order book synced to Client")
        logger.debug(
            f"📗 Lighter 订单簿更新:\n"
            f"   Bid: ${self.lighter_best_bid} x {bid_size}\n"
            f"   Ask: ${self.lighter_best_ask} x {ask_size}"
            f"   时间戳 ${ts:.3f}"
        )
        
        # 触发回调
        if self._orderbook_callback:
            await self._orderbook_callback(self._orderbook)

    def _on_order_update(self, order_update: dict):
        """处理 WebSocket 订单更新（同步回调）"""
        try:
            client_order_index = order_update.get('client_order_index')
            if client_order_index is None:
                logger.debug(f"⏭️ 跳过无 client_order_index 的更新")
                return

            if client_order_index in self._order_status_futures:
                real_order_id = order_update.get('order_id')
                status = order_update.get('status', '').upper()
                side = "short" if order_update["is_ask"] else "long"
                filled_size = order_update.get('filled_base_amount', Decimal('0'))
                price = Decimal(order_update.get('filled_quote_amount', '0')) / Decimal(order_update.get('filled_base_amount'))
                size = order_update.get('base_size', Decimal('0'))
                order_type = order_update.get('type', 'OPEN') # 字段无效
                contract_id = self.client.config.contract_id

                data = {
                    'order_id': real_order_id,
                    'client_order_index': client_order_index,
                    'status': status,
                    'side': side,
                    'order_type': order_type,
                    'size': size,
                    'price': price,
                    'contract_id': contract_id,
                    'filled_size': filled_size
                }
                self._order_status_data[client_order_index] = data

                future = self._order_status_futures.pop(client_order_index, None)
                if future and not future.done():
                    future.set_result(data)
                    logger.debug(f"✅ 订单状态 Future 已完成: {client_order_index} -> {status}")

            # ✅ 日志（节流）
            order_id = order_update.get('order_id')
            status = order_update.get('status')
            filled_size = order_update.get('filled_size', 0)
            price = order_update.get('price', 0)
            logger.info(f"📨 收到订单更新: client_idx={client_order_index}, order_id={order_id}, status={status}, "
                        f"filled_size={filled_size}, price={price}")

        except Exception as e:
            logger.error(f"❌ 处理订单更新失败: {e}")

    async def _wait_for_order_status(self, client_order_index: int, timeout: float = 1.0) -> dict:
        """等待订单状态（使用 Future）"""
        future = self._order_status_futures.get(client_order_index)
        if not future:
            raise ValueError(f"No future for client_order_index: {client_order_index}")
        
        try:
            status_data = await asyncio.wait_for(future, timeout=timeout)
            return status_data
        except asyncio.TimeoutError:
            logger.warning(f"⏰ 订单状态超时 (client_idx={client_order_index})")
            # 清理
            self._order_status_futures.pop(client_order_index, None)
            raise
        except Exception as e:
            logger.error(f"❌ 等待订单状态异常: {e}")
            self._order_status_futures.pop(client_order_index, None)
            raise

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
        
        注意：Lighter 使用 IOC 订单，天然就是"激进"的，
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
        
        注意：Lighter 使用 IOC 订单，天然就是"激进"的，
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
        下市价单（使用限价单 + IOC 模拟）
        
        Args:
            side: 'buy' 或 'sell'
            quantity: 数量
            price: 参考价格
        
        Returns:
            {
                'success': bool,
                'order_id': str,
                'error': str
            }
        """

        order_start_time = time.time()
        place_duration = 0
        wait_duration = 0
        try:
            side_upper = side.upper()
            
            loop = asyncio.get_event_loop()
            future = loop.create_future()
            client_order_index = int(time.time() * 1000) % 1000000

            self._order_status_futures[client_order_index] = future
            if retry_mode == 'aggressive':
                # ✅ 计算订单价格（和 hedge_monitor 一致）
                if side_upper == 'BUY':
                    order_price = Decimal(str(price)) * Decimal('1.0005') if price else self.lighter_best_ask
                else:
                    order_price = Decimal(str(price)) * Decimal('0.9995') if price else self.lighter_best_bid
            else:
                order_price = Decimal(str(price))
            logger.info(
                f"📤 {self.exchange_name} 下单:\n"
                f"   市场: {self.market_index}\n"
                f"   方向: {side_upper}\n"
                f"   原始数量: {quantity} (type: {type(quantity)})\n"
                f"   价格: {order_price}\n"
                f"   base_amount_multiplier: {self.client.base_amount_multiplier}\n"
                f"   price_multiplier: {self.client.price_multiplier}"
            )
            # 计算 base_amount
            base_amount_decimal = Decimal(str(quantity)) * Decimal(str(self.client.base_amount_multiplier))
            base_amount = int(base_amount_decimal)
            
            # 计算 price
            price_decimal = Decimal(str(order_price)) * Decimal(str(self.client.price_multiplier))
            price_int = int(price_decimal)
            
            logger.info(
                f"📋 计算后的订单参数:\n"
                f"   base_amount (decimal): {base_amount_decimal}\n"
                f"   base_amount (int): {base_amount}\n"
                f"   price (decimal): {price_decimal}\n"
                f"   price (int): {price_int}"
            )
            
            # ✅ 验证必要属性
            if not hasattr(self.client, 'base_amount_multiplier'):
                logger.error("❌ client 缺少 base_amount_multiplier")
                return {
                    'success': False,
                    'order_id': None,
                    'error': 'client.base_amount_multiplier not initialized'
                }
            
            if not hasattr(self.client, 'price_multiplier'):
                logger.error("❌ client 缺少 price_multiplier")
                return {
                    'success': False,
                    'order_id': None,
                    'error': 'client.price_multiplier not initialized'
                }
            
            if not hasattr(self.client, 'lighter_client'):
                logger.error("❌ client 缺少 lighter_client (SignerClient)")
                return {
                    'success': False,
                    'order_id': None,
                    'error': 'client.lighter_client not initialized'
                }
            
            # ✅ 确保 market_index 是整数
            try:
                market_index = int(self.market_index)
            except (ValueError, TypeError):
                logger.error(f"❌ 无效的 market_index: {self.market_index}")
                return {
                    'success': False,
                    'order_id': None,
                    'error': f'Invalid market_index: {self.market_index}'
                }

            # ✅ 构造订单参数（和 hedge_monitor 一致）
            order_params = {
                'market_index': market_index,
                'client_order_index': client_order_index,
                'base_amount': int(quantity * self.client.base_amount_multiplier),
                'price': int(order_price * self.client.price_multiplier),
                'is_ask': side_upper == 'SELL',
                'order_type': self.client.lighter_client.ORDER_TYPE_LIMIT,
                'time_in_force': self.client.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                'reduce_only': False,
                'trigger_price': 0,
            }
            
            logger.info(
                f"📋 Lighter 订单参数:\n"
                f"   market_index: {order_params['market_index']}\n"
                f"   client_order_index: {order_params['client_order_index']}\n"
                f"   base_amount: {order_params['base_amount']}\n"
                f"   price: {order_params['price']}\n"
                f"   is_ask: {order_params['is_ask']}"
            )
            
            # ✅ 签名订单
            tx_info, error = self.client.lighter_client.sign_create_order(**order_params)
            
            if error is not None:
                logger.error(f"❌ 签名失败: {error}")
                return {
                    'success': False,
                    'order_id': None,
                    'error': f'Sign error: {error}'
                }
            
            # ✅ 发送交易
            tx_hash = await self.client.lighter_client.send_tx(
                tx_type=self.client.lighter_client.TX_TYPE_CREATE_ORDER,
                tx_info=tx_info
            )
            
            if tx_hash is None:
                logger.error("❌ send_tx 返回 None")
                return {
                    'success': False,
                    'order_id': None,
                    'error': 'Order submission returned None'
                }
            
            logger.info(f"✅ {self.exchange_name} 下单成功: tx_hash={tx_hash}")

            # ✅ 等待订单状态（WebSocket 更新）
            wait_start_time = time.time()
            logger.info(f"⏳ 开始等待订单状态: client_idx={client_order_index}")
            # 下单成功后，判断订单是否成交
            try:
                status_data = await self._wait_for_order_status(client_order_index, timeout=1.0)
                status = status_data.get('status')
                real_order_id = status_data.get('order_id')
                filled_size_from_ws = Decimal(status_data.get('filled_size', '0'))
                price_from_ws = status_data.get('price', order_price)
                
                logger.info(f"订单状态: client_idx={client_order_index} -> {status} (order_id={real_order_id})")
                wait_end_time = time.time()
                wait_duration = (wait_end_time - wait_start_time) * 1000
                logger.info(f"⏱️ 等待状态耗时: {wait_duration:.2f} ms, 状态: {status}")
                
                total_duration = (wait_end_time - order_start_time) * 1000
                logger.info(f"⏱️ 下单总耗时: {total_duration:.2f} ms")
                
                # ✅ 状态标准化（匹配 extended 逻辑）
                if status == 'OPEN' and filled_size_from_ws > 0:
                    status = 'PARTIALLY_FILLED'

                
                if status in ['CANCELED', 'CANCELLED']:  # Lighter 可能用 CANCELLED
                    if filled_size_from_ws > 0:
                        logger.warning(
                            f"⚠️ 部分成交后取消（WebSocket 数据）:\n"
                            f"   订单 ID: {real_order_id}\n"
                            f"   已成交: {filled_size_from_ws} / {quantity}\n"
                            f"   成交价: ${price_from_ws}"
                        )
                        
                        return {
                            'success': False,  # 部分成交标记失败，上层处理
                            'order_id': real_order_id,
                            'error': 'Order CANCELED (partial fill)',
                            'filled_price': price_from_ws,
                            'filled_quantity': filled_size_from_ws,
                            'partial_fill': True,
                            'timestamp': time.time(),
                            'place_duration_ms': place_duration,
                            'execution_duration_ms': wait_duration,
                        }
                    else:
                        logger.info(f"✅ 订单已取消，未成交: {real_order_id}")
                        return {
                            'success': False,
                            'order_id': real_order_id,
                            'error': 'Order CANCELED (no fill)',
                            'filled_price': Decimal('0'),
                            'filled_quantity': Decimal('0'),
                            'timestamp': time.time(),
                            'place_duration_ms': place_duration,
                            'execution_duration_ms': wait_duration,
                        }
                
                if status in ['REJECTED']:
                    return {
                        'success': False,
                        'order_id': real_order_id,
                        'error': f'Order {status}',
                        'filled_price': Decimal('0'),
                        'filled_quantity': Decimal('0'),
                        'timestamp': time.time(),
                        'place_duration_ms': place_duration,
                        'execution_duration_ms': wait_duration,
                    }
                
                if status in ['OPEN', 'PARTIALLY_FILLED', 'FILLED']:
                    # ✅ 对于 Lighter，成交价使用订单价（limit IOC）
                    filled_price = price_from_ws
                    filled_quantity = filled_size_from_ws if filled_size_from_ws > 0 else quantity  # 后备全成交
                    
                    if status == 'PARTIALLY_FILLED':
                        logger.warning(f"⚠️ 部分成交: {filled_quantity} / {quantity} @ {filled_price}")
                    
                    logger.info(
                        f"✅ Lighter 市价单成交:\n"
                        f"   订单 ID: {real_order_id}\n"
                        f"   成交价: ${filled_price}\n"
                        f"   成交量: {filled_quantity}"
                    )
                    
                    return {
                        'success': True,
                        'order_id': real_order_id,
                        'filled_price': filled_price,
                        'filled_quantity': filled_quantity,
                        'error': None,
                        'timestamp': time.time(),
                        'place_duration_ms': place_duration,
                        'execution_duration_ms': wait_duration,
                        'partial_fill': status == 'PARTIALLY_FILLED'
                    }
                
                # 未知状态
                logger.warning(f"⚠️ 未知订单状态: {status}")
                return {
                    'success': False,
                    'order_id': real_order_id,
                    'error': f'Unknown status: {status}',
                    'filled_price': Decimal('0'),
                    'filled_quantity': Decimal('0'),
                    'timestamp': time.time(),
                    'place_duration_ms': place_duration,
                    'execution_duration_ms': wait_duration,
                }
            except asyncio.TimeoutError:
                logger.warning(f"⏰ 订单状态超时 (client_idx={client_order_index})，假设部分成交或失败")
                # ✅ 后备：轮询 get_active_orders 检查
                active_orders = await self.client.get_active_orders(self.client.config.contract_id)
                matching_order = None
                for order in active_orders:
                    if order.side == side and order.price == order_price:  # 粗匹配
                        matching_order = order
                        break
                
                if matching_order and matching_order.remaining_size == 0:
                    # 已成交
                    return {
                        'success': True,
                        'order_id': matching_order.order_id,
                        'filled_price': matching_order.price,
                        'filled_quantity': matching_order.size,
                        'error': 'Timeout, confirmed filled via poll',
                        'timestamp': time.time(),
                        'place_duration_ms': place_duration,
                        'execution_duration_ms': wait_duration + 100,  # 额外轮询时间
                    }
                else:
                    # 超时失败
                    return {
                        'success': False,
                        'order_id': tx_hash,  # 后备 tx_hash
                        'error': 'Order status timeout',
                        'filled_price': Decimal('0'),
                        'filled_quantity': Decimal('0'),
                        'timestamp': time.time(),
                        'place_duration_ms': place_duration,
                        'execution_duration_ms': wait_duration,
                    }
            
            except Exception as wait_e:
                logger.error(f"❌ 等待状态异常: {wait_e}")
                return {
                    'success': False,
                    'order_id': tx_hash,
                    'error': f'Wait error: {wait_e}',
                    'filled_price': Decimal('0'),
                    'filled_quantity': Decimal('0'),
                    'timestamp': time.time(),
                    'place_duration_ms': place_duration,
                    'execution_duration_ms': wait_duration,
                }
        except Exception as e:
            logger.error(f"❌ {self.exchange_name} 下单失败: {e}")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'order_id': None,
                'error': str(e)
            }

    async def place_market_order1(
        self,
        side: str,
        quantity: Decimal,
        price: Optional[Decimal] = None
    ) -> dict:
        """下市价单"""
        try:
            logger.debug(
                f"📤 {self.exchange_name} 下单: {side} {quantity} @ ${price}"
            )

            order_result = await self.client.place_limit_order(
                contract_id=self.market_index,
                quantity=Decimal(quantity),
                price=Decimal(price),
                side=side,
                # time_in_force='IOC'  # 立即成交或取消
            )
            # ✅ 调试：打印 order_result 的类型和属性
            logger.info(
                f"🔍 order_result 类型: {type(order_result)}\n"
                f"   属性: {dir(order_result)}"
            )
            if order_result:
                # 检查是否成功
                if order_result.success:
                    logger.info(
                        f"✅ {self.exchange_name} 下单成功:\n"
                        f"   order_id: {order_result.order_id}\n"
                        f"   side: {order_result.side}\n"
                        f"   size: {order_result.size}\n"
                        f"   price: {order_result.price}\n"
                        f"   status: {order_result.status}\n"
                        f"   filled_size: {order_result.filled_size}"
                    )
                    return {
                        'success': True,
                        'order_id': order_result.order_id,
                        'error': None
                    }
                else:
                    # 下单失败
                    error_msg = order_result.error_message or "Unknown error"
                    logger.error(
                        f"❌ {self.exchange_name} 下单失败:\n"
                        f"   error: {error_msg}\n"
                        f"   status: {order_result.status}"
                    )
                    return {
                        'success': False,
                        'order_id': None,
                        'error': error_msg
                    }
            else:
                error_msg = "Order result is None"
                logger.error(f"❌ {self.exchange_name} 下单失败: {error_msg}")
                return {
                    'success': False,
                    'order_id': None,
                    'error': error_msg
                }
        
        except Exception as e:
            logger.error(f"❌ {self.exchange_name} 下单失败: {e}")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'order_id': None,
                'error': str(e)
            }
    
    def get_latest_orderbook(self) -> Optional[Dict]:
        """获取最新订单簿"""
        return self._orderbook
    
    async def get_position(self, symbol: str) -> Optional[dict]:
        try :
            position = await self.client.get_position_info()

            if position:
                logger.info(
                    f"📊 lighter 持仓:\n"
                    f"   Symbol: {position.symbol}\n"
                    f"   Side: {position.sign}\n"
                    f"   Size: {position.position}\n"
                    f"   Entry Price: ${position.avg_entry_price}"
                )
            else:
                logger.info(f"📊 {self.exchange_name} 无持仓: {symbol}")
            
            return {
                'symbol': symbol,
                'side': 'long' if position.sign == 1 else 'short',
                'size': Decimal(position.position),
                'entry_price': position.avg_entry_price,
                'unrealized_pnl': position.unrealized_pnl,
            }
        except Exception as e:
            logger.error(f"❌ {self.exchange_name} 获取持仓失败: {e}", exc_info=True)
            return None
