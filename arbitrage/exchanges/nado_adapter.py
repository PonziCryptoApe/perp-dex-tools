"""Nado 交易所适配器"""

import asyncio
import logging
from decimal import Decimal
import time
from typing import Optional, Callable, Dict
from .base import ExchangeAdapter
from helpers.util import async_retry  # ✅ 从 util.py 导入

logger = logging.getLogger(__name__)

class NadoAdapter(ExchangeAdapter):
    """
    Nado 适配器
    
    特点：
    - 使用 indicative quote API 获取价格
    - 通过轮询模式更新订单簿（而不是 WebSocket）
    """
    
    def __init__(self, symbol: str, client, config: dict = None):
        """
        Args:
            symbol: 交易币种（如 'ETH'）
            client: NadoClient 实例
            config: 配置字典
        """
        super().__init__(symbol, client, config)
        
        # Nado 特定属性
        self.contract_id = None
        self.tick_size = None
        
        # 轮询模式配置
        self.polling_mode = False  # ✅ 启用轮询模式
        self.polling_interval = config.get('polling_interval', 1.0) if config else 1.0  # 默认 1 秒
        self._polling_task = None
        # # 查询数量（用于 indicative quote）
        # self.query_quantity = config.get('query_quantity', Decimal('0.1')) if config else Decimal('0.1')
        
        # 订单簿缓存
        self._orderbook = None
        self._orderbook_callback = None
                

        # ✅ 订单状态管理
        self.order_status = None  # OPEN, PARTIALLY_FILLED, FILLED, CANCELED, FAILED
        self._order_status_data: Dict[str, dict] = {}
        self._order_status_futures: Dict[str, asyncio.Future] = {}

        self.current_order_id = None
        self.position_size = Decimal('0')
        self.position_is_full = False
        
        # ✅ WebSocket 回调
        self._nado_orderbook_handler = None
        self._position_update_handler = None

        # 缓存最新的价格
        self._latest_bid = None
        self._latest_ask = None
        self._price_timestamp = None
        self._quote_id = None

        # ✅ 时间差统计
        self._orderbook_fetch_time = None  # 订单簿获取时间
        self._order_place_time = None      # 下单时间
        self._time_diffs = []              # 时间差列表（毫秒）
        
        logger.info(
            f"🔧 NadoAdapter 初始化:\n"
            f"   Symbol: {symbol}\n"
            # f"   Polling Interval: {self.polling_interval}s\n"
            # f"   Query Quantity: {self.query_quantity}"
        )
    
    async def connect(self):
        """连接交易所"""
        try:
            logger.info(f"🔌 连接 Nado...")
            
            # 连接客户端
            await self.client.connect()
            
            # 获取合约信息
            self.contract_id, self.tick_size = await self.client.get_contract_attributes()
            logger.info(f"✅ 获取合约信息Nado: Contract ID={self.contract_id}, Tick Size={self.tick_size}")
            
            # ✅ 设置持仓更新回调
            # self.client.setup_order_update_handler(self._on_position_update)
            # if hasattr(self.client, 'setup_order_update_handler'):
            #     self.client.setup_order_update_handler(self._on_position_update)
            #     logger.info("   Position update handler 已注册")

            # # ✅ 4. 等待 WebSocket 连接稳定（增加等待时间）
            logger.info("⏳ 等待 WebSocket 连接建立...")
            await asyncio.sleep(3)
            
            # ✅ 验证连接
            if not self.client.orderbook:  # 检查是否收到初始 orderbook
                logger.warning("⚠️ 初始订单簿未收到，启动回退轮询...")
            #     self.polling_mode = True
            #     self._polling_task = asyncio.create_task(self._polling_loop())
            
            logger.info(
                f"✅ Nado 已连接\n"
                f"   Contract ID: {self.contract_id}\n"
                f"   Tick Size: {self.tick_size}\n"
                f"   WS Mode: {'Active' if not self.polling_mode else 'Fallback Polling'}"
            )
        
        except Exception as e:
            logger.error(f"❌ Nado 连接失败: {e}")
            raise
    
    async def disconnect(self):
        """断开连接"""
        logger.info(f"⏹️ 断开 {self.exchange_name} 连接...")
        
        # ✅ 1. 停止轮询任务（回退模式）
        if self._polling_task:
            logger.info(f"⏹️ 停止 Nado 订单簿轮询...")
            self._polling_task.cancel()
            try:
                await self._polling_task
            except asyncio.CancelledError:
                logger.debug(f"✅ Nado 订单簿轮询已取消")
        
        # ✅ 2. 移除订单簿回调
        if self._nado_orderbook_handler and hasattr(self.client, 'setup_orderbook_handler'):
            logger.info(f"⏹️ 移除 Nado 订单簿回调...")
            self.client.setup_orderbook_handler(None)
            self._nado_orderbook_handler = None
        
        # ✅ 3. 移除订单更新回调
        if hasattr(self.client, 'setup_order_update_handler'):
            self.client.setup_order_update_handler(None)
        
        # ✅ 4. 清理状态
        self._order_status_futures.clear()
        self._order_status_data.clear()
        self._orderbook = None
        self._orderbook_callback = None
        self._latest_bid = None
        self._latest_ask = None
        
        # ✅ 5. 断开客户端
        if self.client:
            await self.client.disconnect()
        
        logger.info(f"✅ Nado 适配器已断开: {self.symbol}")
    
    async def subscribe_orderbook(self, callback: Callable):
        """
        订阅订单簿（WS模式）
        
        Args:
            callback: 回调函数 async def callback(orderbook: dict)
        """
        self._orderbook_callback = callback
        if not self._nado_orderbook_handler:
            self._nado_orderbook_handler = self._on_nado_orderbook_update
            self.client.setup_orderbook_handler(self._nado_orderbook_handler)
            logger.info(f"📡 Nado 订单簿订阅（WS 模式）: {self.contract_id}")
            # return
        else:
            logger.info(f"📡 Nado 订单簿已订阅（WS模式）: {self.contract_id}")
            # return
        # if not self.polling_mode and self._nado_orderbook_handler:
        #     logger.info(f"📡 Nado 订单簿订阅（WS 模式）: {self.contract_id}")
        #     return
        
        # ✅ 回退到轮询
        # if self.polling_mode:
        #     logger.warning(f"⚠️ Nado WS 不可用，使用轮询订阅")
        #     self._polling_task = asyncio.create_task(self._polling_loop())
        #     logger.info(f"📡 Nado 订单簿订阅（轮询模式，间隔 {self.polling_interval}s）")
    async def _on_nado_orderbook_update(self, orderbook_data: dict):
        """处理 Nado WS 订单簿更新"""
        try:
            # logger.info(f"🔔 收到 Nado 订单簿更新: bids={orderbook_data.get('bid', [])}, asks={orderbook_data.get('ask', [])}, ts={orderbook_data.get('ts', 'N/A')}")
            
            # ✅ 提取原始 bid/ask
            raw_bids = orderbook_data.get('bid', [])
            raw_asks = orderbook_data.get('ask', [])
            
            bids = []
            for i, item in enumerate(raw_bids):
                try:
                    p_str = str(item.get('p', '0'))
                    q_str = str(item.get('q', '0'))
                    logger.debug(f"  Bid {i}: p='{p_str}', q='{q_str}'")
                    
                    # Decimal 转换 + 量化（捕获每个 item 异常）
                    p_dec = Decimal(p_str)
                    q_dec = Decimal(q_str)

                    bids.append([p_dec, q_dec])
                except Exception as item_e:
                    logger.error(f"❌ Bid {i} 转换失败: p='{p_str}', e={item_e}")
                    continue  # 跳过坏 item，继续其他
            
            # ✅ 同 asks
            asks = []
            for i, item in enumerate(raw_asks):
                try:
                    p_str = str(item.get('p', '0'))
                    q_str = str(item.get('q', '0'))
                    
                    p_dec = Decimal(p_str)
                    q_dec = Decimal(q_str)

                    asks.append([p_dec, q_dec])
                except Exception as item_e:
                    logger.error(f"❌ Ask {i} 转换失败: p='{p_str}', e={item_e}")
                    continue
            raw_ts = orderbook_data.get('ts', time.time())
            if isinstance(raw_ts, (int, str)) and len(str(raw_ts)) > 10:  # ns 特征：>10 位
                ts_s = float(raw_ts) / 1_000_000_000  # ns → s
            else:
                ts_s = float(raw_ts)  # 已 s 或 ms，保持
            # ✅ 构建 orderbook（即使空，也设 ts）
            self._orderbook = {
                'bids': bids,
                'asks': asks,
                'timestamp': ts_s
            }
                        
            # 更新缓存价格（只如果非空）
            if bids:
                self._latest_bid = Decimal(bids[0][0])
            if asks:
                self._latest_ask = Decimal(asks[0][0])
            self._price_timestamp = time.time()
            self._orderbook_fetch_time = time.time()

            
            # ✅ 推回调
            if self._orderbook_callback:
                await self._orderbook_callback(self._orderbook)
            else:
                logger.warning("⚠️ Nado _orderbook_callback 未设置")
        
        except Exception as e:  # 外层捕获
            logger.error(f"❌ _on_nado_orderbook_update 整体失败: {e}", exc_info=True)
            self._orderbook = {'bids': [], 'asks': []}  # 防崩溃
    async def _on_nado_orderbook_update1(self, orderbook_data: dict):
        """
        处理 Nado WebSocket 订单簿更新
        
        Args:
            orderbook_data: Nado WS 推送的订单簿数据（已标准化）
                {
                    'ts': 1766069395857892063,  # 时间戳（纳秒？）
                    'market': 'ETH-USD',
                    'bid': [{"p": "88064", "q": "0.17045"}],
                    'ask': [{"p": "88072", "q": "0.17045"}]
                }
        """
        try:
            # ✅ 提取数据
            bids = orderbook_data.get('bid', [])
            asks = orderbook_data.get('ask', [])
            ts = orderbook_data.get('ts', time.time() * 1000)
            
            if not bids or not asks:
                logger.debug("⚠️ 空订单簿更新，跳过")
                return
            
            # logger.info(f"🔔 收到 Nado 订单簿更新: bids={bids}, asks={asks}, ts={ts}")

            bid_price = float(bids[0]['p'])
            bid_size = float(bids[0]['q'])
            ask_price = float(asks[0]['p'])
            ask_size = float(asks[0]['q'])
            # ✅ 更新缓存
            self._orderbook = {
                'bids': [[bid_price, bid_size]],
                'asks': [[ask_price, ask_size]],
                'timestamp': ts,
                'poll_duration_ms': 0  # WebSocket 无 API 调用延迟
            }
            self._latest_bid = bid_price if bids else None
            self._latest_ask = ask_price if asks else None
            self._price_timestamp = ts
            self._quote_id = None
            
            self._orderbook_fetch_time = time.time()
            
            # ✅ 触发回调
            if self._orderbook_callback:
                await self._orderbook_callback(orderbook_data)
            
            logger.info(
                f"📊 Nado BBO 更新: bid={self._latest_bid}@{bids[0]['q']}, "
                f"ask={self._latest_ask}@{asks[0]['q']}"
            )
            
        except Exception as e:
            logger.error(f"❌ 处理 Nado 订单簿更新失败: {e}")

    async def _polling_loop(self):
        """轮询循环"""
        logger.info("🔄 启动 Nado 轮询...")
        consecutive_errors = 0
        max_consecutive_errors = 5
        while True:
            try:
                # 获取订单簿
                orderbook = await self.fetch_orderbook()
                
                if orderbook:
                    self._orderbook = orderbook
                    
                    if orderbook['bids']:
                        self._latest_bid = Decimal(str(orderbook['bids'][0][0]))
                    if orderbook['asks']:
                        self._latest_ask = Decimal(str(orderbook['asks'][0][0]))
                    self._price_timestamp = time.time() * 1000  # 毫秒时间戳
                    self._quote_id = orderbook['quote_id']
                    # 触发回调
                    if self._orderbook_callback:
                        await self._orderbook_callback(orderbook)
                else:
                    consecutive_errors += 1
                    logger.warning(
                        f"⚠️ Nado 数据获取失败 ({consecutive_errors}/{max_consecutive_errors})"
                    )
                
                # ✅ 连续失败过多，增加延迟
                if consecutive_errors >= max_consecutive_errors:
                    logger.error(f"🚨 连续失败 {max_consecutive_errors} 次，暂停 10 秒")
                    await asyncio.sleep(10)
                    consecutive_errors = 0  # 重置
                else:
                    await asyncio.sleep(self.polling_interval)
            
            except asyncio.CancelledError:
                logger.info("⏹️ Nado 轮询已停止")
                break
            
            except Exception as e:
                consecutive_errors += 1
                logger.error(
                    f"❌ Nado 轮询错误 ({consecutive_errors}/{max_consecutive_errors}): {e}"
                )
                
                if consecutive_errors >= max_consecutive_errors:
                    await asyncio.sleep(10)
                    consecutive_errors = 0
                else:
                    await asyncio.sleep(self.polling_interval)
        
    async def fetch_orderbook(self) -> Optional[Dict]:
        """
        获取订单簿（通过 indicative quote）
        
        Returns:
            {
                'bids': [[price, size], ...],
                'asks': [[price, size], ...],
                'timestamp': int,
                'quote_id': str
            }
        """
        try:
            # 使用 NadoClient 的 fetch_bbo_prices（扩展为 orderbook）
            best_bid, best_ask, ts = await self.client.fetch_bbo_prices(self.contract_id)
            
            if best_bid <= 0 or best_ask <= 0:
                return None
            
            # 构建简单 orderbook（仅 BBO）
            return {
                'bids': [[str(best_bid), '1.0']],  # [price, qty]
                'asks': [[str(best_ask), '1.0']],
                'ts': ts,
                # 'quote_id': f"nado_rest_{int(time.time())}"
            }
        
        except Exception as e:
            logger.error(f"❌ 获取 Nado 订单簿失败: {e}")
            return None
    
    def get_latest_orderbook(self) -> Optional[Dict]:
        """获取最新订单簿"""
        return self._orderbook
    
    async def place_open_order(
        self,
        side: str,
        quantity: Decimal,
        price: Optional[Decimal] = None,
        retry_mode: str = 'opportunistic',  # 'aggressive' or 'opportunistic'
        quote_id: Optional[str] = None
    ) -> dict:
        """
        下开仓单
        Args:
            side: 'buy' 或 'sell'
            quantity: 数量
            price: 参考价格（可选，不使用时会自动获取 BBO）
            retry_mode:
             - 'opportunistic' 机会主义模式，失败就放弃等待下次
             - 'aggressive' 激进模式，持续重试直到成功
        """
        if retry_mode == 'opportunistic':
            await self.place_limit_order(side, quantity, price)
        else:
            logger.info(f"📤 Nado 下市价单: {side}")
            return await self.place_market_order(
                side=side,
                quantity=quantity,
                # quote_id=quote_id
            )
    async def place_close_order(
        self,
        side: str,
        quantity: Decimal,
        price: Optional[Decimal] = None,
        retry_mode: str = 'opportunistic',  # 'aggressive' or 'opportunistic'
        quote_id: Optional[str] = None
    ) -> dict:
        """
        下关仓单
        Args:
            side: 'buy' 或 'sell'
            quantity: 数量
            price: 参考价格（可选，不使用时会自动获取 BBO）
            retry_mode:
             - 'opportunistic' 机会主义模式，失败就放弃等待下次
             - 'aggressive' 激进模式，持续重试直到成功
        """
        if retry_mode == 'opportunistic':
            return await self.place_limit_order(side, quantity, price)
        else:
            # if quote_id is None:
            #     #_quote_id为空，无法下单
            #     logger.error("❌ 下单失败：缺少 quote_id")
            #     return {
            #         'success': False,
            #         'order_id': None,
            #         'error': 'Missing quote_id'
            #     }
            logger.info(f"📤 Nado 下市价单: {side}")
            return await self.place_market_order(
                side=side,
                quantity=quantity,
                # quote_id=quote_id
            )
        
    
    async def place_market_order(self, side, quantity: Decimal) -> dict:
        """
        下市价单（带重试 + 动态滑点）
        
        重试策略：
        - 第 1 次：滑点 0.01% (0.0001)
        - 第 2 次：滑点 0.05% (0.0005)
        - 第 3 次：滑点 0.10% (0.001)
        """
        max_attempts = 1  # 分别对应 0.01%, 0.05%, 0.10%
        # slippage_levels = [0.0005]
        for attempt in range(max_attempts):
            try:
                # ✅ 记录下单时间
                self._order_place_time = time.time()
                max_slippage = self.slippage or Decimal('0')  # 固定使用 0.05% 滑点
                time_diff = None
                # ✅ 计算与最后一次订单簿获取的时间差
                if self._orderbook_fetch_time:
                    time_diff = (self._order_place_time - self._orderbook_fetch_time) * 1000  # 毫秒
                    self._time_diffs.append(time_diff)
                    
                    logger.info(
                        f"⏱️ 订单簿获取 → 下单时间差: {time_diff:.2f} ms\n"
                        f"   订单簿时间: {self._orderbook_fetch_time:.3f}\n"
                        f"   下单时间:   {self._order_place_time:.3f}"
                    )
                    
                    # ✅ 警告：时间差过大
                    if time_diff > 500:  # 超过 500 毫秒
                        logger.warning(f"⚠️ 订单簿数据过旧！时间差: {time_diff:.0f} ms")
                
                # ✅ 第 1 次尝试使用传入的 quote_id，后续重试重新获取
                # if attempt == 0 and quote_id is not None:
                    # current_quote_id = quote_id
                    # logger.info(f"📤 第 {attempt + 1} 次尝试：使用传入的 quote_id: {current_quote_id[:8]}...")
                # else:
                    # ✅ 重新获取最新的 quote_id
                    # logger.info(f"🔄 第 {attempt + 1} 次尝试：重新获取 quote_id...")
                    
                    # try:
                    #     # ✅ 调用 Nado API 获取最新报价
                    #     quote_data = await self.client._fetch_indicative_quote(
                    #         quantity=str(self.query_quantity),
                    #         contract_id=self.contract_id
                    #     )
                        
                    #     if not quote_data or 'quote_id' not in quote_data:
                    #         logger.error(f"❌ 获取 quote_id 失败: {quote_data}")
                    #         if attempt < max_attempts - 1:
                    #             await asyncio.sleep(0.5)
                    #             continue
                    #         return {
                    #             'success': False,
                    #             'order_id': None,
                    #             'error': 'Failed to fetch quote_id',
                    #             'timestamp': time.time()
                    #         }
                        
                    #     current_quote_id = quote_data['quote_id']
                        
                    #     # ✅ 同时获取最新价格（用于日志）
                    #     latest_price = quote_data.get('price')
                    #     logger.info(
                    #         f"✅ 获取到新的 quote_id: {current_quote_id[:8]}...\n"
                    #         f"   最新价格: ${latest_price}"
                    #     )
                    
                    # except Exception as e:
                    #     logger.error(f"❌ 获取 quote_id 异常: {e}")
                    #     if attempt < max_attempts - 1:
                    #         await asyncio.sleep(0.5)
                    #         continue
                    #     return {
                    #         'success': False,
                    #         'order_id': None,
                    #         'error': f'Failed to fetch quote_id: {e}',
                    #         'timestamp': time.time()
                    #     }
                
                logger.info(
                    f"📤 尝试下单 {attempt + 1}/{max_attempts}:\n"
                    f"   方向: {side}\n"
                    # f"   quote_id: {current_quote_id[:8]}...\n"
                    f"   最大滑点: {max_slippage * 100:.3f}%"
                    f"   订单簿年龄: {time_diff:.2f} ms (订单簿 → 下单)"  # ✅ 添加时间差
                )
                # ✅ 调用客户端下单
                result = await self.client._place_market_order(
                    contract_id=self.contract_id,
                    quantity=quantity,
                    # quote_id=quote_id,
                    side=side,
                    max_slippage=max_slippage
                )
                
                logger.info(f"📊 Market order raw response: {result}")
                
                # ✅ 检查返回格式
                if not result.success:
                    error_msg = result.error_message or "Unknown error"
                    logger.warning(
                        f"⚠️ 第 {attempt + 1} 次下单失败: {error_msg}\n"
                        f"   {'即将重试...' if attempt < max_attempts - 1 else '已达最大重试次数'}"
                    )

                    if attempt < max_attempts - 1:
                        await asyncio.sleep(0.1)  # 等待 0.1 秒后重试
                        continue

                    logger.error(f"❌ 市价单最终失败: {error_msg}(已重试 {max_attempts} 次)")
                    return {
                        'success': False,
                        'order_id': None,
                        'error': result.error_message,
                        'timestamp': time.time()
                    }
                
                rfq_id = result.order_id
                logger.info(f"📤 市价单已发送: {rfq_id}")


                # ✅ 2. 等待 WebSocket 推送订单状态（适配器层负责）
                self.current_order_id = rfq_id
                logger.info(f"✅ 已设置 current_order_id = {rfq_id}")

                # final_status = await self._wait_for_order_fill(rfq_id, timeout=5.0)
                await asyncio.sleep(0.4)  # 确保状态更新完成
                order_info = await self.client.get_order_info(rfq_id)
                if order_info:
                    final_status = order_info.status
                    logger.info(f"📊 通过RESTful API 获取订单状态: final_status={final_status}")
                
                    if not final_status:
                        logger.error(f"❌ 超时且订单状态为 {final_status}")
                        return {
                            'success': False,
                            'order_id': rfq_id,
                            'error': f'Timeout and order status: {final_status}',
                            'filled_price': Decimal('0'),
                            'filled_quantity': Decimal('0'),
                            'timestamp': time.time()
                        }
                    # ✅ 3. 判断最终状态
                    if final_status.upper() in ['FILLED']:
                        logger.info(f"✅ 市价单成功: {rfq_id}")
                        filled_price = Decimal(str(order_info.price))
                        filled_quantity = abs(Decimal(str(order_info.filled_size)))

                        logger.info(
                            f"✅ 获取订单信息:\n"
                            f"   订单 ID: {rfq_id}\n"
                            f"   成交价: ${filled_price}\n"
                            f"   成交量: {filled_quantity}"
                        )
                        
                        return {
                            'success': True,
                            'order_id': rfq_id,
                            'filled_price': filled_price,
                            'filled_quantity': filled_quantity,
                            'error': None,
                            'timestamp': time.time()
                        }
                    elif final_status.upper() in ['CANCELED', 'REJECTED']:
                        logger.error(f"❌ 市价单失败: {final_status}")
                        return {
                            'success': False,
                            'order_id': rfq_id,
                            'error': f'Order {final_status}',
                            'filled_price': Decimal('0'),
                            'filled_quantity': Decimal('0'),
                            'timestamp': time.time()
                        }
                    else:
                        # 未知状态，保守返回失败
                        logger.error(f"❌ 未知订单状态: {final_status}")
                        return {
                            'success': False,
                            'order_id': rfq_id,
                            'error': f'Unknown status: {final_status}',
                            'filled_price': Decimal('0'),
                            'filled_quantity': Decimal('0'),
                            'timestamp': time.time()
                        }
            except Exception as e:
                logger.error(f"❌ place_market_order 异常: {e}")
                import traceback
                traceback.print_exc()
                
                # ✅ 异常时也要返回字典
                return {
                    'success': False,
                    'order_id': None,
                    'error': str(e),
                    'filled_price': Decimal('0'),
                    'filled_quantity': Decimal('0'),
                    'timestamp': time.time()
                }
        
    async def place_limit_order(
        self,
        side: str,
        quantity: Decimal,
        price: Optional[Decimal] = None,
    ) -> dict:
        """
        下限价单（实际使用 post-only 限价单模拟）

        ✅ 核心逻辑（来自 hedge_mode_nado.py）：
        1. 以 BBO 价格下限价单（post-only）
        2. 监听 WebSocket 持仓更新
        3. 超时或价格不优时自动取消重下
        4. 直到订单完全成交
        
        Args:
            side: 'buy' 或 'sell'
            quantity: 数量
            price: 参考价格（可选，不使用时会自动获取 BBO）
            retry_mode:
             - 'opportunistic' 机会主义模式，失败就放弃等待下次
             - 'aggressive' 激进模式，持续重试直到成功

        Returns:
            {
                'success': bool,
                'order_id': str,
                'error': str
            }
        """
        try:
            logger.info(
                f"📤 Nado 下单:\n"
                f"   方向: {side}\n"
                f"   数量: {quantity}\n"
                f"   价格: {price}\n"
            )
            
            # ✅ 重置订单状态
            self.order_status = None
            self.current_order_id = None
            self.position_is_full = False
            
            # ✅ 执行 post-only 挂单
            success = await self._place_post_only_order(side, quantity, price)

            if success:
                logger.info(f"✅ Nado 订单成交: {self.current_order_id}")
                return {
                    'success': True,
                    'order_id': self.current_order_id,
                    'error': None
                }
            else:
                logger.error("❌ Nado 订单失败")
                return {
                    'success': False,
                    'order_id': None,
                    'error': 'Order execution failed'
                }
        
        except Exception as e:
            logger.error(f"❌ Nado 下单失败: {e}")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'order_id': None,
                'error': str(e)
            }

    async def _place_post_only_order(
        self,
        side: str,
        quantity: Decimal, 
        price: Optional[Decimal] = None
    ) -> bool:
        """
        下 post-only 订单（核心逻辑）
        
        ✅ 参考 hedge_mode_nado.py:place_nado_post_only_order()
        """
        logger.info(f"[Nado] [{side.upper()}] 开始挂单")

        # ✅ 下第一单
        order_id, order_price = await self._place_bbo_order(side, quantity, price)
        if not order_id:
            logger.error("❌ 首次下单失败")
            return False
        
        self.current_order_id = order_id
        logger.info(f"下单成功: {order_id} @ {order_price}")
        
        start_time = time.time()
        last_cancel_time = 0        
        # ✅ 监控订单状态
        while True:            
            # ✅ 订单被取消或失败
            if self.order_status in ['CANCELED', 'CANCELLED', 'FAILED']:
                self.order_status = None
                
                # ✅ 重新获取最新价格（B 所必须用最新价格）
                # best_bid, best_ask = await self.client.fetch_bbo_prices(self.contract_id, quantity)
                # price = best_ask if side == 'buy' else best_bid
                # logger.info(f"激进模式：使用最新价格 {price}")

                # order_id, order_price = await self._place_bbo_order(side, quantity, price)
                # if not order_id:
                #     logger.error("❌ 重新下单失败")
                    
                #     continue
                
                # self.current_order_id = order_id
                # logger.info(f"新订单: {order_id} @ {order_price}")
                # start_time = time.time()
                # last_cancel_time = 0
                # await asyncio.sleep(0.5)
                return False
            # ✅ 订单挂起中 → 检查是否需要取消重下
            elif self.order_status in ['OPEN']:
                current_time = time.time()
                await asyncio.sleep(2)
                # ✅ 超时检查（1 秒）
                if current_time - start_time > 1:
                    try:       
                        logger.info(f"取消订单 {order_id}（超时）")
                        cancel_result = await self.client.cancel_order(order_id)
                        if cancel_result.success:
                            last_cancel_time = current_time
                            self.order_status = 'CANCELED'
                        else:
                            logger.error(f"❌ 取消失败: {cancel_result.error_message}")
                        
                    except Exception as e:
                            logger.error(f"❌ 取消订单异常: {e}")

            # ✅ 订单已完全成交 → 退出
            elif self.order_status in ['FILLED', 'CLEARED']:
                logger.info(f"✅ 订单 {order_id} 已完全成交")
                return True
            
            # ✅ 未知状态
            else:
                if self.order_status is not None:
                    logger.error(f"❌ 未知订单状态: {self.order_status}")
                    return False
                else:
                    await asyncio.sleep(0.5)
    
    # @async_retry(max_attempts=3, delay=0.01, backoff=1)
    async def _place_bbo_order(
        self,
        side: str,
        quantity: Decimal,
        price: Optional[Decimal] = None
    ) -> tuple:
        """
        以 BBO 价格下限价单
        
        ✅ 参考 hedge_mode_nado.py:place_bbo_order()
        
        Returns:
            (order_id, order_price)
        """
        try:
            # ✅ 优先使用传入价格
            if price is not None:
                order_price = price
                logger.info(f"使用传入价格: {order_price}")
            # ✅ 其次使用缓存价格
            elif self._latest_bid is not None and self._latest_ask is not None:
                # 毫秒时间差
                price_age = time.time() * 1000 - self._price_timestamp
                if price_age <= 200:  # 价格不超过 200毫秒
                    if side == 'buy':
                        order_price = self._latest_ask
                    else:
                        order_price = self._latest_bid
                    logger.info(f"使用缓存价格: {order_price} (年龄 {price_age:.0f} ms)")
                else:
                    logger.info("缓存价格过旧，重新获取 BBO")
                    best_bid, best_ask = await self.client.fetch_bbo_prices(
                        self.contract_id, quantity
                    )
                    order_price = best_ask if side == 'buy' else best_bid
                    logger.info(f"最新 BBO 价格: {order_price}")
            # ✅ 最后调用 API 获取最新价格
            else:
                logger.info("无缓存价格，调用 API 获取 BBO")
                best_bid, best_ask = await self.client.fetch_bbo_prices(
                    self.contract_id, quantity
                )
                order_price = best_ask if side == 'buy' else best_bid
                logger.info(f"最新 BBO 价格: {order_price}")
            
            # 下单
            order_result = await self.client._place_limit_order(
                side=side.lower(),
                quantity=quantity,
                price=order_price
            )
            
            if order_result.success:
                self.order_status = 'OPEN'
                return order_result.order_id, order_price
            else:
                self.order_status = 'FAILED'
                logger.error(f"❌ 下单失败: {order_result.error_message}")
                return None, None
        
        except Exception as e:
            logger.error(f"❌ 下单异常: {e}")
            return None, None
    
    # ========== WebSocket 回调 ==========
    async def _wait_for_order_fill(self, rfq_id: str, timeout: float = 3.0) -> Optional[str]:
        """
        等待 WebSocket 推送订单状态
        
        ✅ 这个方法在适配器层，因为：
        - 需要访问 WebSocket 回调数据
        - 需要管理等待逻辑
        - 客户端层不应该有这种复杂逻辑
        """
        if not hasattr(self, '_order_status_events'):
            self._order_status_events = {}
        if not hasattr(self, '_order_final_status'):
            self._order_final_status = {}
        
        # 创建等待事件
        event = asyncio.Event()
        self._order_status_events[rfq_id] = event
        
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
            status = self._order_final_status.get(rfq_id)
            return status
        
        except asyncio.TimeoutError:
            logger.warning(f"⚠️ 等待订单状态超时: {rfq_id}")
            return None
        
        finally:
            self._order_status_events.pop(rfq_id, None)
            self._order_final_status.pop(rfq_id, None)
    
    # todo 可能需要修改
    def _on_position_update(self, positions):
        """
        处理持仓更新（WebSocket 回调）
        ✅ 参考 hedge_mode_nado.py:order_update_handler()
        """
        try:
            logger.debug(f"📊 WebSocket 持仓更新: positions={positions}")
            logger.debug(f"📊 当前状态: position_is_full={self.position_is_full}, "
                             f"current_order_id={getattr(self, 'current_order_id', None)}")

            # ✅ 初始状态：仓位为空
            if not positions and self.position_is_full is False:
                logger.debug("初始状态，持仓为空，无需处理")
                return
            
            # ✅ 平仓成功
            if not positions and self.position_is_full:
                logger.info("Nado平仓成功，仓位为空")
                self.position_is_full = False
                self.position_size = Decimal('0')
                self.order_status = 'FILLED'
                # 触发事件
                if hasattr(self, 'current_order_id') and self.current_order_id:
                    if not hasattr(self, '_order_final_status'):
                        self._order_final_status = {}
                    self._order_final_status[self.current_order_id] = 'FILLED'
                    
                    if hasattr(self, '_order_status_events') and self.current_order_id in self._order_status_events:
                        logger.info(f"🔔 触发平仓事件: {self.current_order_id}")
                        self._order_status_events[self.current_order_id].set()
                return
            
            # ✅ 有仓位
            if positions:
                position_data = positions[0]
                self.position_size = Decimal(position_data.get('position_info', {"qty": "0"}).get('qty', '0'))
                
                # ✅ 部分成交
                if Decimal('0') < self.position_size < self.query_quantity and not self.position_is_full:
                    self.order_status = 'PARTIALLY_FILLED'
                    logger.info(f"部分成交: {self.position_size} / {self.query_quantity}")
                    return
                
                # ✅ 完全成交
                if self.position_size == self.query_quantity and not self.position_is_full:
                    self.position_is_full = True
                    self.order_status = 'FILLED'
                    
                    price = Decimal(position_data.get('position_info', {"avg_entry_price": "0"}).get('avg_entry_price', '0'))
                    logger.info(f"✅ 完全成交: {self.query_quantity} @ {price}")
                    # ✅ 通知等待者
                    if hasattr(self, 'current_order_id') and self.current_order_id:
                        if not hasattr(self, '_order_final_status'):
                            self._order_final_status = {}
                        self._order_final_status[self.current_order_id] = 'FILLED'
                        
                        if hasattr(self, '_order_status_events') and self.current_order_id in self._order_status_events:
                            self._order_status_events[self.current_order_id].set()
    
                    return
        
        except Exception as e:
            logger.error(f"❌ 处理持仓更新失败: {e}")
            
    async def get_position(self, symbol: str) -> Optional[dict]:
        """
        获取 Nado 持仓信息（复用 client 方法）
        
        Args:
            symbol: 币种符号（如 'HYPE'）
        
        Returns:
            {
                'symbol': 'HYPE',
                'side': 'long',
                'size': 2.5,
                #'entry_price': 28.3,
                #'unrealized_pnl': 0.08
            }
        """
        try:
            # ✅ 直接调用 NadoClient 的方法
            position = await self.client.get_position(symbol)
            if position:
                logger.info(
                    f"📊 Nado 持仓:\n"
                    f"   Symbol: {position['symbol']}\n"
                    f"   Side: {position['side']}\n"
                    f"   Size: {position['size']}\n"
                )
            else:
                logger.info(f"📊 Nado 无持仓: {symbol}")

            return position
        
        except Exception as e:
            logger.error(f"❌ Nado 获取持仓失败: {e}", exc_info=True)
            return None