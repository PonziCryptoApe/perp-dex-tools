"""Variational 交易所适配器"""

import asyncio
import logging
from decimal import Decimal
import time
from typing import Optional, Callable, Dict
from .base import ExchangeAdapter
from helpers.util import async_retry  # ✅ 从 util.py 导入

logger = logging.getLogger(__name__)

class VariationalAdapter(ExchangeAdapter):
    """
    Variational 适配器
    
    特点：
    - 使用 indicative quote API 获取价格
    - 通过轮询模式更新订单簿（而不是 WebSocket）
    """
    
    def __init__(self, symbol: str, client, config: dict = None):
        """
        Args:
            symbol: 交易币种（如 'ETH'）
            client: VariationalClient 实例
            config: 配置字典
        """
        super().__init__(symbol, client, config)
        
        # Variational 特定属性
        self.contract_id = None
        self.tick_size = None
        
        # 轮询模式配置
        self.polling_mode = True  # ✅ 启用轮询模式
        self.polling_interval = config.get('polling_interval', 1.0) if config else 1.0  # 默认 1 秒
        
        # 查询数量（用于 indicative quote）
        self.query_quantity = config.get('query_quantity', Decimal('0.1')) if config else Decimal('0.1')
        
        # 订单簿缓存
        self._orderbook = None
        self._orderbook_callback = None
        
        # 轮询任务
        self._polling_task = None

        # ✅ 订单状态管理
        self.order_status = None  # OPEN, PARTIALLY_FILLED, FILLED, CANCELED, FAILED
        self.current_order_id = None
        self.position_size = Decimal('0')
        self.position_is_full = False
        
        # ✅ WebSocket 回调
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
            f"🔧 VariationalAdapter 初始化:\n"
            f"   Symbol: {symbol}\n"
            f"   Polling Interval: {self.polling_interval}s\n"
            f"   Query Quantity: {self.query_quantity}"
        )
    
    async def connect(self):
        """连接交易所"""
        try:
            logger.info(f"🔌 连接 Variational...")
            
            # 连接客户端
            await self.client.connect()
            
            # 获取合约信息
            self.contract_id, self.tick_size = await self.client.get_contract_attributes(self.symbol)
            
            # ✅ 设置持仓更新回调
            self.client.setup_order_update_handler(self._on_position_update)

            # ✅ 4. 等待 WebSocket 连接稳定（增加等待时间）
            logger.info("⏳ 等待 WebSocket 连接建立...")
            await asyncio.sleep(3)  # 从 2 秒改为 3 秒
            
            # ✅ 5. 验证 WebSocket 连接状态
            if self.client._portfolio_ws is None:
                raise Exception("Portfolio WebSocket 未连接")
            
            logger.info(
                f"✅ Variational 已连接\n"
                f"   Contract ID: {self.contract_id}\n"
                f"   Tick Size: {self.tick_size}\n"
                f"   WebSocket: {'已连接' if self.client._portfolio_ws else '❌ 未连接'}"
            )
        
        except Exception as e:
            logger.error(f"❌ Variational 连接失败: {e}")
            raise
    
    async def disconnect(self):
        """断开连接"""
        # 停止轮询任务
        if self._polling_task:
            self._polling_task.cancel()
            try:
                await self._polling_task
            except asyncio.CancelledError:
                pass
        
        # 断开客户端
        if self.client:
            await self.client.disconnect()
        
        logger.info(f"⏹️ Variational 已断开")
    
    async def subscribe_orderbook(self, callback: Callable):
        """
        订阅订单簿（轮询模式）
        
        Args:
            callback: 回调函数 async def callback(orderbook: dict)
        """
        self._orderbook_callback = callback
        
        # 启动轮询任务
        self._polling_task = asyncio.create_task(self._polling_loop())
        
        logger.info(f"📡 已订阅 Variational 订单簿（轮询模式，间隔 {self.polling_interval}s）")
    
    async def _polling_loop(self):
        """轮询循环"""
        logger.info("🔄 启动 Variational 轮询...")
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
                        f"⚠️ Variational 数据获取失败 ({consecutive_errors}/{max_consecutive_errors})"
                    )
                
                # ✅ 连续失败过多，增加延迟
                if consecutive_errors >= max_consecutive_errors:
                    logger.warning(f"🚨 连续失败 {max_consecutive_errors} 次，暂停 10 秒")
                    await asyncio.sleep(10)
                    consecutive_errors = 0  # 重置
                else:
                    await asyncio.sleep(self.polling_interval)
            
            except asyncio.CancelledError:
                logger.info("⏹️ Variational 轮询已停止")
                break
            
            except Exception as e:
                consecutive_errors += 1
                logger.error(
                    f"❌ Variational 轮询错误 ({consecutive_errors}/{max_consecutive_errors}): {e}"
                )
                
                if consecutive_errors >= max_consecutive_errors:
                    await asyncio.sleep(10)
                    consecutive_errors = 0
                else:
                    await asyncio.sleep(self.polling_interval)
        
    async def fetch_orderbook(self, quantity: Optional[Decimal] = None) -> Optional[Dict]:
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
            fetch_start = time.time()
            q = quantity if quantity else self.query_quantity
            # ✅ 调用 indicative quote API
            quote_data = await asyncio.wait_for(
                self.client._fetch_indicative_quote(
                    q,
                    self.contract_id
                ),
                timeout=5.0  # 5 秒超时
            )
            fetch_end = time.time()
            self._orderbook_fetch_time = fetch_end  # 记录订单簿获取时间

            if not quote_data or 'bid' not in quote_data or 'ask' not in quote_data:
                logger.debug("Variational quote 数据不完整")
                return None
            
            bid_price = Decimal(str(quote_data['bid']))
            ask_price = Decimal(str(quote_data['ask']))
            mark_price = Decimal(str(quote_data['mark_price']))

            fetch_duration_ms = (fetch_end - fetch_start) * 1000  # 毫秒
            # logger.info(f"📊 订单簿获取耗时: {fetch_duration_ms:.2f} ms")

            # ✅ 构造订单簿格式（兼容 PriceMonitorService）
            orderbook = {
                'bids': [[float(bid_price), float(self.query_quantity)]],  # [price, size]
                'asks': [[float(ask_price), float(self.query_quantity)]],
                'timestamp': fetch_start,  # 秒时间戳
                'quote_id': quote_data.get('quote_id', None),
                'fetch_duration': fetch_duration_ms,
                'mark_price': mark_price
            }
            
            return orderbook
        except asyncio.TimeoutError:
            logger.warning("⚠️ Variational API 超时")
            return None
        except Exception as e:
            logger.exception(f"获取 Variational 订单簿失败: {e}")
            return None
    
    async def get_latest_orderbook(self, quantity: Optional[Decimal]) -> Optional[Dict]:
        """获取最新订单簿"""
        return await self.fetch_orderbook(quantity)
    
    async def place_open_order(
        self,
        side: str,
        quantity: Decimal,
        price: Optional[Decimal] = None,
        retry_mode: str = 'opportunistic',  # 'aggressive' or 'opportunistic'
        quote_id: Optional[str] = None,
        slippage: Optional[Decimal] = None
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
            if quote_id is None:
                #_quote_id为空，无法下单
                logger.error("❌ 下单失败：缺少 quote_id")
                return {
                    'success': False,
                    'order_id': None,
                    'error': 'Missing quote_id'
                }
            logger.info(f"📤 Variational 下市价单: {side} (quote_id: {quote_id})")
            return await self.place_market_order(
                side=side,
                quote_id=quote_id,
                slippage=slippage
            )
    async def place_close_order(
        self,
        side: str,
        quantity: Decimal,
        price: Optional[Decimal] = None,
        retry_mode: str = 'opportunistic',  # 'aggressive' or 'opportunistic'
        quote_id: Optional[str] = None,
        slippage: Optional[Decimal] = None
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
            if quote_id is None:
                #_quote_id为空，无法下单
                logger.error("❌ 下单失败：缺少 quote_id")
                return {
                    'success': False,
                    'order_id': None,
                    'error': 'Missing quote_id'
                }
            logger.info(f"📤 Variational 下市价单: {side} (quote_id: {quote_id})")
            return await self.place_market_order(
                side=side,
                quote_id=quote_id,
                slippage=slippage
            )


    async def place_market_order(self, side, quote_id, slippage) -> dict:
        """
        下市价单
        """
        order_start_time = time.time()

        try:
            # ✅ 记录下单时间
            max_slippage = float(str(slippage) if slippage else float(str(self.slippage)))
            logger.info(f"Placing market order with slippage: {max_slippage}")
                
            self._order_place_time = time.time()

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
                if time_diff > 1000:  # 超过 1 秒
                    logger.warning(f"⚠️ 订单簿数据过旧！时间差: {time_diff:.0f} ms")
            
            logger.info(
                f"   方向: {side}\n"
                f"   quote_id: {quote_id[:8]}...\n"
                f"   最大滑点: {max_slippage * 100:.3f}%"
                f"   订单簿年龄: {time_diff:.2f} ms (订单簿 → 下单)"  # ✅ 添加时间差
            )
            # ✅ 调用客户端下单
            result = await self.client._place_market_order(
                quote_id=quote_id,
                side=side,
                max_slippage=max_slippage
            )
            
            logger.info(f"📊 Market order raw response: {result}")
            place_end = time.time()
            place_duration = (place_end - self._order_place_time) * 1000  # 毫秒
            logger.info(f"✅ {self.exchange_name} 下单完成, 下单耗时:{place_duration:.2f}ms")

            # ✅ 检查返回格式
            if not result.success:
                error_msg = result.error_message or "Unknown error"
                
                logger.error(f"❌ 市价单下单失败: {error_msg}")
                return {
                    'success': False,
                    'order_id': None,
                    'error': result.error_message,
                    'timestamp': time.time()
                }
            
            rfq_id = result.order_id
            logger.info(f"⏳ 开始等待订单状态 rfq_id={rfq_id}")

            # ✅ 2. 等待 WebSocket 推送订单状态（适配器层负责）
            self.current_order_id = rfq_id
            logger.info(f"✅ 已设置 current_order_id = {rfq_id}")

            # final_status = await self._wait_for_order_fill(rfq_id, timeout=5.0)
            logger.info(f" 等待200ms后获取订单{rfq_id} 状态...")
            await asyncio.sleep(0.2)  # 确保状态更新完成
            
            max_order_retries = 40
            retry_interval = 0.01  # 10 ms
            order_data = None
            final_status = None
            retries = 0

            for attempt_idx in range(max_order_retries):
                try:
                    # 获取订单历史，尝试寻找匹配的 rfq_id
                    history_data = await self.client.get_orders_history(limit=20, offset=0)
                    if history_data and 'result' in history_data:
                        # 在结果列表中寻找对应的订单
                        matched_orders = [o for o in history_data['result'] if o.get('rfq_id') == rfq_id]
                        if matched_orders:
                            order_data = matched_orders[0]
                            final_status = order_data.get('status')
                            
                            logger.info(f"📊 第 {attempt_idx + 1} 次尝试成功获取订单状态: {final_status}")
                            retries = attempt_idx + 1
                            break

                    if attempt_idx < max_order_retries - 1:
                        retry_interval = 0.01 if attempt_idx < 10 else 0.05
                        logger.info(f"⏳ 订单 {rfq_id} 尚未入库，{retry_interval}s 后重试 ({attempt_idx + 1}/{max_order_retries})")
                        await asyncio.sleep(retry_interval)
                except Exception as e:
                    logger.warning(f"⚠️ 第 {attempt_idx + 1} 次查询历史订单异常: {e}")
                    await asyncio.sleep(retry_interval)

            execution_duration = (time.time() - place_end) * 1000  # 毫秒
            logger.info(f"⏱️ {self.exchange_name} 等待状态耗时: { execution_duration }ms, 状态: { final_status }")
            logger.info(f"⏱️ {self.exchange_name} 下单总耗时: {(time.time() - order_start_time) * 1000:.2f}ms")

            order_info = {
                'success': False,
                'order_id': rfq_id,
                'error': None,
                'filled_price': Decimal(str(order_data.get('price', '0'))) if order_data else None,
                'filled_quantity': Decimal(str(order_data.get('qty', '0'))) if order_data else None,
                'timestamp': time.time(),
                'place_duration_ms': place_duration,
                'execution_duration_ms': execution_duration,
                'retries': retries
            }
            if not final_status:
                logger.error(f"❌ 达到最大重试次数，仍无法获取订单 {rfq_id} 的信息")
                order_info['error'] = f'Timeout and order status: {final_status}'
            # ✅ 3. 判断最终状态
            if final_status.upper() in ['FILLED', 'CLEARED']:
                logger.info(f"✅ 市价单成功: {rfq_id} {order_info['filled_quantity']} @ {order_info['filled_price']}")
                order_info['success'] = True
                
            elif final_status.upper() in ['CANCELED', 'REJECTED']:
                logger.error(f"❌ 市价单失败: {final_status}")
                order_info['error'] = f'Order {final_status}'
            else:
                # 未知状态，保守返回失败
                logger.error(f"❌ 未知订单状态: {final_status}")
                order_info['error'] = f'Unknown status {final_status}'

            return order_info
        except Exception as e:
            logger.error(f"❌ place_market_order 异常: {e}")
            logger.info(f"⏱️ {self.exchange_name} 从下单到报错共耗时: {(time.time() - order_start_time) * 1000:.2f} ms")

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

        ✅ 核心逻辑（来自 hedge_mode_var.py）：
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
                f"📤 Variational 下单:\n"
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
                logger.info(f"✅ Variational 订单成交: {self.current_order_id}")
                return {
                    'success': True,
                    'order_id': self.current_order_id,
                    'error': None
                }
            else:
                logger.error("❌ Variational 订单失败")
                return {
                    'success': False,
                    'order_id': None,
                    'error': 'Order execution failed'
                }
        
        except Exception as e:
            logger.error(f"❌ Variational 下单失败: {e}")
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
        
        ✅ 参考 hedge_mode_var.py:place_variational_post_only_order()
        """
        logger.info(f"[Variational] [{side.upper()}] 开始挂单")

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
        
        ✅ 参考 hedge_mode_var.py:place_bbo_order()
        
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

    def _on_position_update(self, positions):
        """
        处理持仓更新（WebSocket 回调）
        ✅ 参考 hedge_mode_var.py:order_update_handler()
        """
        try:
            # logger.info(f"📊 WebSocket 持仓更新: positions={positions}")
            # logger.info(f"📊 当前状态: position_is_full={self.position_is_full}, "
            #                  f"current_order_id={getattr(self, 'current_order_id', None)}")

            # ✅ 初始状态：仓位为空
            if not positions and self.position_is_full is False:
                # logger.info("初始状态，持仓为空，无需处理")
                return
            
            # ✅ 平仓成功
            if not positions and self.position_is_full:
                logger.info("Variational平仓成功，仓位为空")
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
                    logger.info(f"{self.current_order_id} 部分成交: {self.position_size} / {self.query_quantity}")
                    return
                
                # ✅ 完全成交
                if self.position_size == self.query_quantity and not self.position_is_full:
                    self.position_is_full = True
                    self.order_status = 'FILLED'
                    
                    price = Decimal(position_data.get('position_info', {"avg_entry_price": "0"}).get('avg_entry_price', '0'))
                    logger.info(f"✅ {self.current_order_id} 完全成交: {self.query_quantity} @ {price}")
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
        获取 Variational 持仓信息（复用 client 方法）
        
        Args:
            symbol: 币种符号（如 'HYPE'）
        
        Returns:
            {
                'symbol': 'HYPE',
                'side': 'long',
                'size': 2.5,
                'entry_price': 28.3,
                'unrealized_pnl': 0.08
            }
        """
        try:
            # ✅ 直接调用 VariationalClient 的方法
            # 注意：需要先在 variational.py 中添加 get_position() 方法
            position = await self.client.get_position(symbol)
            
            if position:
                logger.info(
                    f"📊 Variational 持仓:    {'+' if position['side'] == 'long' else '-'}{position['size']} {position['symbol']} @ {position['entry_price']}"
                )
            else:
                logger.info(f"📊 Variational 无持仓: {symbol}")
            
            return position
        
        except Exception as e:
            logger.error(f"❌ Variational 获取持仓失败: {e}", exc_info=True)
            return None
        
    async def get_trade_volume(self) -> Decimal:
        """
        获取当前交易量（复用 client 方法）
        
        Returns:
            Decimal: 交易量
        """
        try:
            volume = await self.client.getVariationalVolume()
            # logger.info(f"📊 Variational 当前交易量: {volume}")
            return volume
        except Exception as e:
            logger.error(f"❌ Variational 获取交易量失败: {e}", exc_info=True)
            return Decimal('0')
        
    async def get_balance(self) -> Decimal:
        """
        获取账户交易股权余额（复用 client 方法）
        
        Returns:
            Decimal: 余额
        """
        try:
            balance = await self.client.getVariationalBalance()
            # logger.info(f"📊 Variational 账户交易股权余额: {balance}")
            return balance
        except Exception as e:
            logger.error(f"❌ Variational 获取余额失败: {e}", exc_info=True)
            return Decimal('0')