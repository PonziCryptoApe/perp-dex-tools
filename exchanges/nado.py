"""
Nado exchange client implementation.
"""

import os
import asyncio
import json
import traceback
import time
import sys
import getpass
from decimal import Decimal
from typing import Dict, Any, List, Optional, Tuple
from nado_protocol.client import create_nado_client, NadoClientMode
from nado_protocol.utils.subaccount import SubaccountParams
from nado_protocol.engine_client.types import OrderParams, PlaceMarketOrderParams, MarketOrderParams

from nado_protocol.utils.bytes32 import subaccount_to_hex
from nado_protocol.utils.expiration import get_expiration_timestamp
from nado_protocol.utils.math import to_x18, from_x18
from nado_protocol.utils.nonce import gen_order_nonce
from nado_protocol.utils.order import build_appendix, OrderType
from nado_protocol.engine_client.types.execute import CancelOrdersParams

from helpers.encryption_helper import EncryptionHelper

from .base import BaseExchangeClient, OrderResult, OrderInfo, query_retry
from helpers.logger import TradingLogger
import websockets

class NadoClient(BaseExchangeClient):
    """Nado exchange client implementation."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize Nado client."""
        super().__init__(config)

        # Nado credentials from environment
        self.private_key = self._load_private_key()

        self.mode = os.getenv('NADO_MODE', 'MAINNET').upper()
        self.subaccount_name = os.getenv('NADO_SUBACCOUNT_NAME', 'default')
        self.symbol = self.config.ticker + '-PERP'
        
        if not self.private_key:
            raise ValueError("NADO_PRIVATE_KEY_ENCRYPTED must be set in environment variables")

        # Map mode string to NadoClientMode enum
        mode_map = {
            'MAINNET': NadoClientMode.MAINNET,
            'DEVNET': NadoClientMode.DEVNET,
        }
        client_mode = mode_map.get(self.mode, NadoClientMode.MAINNET)

        # Initialize Nado client using official SDK
        self.client = create_nado_client(client_mode, self.private_key)
        self.owner = self.client.context.engine_client.signer.address

        # Initialize logger
        self.logger = TradingLogger(exchange="nado", ticker=self.config.ticker, log_to_console=False)

        self._order_update_handler = None
        self._orderbook_handler = None  # 新增: 订单簿更新回调
        self.orderbook = None  # 新增: WebSocket 缓存的订单簿数据
        
        # For websocket
        self._stop_event = asyncio.Event()  # 新增: 停止事件
        self._tasks: list[asyncio.Task] = []  # 新增: WS 任务列表
        self._ws_task: Optional[asyncio.Task] = None
        self._ws_stop = asyncio.Event()

    def _load_private_key(self) -> str:
        """✅ 加载并解密私钥"""
        encrypted_key = os.getenv('NADO_PRIVATE_KEY_ENCRYPTED')
        if not encrypted_key:
            raise ValueError("Missing NADO_PRIVATE_KEY_ENCRYPTED environment variable")

        # 提示用户输入解密密钥
        # decryption_key = input("请输入 Variational 私钥的解密密钥: ")
        decryption_key = ""
        if sys.stdin.isatty():
            print("\n🔐 Nado 私钥已加密，需要解密密钥")
            decryption_key = getpass.getpass("请输入解密密钥: ")
        else:
            raise RuntimeError("无法从非交互式环境中输入解密密钥")
        # 使用 EncryptionHelper 解密私钥
        encryption_helper = EncryptionHelper()
        private_key = encryption_helper.decrypt(encrypted_key, decryption_key)

        return private_key
    
    def _validate_config(self) -> None:
        """Validate Nado configuration."""
        required_env_vars = ['NADO_PRIVATE_KEY_ENCRYPTED']
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")

    async def connect(self) -> None:
        """Connect to Nado (setup WebSocket if needed)."""
        # Nado SDK may handle WebSocket internally, but we'll set up order monitoring
        # For now, we'll use polling for order updates if WebSocket is not available
        self._stop_event.clear()  # 新增: 清空停止事件
        self._tasks = [  # 新增: 启动 BBO WebSocket 任务
            asyncio.create_task(self._nado_ws_worker())
        ]
        self.logger.log("Nado WebSocket streams started", "INFO")
        self.logger.log("Connected to Nado", "INFO")

    async def disconnect(self) -> None:
        """Disconnect from Nado."""
        try:
            self.logger.log("Starting graceful disconnect from Nado", "INFO")
            
            # 停止 WebSocket 流
            self._stop_event.set()
            
            # 等待任务完成
            if self._tasks:
                for task in self._tasks:
                    if not task.done():
                        task.cancel()
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*self._tasks, return_exceptions=True),
                        timeout=2.0
                    )
                except asyncio.TimeoutError:
                    self.logger.log("⚠️ Some WS tasks did not finish in time", "WARNING")
                self.logger.log("Nado streams stopped", "INFO")
            
            # 停止旧的 _ws_task（如果存在）
            self._ws_stop.set()
            if self._ws_task and not self._ws_task.done():
                await self._ws_task
                
            self.logger.log("Nado disconnected successfully", "INFO")
        except Exception as e:
            self.logger.log(f"Error during Nado disconnect: {e}", "ERROR")

    async def _nado_ws_worker(self):
        """Nado 专用的 WebSocket worker：连接、订阅 BBO、处理更新。"""
        uri = "wss://gateway.prod.nado.xyz/v1/subscribe"
        while not self._stop_event.is_set():
            try:
                async with websockets.connect(
                    uri,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=None
                ) as ws:
                    self.logger.log("✅ Nado WebSocket 连接成功！", "INFO")
                    
                    # 发送订阅消息
                    subscribe_msg = {
                        "method": "subscribe",
                        "stream": {
                            "type": "best_bid_offer",
                            "product_id": self.config.contract_id  # 使用配置中的产品 ID
                        },
                        "id": 11
                    }
                    await ws.send(json.dumps(subscribe_msg))
                    self.logger.log(f"已发送 BBO 订阅 → product_id={self.config.contract_id}", "INFO")

                    # 接收循环
                    async for message in ws:
                        try:
                            data = json.loads(message)
                            
                            # 处理订阅确认
                            if data.get("id") == 11 and data.get("result") is None:
                                self.logger.log("✓ BBO 订阅确认成功！等待实时数据...", "INFO")
                                continue
                            
                            # 处理实时 BBO 更新
                            await self.handle_orderbook(data)
                            
                        except json.JSONDecodeError:
                            self.logger.log(f"收到非 JSON 数据: {message}", "WARNING")
                            continue
                        
            except websockets.exceptions.InvalidHandshake as e:
                status = getattr(e.response, 'status_code', 'Unknown') if hasattr(e, 'response') else 'Unknown'
                if status == 403:
                    self.logger.log("403 Forbidden！请添加 Authorization 头。从浏览器 app.nado.xyz Network 面板复制 Bearer Token。", "ERROR")
                else:
                    self.logger.log(f"握手失败: 状态码 {status}", "ERROR")
                sys.exit(1)
            except websockets.exceptions.ConnectionClosedError as e:
                self.logger.log(f"连接关闭: {e.code} - {e.reason}", "WARNING")
            except Exception as e:
                self.logger.log(f"❌ Nado WS 错误: {e}", "ERROR")
            
            if not self._stop_event.is_set():
                self.logger.log("3 秒后重连...", "INFO")
                await asyncio.sleep(3)
    
    async def handle_orderbook(self, message):
        """处理 Nado BBO 更新（类似于 extended 的 handle_orderbook）。"""
        try:
            self.logger.log("Received BBO update", "DEBUG")

            if message.get("type") == "best_bid_offer":
                ts = int(message["timestamp"])
                product_id = message["product_id"]
                
                # 从 x18 格式转换价格和数量
                bid_price = from_x18(int(message["bid_price"]))
                bid_qty = from_x18(int(message["bid_qty"]))
                ask_price = from_x18(int(message["ask_price"]))
                ask_qty = from_x18(int(message["ask_qty"]))

                # 更新订单簿缓存（匹配 extended 格式）
                self.orderbook = {
                    'ts': ts,
                    'market': f"{self.config.ticker}-USD",  # 模拟市场名称
                    'bid': [{'p': str(bid_price), 'q': str(bid_qty)}],  # str 以匹配 extended
                    'ask': [{'p': str(ask_price), 'q': str(ask_qty)}]
                }
                
                # 调用回调（如果注册）
                if self._orderbook_handler:
                    await self._orderbook_handler(self.orderbook)
                
                self.logger.log(f"BBO 更新: bid={bid_price}@{bid_qty}, ask={ask_price}@{ask_qty}", "INFO")
                
        except Exception as e:
            self.logger.log(f"Error handling BBO update: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
    
    def setup_orderbook_handler(self, handler) -> None:
        """设置订单簿更新回调（类似于 extended）。"""
        self._orderbook_handler = handler
        self.logger.log("Orderbook handler registered", "INFO")
                
    def get_exchange_name(self) -> str:
        """Get the exchange name."""
        return "nado"

    def setup_order_update_handler(self, handler) -> None:
        """Setup order update handler for WebSocket."""
        self._order_update_handler = handler
        # Nado SDK may provide WebSocket callbacks, but for now we'll use polling
        # This can be enhanced if Nado SDK provides WebSocket support

    @query_retry(default_return=(0, 0))
    async def fetch_bbo_prices(self, contract_id: str) -> Tuple[Decimal, Decimal, int]:
        """Fetch best bid/offer prices from Nado."""
        try:
            # Get order book depth
            # order_book = self.client.context.engine_client.get_orderbook(ticker_id=contract_id, depth=1)
            orderbook = self.orderbook
            if orderbook is None:
                self.logger.log(f"Error fetching BBO prices for {contract_id}: orderbook is None", level="ERROR")
                return Decimal(0), Decimal(0), 0

            # 获取最佳买价（最高买价）
            best_bid = Decimal('0')
            if orderbook["bid"] and len(orderbook["bid"]) > 0:
                best_bid = Decimal(orderbook["bid"][0]["p"])
            
            # 获取最佳卖价（最低卖价）  
            best_ask = Decimal('0')
            if orderbook["ask"] and len(orderbook["ask"]) > 0:
                best_ask = Decimal(orderbook["ask"][0]["p"])
            
            ts = orderbook.get('ts', 0)

            return best_bid, best_ask, ts

        except Exception as e:
            self.logger.log(f"Error fetching BBO prices: {e}", "ERROR")
            return Decimal(0), Decimal(0)

    def _get_product_id_from_contract(self, contract_id: str) -> int:
        """Convert contract_id (ticker) to product_id."""
        # Try to parse as int first
        try:
            return int(contract_id)
        except ValueError:
            # If it's a ticker like "BTC", we need to look it up
            # For now, use a simple mapping (this should be improved)
            ticker_to_product_id = {
                'BTC': 2,
                'ETH': 4,
                'BNB': 14,
                'AAVE': 26,
                'SOL': 8,
                'HYPE': 16,
                'SUI': 24,
                'XRP': 10,
            }
            ticker = contract_id.upper()
            return ticker_to_product_id.get(ticker, 1)  # Default to BTC

    async def get_order_price(self, direction: str) -> Decimal:
        """Get the price of an order with Nado."""
        best_bid, best_ask, _ts = await self.fetch_bbo_prices(self.symbol + '_USDT0')
        if best_bid <= 0 or best_ask <= 0:
            self.logger.log("Invalid bid/ask prices", "ERROR")
            raise ValueError("Invalid bid/ask prices")

        if direction == 'buy':
            # For buy orders, place slightly below best ask to ensure execution
            order_price = best_ask - self.config.tick_size
        else:
            # For sell orders, place slightly above best bid to ensure execution
            order_price = best_bid + self.config.tick_size
        return self.round_to_tick(order_price)

    async def place_open_order(self, contract_id: str, quantity: Decimal, direction: str) -> OrderResult:
        """Place an open order with Nado using official SDK."""
        max_retries = 5
        retry_count = 0

        while retry_count < max_retries:
            try:
                best_bid, best_ask = await self.fetch_bbo_prices(self.symbol + '_USDT0')

                if best_bid <= 0 or best_ask <= 0:
                    return OrderResult(success=False, error_message='Invalid bid/ask prices')

                # Determine order price
                if direction == 'buy':
                    order_price = best_ask - self.config.tick_size
                else:
                    order_price = best_bid + self.config.tick_size

                # Build order parameters
                order = OrderParams(
                    sender=SubaccountParams(
                        subaccount_owner=self.owner,
                        subaccount_name=self.subaccount_name,
                    ),
                    priceX18=to_x18(float(str(order_price))),
                    amount=to_x18(float(str(quantity))) if direction == 'buy' else -to_x18(float(str(quantity))),
                    expiration=get_expiration_timestamp(60*60*24*30),
                    nonce=gen_order_nonce(),
                    appendix=build_appendix(order_type=OrderType.POST_ONLY)
                )

                # Place the order
                result = self.client.market.place_order({"product_id": int(contract_id), "order": order})

                if not result:
                    return OrderResult(success=False, error_message='Failed to place order')

                # Extract order ID from response
                order_id = result.data.digest

                # Order successfully placed
                return OrderResult(
                    success=True,
                    order_id=order_id,
                    side=direction,
                    size=quantity,
                    price=order_price,
                    status='OPEN'
                )

            except Exception as e:
                self.logger.log(f"Error placing open order: {e}", "ERROR")
                if retry_count < max_retries - 1:
                    retry_count += 1
                    await asyncio.sleep(0.1)
                    continue
                else:
                    return OrderResult(success=False, error_message=str(e))

        return OrderResult(success=False, error_message='Max retries exceeded')

    async def place_close_order(self, contract_id: str, quantity: Decimal, price: Decimal, side: str) -> OrderResult:
        """Place a close order with Nado using official SDK."""
        max_retries = 5
        retry_count = 0

        while retry_count < max_retries:
            try:
                best_bid, best_ask, _ts = await self.fetch_bbo_prices(self.symbol + '_USDT0')

                if best_bid <= 0 or best_ask <= 0:
                    return OrderResult(success=False, error_message='Invalid bid/ask prices')

                # Adjust order price based on market conditions
                adjusted_price = price
                if side.lower() == 'sell':
                    # For sell orders, ensure price is above best bid to be a maker order
                    if price <= best_bid:
                        adjusted_price = best_bid + self.config.tick_size
                elif side.lower() == 'buy':
                    # For buy orders, ensure price is below best ask to be a maker order
                    if price >= best_ask:
                        adjusted_price = best_ask - self.config.tick_size

                # Build order parameters
                order = OrderParams(
                    sender=SubaccountParams(
                        subaccount_owner=self.owner,
                        subaccount_name=self.subaccount_name,
                    ),
                    priceX18=to_x18(float(adjusted_price)),
                    amount=to_x18(float(quantity)) if side.lower() == 'buy' else -to_x18(float(quantity)),
                    expiration=get_expiration_timestamp(3600),  # 1 hour expiration
                    nonce=gen_order_nonce(),
                    appendix=build_appendix(order_type=OrderType.POST_ONLY)
                )

                # Place the order
                result = self.client.market.place_order({"product_id": int(contract_id), "order": order})

                if not result:
                    return OrderResult(success=False, error_message='Failed to place order')

                # Extract order ID from response
                order_id = result.data.digest
                if not order_id:
                    await asyncio.sleep(0.1)
                    return OrderResult(
                        success=True,
                        side=side,
                        size=quantity,
                        price=adjusted_price,
                        status='OPEN'
                    )

                # Order successfully placed
                return OrderResult(
                    success=True,
                    order_id=order_id,
                    side=side,
                    size=quantity,
                    price=adjusted_price,
                    status='OPEN'
                )

            except Exception as e:
                self.logger.log(f"Error placing close order: {e}", "ERROR")
                if retry_count < max_retries - 1:
                    retry_count += 1
                    await asyncio.sleep(0.1)
                    continue
                else:
                    return OrderResult(success=False, error_message=str(e))

        return OrderResult(success=False, error_message='Max retries exceeded for close order')

    async def cancel_order(self, order_id: str) -> OrderResult:
        """Cancel an order with Nado using official SDK."""
        try:
            sender = subaccount_to_hex(SubaccountParams(
                    subaccount_owner=self.owner,
                    subaccount_name=self.subaccount_name,
                ))
            # Cancel order using Nado SDK
            result = self.client.market.cancel_orders(
                CancelOrdersParams(productIds=[self.config.contract_id], digests=[order_id], sender=sender)
            )

            if not result:
                return OrderResult(success=False, error_message='Failed to cancel order')

            order_info = await self.get_order_info(order_id)

            filled_size = order_info.filled_size if order_info is not None else Decimal(0)
            price = order_info.price if order_info is not None else Decimal(0)

            return OrderResult(success=True, filled_size=filled_size, price=price)

        except Exception as e:
            self.logger.log(f"Error canceling order: {e}", "ERROR")
            return OrderResult(success=False, error_message=str(e))

    @query_retry()
    async def get_order_info(self, order_id: str) -> Optional[OrderInfo]:
        """Get order information from Nado using official SDK."""
        try:
            # Get order info from Nado SDK
            # Note: Adjust method name if SDK uses different API
            order = self.client.context.engine_client.get_order(product_id=self.config.contract_id, digest=order_id)
            price_x18 = getattr(order, 'price_x18', None)
            amount_x18 = getattr(order, 'amount', None)
            unfilled_x18 = getattr(order, 'unfilled_amount', None)
            order_id = str(getattr(order, 'digest', None))

            size = Decimal(str(from_x18(amount_x18))) if amount_x18 else Decimal(0)
            remaining_size = Decimal(str(from_x18(unfilled_x18))) if unfilled_x18 else Decimal(0)
            filled_size = size - remaining_size

            side = 'buy' if size > 0 else 'sell'

            return OrderInfo(
                order_id=order_id,
                side=side,
                size=size,
                price=Decimal(str(from_x18(price_x18))),
                status='OPEN',
                filled_size=filled_size,
                remaining_size=remaining_size
            )

        except Exception as e:
            attempt = 0
            while attempt < 4:
                attempt += 1
                self.logger.log(f"Attempt {attempt} to get archived order info", "INFO")
                try:
                    order_result = self.client.context.indexer_client.get_historical_orders_by_digest([order_id])
                    if order_result.orders != []:
                        order = order_result.orders[0]
                        # Parse order data
                        price_x18 = getattr(order, 'price_x18', None)
                        amount_x18 = getattr(order, 'amount', None)
                        filled_x18 = getattr(order, 'base_filled', None)
                        order_id = str(getattr(order, 'digest', None))

                        if order.base_filled == order.amount:
                            status = 'FILLED'
                        else:
                            status = 'CANCELLED'

                        size = Decimal(str(from_x18(amount_x18))) if amount_x18 else Decimal(0)
                        filled_size = Decimal(str(from_x18(filled_x18))) if filled_x18 else Decimal(0)
                        remaining_size = size - filled_size

                        side = 'buy' if size > 0 else 'sell'

                        return OrderInfo(
                            order_id=order_id,
                            side=side,
                            size=size,
                            price=Decimal(str(from_x18(price_x18))),
                            status=status,
                            filled_size=filled_size,
                            remaining_size=remaining_size
                        )
                except Exception as e:
                    self.logger.log(f"Error getting order info after retry: {e}", "ERROR")

                await asyncio.sleep(0.5)

            return OrderInfo(
                order_id=order_id,
                side='',
                size=Decimal(0),
                price=Decimal(0),
                status='CANCELLED',
                filled_size=Decimal(0),
                remaining_size=Decimal(0)
            )

    @query_retry(default_return=[])
    async def get_active_orders(self, contract_id: str) -> List[OrderInfo]:
        """Get active orders for a contract using official SDK."""
        try:            
            # Get subaccount open orders from Nado SDK
            sender = subaccount_to_hex(SubaccountParams(
                    subaccount_owner=self.owner,
                    subaccount_name=self.subaccount_name,
                ))

            orders_data = self.client.market.get_subaccount_open_orders(
                product_id=contract_id,
                sender=sender)

            if not orders_data:
                return []

            orders = []
            # Handle both list and object with orders attribute
            order_list = orders_data if isinstance(orders_data, list) else getattr(orders_data, 'orders', [])

            for order in order_list:
                price_x18 = getattr(order, 'price_x18', None)
                amount_x18 = getattr(order, 'amount', None)
                unfilled_x18 = getattr(order, 'unfilled_amount', None)
                order_id = str(getattr(order, 'digest', None))

                size = Decimal(str(from_x18(amount_x18))) if amount_x18 else Decimal(0)
                remaining_size = Decimal(str(from_x18(unfilled_x18))) if unfilled_x18 else Decimal(0)
                filled_size = size - remaining_size

                side = 'buy' if size > 0 else 'sell'

                orders.append(OrderInfo(
                    order_id=str(order_id),
                    side=side,
                    size=size,
                    price=Decimal(str(from_x18(price_x18))) if price_x18 else Decimal(0),
                    status='OPEN',
                    filled_size=filled_size,
                    remaining_size=remaining_size
                ))

            return orders

        except Exception as e:
            self.logger.log(f"Error getting active orders: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            return []

    @query_retry(default_return=0)
    async def get_account_positions(self) -> Decimal:
        """Get account positions using official SDK."""
        try:
            # Get subaccount identifier
            resolved_subaccount = subaccount_to_hex(self.client.context.signer.address, self.subaccount_name)
            
            # Get isolated positions from Nado SDK (requires subaccount parameter)
            account_data = self.client.context.engine_client.get_subaccount_info(resolved_subaccount)
            position_data = account_data.perp_balances

            # Find position for current contract
            product_id = self.config.contract_id
            
            for position in position_data:
                if position.product_id == product_id:
                    position_size = position.balance.amount
                    return Decimal(str(from_x18(position_size)))

            return Decimal(0)

        except Exception as e:
            self.logger.log(f"Error getting account positions: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            return Decimal(0)

    async def get_contract_attributes(self) -> Tuple[str, Decimal]:
        """Get contract ID and tick size for a ticker."""
        ticker = self.config.ticker
        if len(ticker) == 0:
            self.logger.log("Ticker is empty", "ERROR")
            raise ValueError("Ticker is empty")

        try:
            # Get markets/products from Nado SDK
            symbols = self.client.market.get_all_product_symbols()
            product_id = None
            for symbol in symbols:
                symbol_str = symbol.symbol if hasattr(symbol, 'symbol') else str(symbol)
                if symbol_str == f"{ticker.upper()}-PERP":
                    product_id = symbol.product_id if hasattr(symbol, 'product_id') else symbol
                    self.config.contract_id = product_id
                    break
            all_markets = self.client.market.get_all_engine_markets()
            markets = all_markets.perp_products
            current_market = None
            for market in markets:
                if market.product_id == product_id:
                    current_market = market
                    break

            if current_market is None:
                self.logger.log(f"Failed to get market for ticker {ticker}", "ERROR")
                raise ValueError(f"Failed to get market for ticker {ticker}")

            # Get tick size and min quantity
            tick_size_x18 = current_market.book_info.price_increment_x18
            min_quantity_x18 = current_market.book_info.size_increment

            self.config.tick_size = Decimal(str(from_x18(tick_size_x18)))

            min_quantity = Decimal(str(from_x18(min_quantity_x18)))

            if self.config.quantity < min_quantity:
                self.logger.log(f"Order quantity is less than min quantity: {self.config.quantity} < {min_quantity}", "ERROR")
                raise ValueError(f"Order quantity is less than min quantity: {self.config.quantity} < {min_quantity}")

            return self.config.contract_id, self.config.tick_size

        except Exception as e:
            self.logger.log(f"Error getting contract attributes: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            raise

    async def get_position(self, symbol: str) -> Decimal:
        """Get position size for a given symbol using official SDK."""
        try:
            # Get subaccount identifier
            resolved_subaccount = subaccount_to_hex(self.client.context.signer.address, self.subaccount_name)
            
            # Get isolated positions from Nado SDK (requires subaccount parameter)
            account_data = self.client.context.engine_client.get_subaccount_info(resolved_subaccount)
            position_data = account_data.perp_balances
            # Find position for current contract
            product_id = self._get_product_id_from_contract(symbol)
            for position in position_data:
                if position.product_id == product_id:
                    print(f"Found position for {symbol}: {position.balance}", "INFO")
                    position_size = position.balance.amount
                    return {
                        'symbol': symbol,
                        'side': Decimal(str(position_size)) > 0 and 'long' or 'short',
                        'size': abs(Decimal(str(from_x18(position_size)))),
                        'entry_price': None,
                        'unrealized_pnl': None
                    }

            return None

        except Exception as e:
            self.logger.log(f"Error getting account positions: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            return None

    async def _place_market_order(self, contract_id: str, quantity: Decimal, side: str, max_slippage: Decimal) -> OrderResult:
        """Place a market order with Nado using official SDK."""
        # For Nado, we simulate market orders with post-only limit orders
        # best_bid, best_ask = await self.fetch_bbo_prices(self.symbol + '_USDT0')

        # if best_bid <= 0 or best_ask <= 0:
            # return OrderResult(success=False, error_message='Invalid bid/ask prices')

        # Determine order price
        # if direction == 'buy':
        #     order_price = best_ask - self.config.tick_size
        # else:
        #     order_price = best_bid + self.config.tick_size
        # Place the order
        product_id = self._get_product_id_from_contract(contract_id)
        sender=subaccount_to_hex(SubaccountParams(
                        subaccount_owner=self.owner,
                        subaccount_name=self.subaccount_name,
                    ))
        params_dict = {
            "product_id": product_id,
            "market_order": MarketOrderParams(
                nonce=None,
                sender=sender,
                amount=to_x18(float(str(quantity))) if side == 'buy' else -to_x18(float(str(quantity)))
            ),
            "slippage": float(str(max_slippage)),
            "reduce_only": False
        }
        params = PlaceMarketOrderParams(**params_dict)
        result = self.client.market.place_market_order(params)

        if not result:
            return OrderResult(success=False, error_message='Failed to place order')

        # Extract order ID from response
        order_id = result.data.digest
        if not order_id:
            raise ValueError("SDK 返回无 order_id")
        
        self.logger.log(f"✅ Nado 市价单成功: ID={order_id}", "INFO")
        # Order successfully placed
        return OrderResult(
            success=True,
            order_id=order_id,
            side=side,
            size=quantity,
            status='OPEN'
        )
    async def get_portfolio(self):
        return {
            'balance': '-',
            'upnl': '-',
        }


