# 实现extended的秒开仓秒关仓功能，市价单成交，不对冲
import asyncio
import json
import time
import csv
import signal
from typing import Tuple
import pytz

from datetime import datetime
from decimal import Decimal

from helpers import TradingLogger
from exchanges.extended import ExtendedClient
from exchanges.factory import ExchangeFactory
from helpers.util import Config


class SingleBot:
    def __init__(self, ticker, order_quantity, iterations, side, interval = 6):
        # 初始化参数、日志、交易所 client
        self.ticker = ticker
        self.order_quantity = order_quantity
        self.iterations = iterations
        self.interval = interval  # 每次开平仓间隔，秒
        self.side = side
        trading_logger = TradingLogger('extended', ticker, log_to_console=True)
        self.logger = trading_logger.logger
        self.log_transaction = trading_logger.log_transaction
        self.close_order_side = 'sell' if side == 'buy' else 'buy'

        # extended Status
        self.exchange_client = None
        self.extended_contract_id = None
        self.extended_order_status = None
        self.tick_size = None

        # extended order book state
        self.extended_order_book = {
            'bids': {},
            'asks': {}
        }
        self.extended_order_book_ready = False
        self.extended_best_bid = None
        self.extended_best_ask = None

        self.stop_flag = False


        config_dict = {
            'exchange': 'extended',
            'ticker': ticker,
            'quantity': Decimal(order_quantity),
            'iterations': iterations,
            'tick_size': Decimal('0.01'),
            'contract_id': '',  # will be set later if needed
            'side': side,
            'take_profit': 0,
            'close_order_side': self.close_order_side,
            # 'stop_loss': None
        }
        # 扩展时使用factory创建client的方式,暂时手动
        config = Config(config_dict)
        self.config = config
        self.extended_position = Decimal('0')
        self.exchange_client = ExtendedClient(config)
        self.depth_ws_task = None
        self._shutdown_called = False  # 防止重复调用 shutdown
        # try:
            # self.exchange_client = ExchangeFactory.create_exchange(
            #     config.exchange,
            #     config
            # )
        # except ValueError as e:
            # raise ValueError(f"Failed to create exchange client: {e}")
    # def log_trade_to_csv(self, exchange: str, side: str, price: str, quantity: str):
    #     """Log trade details to CSV file."""
    #     timestamp = datetime.now(pytz.UTC).isoformat()

    #     with open(self.csv_filename, 'a', newline='') as csvfile:
    #         writer = csv.writer(csvfile)
    #         writer.writerow([
    #             exchange,
    #             timestamp,
    #             side,
    #             price,
    #             quantity
    #         ])

        # self.logger.info(f"📊 Trade logged to CSV: {exchange} {side} {quantity} @ {price}")
    def handle_extended_order_update(self, order_data):
        """Handle Extended order updates from WebSocket."""
        side = order_data.get('side', '').lower()
        filled_size = Decimal(order_data.get('filled_size', '0'))
        price = Decimal(order_data.get('price', '0'))

        if side == 'buy':
            self.extended_position += filled_size
            lighter_side = 'sell'
        else:
            self.extended_position -= filled_size
            lighter_side = 'buy'

        # Store order details for immediate execution
        self.current_lighter_side = lighter_side
        self.current_lighter_quantity = filled_size
        self.current_lighter_price = price

        self.lighter_order_info = {
            'lighter_side': lighter_side,
            'quantity': filled_size,
            'price': price
        }

        self.waiting_for_lighter_fill = True

        self.logger.info(f"📋 Ready to place Lighter order: {lighter_side} {filled_size} @ {price}")
    
    async def setup_extended_websocket(self):
        """Setup Extended websocket for order updates and order book data."""
        if not self.exchange_client:
            raise Exception("Extended client not initialized")

        def order_update_handler(order_data):
            """Handle order updates from Extended WebSocket."""
            if order_data.get('contract_id') != self.extended_contract_id:
                self.logger.info(f"Ignoring order update from {order_data.get('contract_id')}")
                return

            try:
                order_id = order_data.get('order_id')
                status = order_data.get('status')
                side = order_data.get('side', '').lower()
                filled_size = Decimal(order_data.get('filled_size', '0'))
                size = Decimal(order_data.get('size', '0'))
                price = order_data.get('averagePrice', '0')

                if side == 'buy':
                    order_type = "OPEN"
                else:
                    order_type = "CLOSE"

                # Handle the order update
                if status == 'FILLED':
                    if side == 'buy':
                        self.extended_position += filled_size
                    else:
                        self.extended_position -= filled_size
                    self.logger.info(f"[{order_id}] [{side.upper()}] [Extended] [{status}]: {filled_size} @ {price}")
                    self.extended_order_status = status

                    self.log_transaction(
                        order_id=order_id,
                        side=side,
                        quantity=filled_size,
                        price=Decimal(price),
                        status=status
                    )

                    self.handle_extended_order_update({
                        'order_id': order_id,
                        'side': side,
                        'status': status,
                        'size': size,
                        'price': price,
                        'contract_id': self.extended_contract_id,
                        'filled_size': filled_size
                    })
                else:
                    if status == 'OPEN':
                        self.logger.info(f"[{order_id}] [{order_type}] [Extended] [{status}]: {size} @ {price}")
                    else:
                        self.logger.info(f"[{order_id}] [{order_type}] [Extended] [{status}]: {filled_size} @ {price}")
                    # Update order status for all non-filled statuses
                    if status == 'PARTIALLY_FILLED':
                        self.extended_order_status = "OPEN"
                    elif status in ['CANCELED', 'CANCELLED']:
                        self.extended_order_status = status
                    elif status in ['NEW', 'OPEN', 'PENDING', 'CANCELING']:
                        self.extended_order_status = status
                    else:
                        self.logger.warning(f"Unknown order status: {status}")
                        self.extended_order_status = status

            except Exception as e:
                self.logger.error(f"Error handling Extended order update: {e}")

        try:
            # Setup order update handler
            self.exchange_client.setup_order_update_handler(order_update_handler)
            self.logger.info("✅ Extended WebSocket order update handler set up")

            # Connect to Extended WebSocket
            await self.exchange_client.connect()
            self.logger.info("✅ Extended WebSocket connection established")

            # Setup separate WebSocket connection for depth updates
            await self.setup_extended_depth_websocket()

        except Exception as e:
            self.logger.error(f"Could not setup Extended WebSocket handlers: {e}")

    async def setup_extended_depth_websocket(self):
        """Setup separate WebSocket connection for Extended depth updates."""
        try:
            import websockets

            async def handle_depth_websocket():
                """Handle depth WebSocket connection."""
                # Use the correct Extended WebSocket URL for order book stream
                market_name = f"{self.ticker}-USD"  # Extended uses format like BTC-USD
                url = f"wss://api.starknet.extended.exchange/stream.extended.exchange/v1/orderbooks/{market_name}?depth=1"

                while not self.stop_flag:
                    try:
                        async with websockets.connect(url) as ws:
                            self.logger.info(f"✅ Connected to Extended order book stream for {market_name}")

                            # Listen for messages
                            async for message in ws:
                                if self.stop_flag:
                                    break

                                try:
                                    # Handle ping frames
                                    if isinstance(message, bytes) and message == b'\x09':
                                        await ws.pong()
                                        continue

                                    data = json.loads(message)
                                    self.logger.debug(f"Received Extended order book message: {data}")

                                    # Handle order book updates
                                    if data.get("type") in ["SNAPSHOT", "DELTA"]:
                                        self.handle_extended_order_book_update(data)

                                except json.JSONDecodeError as e:
                                    self.logger.warning(f"Failed to parse Extended order book message: {e}")
                                except Exception as e:
                                    self.logger.error(f"Error handling Extended order book message: {e}")

                    except websockets.exceptions.ConnectionClosed:
                        self.logger.warning("Extended order book WebSocket connection closed, reconnecting...")
                    except Exception as e:
                        self.logger.error(f"Extended order book WebSocket error: {e}")

                    # Wait before reconnecting
                    if not self.stop_flag:
                        await asyncio.sleep(2)
                    else:
                        break

            # Start depth WebSocket in background
            self.depth_ws_task = asyncio.create_task(handle_depth_websocket())
            self.logger.info("✅ Extended order book WebSocket task started")

        except Exception as e:
            self.logger.error(f"Could not setup Extended order book WebSocket: {e}")
    
    def handle_extended_order_book_update(self, message):
        """Handle Extended order book updates from WebSocket."""
        try:
            if isinstance(message, str):
                message = json.loads(message)

            self.logger.debug(f"Received Extended order book message: {message}")

            # Check if this is an order book update message
            if message.get("type") in ["SNAPSHOT", "DELTA"]:
                data = message.get("data", {})

                if data:
                    # Handle SNAPSHOT - replace entire order book
                    if message.get("type") == "SNAPSHOT":
                        self.extended_order_book['bids'].clear()
                        self.extended_order_book['asks'].clear()

                    # Update bids - Extended format is [{"p": "price", "q": "size"}, ...]
                    bids = data.get('b', [])
                    for bid in bids:
                        if isinstance(bid, dict):
                            price = Decimal(bid.get('p', '0'))
                            size = Decimal(bid.get('q', '0'))
                        else:
                            # Fallback for array format [price, size]
                            price = Decimal(bid[0])
                            size = Decimal(bid[1])
                        
                        if size > 0:
                            self.extended_order_book['bids'][price] = size
                        else:
                            # Remove zero size orders
                            self.extended_order_book['bids'].pop(price, None)

                    # Update asks - Extended format is [{"p": "price", "q": "size"}, ...]
                    asks = data.get('a', [])
                    for ask in asks:
                        if isinstance(ask, dict):
                            price = Decimal(ask.get('p', '0'))
                            size = Decimal(ask.get('q', '0'))
                        else:
                            # Fallback for array format [price, size]
                            price = Decimal(ask[0])
                            size = Decimal(ask[1])
                        
                        if size > 0:
                            self.extended_order_book['asks'][price] = size
                        else:
                            # Remove zero size orders
                            self.extended_order_book['asks'].pop(price, None)

                    # Update best bid and ask
                    if self.extended_order_book['bids']:
                        self.extended_best_bid = max(self.extended_order_book['bids'].keys())
                    if self.extended_order_book['asks']:
                        self.extended_best_ask = min(self.extended_order_book['asks'].keys())

                    if not self.extended_order_book_ready:
                        self.extended_order_book_ready = True
                        self.logger.info(f"📊 Extended order book ready - Best bid: {self.extended_best_bid}, "
                                         f"Best ask: {self.extended_best_ask}")
                    else:
                        self.logger.debug(f"📊 Order book updated - Best bid: {self.extended_best_bid}, "
                                          f"Best ask: {self.extended_best_ask}")

        except Exception as e:
            self.logger.error(f"Error handling Extended order book update: {e}")
            self.logger.error(f"Message content: {message}")

    async def get_extended_contract_info(self) -> Tuple[str, Decimal]:
        """Get Extended contract ID and tick size."""
        if not self.exchange_client:
            raise Exception("Extended client not initialized")

        contract_id, tick_size = await self.exchange_client.get_contract_attributes()

        # if self.order_quantity < self.exchange_client.quantity:
        #     raise ValueError(
        #         f"Order quantity is less than min quantity: {self.order_quantity} < {self.exchange_client.config.quantity}")

        return contract_id, tick_size
    
    async def shutdown(self, signum=None, frame=None):
        """Graceful shutdown handler."""
        # 使用一个标志防止重复调用
        if hasattr(self, '_shutdown_called') and self._shutdown_called:
            return
        self._shutdown_called = True
        self.logger.info("\n🛑 Stopping...")

        try:
            # 1. 先取消后台 WebSocket 任务
            if self.depth_ws_task and not self.depth_ws_task.done():
                self.depth_ws_task.cancel()
                try:
                    await self.depth_ws_task
                except asyncio.CancelledError:
                    pass
                self.logger.info("🔄 Depth WebSocket task cancelled")
            
            # 2. 等待一下
            await asyncio.sleep(0.5)
            
            # 3. 断开 exchange_client（里面会关闭所有 session）
            if self.exchange_client:
                try:
                    await asyncio.wait_for(self.exchange_client.disconnect(), timeout=5.0)
                    self.logger.info("🔌 Extended client disconnected")
                except asyncio.TimeoutError:
                    self.logger.warning("⚠️ Exchange disconnect timeout")
                except Exception as e:
                    self.logger.error(f"Error disconnecting exchange client: {e}")
            
            # 4. 最后等待
            await asyncio.sleep(1)
            
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}", exc_info=True)
        finally:
            self.logger.info("🔌 Extended WebSocket will be disconnected")
            
            # 关闭日志处理器
            for handler in self.logger.handlers[:]:
                try:
                    handler.close()
                    self.logger.removeHandler(handler)
                except Exception:
                    pass
            
            self.logger.info("✅ Shutdown complete.")

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            """Synchronous signal handler."""
            if not self.stop_flag:  # 防止重复触发
                self.logger.info("\n🛑 Received shutdown signal...")
                self.stop_flag = True
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    async def init_ws(self):
        try:
            await self.setup_extended_websocket()
            self.logger.info("✅ Extended WebSocket connection established")

            # Wait for initial order book data with timeout
            self.logger.info("⏳ Waiting for initial order book data...")
            timeout = 10  # seconds
            start_time = time.time()
            while not self.extended_order_book_ready and not self.stop_flag:
                if time.time() - start_time > timeout:
                    self.logger.warning(f"⚠️ Timeout waiting for WebSocket order book data after {timeout}s")
                    break
                await asyncio.sleep(0.5)

            if self.extended_order_book_ready:
                self.logger.info("✅ WebSocket order book data received")
            else:
                self.logger.warning("⚠️ WebSocket order book not ready, will use REST API fallback")

        except Exception as e:
            self.logger.error(f"❌ Failed to setup Extended websocket: {e}")
            return

    async def place_and_monitor_order(self, side: str, order_type: str, iter_num: int) -> bool:
        """
        通用的下单和监控逻辑
        
        Args:
            side: 'buy' or 'sell'
            order_type: 'OPEN' or 'CLOSE'
            iter_num: 当前迭代次数
        
        Returns:
            bool: 订单是否成功成交
        """
        success = False
        order_id = None
        
        while not success and not self.stop_flag:
            # 下单
            self.logger.info(f"[第{iter_num}次] 尝试{order_type}单 ({side})...")
            order_result = await self.exchange_client.place_market_order(
                self.extended_contract_id, self.order_quantity, side
            )
            
            if order_result.success:
                order_id = order_result.order_id
                self.logger.info(f"{order_type}单已提交: {order_id}")
                
                # 等待订单成交，最多等待10秒
                wait_time = 0
                max_wait = 10
                
                while wait_time < max_wait and not self.stop_flag:
                    await asyncio.sleep(0.5)
                    wait_time += 0.5
                    
                    # 检查订单状态
                    order_info = await self.exchange_client.get_order_info(order_id)
                    
                    if order_info and order_info.status == "FILLED":
                        success = True
                        self.logger.info(f"✅ {order_type}单成交: {order_result}，订单ID: {order_id}")
                        break
                    elif order_info and order_info.status in ["CANCELED", "CANCELLED", "REJECTED"]:
                        self.logger.warning(f"❌ {order_type}单被取消或拒绝，重新下单")
                        break
                
                # 如果超时未成交，取消订单
                if not success:
                    self.logger.warning(f"⏰ {order_type}单 {order_id} 超时未成交，取消并重新下单")
                    try:
                        cancel_result = await self.exchange_client.cancel_order(order_id)
                        self.logger.info(f"取消结果: {cancel_result}")
                    except Exception as e:
                        self.logger.error(f"取消订单失败: {e}")
                    
                    # 等待一下再重新下单
                    await asyncio.sleep(1)
            else:
                self.logger.error(f"❌ 下{order_type}单失败: {order_result.error_message}")
                await asyncio.sleep(2)
        
        return success

    async def run(self):
        self.setup_signal_handlers()
        try:
            self.extended_contract_id, self.extended_tick_size = await self.get_extended_contract_info()

            await self.init_ws()
            iter = 0
            
            while not self.stop_flag and iter < self.iterations:
                self.logger.info(f"\n{'='*50}")
                self.logger.info(f"开始第 {iter+1}/{self.iterations} 次交易")
                self.logger.info(f"{'='*50}\n")
                
                # 开仓逻辑
                open_success = await self.place_and_monitor_order(
                    side=self.side,
                    order_type="OPEN",
                    iter_num=iter+1
                )
                
                # 如果开仓成功且未收到停止信号，才平仓
                if open_success and not self.stop_flag:
                    # 平仓逻辑：方向相反
                    close_success = await self.place_and_monitor_order(
                        side=self.close_order_side,
                        order_type="CLOSE",
                        iter_num=iter+1
                    )
                    
                    if close_success:
                        self.logger.info(f"🎉 第{iter+1}次开平仓完成")
                    else:
                        self.logger.warning(f"⚠️ 第{iter+1}次平仓未成功")
                else:
                    self.logger.warning(f"⚠️ 第{iter+1}次开仓未成功")

                    # if not open_success:
                    #     self.logger.warning(f"⚠️ 第{iter+1}次开仓未成功")
                    # if self.stop_flag:
                    #     self.logger.warning(f"⚠️ 收到停止信号，跳过平仓")
                
                # 每次循环间隔
                iter += 1
                if iter < self.iterations and not self.stop_flag:
                    self.logger.info(f"⏳ 等待{self.interval}秒后进行下一次开平仓...")
                    for _ in range(self.interval):  # 分成多次检查，提高响应速度
                        if self.stop_flag:
                            break
                        await asyncio.sleep(1)
            # 正常退出
            if not self.stop_flag:
                self.logger.info("✅ 所有交易完成")
        except KeyboardInterrupt:
            self.logger.info("\n🛑 Received interrupt signal...")
            self.stop_flag = True

        except Exception as e:
            self.logger.error(f"❌ Run loop error: {e}", exc_info=True)
            self.stop_flag = True

        finally:
            # 确保 shutdown 被调用
            if not self.stop_flag:
                self.stop_flag = True
            await self.shutdown()