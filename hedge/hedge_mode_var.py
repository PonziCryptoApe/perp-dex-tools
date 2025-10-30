import asyncio
import json
import signal
import logging
import os
import sys
import time
import requests
import argparse
import traceback
import csv
from decimal import Decimal
from typing import Tuple

from lighter.signer_client import SignerClient
import sys
import os

from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from helpers.lark_bot import LarkBot
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from exchanges.variational import VariationalClient
import websockets
from datetime import datetime
import pytz


class Config:
    """Simple config class to wrap dictionary for Variational client."""
    def __init__(self, config_dict):
        for key, value in config_dict.items():
            setattr(self, key, value)


class HedgeBot:
    """Trading bot that places post-only orders on Variational and hedges with market orders on Lighter."""

    def __init__(self, ticker: str, order_quantity: Decimal, fill_timeout: int = 5, iterations: int = 20, oi_interval_seconds: int = 2):
        self.ticker = ticker
        self.order_quantity = order_quantity
        self.fill_timeout = fill_timeout
        self.lighter_order_filled = False
        self.iterations = iterations
        self.variational_position = Decimal('0')
        self.lighter_position = Decimal('0')
        self.current_order = {}
        self.oi_interval_seconds = oi_interval_seconds

        # Initialize logging to file
        os.makedirs("logs", exist_ok=True)
        self.log_filename = f"logs/variational_{ticker}_hedge_mode_log.txt"
        self.csv_filename = f"logs/variational_{ticker}_hedge_mode_trades.csv"
        self.original_stdout = sys.stdout

        # Initialize CSV file with headers if it doesn't exist
        self._initialize_csv_file()

        # Setup logger
        self.logger = logging.getLogger(f"hedge_bot_{ticker}")
        self.logger.setLevel(logging.INFO)

        # Clear any existing handlers to avoid duplicates
        self.logger.handlers.clear()

        # Disable verbose logging from external libraries
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('requests').setLevel(logging.WARNING)
        logging.getLogger('websockets').setLevel(logging.WARNING)

        # Create file handler
        file_handler = logging.FileHandler(self.log_filename)
        file_handler.setLevel(logging.INFO)

        # Create console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)

        # Create different formatters for file and console
        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')

        file_handler.setFormatter(file_formatter)
        console_handler.setFormatter(console_formatter)

        # Add handlers to logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

        # Prevent propagation to root logger to avoid duplicate messages
        self.logger.propagate = False

        # State management
        self.stop_flag = False
        self.order_counter = 0

        # Variational state
        self.variational_client = None
        self.variational_contract_id = None
        self.variational_tick_size = None
        self.variational_order_status = None
        # self.variational_position_size = Decimal('0')
        self.position_is_full = False

        # Variational order book state for websocket-based BBO
        self.variational_order_book = {'bids': {}, 'asks': {}}
        self.variational_best_bid = None
        self.variational_best_ask = None
        self.variational_order_book_ready = False

        # Lighter order book state
        self.lighter_client = None
        self.lighter_order_book = {"bids": {}, "asks": {}}
        self.lighter_best_bid = None
        self.lighter_best_ask = None
        self.lighter_order_book_ready = False
        self.lighter_order_book_offset = 0
        self.lighter_order_book_sequence_gap = False
        self.lighter_snapshot_loaded = False
        self.lighter_order_book_lock = asyncio.Lock()

        # Lighter WebSocket state
        self.lighter_ws_task = None
        self.lighter_order_result = None

        # Lighter order management
        self.lighter_order_status = None
        self.lighter_order_price = None
        self.lighter_order_side = None
        self.lighter_order_size = None
        self.lighter_order_start_time = None

        # Strategy state
        self.waiting_for_lighter_fill = False
        self.wait_start_time = None
        self.oi_waiting = False

        # Order execution tracking
        self.order_execution_complete = False

        # Current order details for immediate execution
        self.current_lighter_side = None
        self.current_lighter_quantity = None
        # self.current_lighter_price = None
        # self.lighter_order_info = None

        # Lighter API configuration
        self.lighter_base_url = "https://mainnet.zklighter.elliot.ai"
        self.account_index = int(os.getenv('LIGHTER_ACCOUNT_INDEX'))
        self.api_key_index = int(os.getenv('LIGHTER_API_KEY_INDEX'))

        # Variational configuration
        self.var_private_key = os.getenv('VAR_PRIVATE_KEY')
        self.var_wallet_address = os.getenv('VAR_WALLET_ADDRESS')

    def shutdown(self, signum=None, frame=None):
        """Graceful shutdown handler."""
        self.stop_flag = True
        self.logger.info("\n🛑 Stopping...")

        # Close WebSocket connections
        if self.variational_client:
            try:
                # Note: disconnect() is async, but shutdown() is sync
                # We'll let the cleanup happen naturally
                self.logger.info("🔌 Variational WebSocket will be disconnected")
            except Exception as e:
                self.logger.error(f"Error disconnecting Variational WebSocket: {e}")

        # Cancel Lighter WebSocket task
        if self.lighter_ws_task and not self.lighter_ws_task.done():
            try:
                self.lighter_ws_task.cancel()
                self.logger.info("🔌 Lighter WebSocket task cancelled")
            except Exception as e:
                self.logger.error(f"Error cancelling Lighter WebSocket task: {e}")

        # Close logging handlers properly
        for handler in self.logger.handlers[:]:
            try:
                handler.close()
                self.logger.removeHandler(handler)
            except Exception:
                pass

    def _initialize_csv_file(self):
        """Initialize CSV file with headers if it doesn't exist."""
        if not os.path.exists(self.csv_filename):
            with open(self.csv_filename, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['exchange', 'timestamp', 'side', 'price', 'quantity'])

    def log_trade_to_csv(self, exchange: str, side: str, price: str, quantity: str):
        """Log trade details to CSV file."""
        timestamp = datetime.now(pytz.UTC).isoformat()

        with open(self.csv_filename, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([
                exchange,
                timestamp,
                side,
                price,
                quantity
            ])

        self.logger.info(f"📊 Trade logged to CSV: {exchange} {side} {quantity} @ {price}")

    async def notify(self, message: str, level: str = "INFO"):
        """发送 Lark 通知"""
        if not os.getenv("LARK_TOKEN"):
            return
        
        try:
            emoji = {"INFO": "ℹ️", "ERROR": "❌", "SUCCESS": "✅", "WARNING": "⚠️"}.get(level, "📢")
            async with LarkBot(os.getenv("LARK_TOKEN")) as lark:
                await lark.send_text(f"{emoji} {message}")
        except:
            pass  # 通知失败不影响主程序

    def handle_lighter_order_result(self, order_data):
        """Handle Lighter order result from WebSocket."""
        try:
            order_data["avg_filled_price"] = (Decimal(order_data["filled_quote_amount"]) /
                                              Decimal(order_data["filled_base_amount"]))
            if order_data["is_ask"]:
                order_data["side"] = "SHORT"
                self.lighter_position -= Decimal(order_data["filled_base_amount"])
            else:
                order_data["side"] = "LONG"
                self.lighter_position += Decimal(order_data["filled_base_amount"])

            self.logger.info(f"📊 Lighter order filled: {order_data['side']} "
                             f"{order_data['filled_base_amount']} @ {order_data['avg_filled_price']}")

            # Log Lighter trade to CSV
            self.log_trade_to_csv(
                exchange='Lighter',
                side=order_data['side'],
                price=str(order_data['avg_filled_price']),
                quantity=str(order_data['filled_base_amount'])
            )

            # Mark execution as complete
            self.lighter_order_filled = True  # Mark order as filled
            self.order_execution_complete = True

        except Exception as e:
            self.logger.error(f"Error handling Lighter order result: {e}")

    async def reset_lighter_order_book(self):
        """Reset Lighter order book state."""
        async with self.lighter_order_book_lock:
            self.lighter_order_book["bids"].clear()
            self.lighter_order_book["asks"].clear()
            self.lighter_order_book_offset = 0
            self.lighter_order_book_sequence_gap = False
            self.lighter_snapshot_loaded = False
            self.lighter_best_bid = None
            self.lighter_best_ask = None

    def update_lighter_order_book(self, side: str, levels: list):
        """Update Lighter order book with new levels."""
        for level in levels:
            # Handle different data structures - could be list [price, size] or dict {"price": ..., "size": ...}
            if isinstance(level, list) and len(level) >= 2:
                price = Decimal(level[0])
                size = Decimal(level[1])
            elif isinstance(level, dict):
                price = Decimal(level.get("price", 0))
                size = Decimal(level.get("size", 0))
            else:
                self.logger.warning(f"⚠️ Unexpected level format: {level}")
                continue

            if size > 0:
                self.lighter_order_book[side][price] = size
            else:
                # Remove zero size orders
                self.lighter_order_book[side].pop(price, None)

    def validate_order_book_offset(self, new_offset: int) -> bool:
        """Validate order book offset sequence."""
        if new_offset <= self.lighter_order_book_offset:
            self.logger.warning(
                f"⚠️ Out-of-order update: new_offset={new_offset}, current_offset={self.lighter_order_book_offset}")
            return False
        return True

    def validate_order_book_integrity(self) -> bool:
        """Validate order book integrity."""
        # Check for negative prices or sizes
        for side in ["bids", "asks"]:
            for price, size in self.lighter_order_book[side].items():
                if price <= 0 or size <= 0:
                    self.logger.error(f"❌ Invalid order book data: {side} price={price}, size={size}")
                    return False
        return True

    def get_lighter_best_levels(self) -> Tuple[Tuple[Decimal, Decimal], Tuple[Decimal, Decimal]]:
        """Get best bid and ask levels from Lighter order book."""
        best_bid = None
        best_ask = None

        if self.lighter_order_book["bids"]:
            best_bid_price = max(self.lighter_order_book["bids"].keys())
            best_bid_size = self.lighter_order_book["bids"][best_bid_price]
            best_bid = (best_bid_price, best_bid_size)

        if self.lighter_order_book["asks"]:
            best_ask_price = min(self.lighter_order_book["asks"].keys())
            best_ask_size = self.lighter_order_book["asks"][best_ask_price]
            best_ask = (best_ask_price, best_ask_size)

        return best_bid, best_ask

    def get_lighter_mid_price(self) -> Decimal:
        """Get mid price from Lighter order book."""
        best_bid, best_ask = self.get_lighter_best_levels()

        if best_bid is None or best_ask is None:
            raise Exception("Cannot calculate mid price - missing order book data")

        mid_price = (best_bid[0] + best_ask[0]) / Decimal('2')
        return mid_price

    def get_lighter_order_price(self, is_ask: bool) -> Decimal:
        """Get order price from Lighter order book."""
        best_bid, best_ask = self.get_lighter_best_levels()

        if best_bid is None or best_ask is None:
            raise Exception("Cannot calculate order price - missing order book data")

        if is_ask:
            order_price = best_bid[0] + Decimal('0.1')
        else:
            order_price = best_ask[0] - Decimal('0.1')

        return order_price

    def calculate_adjusted_price(self, original_price: Decimal, side: str, adjustment_percent: Decimal) -> Decimal:
        """Calculate adjusted price for order modification."""
        adjustment = original_price * adjustment_percent

        if side.lower() == 'buy':
            # For buy orders, increase price to improve fill probability
            return original_price + adjustment
        else:
            # For sell orders, decrease price to improve fill probability
            return original_price - adjustment

    async def request_fresh_snapshot(self, ws):
        """Request fresh order book snapshot."""
        await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{self.lighter_market_index}"}))

    async def handle_lighter_ws(self):
        """Handle Lighter WebSocket connection and messages."""
        url = "wss://mainnet.zklighter.elliot.ai/stream"
        cleanup_counter = 0

        while not self.stop_flag:
            timeout_count = 0
            try:
                # Reset order book state before connecting
                await self.reset_lighter_order_book()

                async with websockets.connect(url) as ws:
                    # Subscribe to order book updates
                    await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{self.lighter_market_index}"}))

                    # Subscribe to account orders updates
                    account_orders_channel = f"account_orders/{self.lighter_market_index}/{self.account_index}"

                    # Get auth token for the subscription
                    try:
                        # Set auth token to expire in 10 minutes
                        ten_minutes_deadline = int(time.time() + 10 * 60)
                        auth_token, err = self.lighter_client.create_auth_token_with_expiry(ten_minutes_deadline)
                        if err is not None:
                            self.logger.warning(f"⚠️ Failed to create auth token for account orders subscription: {err}")
                        else:
                            auth_message = {
                                "type": "subscribe",
                                "channel": account_orders_channel,
                                "auth": auth_token
                            }
                            await ws.send(json.dumps(auth_message))
                            self.logger.info("✅ Subscribed to account orders with auth token (expires in 10 minutes)")
                    except Exception as e:
                        self.logger.warning(f"⚠️ Error creating auth token for account orders subscription: {e}")
                        await self.notify(f"⚠️ Error creating auth token for account orders subscription: {e}", level="ERROR")
                        raise Exception(f"Error creating auth token for account orders subscription: {e}")

                    while not self.stop_flag:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=1)

                            try:
                                data = json.loads(msg)
                            except json.JSONDecodeError as e:
                                self.logger.warning(f"⚠️ JSON parsing error in Lighter websocket: {e}")
                                continue

                            # Reset timeout counter on successful message
                            timeout_count = 0

                            async with self.lighter_order_book_lock:
                                if data.get("type") == "subscribed/order_book":
                                    # Initial snapshot - clear and populate the order book
                                    self.lighter_order_book["bids"].clear()
                                    self.lighter_order_book["asks"].clear()

                                    # Handle the initial snapshot
                                    order_book = data.get("order_book", {})
                                    if order_book and "offset" in order_book:
                                        self.lighter_order_book_offset = order_book["offset"]
                                        self.logger.info(f"✅ Initial order book offset set to: {self.lighter_order_book_offset}")

                                    # Debug: Log the structure of bids and asks
                                    bids = order_book.get("bids", [])
                                    asks = order_book.get("asks", [])
                                    if bids:
                                        self.logger.debug(f"📊 Sample bid structure: {bids[0] if bids else 'None'}")
                                    if asks:
                                        self.logger.debug(f"📊 Sample ask structure: {asks[0] if asks else 'None'}")

                                    self.update_lighter_order_book("bids", bids)
                                    self.update_lighter_order_book("asks", asks)
                                    self.lighter_snapshot_loaded = True
                                    self.lighter_order_book_ready = True

                                    self.logger.info(f"✅ Lighter order book snapshot loaded with "
                                                     f"{len(self.lighter_order_book['bids'])} bids and "
                                                     f"{len(self.lighter_order_book['asks'])} asks")

                                elif data.get("type") == "update/order_book" and self.lighter_snapshot_loaded:
                                    # Extract offset from the message
                                    order_book = data.get("order_book", {})
                                    if not order_book or "offset" not in order_book:
                                        self.logger.warning("⚠️ Order book update missing offset, skipping")
                                        continue

                                    new_offset = order_book["offset"]

                                    # Validate offset sequence
                                    if not self.validate_order_book_offset(new_offset):
                                        self.lighter_order_book_sequence_gap = True
                                        break

                                    # Update the order book with new data
                                    self.update_lighter_order_book("bids", order_book.get("bids", []))
                                    self.update_lighter_order_book("asks", order_book.get("asks", []))

                                    # Validate order book integrity after update
                                    if not self.validate_order_book_integrity():
                                        self.logger.warning("🔄 Order book integrity check failed, requesting fresh snapshot...")
                                        break

                                    # Get the best bid and ask levels
                                    best_bid, best_ask = self.get_lighter_best_levels()

                                    # Update global variables
                                    if best_bid is not None:
                                        self.lighter_best_bid = best_bid[0]
                                    if best_ask is not None:
                                        self.lighter_best_ask = best_ask[0]

                                elif data.get("type") == "ping":
                                    # Respond to ping with pong
                                    await ws.send(json.dumps({"type": "pong"}))
                                elif data.get("type") == "update/account_orders":
                                    # Handle account orders updates
                                    orders = data.get("orders", {}).get(str(self.lighter_market_index), [])
                                    if len(orders) == 1:
                                        order_data = orders[0]
                                        if order_data.get("status") == "filled":
                                            self.handle_lighter_order_result(order_data)
                                elif data.get("type") == "update/order_book" and not self.lighter_snapshot_loaded:
                                    # Ignore updates until we have the initial snapshot
                                    continue

                            # Periodic cleanup outside the lock
                            cleanup_counter += 1
                            if cleanup_counter >= 1000:
                                cleanup_counter = 0

                            # Handle sequence gap and integrity issues outside the lock
                            if self.lighter_order_book_sequence_gap:
                                try:
                                    await self.request_fresh_snapshot(ws)
                                    self.lighter_order_book_sequence_gap = False
                                except Exception as e:
                                    self.logger.error(f"⚠️ Failed to request fresh snapshot: {e}")
                                    break

                        except asyncio.TimeoutError:
                            timeout_count += 1
                            if timeout_count % 3 == 0:
                                self.logger.warning(f"⏰ No message from Lighter websocket for {timeout_count} seconds")
                            continue
                        except websockets.exceptions.ConnectionClosed as e:
                            self.logger.warning(f"⚠️ Lighter websocket connection closed: {e}")
                            break
                        except websockets.exceptions.WebSocketException as e:
                            self.logger.warning(f"⚠️ Lighter websocket error: {e}")
                            break
                        except Exception as e:
                            self.logger.error(f"⚠️ Error in Lighter websocket: {e}")
                            self.logger.error(f"⚠️ Full traceback: {traceback.format_exc()}")
                            break
            except Exception as e:
                self.logger.error(f"⚠️ Failed to connect to Lighter websocket: {e}")

            # Wait a bit before reconnecting
            await asyncio.sleep(2)

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def initialize_lighter_client(self):
        """Initialize the Lighter client."""
        if self.lighter_client is None:
            api_key_private_key = os.getenv('API_KEY_PRIVATE_KEY')
            if not api_key_private_key:
                raise Exception("API_KEY_PRIVATE_KEY environment variable not set")

            self.lighter_client = SignerClient(
                url=self.lighter_base_url,
                private_key=api_key_private_key,
                account_index=self.account_index,
                api_key_index=self.api_key_index,
            )

            # Check client
            err = self.lighter_client.check_client()
            if err is not None:
                # await self.notify(f"❌ Lighter client initialization failed: {err}", level="ERROR")
                raise Exception(f"CheckClient error: {err}")

            self.logger.info("✅ Lighter client initialized successfully")
        return self.lighter_client

    def initialize_variational_client(self):
        """Initialize the Variational client."""
        if not all([self.var_private_key, self.var_wallet_address]):
            raise ValueError("VAR_PRIVATE_KEY and VAR_WALLET_ADDRESS must be set in environment variables")

        # Create config for Variational client
        config_dict = {
            'ticker': self.ticker,
            'contract_id': '',  # Will be set when we get contract info
            'quantity': self.order_quantity,
            'tick_size': Decimal('0.01'),  # Will be updated when we get contract info
            'close_order_side': 'sell'  # Default, will be updated based on strategy
        }

        # Wrap in Config class for Variational client
        config = Config(config_dict)

        # Initialize Variational client
        self.variational_client = VariationalClient(config)

        self.logger.info("✅ Variational client initialized successfully")
        return self.variational_client

    def get_lighter_market_config(self) -> Tuple[int, int, int]:
        """Get Lighter market configuration."""
        url = f"{self.lighter_base_url}/api/v1/orderBooks"
        headers = {"accept": "application/json"}

        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()

            if not response.text.strip():
                raise Exception("Empty response from Lighter API")

            data = response.json()

            if "order_books" not in data:
                raise Exception("Unexpected response format")

            for market in data["order_books"]:
                if market["symbol"] == self.ticker:
                    return (market["market_id"],
                            pow(10, market["supported_size_decimals"]),
                            pow(10, market["supported_price_decimals"]))

            raise Exception(f"Ticker {self.ticker} not found")

        except Exception as e:
            self.logger.error(f"⚠️ Error getting market config: {e}")
            raise
    
    @retry(
        stop=stop_after_attempt(10),
        wait=wait_fixed(3),
        retry=retry_if_exception_type(Exception),
        reraise=True)
    async def get_lighter_positions(self):
        self.logger.info("Fetching Lighter positions...")
        account_index = os.getenv('LIGHTER_ACCOUNT_INDEX')
        if not account_index: 
            raise Exception("LIGHTER_ACCOUNT_INDEX environment variable not set")
        url = f"{self.lighter_base_url}/api/v1/account?by=index&value={account_index}"
        headers = {"accept": "application/json"}

        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()

            if not response.text.strip():
                raise Exception("Empty response from Lighter API")

            data = response.json()
            # self.logger.info(f"📊 Lighter positions data: {data}")

            if "accounts" not in data or not data["accounts"]:
              raise Exception("No accounts found in response")
            
            account = data["accounts"][0]  # 取第一个账户
            positions = account.get("positions", [])
        
            self.logger.info(f"📊 Lighter market_index: {self.lighter_market_index}")

            # 🟢 找到对应市场的仓位
            for pos in positions:
                if pos["market_id"] == self.lighter_market_index:
                # 🟢 正确解析仓位数据
                    position_size = Decimal(pos.get("position", "0"))
                    sign = pos.get("sign", 1)  # 1=多头, -1=空头
                    
                    # 🟢 计算净仓位（考虑方向）
                    net_position = position_size * sign
                    
                    self.logger.info(f"📊 Found position - Size: {position_size}, Sign: {sign}, Net: {net_position}")
                    return net_position
                
            # 如果没找到对应市场，返回 0
            self.logger.info(f"📊 No position found for market_id {self.lighter_market_index}")
            return Decimal('0')
        except Exception as e:
            self.logger.error(f"⚠️ Error getting lighter positions: {e}")
            raise

    async def get_variational_contract_info(self) -> Tuple[str, Decimal]:
        """Get Variational contract ID and tick size."""
        if not self.variational_client:
            raise Exception("Variational client not initialized")

        contract_id, tick_size = await self.variational_client.get_contract_attributes()

        if self.order_quantity < self.variational_client.config.quantity:
            raise ValueError(
                f"Order quantity is less than min quantity: {self.order_quantity} < {self.variational_client.config.quantity}")

        return contract_id, tick_size

    # async def fetch_variational_bbo_prices(self, quantity) -> Tuple[Decimal, Decimal]:
    #     """Fetch best bid/ask prices from Variational using websocket data."""
    #     # Use WebSocket data if available
    #     # webscoket bbo暂不可用，暂隐藏
    #     # if self.variational_order_book_ready and self.variational_best_bid and self.variational_best_ask:
    #     #     if self.variational_best_bid > 0 and self.variational_best_ask > 0 and self.variational_best_bid < self.variational_best_ask:
    #     #         return self.variational_best_bid, self.variational_best_ask

    #     # # Fallback to REST API if websocket data is not available
    #     # self.logger.warning("WebSocket BBO data not available, falling back to REST API")
    #     if not self.variational_client:
    #         raise Exception("Variational client not initialized")

    #     best_bid, best_ask = await self.variational_client.fetch_bbo_prices(self.variational_contract_id, quantity)

    #     return best_bid, best_ask

    def round_to_tick(self, price: Decimal) -> Decimal:
        """Round price to tick size."""
        if self.variational_tick_size is None:
            return price
        return (price / self.variational_tick_size).quantize(Decimal('1')) * self.variational_tick_size

    async def place_bbo_order(self, side: str, quantity: Decimal):
        # Get best bid/ask prices
        # best_bid, best_ask = await self.fetch_variational_bbo_prices(quantity)

        # Place the order using Variational client
        order_result = await self.variational_client.place_open_order(
            contract_id=self.variational_contract_id,
            quantity=quantity,
            direction=side.lower()
        )

        self.logger.info(f"【place_bbo_order】order_result: {order_result}")
        if order_result.success:
            self.variational_order_status = 'OPEN'
            return order_result.order_id, order_result.price
        else:
            self.variational_order_status = 'FAILED'
            # 更新订单状态
            self.logger.error(f"❌ Failed to place Variational order: {order_result.error_message}")
            raise Exception(f"Failed to place order: {order_result.error_message}")

    async def place_variational_post_only_order(self, side: str, quantity: Decimal):
        """Place a post-only order on Variational."""
        if not self.variational_client:
            raise Exception("Variational client not initialized")

        self.variational_order_status = None
        self.logger.info(f"[OPEN] [Variational] [{side}] Placing Variational POST-ONLY order")
        order_id, order_price = await self.place_bbo_order(side, quantity)
        self.logger.info(f"Placed Variational order {order_id} at price {order_price}")
        start_time = time.time()
        last_cancel_time = 0
         
        while not self.stop_flag:
            self.logger.info(f"Monitoring Variational order {order_id} | Status: {self.variational_order_status}")
            if self.variational_order_status in ['CANCELED', 'CANCELLED', 'FAILED']:
                self.logger.info(f"Order {order_id} was canceled or failed, placing new order")
                self.variational_order_status = None  # Reset to None to trigger new order
                order_id, order_price = await self.place_bbo_order(side, quantity)
                self.logger.info(f"Placed new Variational order {order_id} at price {order_price}")
                start_time = time.time()
                last_cancel_time = 0  # Reset cancel timer
                await asyncio.sleep(0.5)
            elif self.variational_order_status in ['OPEN', 'PARTIALLY_FILLED']:
                current_time = time.time()
                await asyncio.sleep(0.5)                
                if current_time - start_time > 10:
                    if current_time - last_cancel_time > 5:  # Prevent rapid cancellations
                        try:
                            self.variational_best_bid, self.variational_best_ask = await self.variational_client.fetch_bbo_prices(self.variational_contract_id, quantity)
                            # Check if we need to cancel and replace the order
                            should_cancel = False
                            if side == 'buy':
                                if order_price < self.variational_best_bid:
                                    should_cancel = True
                            else:
                                if order_price > self.variational_best_ask:
                                    should_cancel = True
                            if should_cancel is False:
                                continue
                            self.logger.info(f"Canceling order {order_id} due to timeout/price mismatch")
                            cancel_result = await self.variational_client.cancel_order(order_id)
                            self.logger.info(f"cancel_result: {cancel_result}")
                            if cancel_result.success:
                                last_cancel_time = current_time
                                self.variational_order_status = 'CANCELED'
                                # Don't reset start_time here, let the cancellation trigger new order
                            else:
                                self.logger.error(f"❌ Error canceling Variational order: {cancel_result.error_message}")
                        except Exception as e:
                            self.logger.error(f"❌ Error canceling Variational order: {e}")
                    elif not should_cancel:
                        self.logger.info(f"Waiting for Variational order to be filled (order price is at best bid/ask)")
            elif self.variational_order_status == 'FILLED':
                self.logger.info(f"Order {order_id} filled successfully")
                break
            else:
                if self.variational_order_status is not None:
                    self.logger.error(f"❌ Unknown Variational order status: {self.variational_order_status}")
                    break
                else:
                    await asyncio.sleep(0.5)

    # 设置lighter的下单状态   
    def handle_variational_order_update(self, order_data):
        """Handle Variational order updates from WebSocket."""
        side = order_data.get('side', '').lower()
        filled_size = Decimal(order_data.get('filled_size', '0'))

        if side == 'buy':
            lighter_side = 'sell'
        else:
            lighter_side = 'buy'

        # Store order details for immediate execution
        self.current_lighter_side = lighter_side
        self.current_lighter_quantity = filled_size
        self.waiting_for_lighter_fill = True


    async def place_lighter_market_order(self, lighter_side: str, quantity: Decimal):
        """Place a limit order on Lighter with cancel and replace mechanism (similar to Variational)."""
        if not self.lighter_client:
            self.initialize_lighter_client()

        self.lighter_order_filled = False
        self.logger.info(f"[OPEN] [Lighter] [{lighter_side}] Placing Lighter limit order with cancel/replace mechanism")
        
        # Place initial order
        order_id, order_price = await self._place_lighter_limit_order(lighter_side, quantity)
        if not order_id:
            self.logger.error("❌ Failed to place initial Lighter order")
            return None
            
        self.logger.info(f"Placed Lighter order {order_id} at price {order_price}")
        start_time = time.time()
        last_cancel_time = 0
        
        while not self.lighter_order_filled and not self.stop_flag:
            self.logger.info(f"Monitoring Lighter order {order_id} | Price: {order_price}")
            
            # Check if order needs to be cancelled and replaced
            await asyncio.sleep(0.5)
            
            # Get current market prices
            try:
                best_bid, best_ask = self.get_lighter_best_levels()
                if not best_bid or not best_ask:
                    await asyncio.sleep(1)
                    continue
                    
                current_best_bid = best_bid[0]
                current_best_ask = best_ask[0]
                
                # Check if our order price is still competitive
                should_cancel = False
                if lighter_side.lower() == 'buy':
                    # For buy orders, cancel if our price is below current best bid
                    if order_price < current_best_bid:
                        should_cancel = True
                else:
                    # For sell orders, cancel if our price is above current best ask
                    if order_price > current_best_ask:
                        should_cancel = True
                        
            except Exception as e:
                self.logger.warning(f"⚠️ Could not get Lighter BBO prices: {e}")
                should_cancel = False
            
            # Cancel and replace logic with 30-second timeout
            current_time = time.time()
            if current_time - start_time > 20:  # 30 seconds timeout for Lighter
                if should_cancel and current_time - last_cancel_time > 10:  # Prevent rapid cancellations
                    try:
                        self.logger.info(f"Canceling Lighter order {order_id} due to timeout/price mismatch")
                        
                        # Cancel the current order
                        cancel_success = await self._cancel_lighter_order(order_id)
                        
                        if cancel_success:
                            last_cancel_time = current_time
                            
                            # Place new order
                            self.logger.info(f"Order {order_id} canceled, placing new order")
                            
                            order_id, order_price = await self._place_lighter_limit_order(lighter_side, quantity)
                            if not order_id:
                                self.logger.error("❌ Failed to place replacement Lighter order")
                                break
                                
                            self.logger.info(f"Placed new Lighter order {order_id} at price {order_price}")
                            start_time = time.time()  # Reset timer for new order
                        else:
                            self.logger.error(f"❌ Failed to cancel Lighter order {order_id}")
                            
                    except Exception as e:
                        self.logger.error(f"❌ Error during Lighter order cancel/replace: {e}")
                elif not should_cancel:
                    self.logger.info(f"Waiting for Lighter order to be filled (order price is competitive)")
    
            # Check if order has been filled (this will be handled by WebSocket callback)
            if self.lighter_order_filled:
                self.logger.info(f"Lighter order {order_id} filled successfully")
                break
                
            await asyncio.sleep(1)

    async def _place_lighter_limit_order(self, lighter_side: str, quantity: Decimal) -> Tuple[str, Decimal]:
        """Place a single limit order on Lighter and return order ID and price."""
        try:
            best_bid, best_ask = self.get_lighter_best_levels()
            if not best_bid or not best_ask:
                raise Exception("Cannot get best bid/ask levels")

            # Determine order parameters
            if lighter_side.lower() == 'buy':
                is_ask = False
                # Place buy order slightly below best ask to improve fill probability
                price = best_ask[0] - Decimal('0.1')
            else:
                is_ask = True  
                # Place sell order slightly above best bid to improve fill probability  
                price = best_bid[0] + Decimal('0.1')
            
            self.logger.info(f"Placing Lighter limit order: {lighter_side} {quantity} @ {price} | is_ask: {is_ask}")

            # Reset order state
            self.lighter_order_price = price
            self.lighter_order_side = lighter_side
            self.lighter_order_size = quantity

            client_order_index = int(time.time() * 1000)
            
            # Sign the order transaction
            tx_info, error = self.lighter_client.sign_create_order(
                market_index=self.lighter_market_index,
                client_order_index=client_order_index,
                base_amount=int(quantity * self.base_amount_multiplier),
                price=int(price * self.price_multiplier),
                is_ask=is_ask,
                order_type=self.lighter_client.ORDER_TYPE_LIMIT,
                time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                reduce_only=False,
                trigger_price=0,
            )
            
            if error is not None:
                raise Exception(f"Sign error: {error}")

            # Send the transaction
            tx_hash = await self.lighter_client.send_tx(
                tx_type=self.lighter_client.TX_TYPE_CREATE_ORDER,
                tx_info=tx_info
            )
            
            self.logger.info(f"🚀 Lighter limit order sent: {lighter_side} {quantity} @ {price}")
            
            # Return order ID (using client_order_index as identifier) and price
            return str(client_order_index), price
            
        except Exception as e:
            self.logger.error(f"❌ Error placing Lighter limit order: {e}")
            raise Exception(f"Error placing Lighter limit order: {e}")
            # return None, None

    async def _cancel_lighter_order(self, order_id: str) -> bool:
        try:
            # Convert order_id to order_index
            order_index = int(order_id)
            
            # 方案1：使用高级方法（推荐）
            cancel_order_obj, tx_response, error = await self.lighter_client.cancel_order(
                market_index=self.lighter_market_index,
                order_index=order_index
            )
            
            if error is not None:
                self.logger.error(f"Cancel order error: {error}")
                return False
            
            if tx_response and hasattr(tx_response, 'tx_hash'):
                self.logger.info(f"🚫 Lighter cancel order successful, tx_hash: {tx_response.tx_hash}")
            else:
                self.logger.info(f"🚫 Lighter cancel order sent for order {order_id}")
            
            # Wait for blockchain confirmation
            await asyncio.sleep(2)
            return True
        
        except Exception as e:
            self.logger.error(f"❌ Error canceling Lighter order {order_id}: {e}")
            return False

    async def monitor_lighter_order(self, client_order_index: int):
        """Monitor Lighter order (now integrated into place_lighter_market_order)."""
        self.logger.info(f"🔍 Lighter order monitoring is now integrated into place_lighter_market_order")
        
        # The monitoring logic has been moved to place_lighter_market_order
        # This method can be kept for backward compatibility or removed
        pass

    async def setup_variational_websocket(self):
        """Setup Variational websocket for order updates and order book data."""
        if not self.variational_client:
            raise Exception("Variational client not initialized")

        def order_update_handler(positions):
            """Handle order updates from Variational WebSocket."""
            # self.logger.info(f"Variational order update received position_data")
            # self.logger.info(f"variational_contract_id: {self.variational_contract_id}")
            position_data = positions[0] if positions else {}

            # if position_data and position_data.get('instrument').get('underlying') != self.variational_contract_id:
            #     self.logger.info(f"Ignoring order update from {position_data.get('instrument').get('underlying')}")
            #     return
            try:
                # 初始状态下，仓位为0, 且position_is_full为False
                if positions == [] and self.position_is_full is False:
                    # self.logger.info("Variational 仓位为空，等待开仓或订单成交")
                    return
                # 平仓成功了
                if positions == [] and self.position_is_full is True:
                    # self.logger.info("Variational 平仓成功，仓位为空")
                    self.position_is_full = False
                    self.variational_position = Decimal('0')
                    self.variational_order_status = 'FILLED'
                    price = 'unknown'
                    # self.logger.info(f"📊 Variational order filled: {self.order_quantity} @ {price}")
                    self.logger.info(f"Variational 平仓成功, 当前仓位：{self.variational_position}")
                    self.log_trade_to_csv(
                        exchange='Variational',
                        side= 'SELL',
                        price=str(price),
                        quantity=str(self.order_quantity)
                    )

                    self.handle_variational_order_update({
                        'side': 'sell',
                        'contract_id': self.variational_contract_id,
                        'filled_size': str(self.order_quantity)
                    })
                    return
                # 有仓位
                if positions:
                    self.variational_position = Decimal(position_data.get('position_info', {"qty": "0"}).get('qty', '0'))
                    if self.oi_waiting is False:
                        self.logger.info(f"Variational 当前仓位： {self.variational_position}")
                        self.logger.info(f"lighter 当前仓位: {self.lighter_position}")
                    # 开仓中
                    if 0 < self.variational_position < self.order_quantity and self.position_is_full is False:
                        self.variational_order_status = 'PARTIALLY_FILLED'
                        self.logger.info(f"Variational 开仓中： {self.variational_position} / {self.order_quantity}")
                        return
                    # 开仓成功
                    if self.variational_position == self.order_quantity and self.position_is_full is False:
                        self.position_is_full = True
                        self.variational_order_status = 'FILLED'
                        price = Decimal(position_data.get('position_info', {"avg_entry_price": "0"}).get('avg_entry_price', '0'))
                        self.logger.info(f"📊 Variational order filled: {self.order_quantity} @ {price}")
                        # Log Variational trade to CSV
                        self.log_trade_to_csv(
                            exchange='Variational',
                            side= 'BUY',
                            price=str(price),
                            quantity=str(self.order_quantity)
                        )
                        
                        self.handle_variational_order_update({
                            'side': 'buy',
                            'filled_size': self.variational_position
                        })
                        self.logger.info("Variational 开仓成功")
                        return
                    
                    # 等待平仓中
                    if self.variational_position == self.order_quantity and self.position_is_full is True:
                        if self.lighter_order_filled:
                            if self.oi_waiting is False:
                                self.logger.info(f"Variational {self.variational_contract_id}建仓成功，lighter建仓成功，等待Variational平仓中")
                        else :
                            self.logger.info(f"Variational {self.variational_contract_id}建仓成功，等待lighter建仓中")
                        # self.logger.info(f"Variational {self.variational_contract_id} 等待平仓中： {self.order_quantity}")
                        return
                    if 0 < self.variational_position < self.order_quantity and self.position_is_full is True:
                        self.variational_order_status = 'PARTIALLY_FILLED'
                        self.logger.info(f"Variational {self.variational_contract_id} 平仓中： {self.variational_position} / {self.order_quantity}")
                        return

            except Exception as e:
                self.logger.error(f"Error handling Variational order update: {e}")

        try:
            # Setup order update handler
            self.variational_client.setup_order_update_handler(order_update_handler)
            self.logger.info("✅ Variational WebSocket order update handler set up")

            # Connect to Variational WebSocket
            await self.variational_client.connect()
            self.logger.info("✅ Variational WebSocket connection established")

        except Exception as e:
            self.logger.error(f"Could not setup Variational WebSocket handlers: {e}")

    async def trading_loop(self):
        """Main trading loop implementing the new strategy."""
        self.logger.info(f"🚀 Starting hedge bot for {self.ticker}")

        # Initialize clients
        try:
            self.initialize_lighter_client()
            self.initialize_variational_client()

            # # Get contract info
            self.variational_contract_id, self.variational_tick_size = await self.get_variational_contract_info()
            self.lighter_market_index, self.base_amount_multiplier, self.price_multiplier = self.get_lighter_market_config()

            self.logger.info(f"Contract info loaded - Variational: {self.variational_contract_id}, "
                             f"Lighter: {self.lighter_market_index}")

        except Exception as e:
            self.logger.error(f"❌ Failed to initialize: {e}")
            await self.notify(f"❌ Hedge bot initialization failed: {e}", level="ERROR")
            return

        # Setup Variational websocket
        try:
            await self.setup_variational_websocket()
            self.logger.info("✅ Variational WebSocket connection established")

            # Wait for initial order book data with timeout
            # self.logger.info("⏳ Waiting for initial order book data...")
            # timeout = 10  # seconds
            # start_time = time.time()
            # while not self.variational_order_book_ready and not self.stop_flag:
            #     if time.time() - start_time > timeout:
            #         self.logger.warning(f"⚠️ Timeout waiting for WebSocket order book data after {timeout}s")
            #         break
            #     await asyncio.sleep(0.5)

            # if self.variational_order_book_ready:
            #     self.logger.info("✅ WebSocket order book data received")
            # else:
            #     self.logger.warning("⚠️ WebSocket order book not ready, will use REST API fallback")

        except Exception as e:
            self.logger.error(f"❌ Failed to setup Variational websocket: {e}")
            await self.notify(f"❌ Hedge bot Variational websocket setup failed: {e}", level="ERROR")
            return

        # Setup Lighter websocket
        try:
            self.lighter_ws_task = asyncio.create_task(self.handle_lighter_ws())
            self.logger.info("✅ Lighter WebSocket task started")

            # Wait for initial Lighter order book data with timeout
            self.logger.info("⏳ Waiting for initial Lighter order book data...")
            timeout = 10  # seconds
            start_time = time.time()
            while not self.lighter_order_book_ready and not self.stop_flag:
                if time.time() - start_time > timeout:
                    self.logger.warning(f"⚠️ Timeout waiting for Lighter WebSocket order book data after {timeout}s")
                    break
                await asyncio.sleep(0.5)

            if self.lighter_order_book_ready:
                self.logger.info("✅ Lighter WebSocket order book data received")
            else:
                self.logger.warning("⚠️ Lighter WebSocket order book not ready")

        except Exception as e:
            self.logger.error(f"❌ Failed to setup Lighter websocket: {e}")
            await self.notify(f"❌ Hedge bot Lighter websocket setup failed: {e}", level="ERROR")
            return

        await asyncio.sleep(5)

        iterations = 0
        while iterations < self.iterations and not self.stop_flag:
            iterations += 1
            self.logger.info("-----------------------------------------------")
            self.logger.info(f"🔄 Trading loop iteration {iterations}")
            self.logger.info("-----------------------------------------------")

            self.logger.info(f"[STEP 1] Variational position: {self.variational_position} | Lighter position: {self.lighter_position}")

            if abs(self.variational_position + self.lighter_position) > self.order_quantity * 2:
                self.logger.error(f"❌ Position diff is too large: {self.variational_position + self.lighter_position}")
                self.stop_flag = True
                await self.notify(f"❌ Hedge bot stopped due to large position diff: {self.variational_position + self.lighter_position}")
                break

            self.order_execution_complete = False
            self.waiting_for_lighter_fill = False
            try:
                # Determine side based on some logic (for now, alternate)
                side = 'buy'
                await self.place_variational_post_only_order(side, self.order_quantity)
            except Exception as e:
                self.logger.error(f"⚠️ Error in trading loop: {e}")
                self.logger.error(f"⚠️ Full traceback: {traceback.format_exc()}")
                await self.notify(f"⚠️ Hedge bot trading loop error, variational buy failed: {e}", level="ERROR")
                break

            start_time = time.time()
            while not self.order_execution_complete and not self.stop_flag:
                # Check if Variational order filled and we need to place Lighter order
                if self.waiting_for_lighter_fill:
                    await self.place_lighter_market_order(
                        self.current_lighter_side,
                        self.current_lighter_quantity,
                        # self.current_lighter_price
                    )
                    break

                await asyncio.sleep(0.01)
                if time.time() - start_time > 180:
                    self.logger.error("❌ Timeout waiting for trade completion")
                    break

            if self.stop_flag:
                break
            self.oi_waiting = True
            self.logger.info(f"Waiting for {self.oi_interval_seconds} seconds before next step...")
            await asyncio.sleep(self.oi_interval_seconds)
            self.oi_waiting = False

            # Close position
            self.logger.info(f"[STEP 2] Variational position: {self.variational_position} | Lighter position: {self.lighter_position}")
            self.order_execution_complete = False
            self.waiting_for_lighter_fill = False
            try:
                # Determine side based on some logic (for now, alternate)
                side = 'sell'
                await self.place_variational_post_only_order(side, self.order_quantity)
            except Exception as e:
                self.logger.error(f"⚠️ Error in trading loop: {e}")
                self.logger.error(f"⚠️ Full traceback: {traceback.format_exc()}")
                await self.notify(f"⚠️ Hedge bot trading loop error, variational sell failed: {e}", level="ERROR")
                break

            while not self.order_execution_complete and not self.stop_flag:
                # Check if Variational order filled and we need to place Lighter order
                if self.waiting_for_lighter_fill:
                    await self.place_lighter_market_order(
                        self.current_lighter_side,
                        self.current_lighter_quantity,
                        # self.current_lighter_price
                    )
                    break

                await asyncio.sleep(0.01)
                if time.time() - start_time > 180:
                    self.logger.error("❌ Timeout waiting for trade completion")
                    break

            # Close remaining position
            self.lighter_position = await self.get_lighter_positions()
            self.logger.info(f"[STEP 3] Variational position: {self.variational_position} | Lighter position: {self.lighter_position}")
            self.order_execution_complete = False
            self.waiting_for_lighter_fill = False
            if self.variational_position == 0 and self.lighter_position == 0:
                continue
            if self.variational_position > 0 or self.variational_position < 0:
                side = 'sell' if self.variational_position > 0 else 'buy'
                self.logger.info(f"variational_position before closing: {self.variational_position}")
                try:
                    await self.place_variational_post_only_order(side, abs(self.variational_position))
                except Exception as e:
                    self.logger.error(f"⚠️ Error in trading loop: {e}")
                    self.logger.error(f"⚠️ Full traceback: {traceback.format_exc()}")
                    await self.notify(f"⚠️ Hedge bot trading loop error, variational close failed: {e}", level="ERROR")
                    break
            else:
                self.waiting_for_lighter_fill = True
                self.logger.info("No remaining Variational position to close")

            self.logger.info(f"lighter_position before closing: {self.lighter_position}")
            self.logger.info(f"self.order_execution_complete before closing: {self.order_execution_complete}")

            # Wait for order to be filled via WebSocket
            while not self.order_execution_complete and not self.stop_flag:
                # Check if Variational order filled and we need to place Lighter order
                if self.waiting_for_lighter_fill:
                    if self.lighter_position == 0:
                        break
                    if self.lighter_position > 0:
                        lighter_side = 'sell'
                    if self.lighter_position < 0:
                        lighter_side = 'buy'
                    await self.place_lighter_market_order(
                        lighter_side,
                        abs(self.lighter_position),
                        # self.current_lighter_price
                    )
                    break

                await asyncio.sleep(0.01)
                if time.time() - start_time > 180:
                    self.logger.error("❌ Timeout waiting for trade completion")
                    break
        await asyncio.sleep(20)
            
        self.logger.info("✅ Trading loop exited")
        await self.notify("✅ Hedge bot trading loop exited")
    
    async def run(self):
        """Run the hedge bot."""
        self.setup_signal_handlers()

        try:
            await self.trading_loop()
        except KeyboardInterrupt:
            self.logger.info("\n🛑 Received interrupt signal...")
        finally:
            self.logger.info("🔄 Cleaning up...")
            self.shutdown()