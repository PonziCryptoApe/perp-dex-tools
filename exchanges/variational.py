"""
Variational exchange client implementation.
"""

import os
import asyncio
import json
import websocket
import threading
import time
import traceback
from typing import Dict, Any, Optional, Tuple, List
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timezone, timedelta

import cloudscraper
import aiohttp
from eth_account import Account
from eth_account.messages import encode_defunct

from .base import BaseExchangeClient, OrderResult, OrderInfo
from helpers.logger import TradingLogger

from dotenv import load_dotenv

load_dotenv()


class VariationalClient(BaseExchangeClient):
    """Variational exchange client implementation."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize the Variational exchange client with configuration."""
        super().__init__(config)
        
        # Variational 特定的配置
        self.private_key = os.getenv('VAR_PRIVATE_KEY')
        self.wallet_address = os.getenv('VAR_WALLET_ADDRESS')
        
        # API endpoints
        self.api_base = "https://omni.variational.io/api"
        self.ws_prices_url = "wss://omni-ws-server.prod.ap-northeast-1.variational.io/prices"
        self.ws_portfolio_url = "wss://omni-ws-server.prod.ap-northeast-1.variational.io/portfolio"
        
        # Authentication
        self.auth_token = None
        # 为 Variational API 使用 cloudscraper
        self.scraper = cloudscraper.create_scraper()
        
        # 初始化日志
        self.logger = TradingLogger(
            exchange="variational", 
            ticker=self.config.ticker, 
            log_to_console=True
        )
        
        # WebSocket 相关
        self._stop_event = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self._prices_ws = None
        self._portfolio_ws = None
        self._ws_connected = threading.Event()
        
        # 订单簿和状态管理
        self.orderbook = None
        self.open_orders = {}
        self._order_update_handler = None
        
        # 交易参数
        self.instrument = {
            "underlying": self.config.ticker,
            "instrument_type": "perpetual_future",
            "settlement_asset": "USDC",
            "funding_interval_s": 3600
        }

        self.logger.log("VariationalClient initialized", "INFO")

    def _validate_config(self) -> None:
        """Validate the configuration for Variational exchange."""
        required_configs = ['ticker']
        for config_key in required_configs:
            if not hasattr(self.config, config_key) or getattr(self.config, config_key) is None:
                raise ValueError(f"Missing required config: {config_key}")
        
        # 验证环境变量
        required_env_vars = ['VAR_PRIVATE_KEY']
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing environment variables: {missing_vars}")

    async def _make_var_request(self, method: str, url: str, **kwargs) -> Dict[str, Any]:
        """使用 cloudscraper 发起 Variational API 请求"""
        loop = asyncio.get_event_loop()
        
        try:
            if method.upper() == 'POST':
                response = await loop.run_in_executor(
                    None, 
                    lambda: self.scraper.post(url, **kwargs)
                )
            elif method.upper() == 'GET':
                response = await loop.run_in_executor(
                    None, 
                    lambda: self.scraper.get(url, **kwargs)
                )
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            self.logger.log(f"Variational API request failed: {e}", "ERROR")
            raise

    async def connect(self) -> None:
        """Connect to Variational exchange."""
        self.logger.log("Connecting to Variational exchange...", "INFO")
        
        # 1. 认证登录
        await self._authenticate()
        
        # 2. 启动 WebSocket 连接
        self._stop_event.clear()
        self._start_websockets()
        
        # 3. 等待 WebSocket 连接建立
        await asyncio.sleep(2)
        
        self.logger.log("Connected to Variational exchange", "INFO")

    async def disconnect(self) -> None:
        """Disconnect from Variational exchange."""
        self.logger.log("Disconnecting from Variational exchange...", "INFO")
        
        try:
            # 1. 取消未完成的买单
            active_orders = await self.get_active_orders(self.config.contract_id)
            for order in active_orders:
                if order.side == "buy":
                    await self.cancel_order(order.order_id)
            
            # 2. 停止 WebSocket
            self._stop_event.set()
            if self._prices_ws:
                self._prices_ws.close()
            if self._portfolio_ws:
                self._portfolio_ws.close()
            
            # 3. 等待任务完成
            for task in self._tasks:
                if not task.done():
                    task.cancel()
            
            if self._tasks:
                await asyncio.gather(*self._tasks, return_exceptions=True)
            
            self._tasks.clear()
            
            # 4. 重置状态
            self.orderbook = None
            self._order_update_handler = None
            self.auth_token = None
            
            self.logger.log("Variational exchange disconnected successfully", "INFO")
            
        except Exception as e:
            self.logger.log(f"Error during Variational disconnect: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            raise

    async def _authenticate(self) -> None:
        """Authenticate with Variational exchange."""
        try:
            # 1. 获取签名数据
            signing_data = await self._fetch_signing_data()
            message = signing_data["message"]
            self.logger.log(f"获取签名数据: {message}", "INFO")
            
            # 2. 签名消息
            signature_hex = self._sign_message(message)
            
            # 3. 登录获取 token
            login_response = await self._login_with_signature(signature_hex)
            self.auth_token = login_response.get("token")
            
            if not self.auth_token:
                raise ValueError("Failed to get auth token")
                
            self.logger.log("Authentication successful", "INFO")
            
        except Exception as e:
            self.logger.log(f"Authentication failed: {e}", "ERROR")
            raise

    async def _fetch_signing_data(self) -> Dict[str, Any]:
        """使用 cloudscraper 获取签名数据"""
        url = f"{self.api_base}/auth/generate_signing_data"
        payload = {"address": self.wallet_address}
        
        return await self._make_var_request('POST', url, json=payload)

    def _sign_message(self, message: str) -> str:
        """Sign message with private key."""
        acct = Account.from_key(self.private_key)
        msg = encode_defunct(text=message)
        signed = acct.sign_message(msg)
        return signed.signature.hex()

    async def _login_with_signature(self, signed_message_hex: str) -> Dict[str, Any]:
        """Login with signed message."""
        url = f"{self.api_base}/auth/login"
        payload = {
            "address": self.wallet_address,
            "signed_message": signed_message_hex[2:] if signed_message_hex.startswith("0x") else signed_message_hex
        }
        
        return await self._make_var_request('POST', url, json=payload)

    def _start_websockets(self) -> None:
        """启动 WebSocket 连接"""
        # 启动价格 WebSocket
        # prices_thread = threading.Thread(target=self._run_prices_ws)
        # prices_thread.daemon = True
        # prices_thread.start()
        
        # 启动投资组合 WebSocket
        portfolio_thread = threading.Thread(target=self._run_portfolio_ws)
        portfolio_thread.daemon = True
        portfolio_thread.start()

    def _run_prices_ws(self) -> None:
        """运行价格 WebSocket"""
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        }
        
        self._prices_ws = websocket.WebSocketApp(
            self.ws_prices_url,
            header=headers,
            on_open=self._on_prices_open,
            on_message=self._on_prices_message,
            on_error=self._on_prices_error,
            on_close=self._on_prices_close
        )
        
        while not self._stop_event.is_set():
            try:
                self._prices_ws.run_forever()
            except Exception as e:
                self.logger.log(f"Prices WebSocket error: {e}", "ERROR")
                time.sleep(3)

    def _run_portfolio_ws(self) -> None:
        """运行投资组合 WebSocket"""
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        }
        self.logger.log("Starting Portfolio WebSocket...", "INFO")
        self._portfolio_ws = websocket.WebSocketApp(
            self.ws_portfolio_url,
            header=headers,
            on_open=self._on_portfolio_open,
            on_message=self._on_portfolio_message,
            on_error=self._on_portfolio_error,
            on_close=self._on_portfolio_close
        )
        
        while not self._stop_event.is_set():
            try:
                self._portfolio_ws.run_forever()
            except Exception as e:
                self.logger.log(f"Portfolio WebSocket error: {e}", "ERROR")
                time.sleep(3)

    def _on_prices_open(self, ws):
        """价格 WebSocket 连接建立"""
        self.logger.log("Prices WebSocket connected", "INFO")
        subscribe_msg = {
            "action": "subscribe",
            "instruments": [self.instrument]
        }
        ws.send(json.dumps(subscribe_msg))
        self._ws_connected.set()

    def _on_prices_message(self, ws, message):
        """处理价格 WebSocket 消息"""
        self.logger.log(f"Received prices message: {message[:200]}", "DEBUG")
        try:
            data = json.loads(message)
            # 更新订单簿数据
            if 'bid' in data and 'ask' in data:
                self.orderbook = {
                    'bids': [[data['bid'], data.get('bid_size', '0')]],
                    'asks': [[data['ask'], data.get('ask_size', '0')]],
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }
                self.logger.log(f"Orderbook updated: bid={data['bid']}, ask={data['ask']}", "DEBUG")
        except json.JSONDecodeError:
            self.logger.log(f"Failed to parse prices message: {message[:200]}", "WARNING")
        except Exception as e:
            self.logger.log(f"Error handling prices message: {e}", "ERROR")

    def _on_prices_error(self, ws, error):
        """价格 WebSocket 错误"""
        self.logger.log(f"Prices WebSocket error: {error}", "ERROR")

    def _on_prices_close(self, ws, close_status_code, close_msg):
        """价格 WebSocket 关闭"""
        self.logger.log(f"Prices WebSocket closed: {close_status_code} - {close_msg}", "INFO")

    def _on_portfolio_open(self, ws):
        """投资组合 WebSocket 连接建立"""
        self.logger.log("Portfolio WebSocket connected", "INFO")
        if self.auth_token:
            subscribe_msg = {
                "claims": self.auth_token
            }
            ws.send(json.dumps(subscribe_msg))

    def _on_portfolio_message(self, ws, message):
        """处理投资组合 WebSocket 消息"""
        try:
            data = json.loads(message)
            # 处理仓位更新
            if 'positions' in data:
                # for position_update in data['positions']:
                    if self._order_update_handler:
                        self._order_update_handler(data['positions'])
        except json.JSONDecodeError:
            self.logger.log(f"Failed to parse portfolio message: {message[:200]}", "WARNING")
        except Exception as e:
            self.logger.log(f"Error handling portfolio message: {e}", "ERROR")

    def _on_portfolio_error(self, ws, error):
        """投资组合 WebSocket 错误"""
        self.logger.log(f"Portfolio WebSocket error: {error}", "ERROR")

    def _on_portfolio_close(self, ws, close_status_code, close_msg):
        """投资组合 WebSocket 关闭"""
        self.logger.log(f"Portfolio WebSocket closed: {close_status_code} - {close_msg}", "INFO")

    async def fetch_bbo_prices(self, contract_id: str, quantity: Decimal = None) -> Tuple[Decimal, Decimal]:
        """从订单簿获取最佳买卖价格"""
        try:
            if not self.orderbook:
                # 获取指示性报价
                if quantity is None:
                    quantity = Decimal('0.00001')  # 默认小量
                indicative_data = await self._fetch_indicative_quote(quantity)
                qty_limits = indicative_data.get('qty_limits', {})

                if qty_limits:
                    min_qty = Decimal(str(qty_limits.get('ask').get('min_qty', '0.00001')))
                    self.config.tick_size = min_qty
                
                # self.logger.log(f"Fetched indicative quote for BBO prices: ", "INFO")
                if indicative_data and 'bid' in indicative_data and 'ask' in indicative_data:
                    return Decimal(str(indicative_data['bid'])), Decimal(str(indicative_data['ask']))
                return Decimal('0'), Decimal('0')
            
            best_bid = Decimal('0')
            best_ask = Decimal('0')
            
            if self.orderbook.get('bids') and len(self.orderbook['bids']) > 0:
                best_bid = Decimal(str(self.orderbook['bids'][0][0]))
            
            if self.orderbook.get('asks') and len(self.orderbook['asks']) > 0:
                best_ask = Decimal(str(self.orderbook['asks'][0][0]))
            
            return best_bid, best_ask
            
        except Exception as e:
            self.logger.log(f"Error fetching BBO prices: {e}", "ERROR")
            return Decimal('0'), Decimal('0')

    async def _fetch_indicative_quote(self, qty: Decimal) -> Dict[str, Any]:
        """获取指示性报价"""
        url = f"{self.api_base}/quotes/indicative"
        payload = {
            "instrument": self.instrument,
            "qty": str(qty)
        }
        
        # 设置 cookies
        cookies = {"vr-token": self.auth_token} if self.auth_token else {}
        self.logger.log(f"Fetching indicative quote with payload: {payload}", "INFO")
        self.logger.log(f"Using cookies: {cookies}", "INFO")
        try:
            return await self._make_var_request('POST', url, json=payload, cookies=cookies)
        except Exception as e:
            self.logger.log(f"Failed to fetch indicative quote: {e}", "ERROR")
            return {}

    async def place_open_order(self, contract_id: str, quantity: Decimal, direction: str) -> OrderResult:
        """下开仓订单"""
        try:
            best_bid, best_ask = await self.fetch_bbo_prices(contract_id, quantity)
            self.logger.log(f"Placing open order: direction={direction}, quantity={quantity}", "INFO")
            self.logger.log(f"Best Bid: {best_bid}, Best Ask: {best_ask}", "INFO")
            self.logger.log(f"Tick Size: {self.config.tick_size}", "INFO")
            if best_bid <= 0 or best_ask <= 0:
                return OrderResult(success=False, error_message='Invalid bid/ask prices')
            
            # 计算订单价格
            if direction == 'buy':
                order_price = best_ask - self.config.tick_size
            else:
                order_price = best_bid + self.config.tick_size
            
            # 下限价单
            result = await self._place_limit_order(direction, quantity, order_price)
            return result
            
        except Exception as e:
            return OrderResult(success=False, error_message=str(e))

    async def place_close_order(self, contract_id: str, quantity: Decimal, price: Decimal, side: str) -> OrderResult:
        """下平仓订单"""
        try:
            result = await self._place_limit_order(side, quantity, price)
            return result
        except Exception as e:
            return OrderResult(success=False, error_message=str(e))

    async def _place_limit_order(self, side: str, quantity: Decimal, price: Decimal) -> OrderResult:
        """使用 cloudscraper 下限价单"""
        url = f"{self.api_base}/orders/new/limit"
        payload = {
            "order_type": "limit",
            "limit_price": str(price),
            "side": side,
            "instrument": self.instrument,
            "qty": str(quantity),
            "is_auto_resize": False,
            "use_mark_price": False,
            "is_reduce_only": False
        }
        
        # 设置 cookies
        cookies = {"vr-token": self.auth_token} if self.auth_token else {}
        
        try:
            data = await self._make_var_request('POST', url, json=payload, cookies=cookies)
            self.logger.log(f"Placed limit order response: {data}", "INFO")
            order_id = data.get('rfq_id')
            if order_id:
                return OrderResult(
                    success=True,
                    order_id=order_id,
                    side=side,
                    size=quantity,
                    price=price,
                    status='OPEN'
                )
            else:
                return OrderResult(success=False, error_message=data.get('error', 'Unknown error'))
                
        except Exception as e:
            return OrderResult(success=False, error_message=str(e))

    async def cancel_order(self, order_id: str) -> OrderResult:
        """使用 cloudscraper 取消订单"""
        url = f"{self.api_base}/orders/cancel"
        payload = {"rfq_id": order_id}
        
        # 设置 cookies
        cookies = {"vr-token": self.auth_token} if self.auth_token else {}
        
        try:
            data = await self._make_var_request('POST', url, json=payload, cookies=cookies)
            
            if data is None:
                return OrderResult(success=True, order_id=order_id, status='CANCELED')

        except Exception as e:
            return OrderResult(success=False, error_message=str(e))

    async def get_order_info(self, order_id: str) -> Optional[OrderInfo]:
        """获取订单信息"""
        # Variational 可能需要不同的 API 端点来获取订单状态
        # 这里先返回一个基本实现
        try:
            # 从本地缓存获取订单信息
            if order_id in self.open_orders:
                order_data = self.open_orders[order_id]
                return OrderInfo(
                    order_id=order_id,
                    side=order_data.get('side', '').lower(),
                    size=Decimal(str(order_data.get('quantity', '0'))),
                    price=Decimal(str(order_data.get('price', '0'))),
                    status=order_data.get('status', 'UNKNOWN'),
                    filled_size=Decimal(str(order_data.get('filled_size', '0'))),
                    remaining_size=Decimal(str(order_data.get('remaining_size', '0')))
                )
            return None
        except Exception as e:
            self.logger.log(f"Error getting order info: {e}", "ERROR")
            return None

    async def get_active_orders(self, contract_id: str) -> List[OrderInfo]:
        """使用 cloudscraper 获取活跃订单"""
        try:
            url = f"{self.api_base}/positions"
            cookies = {"vr-token": self.auth_token} if self.auth_token else {}
            
            data = await self._make_var_request('GET', url, cookies=cookies)
            
            # 解析订单数据并转换为 OrderInfo 对象
            orders = []
            if 'orders' in data:
                for order_data in data['orders']:
                    if order_data.get('instrument', {}).get('underlying') == self.config.ticker:
                        orders.append(OrderInfo(
                            order_id=order_data.get('rfq_id', ''),
                            side=order_data.get('side', '').lower(),
                            size=Decimal(str(order_data.get('qty', '0'))),
                            price=Decimal(str(order_data.get('price', '0'))),
                            status=order_data.get('status', 'UNKNOWN'),
                            filled_size=Decimal(str(order_data.get('filled_qty', '0'))),
                            remaining_size=Decimal(str(order_data.get('remaining_qty', '0')))
                        ))
            return orders
            
        except Exception as e:
            self.logger.log(f"Error getting active orders: {e}", "ERROR")
            return []

    async def get_account_positions(self) -> Decimal:
        """使用 cloudscraper 获取账户持仓"""
        try:
            url = f"{self.api_base}/positions"
            cookies = {"vr-token": self.auth_token} if self.auth_token else {}
            
            data = await self._make_var_request('GET', url, cookies=cookies)
            
            # 解析持仓数据
            if 'positions' in data:
                for position in data['positions']:
                    instrument = position.get('instrument', {})
                    if instrument.get('underlying') == self.config.ticker:
                        return abs(Decimal(str(position.get('size', '0'))))
            return Decimal('0')
            
        except Exception as e:
            self.logger.log(f"Error getting account positions: {e}", "ERROR")
            return Decimal('0')

    def setup_order_update_handler(self, handler) -> None:
        """设置订单更新处理器"""
        self._order_update_handler = handler

    def get_exchange_name(self) -> str:
        """获取交易所名称"""
        return "variational"

    async def get_contract_attributes(self) -> Tuple[str, Decimal]:
        """获取合约属性"""
        ticker = self.config.ticker
        if not ticker:
            raise ValueError("Ticker is empty")
        
        # Variational 使用不同的合约标识
        self.config.contract_id = f"{ticker}-PERP"
        
        # 设置默认的 tick size（可能需要从 API 获取）
        self.config.tick_size = Decimal('0.01')  # 根据实际情况调整
        
        return self.config.contract_id, self.config.tick_size

    async def get_order_price(self, direction: str) -> Decimal:
        """获取订单价格"""
        best_bid, best_ask = await self.fetch_bbo_prices(self.config.contract_id)
        
        if best_bid <= 0 or best_ask <= 0:
            raise ValueError("Invalid bid/ask prices")
        
        if direction == 'buy':
            return best_ask - self.config.tick_size
        else:
            return best_bid + self.config.tick_size

    def round_to_tick(self, price: Decimal) -> Decimal:
        """将价格四舍五入到 tick size"""
        if not hasattr(self.config, 'tick_size') or self.config.tick_size <= 0:
            return price
        
        return (price / self.config.tick_size).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * self.config.tick_size