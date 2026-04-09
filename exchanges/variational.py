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
import copy
from typing import Dict, Any, Optional, Tuple, List
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timezone, timedelta
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

import cloudscraper
import aiohttp
from eth_account import Account
from eth_account.messages import encode_defunct

from helpers.lark_bot import LarkBot

from .base import BaseExchangeClient, OrderResult, OrderInfo
from helpers.logger import TradingLogger
from helpers.encryption_helper import EncryptionHelper  # ✅ 新增导入
import getpass
import sys

from dotenv import load_dotenv

load_dotenv()


class VariationalClient(BaseExchangeClient):
    """Variational exchange client implementation."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize the Variational exchange client with configuration."""
        super().__init__(config)
        
        # Variational 特定的配置
        self.private_key = self._load_private_key()
        self.wallet_address = os.getenv('VAR_WALLET_ADDRESS')
        
        # API endpoints
        self.api_base = "https://omni.variational.io/api"
        self.ws_prices_url = "wss://omni-ws-server.prod.ap-northeast-1.variational.io/prices"
        self.ws_portfolio_url = "wss://omni-ws-server.prod.ap-northeast-1.variational.io/portfolio"
        
        # Authentication
        self.auth_token = None
        self.cookies = None
        self._request_lock = asyncio.Lock()
        # 为 Variational API 使用 cloudscraper
        self.scraper = self._create_scraper()
        self._post_scraper = self._create_scraper()
        self.request_timing_log_threshold_ms = float(
            self._get_config_value('request_timing_log_threshold_ms', 40.0)
        )
        self.request_timing_always_log = bool(
            self._get_config_value('request_timing_always_log', False)
        )
        self._last_request_timing: Dict[str, Any] = {}
        
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
        # ✅ 添加连接状态标志
        self._portfolio_ws_connected = False
        self._portfolio_ws_lock = threading.Lock()
        self._portfolio_snapshot_lock = threading.Lock()
        self._portfolio_snapshot_raw = {}
        self._portfolio_snapshot_normalized = {}
        self._portfolio_snapshot_seen = False
        self._portfolio_snapshot_ts = 0.0
        
        # 订单簿和状态管理
        self.orderbook = None
        self.open_orders = {}
        self._order_update_handler = None

        self.logger.log("【VARIATIONAL】VariationalClient initialized", "INFO")

    def _get_config_value(self, key: str, default: Any) -> Any:
        """兼容对象/字典两种配置读取方式。"""
        if isinstance(self.config, dict):
            return self.config.get(key, default)
        return getattr(self.config, key, default)

    @staticmethod
    def _create_scraper():
        """创建新的 cloudscraper 实例。"""
        return cloudscraper.create_scraper(
            browser='chrome',                  # 模仿 Chrome
            enable_tls_fingerprinting=True,    # 启用 TLS 指纹伪装
            enable_tls_rotation=True,          # 启用 JA3/ cipher 旋转
            enable_enhanced_spoofing=True,     # 额外 spoofing
            enable_stealth=False,              # 关闭人类模拟（最大减延迟）
            compatibility_mode=True,
        )

    @staticmethod
    def _clone_request_kwargs(kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """复制请求参数，避免共享可变字典在 requests/cloudscraper 内部被并发修改。"""
        request_kwargs: Dict[str, Any] = {}
        for key, value in kwargs.items():
            if isinstance(value, dict):
                request_kwargs[key] = copy.deepcopy(value)
            elif isinstance(value, list):
                request_kwargs[key] = list(value)
            else:
                request_kwargs[key] = value
        return request_kwargs

    @staticmethod
    def _execute_scraper_request(scraper, method: str, url: str, request_kwargs: Dict[str, Any]):
        """同步执行单次 cloudscraper 请求。"""
        if method.upper() == 'POST':
            return scraper.post(url, **request_kwargs)
        if method.upper() == 'GET':
            return scraper.get(url, **request_kwargs)
        raise ValueError(f"Unsupported HTTP method: {method}")

    @classmethod
    def _execute_scraper_request_timed(
        cls,
        scraper,
        method: str,
        url: str,
        request_kwargs: Dict[str, Any]
    ):
        """同步执行单次请求，并返回 requests/cloudscraper 实际执行耗时。"""
        start = time.perf_counter()
        response = cls._execute_scraper_request(scraper, method, url, request_kwargs)
        elapsed_ms = (time.perf_counter() - start) * 1000
        return response, elapsed_ms

    def _log_request_timing(
        self,
        method: str,
        url: str,
        lock_wait_ms: float,
        executor_wall_ms: float,
        scraper_exec_ms: float,
        json_parse_ms: float,
        total_ms: float,
        retried: bool = False,
    ) -> None:
        """打印 Variational 请求路径耗时拆解日志。"""
        endpoint = url.replace(self.api_base, "")
        executor_overhead_ms = max(0.0, executor_wall_ms - scraper_exec_ms)
        self._last_request_timing = {
            'timestamp': time.time(),
            'method': method.upper(),
            'endpoint': endpoint,
            'lock_wait_ms': lock_wait_ms,
            'executor_wall_ms': executor_wall_ms,
            'scraper_exec_ms': scraper_exec_ms,
            'executor_overhead_ms': executor_overhead_ms,
            'json_parse_ms': json_parse_ms,
            'total_ms': total_ms,
            'retried': retried,
            'cookies_count': len(self.cookies) if self.cookies is not None else 0,
        }
        if not self.request_timing_always_log and total_ms < self.request_timing_log_threshold_ms:
            return

        level = "WARNING" if total_ms >= self.request_timing_log_threshold_ms else "INFO"
        retry_text = " | 重试请求" if retried else ""
        self.logger.log(
            "【VARIATIONAL】请求耗时拆解"
            f"{retry_text}: {method.upper()} {endpoint} | "
            f"锁等待 {lock_wait_ms:.2f}ms, "
            f"executor总耗时 {executor_wall_ms:.2f}ms, "
            f"scraper执行 {scraper_exec_ms:.2f}ms, "
            f"executor额外开销 {executor_overhead_ms:.2f}ms, "
            f"JSON解析 {json_parse_ms:.2f}ms, "
            f"总耗时 {total_ms:.2f}ms",
            level,
        )

    def get_request_diagnostics(self) -> Dict[str, Any]:
        """返回最近一次请求耗时拆解快照，供进程级诊断汇总。"""
        snapshot = dict(self._last_request_timing)
        if 'cookies_count' not in snapshot:
            snapshot['cookies_count'] = len(self.cookies) if self.cookies is not None else 0
        return snapshot
    def _load_private_key(self) -> str:
        """✅ 加载并解密私钥"""
        encrypted_key = os.getenv('VAR_PRIVATE_KEY_ENCRYPTED')
        if not encrypted_key:
            raise ValueError("Missing VAR_PRIVATE_KEY_ENCRYPTED environment variable")
        
        # 提示用户输入解密密钥
        # decryption_key = input("请输入 Variational 私钥的解密密钥: ")
        decryption_key = ""
        if sys.stdin.isatty():
            print("\n🔐 Variational 私钥已加密，需要解密密钥")
            decryption_key = getpass.getpass("请输入解密密钥: ")
        else:
            raise RuntimeError("无法从非交互式环境中输入解密密钥")
        # 使用 EncryptionHelper 解密私钥
        encryption_helper = EncryptionHelper()
        private_key = encryption_helper.decrypt(encrypted_key, decryption_key)

        return private_key
        
    def _build_instrument(self, ticker: str = None) -> Dict[str, Any]:
        """✅ 动态构建 instrument"""
        underlying = ticker or self.config.ticker
        return {
            "underlying": underlying,
            "instrument_type": "perpetual_future",
            "settlement_asset": "USDC",
            "funding_interval_s": 3600
        }
    def _validate_config(self) -> None:
        """Validate the configuration for Variational exchange."""
        required_configs = ['ticker']
        for config_key in required_configs:
            if not hasattr(self.config, config_key) or getattr(self.config, config_key) is None:
                raise ValueError(f"Missing required config: {config_key}")
        
        # 验证环境变量
        required_env_vars = ['VAR_PRIVATE_KEY_ENCRYPTED']
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing environment variables: {missing_vars}")
        
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
    
    async def _make_var_request(self, method: str, url: str, **kwargs) -> Dict[str, Any]:
        """使用 cloudscraper 发起 Variational API 请求"""
        loop = asyncio.get_event_loop()
        request_kwargs = self._clone_request_kwargs(kwargs)
        total_start = time.perf_counter()
        lock_wait_start = time.perf_counter()
        
        try:
            async with self._request_lock:
                lock_wait_ms = (time.perf_counter() - lock_wait_start) * 1000
                # POST 请求使用独立的专用 scraper，既隔离下单状态，又避免每次请求
                # 都重建 scraper 触发 cloudscraper 内部初始化噪声日志。
                request_scraper = self._post_scraper if method.upper() == 'POST' else self.scraper
                executor_start = time.perf_counter()
                response, scraper_exec_ms = await loop.run_in_executor(
                    None,
                    lambda: self._execute_scraper_request_timed(
                        request_scraper, method, url, request_kwargs
                    )
                )
                executor_wall_ms = (time.perf_counter() - executor_start) * 1000
            
            response.raise_for_status()
            json_start = time.perf_counter()
            response_json = response.json()
            json_parse_ms = (time.perf_counter() - json_start) * 1000
            total_ms = (time.perf_counter() - total_start) * 1000
            self._log_request_timing(
                method=method,
                url=url,
                lock_wait_ms=lock_wait_ms,
                executor_wall_ms=executor_wall_ms,
                scraper_exec_ms=scraper_exec_ms,
                json_parse_ms=json_parse_ms,
                total_ms=total_ms,
            )
            return response_json
            
        except Exception as e:
            if "dictionary changed size during iteration" in str(e):
                self.logger.log(
                    "【VARIATIONAL】检测到共享字典并发修改异常，重建 scraper 后重试一次",
                    "WARNING"
                )
                retry_total_start = time.perf_counter()
                retry_lock_wait_start = time.perf_counter()
                async with self._request_lock:
                    retry_lock_wait_ms = (time.perf_counter() - retry_lock_wait_start) * 1000
                    if method.upper() == 'POST':
                        self._post_scraper = self._create_scraper()
                        retry_scraper = self._post_scraper
                    else:
                        self.scraper = self._create_scraper()
                        retry_scraper = self.scraper
                    retry_kwargs = self._clone_request_kwargs(kwargs)
                    retry_executor_start = time.perf_counter()
                    response, retry_scraper_exec_ms = await loop.run_in_executor(
                        None,
                        lambda: self._execute_scraper_request_timed(
                            retry_scraper, method, url, retry_kwargs
                        )
                    )
                    retry_executor_wall_ms = (time.perf_counter() - retry_executor_start) * 1000
                response.raise_for_status()
                retry_json_start = time.perf_counter()
                response_json = response.json()
                retry_json_parse_ms = (time.perf_counter() - retry_json_start) * 1000
                retry_total_ms = (time.perf_counter() - retry_total_start) * 1000
                self._log_request_timing(
                    method=method,
                    url=url,
                    lock_wait_ms=retry_lock_wait_ms,
                    executor_wall_ms=retry_executor_wall_ms,
                    scraper_exec_ms=retry_scraper_exec_ms,
                    json_parse_ms=retry_json_parse_ms,
                    total_ms=retry_total_ms,
                    retried=True,
                )
                return response_json
            self.logger.log(f"【VARIATIONAL】Variational API request failed: {e}", "ERROR")
            raise

    async def connect(self) -> None:
        """Connect to Variational exchange."""
        self.logger.log("【VARIATIONAL】Connecting to Variational exchange...", "INFO")

        if self.auth_token:
            try:
                portfolio = await self.get_portfolio()  # 轻量检查
                if portfolio and 'balance' in portfolio:  # token 有效
                    self.logger.log("【VARIATIONAL】Token valid, skipping auth", "DEBUG")
                    # 只启动/重连 WS
                    if not self._portfolio_ws_connected:
                        self._start_websockets()
                    return
            except Exception as e:
                self.logger.log(f"【VARIATIONAL】Token invalid ({e}), re-authenticating", "WARNING")
                self.auth_token = None  # 强制重认证
        else:
            # 1. 认证登录
            await self._authenticate()
            
            # 2. 启动 WebSocket 连接
            self._stop_event.clear()
            self._start_websockets()
            
            # 3. 等待 WebSocket 连接建立
            await asyncio.sleep(2)

            self.logger.log("【VARIATIONAL】Connected to Variational exchange", "INFO")

    async def disconnect(self) -> None:
        """Disconnect from Variational exchange."""
        self.logger.log("【VARIATIONAL】Disconnecting from Variational exchange...", "INFO")

        try:
            # 1. 停止 WebSocket
            self._stop_event.set()
            
            # ✅ 关闭 WebSocket 连接
            if self._prices_ws:
                try:
                    self._prices_ws.close()
                except:
                    pass
            
            if self._portfolio_ws:
                try:
                    self._portfolio_ws.close()
                except:
                    pass
            
            # ✅ 等待 WebSocket 线程退出，避免在事件循环收尾时残留底层 transport
            await asyncio.sleep(2)
            
            # ✅ 重置状态
            with self._portfolio_ws_lock:
                self._portfolio_ws_connected = False
            
            self._portfolio_reconnect_count = 0

            # 1. 取消未完成的买单
            active_orders = await self.get_active_orders(self.config.contract_id)
            for order in active_orders:
                if order.side == "buy":
                    await self.cancel_order(order.order_id)
            
            # 2. 停止 WebSocket
            # self._stop_event.set()
            # if self._prices_ws:
            #     self._prices_ws.close()
            # if self._portfolio_ws:
            #     self._portfolio_ws.close()
            
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

            self.logger.log("【VARIATIONAL】Variational exchange disconnected successfully", "INFO")

        except Exception as e:
            self.logger.log(f"【VARIATIONAL】Error during Variational disconnect: {e}", "ERROR")
            self.logger.log(f"【VARIATIONAL】Traceback: {traceback.format_exc()}", "ERROR")
            raise

    async def _authenticate(self) -> None:
        """Authenticate with Variational exchange."""
        try:
            # 1. 获取签名数据
            signing_data = await self._fetch_signing_data()
            message = signing_data
            self.logger.log(f"【VARIATIONAL】获取签名数据: {message}", "INFO")

            # 2. 签名消息
            signature_hex = self._sign_message(message)
            
            # 3. 登录获取 token
            login_response = await self._login_with_signature(signature_hex)
            self.auth_token = login_response.get("token")

            self.cookies = {"vr-token": self.auth_token,"vr-connected-address": self.wallet_address } if self.auth_token else {}
            
            if not self.auth_token:
                raise ValueError("【VARIATIONAL】Failed to get auth token")

            self.logger.log("【VARIATIONAL】Authentication successful", "INFO")

        except Exception as e:
            await self.notify(f"❌ Variational authentication failed: {e}", level="ERROR")
            self.logger.log(f"【VARIATIONAL】Authentication failed: {e}", "ERROR")
            raise

    async def _fetch_signing_data(self) -> Dict[str, Any]:
        """使用 cloudscraper 获取签名数据"""
        url = f"{self.api_base}/auth/generate_signing_data"
        payload = {"address": self.wallet_address}
        loop = asyncio.get_event_loop()
        
        try:
            response = await loop.run_in_executor(
                None, 
                lambda: self.scraper.post(url, json=payload)
            )
            
            response.raise_for_status()
            return response.text
            
        except Exception as e:
            self.logger.log(f"【VARIATIONAL】Variational API request failed: {e}", "ERROR")
            raise

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
                self.logger.log(f"【VARIATIONAL】Prices WebSocket error: {e}", "ERROR")
                time.sleep(3)

    def _run_portfolio_ws(self) -> None:
        """运行投资组合 WebSocket"""
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        }
        # self.logger.log("【VARIATIONAL】Starting Portfolio WebSocket...", "INFO")
        # self._portfolio_ws = websocket.WebSocketApp(
        #     self.ws_portfolio_url,
        #     header=headers,
        #     on_open=self._on_portfolio_open,
        #     on_message=self._on_portfolio_message,
        #     on_error=self._on_portfolio_error,
        #     on_close=self._on_portfolio_close
        # )
        
        while not self._stop_event.is_set():
            try:
                # ✅ 检查是否已连接
                with self._portfolio_ws_lock:
                    if self._portfolio_ws_connected:
                        self.logger.log("【VARIATIONAL】Portfolio WebSocket already connected, skipping", "WARNING")
                        time.sleep(3)
                        continue
                    
                    # ✅ 标记为连接中
                    self._portfolio_ws_connected = True
                
                self.logger.log("【VARIATIONAL】Starting Portfolio WebSocket...", "INFO")
                # ✅ 关闭旧连接
                if self._portfolio_ws is not None:
                    try:
                        self._portfolio_ws.close()
                        time.sleep(1)
                    except:
                        pass
                
                # ✅ 标记为连接中
                with self._portfolio_ws_lock:
                    self._portfolio_ws_connected = True

                # ✅ 创建新连接
                self._portfolio_ws = websocket.WebSocketApp(
                    self.ws_portfolio_url,
                    header=headers,
                    on_open=self._on_portfolio_open,
                    on_message=self._on_portfolio_message,
                    on_error=self._on_portfolio_error,
                    on_close=self._on_portfolio_close
                )
                
                # ✅ 运行 WebSocket（会阻塞）
                self._portfolio_ws.run_forever()
                
            except Exception as e:
                error_msg = str(e).lower()
                
                # ✅ 捕获 "already opened" 错误
                if 'already opened' in error_msg or 'already connected' in error_msg:
                    self.logger.log("【VARIATIONAL】Portfolio WebSocket already opened, waiting...", "WARNING")
                    time.sleep(5)  # 等待旧连接关闭
                else:
                    self.logger.log(f"【VARIATIONAL】Portfolio WebSocket error: {e}", "ERROR")
                    time.sleep(3)
            
            finally:
                # ✅ 连接关闭后重置状态
                with self._portfolio_ws_lock:
                    self._portfolio_ws_connected = False
                
                self.logger.log("【VARIATIONAL】Portfolio WebSocket disconnected", "INFO")
    

    def _on_prices_open(self, ws):
        """价格 WebSocket 连接建立"""
        self.logger.log("【VARIATIONAL】Prices WebSocket connected", "INFO")
        subscribe_msg = {
            "action": "subscribe",
            "instruments": [self._build_instrument()]
        }
        ws.send(json.dumps(subscribe_msg))
        self._ws_connected.set()

    def _on_prices_message(self, ws, message):
        """处理价格 WebSocket 消息"""
        self.logger.log(f"【VARIATIONAL】Received prices message: {message[:200]}", "DEBUG")
        try:
            data = json.loads(message)
            # 更新订单簿数据
            if 'bid' in data and 'ask' in data:
                self.orderbook = {
                    'bids': [[data['bid'], data.get('bid_size', '0')]],
                    'asks': [[data['ask'], data.get('ask_size', '0')]],
                    'timestamp': time.time()
                }
                self.logger.log(f"【VARIATIONAL】Orderbook updated: bid={data['bid']}, ask={data['ask']}", "DEBUG")
        except json.JSONDecodeError:
            self.logger.log(f"【VARIATIONAL】Failed to parse prices message: {message[:200]}", "WARNING")
        except Exception as e:
            self.logger.log(f"【VARIATIONAL】Error handling prices message: {e}", "ERROR")

    def _on_prices_error(self, ws, error):
        """价格 WebSocket 错误"""
        self.logger.log(f"【VARIATIONAL】Prices WebSocket error: {error}", "ERROR")

    def _on_prices_close(self, ws, close_status_code, close_msg):
        """价格 WebSocket 关闭"""
        self.logger.log(f"【VARIATIONAL】Prices WebSocket closed: {close_status_code} - {close_msg}", "INFO")

    def _on_portfolio_open(self, ws):
        """投资组合 WebSocket 连接建立"""
        self.logger.log("【VARIATIONAL】Portfolio WebSocket connected", "INFO")
        if self.auth_token:
            subscribe_msg = {
                "claims": self.auth_token
            }
            ws.send(json.dumps(subscribe_msg))

    def _on_portfolio_message(self, ws, message):
        """处理投资组合 WebSocket 消息"""
        try:
            data = json.loads(message)
            portfolio_data = self._extract_portfolio_snapshot(data)

            if portfolio_data is not None:
                with self._portfolio_snapshot_lock:
                    merged_snapshot = self._merge_portfolio_snapshot(
                        self._portfolio_snapshot_normalized if self._portfolio_snapshot_seen else None,
                        portfolio_data,
                    )
                    self._portfolio_snapshot_raw = dict(data) if isinstance(data, dict) else {}
                    self._portfolio_snapshot_normalized = merged_snapshot
                    self._portfolio_snapshot_seen = True
                    self._portfolio_snapshot_ts = time.time()

            # 处理仓位更新
            positions = portfolio_data.get('positions') if isinstance(portfolio_data, dict) else None
            if positions is None and isinstance(data, dict):
                positions = data.get('positions')

            if positions is not None:
                # for position_update in data['positions']:
                    if self._order_update_handler:
                        result = self._order_update_handler(positions)
                        if asyncio.iscoroutine(result):
                            asyncio.run(result)
        except json.JSONDecodeError:
            self.logger.log(f"【VARIATIONAL】Failed to parse portfolio message: {message[:200]}", "WARNING")
        except Exception as e:
            self.logger.log(f"【VARIATIONAL】Error handling portfolio message: {e}", "ERROR")

    def _extract_portfolio_snapshot(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """从 portfolio WS 消息中提取账户快照。"""
        if not isinstance(data, dict):
            return None

        risk_payload = data.get("pool_portfolio_result")
        if not isinstance(risk_payload, dict):
            risk_payload = self._find_portfolio_payload(data)
        if not isinstance(risk_payload, dict):
            return None

        # Variational 的 portfolio WS 存在两种常见结构：
        # 1. 风险字段和 positions 都在同一层
        # 2. 风险字段位于 pool_portfolio_result，而 positions 与其同级
        # 因此这里需要兼容“内层没有 positions，外层同级补齐”的情况。
        raw_positions = risk_payload.get('positions')
        if raw_positions is None:
            raw_positions = data.get('positions')

        has_risk_fields = any(
            key in risk_payload
            for key in ('balance', 'upnl', 'positions', 'published_at', 'leverage')
        ) or raw_positions is not None
        if not has_risk_fields:
            return None

        normalized_positions = self._normalize_portfolio_positions(raw_positions)
        snapshot = {
            'balance': risk_payload.get('balance'),
            'upnl': risk_payload.get('upnl'),
            'positions': normalized_positions,
            'published_at': risk_payload.get('published_at') or data.get('published_at'),
        }
        if 'leverage' in risk_payload:
            snapshot['leverage'] = risk_payload.get('leverage')
        if 'portfolio_value' in risk_payload:
            snapshot['portfolio_value'] = risk_payload.get('portfolio_value')
        if normalized_positions is None:
            self.logger.log(
                "【VARIATIONAL】Portfolio WS 快照缺少 positions，"
                f"payload keys={list(risk_payload.keys())[:10]}, "
                f"outer keys={list(data.keys())[:10]}",
                "INFO"
            )
        return snapshot

    def _merge_portfolio_snapshot(
        self,
        previous: Optional[Dict[str, Any]],
        current: Dict[str, Any],
    ) -> Dict[str, Any]:
        """合并 portfolio 增量消息，避免局部更新把完整快照冲掉。"""
        if not isinstance(previous, dict):
            return dict(current)

        merged = dict(previous)
        for key in ("balance", "upnl", "published_at", "leverage", "portfolio_value"):
            current_value = current.get(key)
            if current_value is not None:
                merged[key] = current_value

        merged["positions"] = self._merge_portfolio_positions(
            previous.get("positions"),
            current.get("positions"),
        )
        return merged

    def _merge_portfolio_positions(
        self,
        previous_positions: Any,
        current_positions: Any,
    ) -> Optional[List[Dict[str, Any]]]:
        """按标的合并仓位，保留旧快照中的清算价等稳定字段。"""
        normalized_previous = self._normalize_portfolio_positions(previous_positions) or []
        normalized_current = self._normalize_portfolio_positions(current_positions)
        if normalized_current is None:
            return normalized_previous or None

        merged_by_key: Dict[str, Dict[str, Any]] = {}
        order: List[str] = []

        for entry in normalized_previous:
            key = self._portfolio_position_merge_key(entry)
            if key not in merged_by_key:
                order.append(key)
            merged_by_key[key] = entry

        for entry in normalized_current:
            key = self._portfolio_position_merge_key(entry)
            previous_entry = merged_by_key.get(key)
            merged_entry = self._deep_merge_position_entry(previous_entry, entry)
            if key not in merged_by_key:
                order.append(key)
            merged_by_key[key] = merged_entry

        return [merged_by_key[key] for key in order]

    @staticmethod
    def _portfolio_position_merge_key(entry: Dict[str, Any]) -> str:
        """生成仓位合并键，优先按标的匹配。"""
        if not isinstance(entry, dict):
            return "unknown"

        position_info = entry.get("position_info")
        if isinstance(position_info, dict):
            instrument = position_info.get("instrument")
            if isinstance(instrument, dict):
                underlying = instrument.get("underlying")
                instrument_type = instrument.get("instrument_type")
                if underlying:
                    return f"{instrument_type}:{underlying}"

        symbol = entry.get("symbol")
        if symbol:
            return str(symbol)
        return str(id(entry))

    def _deep_merge_position_entry(
        self,
        previous: Optional[Dict[str, Any]],
        current: Dict[str, Any],
    ) -> Dict[str, Any]:
        """浅层递归合并仓位字段。

        对大多数字段仍保持“空值回退旧值”，但清算价这类风险字段需要允许
        WebSocket 显式下发 `null` 来清空旧值，避免反转过零后继续沿用上一次仓位
        的清算价。
        """
        if not isinstance(previous, dict):
            return dict(current)

        merged = dict(previous)
        for key, current_value in current.items():
            previous_value = merged.get(key)
            if isinstance(previous_value, dict) and isinstance(current_value, dict):
                merged[key] = self._deep_merge_position_entry(previous_value, current_value)
                continue
            if key == "estimated_liquidation_price" and key in current:
                merged[key] = current_value
                continue
            if current_value is not None:
                merged[key] = current_value
        return merged

    def _find_portfolio_payload(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """递归查找真正承载 portfolio 风险字段的那层字典。"""
        queue: List[Any] = []
        preferred_keys = (
            'pool_portfolio_result',
            'portfolio',
            'result',
            'data',
            'payload',
        )
        for key in preferred_keys:
            value = data.get(key)
            if isinstance(value, dict):
                queue.append(value)

        queue.append(data)
        seen: set[int] = set()
        best_with_positions: Optional[Dict[str, Any]] = None
        best_risk_like: Optional[Dict[str, Any]] = None

        while queue:
            current = queue.pop(0)
            if not isinstance(current, dict):
                continue
            current_id = id(current)
            if current_id in seen:
                continue
            seen.add(current_id)

            current_keys = set(current.keys())
            has_positions_key = 'positions' in current_keys
            has_risk_like = bool(current_keys & {'balance', 'upnl', 'positions', 'published_at', 'leverage'})

            if has_positions_key:
                positions_value = current.get('positions')
                if isinstance(positions_value, list):
                    return current
                if best_with_positions is None:
                    best_with_positions = current

            if has_risk_like and best_risk_like is None:
                best_risk_like = current

            for value in current.values():
                if isinstance(value, dict):
                    queue.append(value)
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            queue.append(item)

        return best_with_positions or best_risk_like

    @staticmethod
    def _normalize_portfolio_positions(positions: Any) -> Optional[List[Dict[str, Any]]]:
        """统一将 portfolio positions 归一化为 list[dict]。"""
        if isinstance(positions, list):
            return [item for item in positions if isinstance(item, dict)]
        if isinstance(positions, dict):
            normalized = []
            for value in positions.values():
                if isinstance(value, dict):
                    normalized.append(value)
                elif isinstance(value, list):
                    normalized.extend(item for item in value if isinstance(item, dict))
            return normalized
        return None

    def get_cached_portfolio_ws(self) -> Optional[Dict[str, Any]]:
        """获取最近一次 portfolio WS 快照（供风控读取）。"""
        with self._portfolio_snapshot_lock:
            if not self._portfolio_snapshot_seen:
                return None
            return {
                'timestamp': self._portfolio_snapshot_ts,
                'raw': dict(self._portfolio_snapshot_raw) if isinstance(self._portfolio_snapshot_raw, dict) else {},
                'portfolio': dict(self._portfolio_snapshot_normalized),
            }

    def _on_portfolio_error(self, ws, error):
        """投资组合 WebSocket 错误"""
        self.logger.log(f"【VARIATIONAL】Portfolio WebSocket error: {error}", "ERROR")

    def _on_portfolio_close(self, ws, close_status_code, close_msg):
        """投资组合 WebSocket 关闭"""
        self.logger.log(f"【VARIATIONAL】Portfolio WebSocket closed: {close_status_code} - {close_msg}", "INFO")
        # ✅ 重置连接状态
        with self._portfolio_ws_lock:
            self._portfolio_ws_connected = False
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
            self.logger.log(f"【VARIATIONAL】Error fetching BBO prices: {e}", "ERROR")
            return Decimal('0'), Decimal('0')

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_fixed(3),
        retry=retry_if_exception_type(Exception),
        reraise=True)
    async def _fetch_indicative_quote(self, qty: Decimal, contract_id: Optional[str] = None) -> Dict[str, Any]:
        """获取指示性报价"""
        url = f"{self.api_base}/quotes/indicative"
    
        if contract_id:
            ticker = contract_id.split('-')[0]
        else:
            ticker = self.config.ticker
        
        payload = {
            "instrument": self._build_instrument(ticker),
            "qty": str(qty)
        }
        
        try:
            return await self._make_var_request('POST', url, json=payload, cookies=self.cookies)
        except Exception as e:
            self.logger.log(f"【VARIATIONAL】Failed to fetch indicative quote: {e}", "ERROR")
            return {}

    async def place_open_order(self, contract_id: str, quantity: Decimal, direction: str) -> OrderResult:
        """下开仓订单"""
        try:
            best_bid, best_ask = await self.fetch_bbo_prices(contract_id, quantity)
            self.logger.log(f"【VARIATIONAL】Placing open order: direction={direction}, quantity={quantity}", "INFO")
            self.logger.log(f"【VARIATIONAL】Best Bid: {best_bid}, Best Ask: {best_ask}", "INFO")
            self.logger.log(f"【VARIATIONAL】Tick Size: {self.config.tick_size}", "INFO")
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
            "instrument": self._build_instrument(),
            "qty": str(quantity),
            "is_auto_resize": False,
            "use_mark_price": False,
            "is_reduce_only": False
        }
                
        try:
            data = await self._make_var_request('POST', url, json=payload, cookies=self.cookies)
            self.logger.log(f"【VARIATIONAL】Placed limit order response: {data}", "INFO")
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
    
    async def _place_market_order(self, quote_id: str, side: str, max_slippage: int) -> OrderResult:
        url = f"{self.api_base}/orders/new/market"
        payload = {
            "side": side,
            "is_reduce_only": False,
            "max_slippage": max_slippage,
            "quote_id": quote_id
        }

        try:
            data = await self._make_var_request('POST', url, json=payload, cookies=self.cookies)
            self.logger.log(f"【VARIATIONAL】Placed market order response: {data}", "INFO")
            order_id = data.get('rfq_id')
            if not order_id:
                return OrderResult(
                    success=False, 
                    error_message=data.get('error', 'No rfq_id returned')
                )
            return OrderResult(
                success=True,
                order_id=order_id,
                side=side,
                size='',
                price='',
                status='PENDING'
            )
        except Exception as e:
            return OrderResult(success=False, error_message=str(e))

    async def _place_accept_order(self, quote_id: str, side: str, max_slippage: int) -> OrderResult:
        url = f"{self.api_base}/quotes/accept"
        payload = {
            "side": side,
            "is_reduce_only": True,
            "max_slippage": max_slippage,
            "quote_id": quote_id
        }
        
        try:
            data = await self._make_var_request('POST', url, json=payload, cookies=self.cookies)
            self.logger.log(f"【VARIATIONAL】Placed accept order response: {data}", "INFO")
            order_id = data.get('rfq_id')
            if order_id:
                return OrderResult(
                    success=True,
                    order_id=order_id,
                    side=side,
                    size='',
                    price='',
                    status='PENDING'
                )
            else:
                return OrderResult(success=False, error_message=data.get('error', 'Unknown error'))
                
        except Exception as e:
            return OrderResult(success=False, error_message=str(e))
    
    async def cancel_order(self, order_id: str) -> OrderResult:
        """使用 cloudscraper 取消订单"""
        url = f"{self.api_base}/orders/cancel"
        payload = {"rfq_id": order_id}
                
        try:
            data = await self._make_var_request('POST', url, json=payload, cookies=self.cookies)
            
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
            self.logger.log(f"【VARIATIONAL】Error getting order info: {e}", "ERROR")
            return None

    async def get_active_orders(self, contract_id: str) -> List[OrderInfo]:
        """使用 cloudscraper 获取活跃订单"""
        try:
            url = f"{self.api_base}/positions"
            
            data = await self._make_var_request('GET', url, cookies=self.cookies)
            
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
            self.logger.log(f"【VARIATIONAL】Error getting active orders: {e}", "ERROR")
            return []
    
    # 获取订单历史记录
    async def get_orders_history(
        self, 
        limit: int = 20, 
        offset: int = 0, 
        # start_date: Optional[datetime] = None,
        # end_date: Optional[datetime] = None,
        rfq_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        获取订单历史记录
        
        Args:
            limit: 返回记录数量限制 (默认20)
            offset: 偏移量 (默认0) 
            start_date: 开始时间 (可选)
            end_date: 结束时间 (可选)
            rfq_id: 特定订单ID过滤 (可选)
            
        Returns:
            包含订单历史和分页信息的字典
        """
        try:
            url = f"{self.api_base}/orders/v2"
            
            # 构建查询参数
            params = {
                'limit': str(limit),
                'offset': str(offset),
                'order_by': 'created_at',
                'order': 'desc',
                'status': 'canceled,cleared,rejected'
            }
            
            # 获取当前时间
            now = datetime.now(timezone.utc)

            # 设置开始时间：前天的16:00:00 UTC
            # start_date = (now - timedelta(days=2)).replace(hour=16, minute=0, second=0, microsecond=0)
            # start_iso = start_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-3] + 'Z'
            # params['created_at_gte'] = start_iso

            # # 设置结束时间：今天的15:59:59.999 UTC
            # end_date = now.replace(hour=15, minute=59, second=59, microsecond=999000)
            # end_iso = end_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-3] + 'Z'
            # params['created_at_lte'] = end_iso

            # 设置认证 cookies
            
            self.logger.log(f"【VARIATIONAL】Fetching orders history with params: {params}", "DEBUG")
            
            # 发起请求
            response_data = await self._make_var_request('GET', url, params=params, cookies=self.cookies)
            # self.logger.log(f"【VARIATIONAL】Orders history response: {response_data}", "INFO")
            # 如果指定了 rfq_id，进行过滤
            if rfq_id and 'result' in response_data:
                filtered_orders = [
                    order for order in response_data['result'] 
                    if order.get('rfq_id') == rfq_id
                ]
                
                response_data['result'] = filtered_orders
                # 更新对象计数
                response_data['pagination']['object_count'] = len(filtered_orders)
                
                self.logger.log(f"【VARIATIONAL】Filtered {len(filtered_orders)} orders for rfq_id: {rfq_id}", "DEBUG")
            
            self.logger.log(f"【VARIATIONAL】Retrieved {len(response_data.get('result', []))} orders", "DEBUG")
            return response_data
            
        except Exception as e:
            self.logger.log(f"【VARIATIONAL】Error fetching orders history: {e}", "ERROR")
            return {
                'pagination': {
                    'object_count': 0,
                    'next_page': None,
                    'last_page': None
                },
                'result': []
            }

    async def get_account_positions(self) -> Decimal:
        """使用 cloudscraper 获取账户持仓"""
        try:
            url = f"{self.api_base}/positions"
            
            data = await self._make_var_request('GET', url, cookies=self.cookies)
            
            # 解析持仓数据
            if 'positions' in data:
                for position in data['positions']:
                    instrument = position.get('instrument', {})
                    if instrument.get('underlying') == self.config.ticker:
                        return abs(Decimal(str(position.get('size', '0'))))
            return Decimal('0')
            
        except Exception as e:
            self.logger.log(f"【VARIATIONAL】Error getting account positions: {e}", "ERROR")
            return Decimal('0')
    
        # Line 845-900
    
    async def get_position(self, symbol: str) -> Optional[dict]:
        """
        获取指定币种的持仓信息
        
        Args:
            symbol: 币种符号（如 'HYPE'）
        
        Returns:
            {
                'symbol': 'HYPE',
                'side': 'short',
                'size': 0.01,
                'entry_price': 29.7032,
                'unrealized_pnl': 0.000012
            }
        """
        try:
            url = f"{self.api_base}/positions"
            
            # ✅ 调用 API
            data = await self._make_var_request('GET', url, cookies=self.cookies)
            
            # ✅ 检查返回数据
            if not data:
                self.logger.log("No positions data", "INFO")
                return  {
                    'symbol': symbol,
                    'side': 'neutral',
                    'size': 0,
                    'entry_price': '--',
                    'unrealized_pnl': 0
                }
            
            # ✅ 关键修正：data 本身就是数组，不需要 data['positions']
            positions = data if isinstance(data, list) else []
            
            # ✅ 遍历持仓列表
            for pos in positions:
                # ✅ 获取 position_info
                position_info = pos.get('position_info', {})
                
                # ✅ 获取 instrument 信息
                instrument = position_info.get('instrument', {})
                
                # ✅ 匹配币种
                if instrument.get('underlying') == symbol:
                    # ✅ 获取持仓数量
                    qty_str = position_info.get('qty', '0')
                    size = Decimal(str(qty_str))
                    
                    # ✅ 无持仓
                    if size == 0:
                        self.logger.log(f"Found {symbol} position but size is 0", "INFO")
                        return {
                            'symbol': symbol,
                            'side': 'neutral',
                            'size': 0,
                            'entry_price': '--',
                            'unrealized_pnl': 0
                        }
                    
                    # ✅ 判断多空方向
                    side = 'short' if size < 0 else 'long'
                    
                    # ✅ 获取开仓均价
                    avg_entry_price = Decimal(str(position_info.get('avg_entry_price', '0')))
                    
                    # ✅ 获取未实现盈亏
                    unrealized_pnl = Decimal(str(pos.get('upnl', '0')))
                    
                    result = {
                        'symbol': symbol,
                        'side': side,
                        'size': abs(size),
                        'entry_price': avg_entry_price,
                        'unrealized_pnl': unrealized_pnl
                    }
                    
                    self.logger.log(
                        f"Found {symbol} position:\n"
                        f"  Side: {side}\n"
                        f"  Size: {abs(size)}\n"
                        f"  Entry Price: {avg_entry_price}\n"
                        f"  Unrealized PnL: {unrealized_pnl}",
                        "DEBUG"
                    )
                    
                    return result
            
            # ✅ 未找到该币种的持仓
            self.logger.log(f"No position found for {symbol}", "DEBUG")
            return {
                'symbol': symbol,
                'side': 'neutral',
                'size': 0,
                'entry_price': '--',
                'unrealized_pnl': 0
            }
        
        except Exception as e:
            self.logger.log(f"❌ Error getting position for {symbol}: {e}", "ERROR")
            import traceback
            self.logger.log(f"❌ Traceback: {traceback.format_exc()}", "ERROR")
            return None
    
    def setup_order_update_handler(self, handler) -> None:
        """设置订单更新处理器"""
        self._order_update_handler = handler

    def get_exchange_name(self) -> str:
        """获取交易所名称"""
        return "variational"

    async def get_contract_attributes(self, ticker: Optional[str] = None) -> Tuple[str, Decimal]:
        """获取合约属性"""
        ticker = ticker or self.config.ticker
        if not ticker:
            raise ValueError("Ticker is empty")
        
        # ✅ 使用动态 ticker（重要！）
        self.config.contract_id = f"{ticker}-PERP"
        
        # ✅ 尝试从 API 获取 tick size
        try:
            quote = await self._fetch_indicative_quote(Decimal('0.00001'))
            qty_limits = quote.get('qty_limits', {})
            
            if qty_limits and 'ask' in qty_limits:
                min_qty = Decimal(str(qty_limits['ask'].get('min_qty', '0.01')))
                self.config.tick_size = min_qty
                self.logger.log(f"【VARIATIONAL】Tick size for {ticker}: {min_qty}", "INFO")
        except:
            # 使用默认值
            self.config.tick_size = Decimal('0.01')

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
    
    async def getVariationalVolume(self) -> Decimal:
        url = f"{self.api_base}/portfolio/trade_volume"

        data = await self._make_var_request('GET', url, cookies=self.cookies)

        if 'all_time' in data:
            return Decimal(data['all_time'])
        return Decimal('0')

    async def getVariationalBalance(self) -> Decimal:
        url = f"{self.api_base}/portfolio?compute_margin=true"
            
        data = await self._make_var_request('GET', url, cookies=self.cookies)

        if 'balance' in data:
            return Decimal(data['balance'])
        return Decimal('0')
    
    async def get_portfolio(self) -> Optional[dict]:
        """
        获取投资组合信息
        Returns:
            {
                "margin_usage": {
                    "initial_margin": "0",
                    "maintenance_margin": "0"
                },
                "balance": "1.889881",
                "upnl": "0",
                "positions": []
            }
        """
        try:
            url = f"{self.api_base}/portfolio?compute_margin=true"
            
            data = await self._make_var_request('GET', url, cookies=self.cookies)
            
            return data
            
        except Exception as e:
            self.logger.log(f"【VARIATIONAL】Error getting portfolio {e}", "ERROR")
            return None

    async def get_trade_volume(self) -> Optional[dict]:
        """
        获取交易量相关信息
        Returns:
            {
                "last_30d": "1.845642",
                "all_time": "912206.296153",
                "own": {
                    "lifetime": "912206.296153",
                    "last_30d": "1.845642"
                },
                "referred": {
                    "lifetime": "45632098.489581",
                    "last_30d": "43513644.10272"
                },
                "total": {
                    "lifetime": "10038625.9940692",
                    "last_30d": "8702730.666186"
                },
                "current_tier": {
                    "id": 2,
                    "name": "Silver",
                    "total_volume_30d": "5000000",
                    "referral_reward_rate": "0.21",
                    "points_rate": "1.0174",
                    "spread_discount": "0.1189",
                    "loss_rakeback_odds_boost": "1",
                    "loss_rakeback_referral_cut": "0.0125"
                },
                "boosted_tier": null
            }
        """
        try:
            url = f"{self.api_base}/portfolio/trade_volume"
            
            data = await self._make_var_request('GET', url, cookies=self.cookies)
            
            return data
            
        except Exception as e:
            self.logger.log(f"【VARIATIONAL】Error getting trade_volume {e}", "ERROR")
            return None

    async def get_points(self) -> Optional[dict]:
        """
        获取积分信息
        Returns:
            {
                "company": "3fedfbfb-9ce3-47e0-bd86-b71bbb1fd782",
                "total_points": "147.620000",
                "self_points": "80.560000",
                "referral_points": "67.060000",
                "rank": 6519
            }
        """
        try:
            url = f"{self.api_base}/points/summary"
            cookies = {"vr-token": self.auth_token} if self.auth_token else {}
            
            data = await self._make_var_request('GET', url, cookies=self.cookies)
            
            return data
            
        except Exception as e:
            self.logger.log(f"【VARIATIONAL】Error getting points {e}", "ERROR")
            return None
    
    async def get_points_history(self) -> Optional[list]:
        """
        获取历史积分
        Returns:
            {
                "pagination": {
                    "last_page": {
                        "limit": "20",
                        "offset": "0"
                    },
                    "next_page": null,
                    "object_count": 2
                },
                "result": [
                    {
                        "end_window": "2025-12-18T00:00:00Z",
                        "referral_points": "1.110000",
                        "self_points": "0",
                        "start_window": "2025-12-12T00:00:00Z",
                        "total_points": "1.110000"
                    },
                    {
                        "end_window": "2025-12-12T00:00:00Z",
                        "referral_points": "65.950000",
                        "self_points": "80.560000",
                        "start_window": "2025-01-29T00:00:00Z",
                        "total_points": "146.510000"
                    }
                ]
            }
        """
        try:
            url = f"{self.api_base}/points/history?limit=20&offset=0"
            
            data = await self._make_var_request('GET', url, cookies=self.cookies)
            return data.get('result', [])
            
        except Exception as e:
            self.logger.log(f"【VARIATIONAL】Error getting points {e}", "ERROR")
            return None
    
    
