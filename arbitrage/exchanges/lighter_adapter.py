"""Lighter 交易所适配器"""

import asyncio
import logging
import json
import os
import random
import time
import lighter
import websockets
from decimal import Decimal
from typing import Optional, Callable, Dict, Any
from .base import ExchangeAdapter
from helpers.lighter_ws import build_lighter_ws_url, lighter_ws_connect_kwargs

logger = logging.getLogger(__name__)

class LighterAdapter(ExchangeAdapter):
    """Lighter 交易所适配器"""
    
    def __init__(self, symbol: str, client, config: dict = None):
        super().__init__(symbol, client, config)

        # ✅ 调试：打印客户端的所有方法
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("🔍 LighterClient 可用方法:")
            for attr in dir(self.client):
                if not attr.startswith('_') and callable(getattr(self.client, attr)):
                    logger.debug(f"   - {attr}")

        self.market_index = None
        self.ws_task = None
        self.ws = None
        self._orderbook_watchdog_task = None
        self._ws_close_reason = None
        self._watchdog_reconnect_times = []
        self._watchdog_cooldown_until = 0.0
        self.account_index = int(os.getenv('LIGHTER_ACCOUNT_INDEX'))
        self.reconnect_base_delay = float(self.config.get('reconnect_base_delay', 0.3))
        self.reconnect_max_delay = float(self.config.get('reconnect_max_delay', 10.0))
        if self.reconnect_base_delay <= 0:
            logger.warning(f"⚠️ reconnect_base_delay 非法({self.reconnect_base_delay})，使用默认值 0.3")
            self.reconnect_base_delay = 0.3
        if self.reconnect_max_delay <= 0:
            logger.warning(f"⚠️ reconnect_max_delay 非法({self.reconnect_max_delay})，使用默认值 10.0")
            self.reconnect_max_delay = 10.0
        if self.reconnect_max_delay < self.reconnect_base_delay:
            logger.warning(
                f"⚠️ reconnect_max_delay({self.reconnect_max_delay}) < reconnect_base_delay({self.reconnect_base_delay})，"
                f"已自动调整为相同值"
            )
            self.reconnect_max_delay = self.reconnect_base_delay

        
        # ✅ Lighter 订单簿数据
        self.lighter_order_book = {
            "bids": {},  # {Decimal(price): Decimal(size)}
            "asks": {}
        }
        self.lighter_order_book_lock = asyncio.Lock()
        self.lighter_best_bid = None
        self.lighter_best_ask = None
        self.lighter_snapshot_loaded = False
        self.lighter_last_update_ts = 0.0  # 最近一次收到有效订单簿消息的时间戳
        self._order_book_fingerprint = None  # 订单簿内容指纹，用于检测“内容未变”场景
        self.lighter_last_notify_ts = 0.0  # 最近一次向上游回调的时间戳
        self._last_orderbook_message_ts = 0.0  # 最近一次收到 order_book 消息的时间
        self._last_any_ws_message_ts = 0.0  # 最近一次收到任意 WS 消息的时间
        self._consecutive_ws_timeouts = 0
        self.order_book_offset = None
        self.order_book_sequence_gap = False
        self._orderbook_cleanup_counter = 0
        self._is_running = False
        
        # 消息计数器
        self.message_count = 0

        self._order_status_data: Dict[int, dict] = {}  # key: client_order_index
        self._order_status_futures: Dict[int, asyncio.Future] = {}
        self._last_client_order_ms = 0
        self._client_order_seq = 0
        # ✅ Lighter 账户统计（WS: user_stats）缓存
        self._lighter_user_stats_raw: Dict[str, Any] = {}
        self._lighter_user_stats_ts: float = 0.0
        self._lighter_user_stats_seen: bool = False
        self._lighter_user_stats_parsed: Dict[str, Optional[Decimal]] = {
            'leverage': None,
            'collateral': None,
            'portfolio_value': None,
        }
        # ✅ Lighter 市场统计（WS: market_stats）缓存
        self._lighter_market_stats_raw: Dict[str, Any] = {}
        self._lighter_market_stats_ts: float = 0.0
        self._lighter_market_stats_seen: bool = False
        self._lighter_market_stats_parsed: Dict[str, Optional[Decimal]] = {
            'mark_price': None,
            'index_price': None,
        }
        # ClientOrderIndex 上限：2^48 - 1（281474976710655）
        self._client_order_index_max = (1 << 48) - 1
        # 使用固定 epoch 降低时间戳位宽，保证 index 始终在 48 位内。
        self._client_order_epoch_ms = 1704067200000  # 2024-01-01 00:00:00 UTC
        self._client_order_seq_bits = 8              # 每毫秒最多 256 个序号
        self._client_order_seq_max = (1 << self._client_order_seq_bits) - 1
        self._client_order_max_delta_ms = (1 << (48 - self._client_order_seq_bits)) - 1
        # ✅ 盘口定价：最多吃到第 N 档（1=只吃买一/卖一，2=最多吃到买二/卖二）
        try:
            self.depth_price_max_levels = int(self.config.get('depth_price_max_levels', 2))
        except (TypeError, ValueError):
            self.depth_price_max_levels = 2
        try:
            self.orderbook_silence_reconnect_seconds = float(
                self.config.get('orderbook_silence_reconnect_seconds', 3.0)
            )
        except (TypeError, ValueError):
            self.orderbook_silence_reconnect_seconds = 3.0
        try:
            self.orderbook_fingerprint_levels = int(
                self.config.get('orderbook_fingerprint_levels', 3)
            )
        except (TypeError, ValueError):
            self.orderbook_fingerprint_levels = 3
        if self.orderbook_fingerprint_levels <= 0:
            self.orderbook_fingerprint_levels = 3
        try:
            self.orderbook_cleanup_levels = int(
                self.config.get('orderbook_cleanup_levels', 100)
            )
        except (TypeError, ValueError):
            self.orderbook_cleanup_levels = 100
        if self.orderbook_cleanup_levels <= 0:
            self.orderbook_cleanup_levels = 100
        try:
            self.orderbook_cleanup_interval = int(
                self.config.get('orderbook_cleanup_interval', 1000)
            )
        except (TypeError, ValueError):
            self.orderbook_cleanup_interval = 1000
        if self.orderbook_cleanup_interval <= 0:
            self.orderbook_cleanup_interval = 1000
        try:
            self.watchdog_burst_window_seconds = float(
                self.config.get('watchdog_burst_window_seconds', 600.0)
            )
        except (TypeError, ValueError):
            self.watchdog_burst_window_seconds = 600.0
        if self.watchdog_burst_window_seconds <= 0:
            self.watchdog_burst_window_seconds = 600.0
        try:
            self.watchdog_burst_threshold = int(
                self.config.get('watchdog_burst_threshold', 5)
            )
        except (TypeError, ValueError):
            self.watchdog_burst_threshold = 5
        if self.watchdog_burst_threshold <= 0:
            self.watchdog_burst_threshold = 5
        try:
            self.watchdog_cooldown_seconds = float(
                self.config.get('watchdog_cooldown_seconds', 30.0)
            )
        except (TypeError, ValueError):
            self.watchdog_cooldown_seconds = 30.0
        if self.watchdog_cooldown_seconds <= 0:
            self.watchdog_cooldown_seconds = 30.0
    
    async def connect(self):
        """连接 Lighter"""
        try:
            self._is_running = True
            if self.client.config.contract_id is not None and self.client.config.contract_id != '':
                logger.info(
                    f"✅ {self.exchange_name} 已连接: "
                    f"{self.symbol} contract_id={self.client.config.contract_id}"
                )
            else:
                logger.warning(
                    f"⚠️ {self.exchange_name} contract_id 未设置 ({self.symbol})，"
                    f"将在订阅订单簿时获取"
                )

            if hasattr(self.client, 'setup_order_update_handler'):
                self.client.setup_order_update_handler(self._on_order_update)
                logger.info(f"📡 {self.exchange_name} 订单更新回调已注册: {self.symbol}")
            else:
                logger.warning(f"⚠️ {self.exchange_name} 不支持订单更新回调: {self.symbol}")

        except Exception as e:
            logger.exception(f"❌ {self.exchange_name} 连接失败 ({self.symbol}): {e}")
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
            logger.info(f"✅ Lighter market_index: {market_index} ({self.symbol})")
            return market_index
        except Exception as e:
            logger.exception(f"获取 market_index 失败: {e}")
            raise
    
    async def disconnect(self):
        self._is_running = False
        self._order_status_futures.clear()
        self._order_status_data.clear()
        """断开连接"""
        if self.ws_task:
            self.ws_task.cancel()
            try:
                await self.ws_task
            except asyncio.CancelledError:
                pass
        if self._orderbook_watchdog_task:
            self._orderbook_watchdog_task.cancel()
            try:
                await self._orderbook_watchdog_task
            except asyncio.CancelledError:
                pass
            self._orderbook_watchdog_task = None
        
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
        
        logger.info(f"📡 {self.exchange_name} 订阅订单簿: {self.symbol} market {self.market_index}")
    
    async def _handle_lighter_ws(self):
        """处理 Lighter WebSocket"""
        url = build_lighter_ws_url()
        reconnect_count = 0
        
        while self._is_running:
            try:
                logger.info(f"🔌 连接 Lighter WebSocket: {self.symbol} {url}")

                # 每次重连前重置本地订单簿状态，避免沿用旧缓存
                self._reset_lighter_orderbook_state()
                
                # 调整心跳/超时参数，降低误判断开
                async with websockets.connect(
                    url,
                    **lighter_ws_connect_kwargs()
                    # ping_interval=20,   # 显式设置心跳间隔
                    # ping_timeout=40,    # 放宽 pong 超时
                    # close_timeout=5,    # 关闭握手超时
                    # max_queue=None      # 避免队列背压导致 ping 超时
                ) as ws:
                    self.ws = ws
                    self._ws_close_reason = None
                    reconnect_count = 0
                    self._consecutive_ws_timeouts = 0
                    
                    # ✅ 订阅订单簿
                    subscribe_msg = {
                        "type": "subscribe",
                        "channel": f"order_book/{self.market_index}"
                    }
                    await ws.send(json.dumps(subscribe_msg))
                    logger.info(f"📡 已订阅 Lighter 订单簿: {self.symbol} market {self.market_index}")
                    subscribe_market_stats_msg = {
                        "type": "subscribe",
                        "channel": f"market_stats/{self.market_index}"
                    }
                    await ws.send(json.dumps(subscribe_market_stats_msg))
                    logger.info(f"📡 已订阅 Lighter 市场统计: {self.symbol} market {self.market_index}")
                    try:

                        auth_token, err = self.client.lighter_client.create_auth_token_with_expiry(api_key_index=int(os.getenv('LIGHTER_API_KEY_INDEX', '4')))

                        logger.info(f"🔑 创建 auth token for account orders subscription: {self.symbol} token={auth_token} err={err}")
                        if err is not None:
                            logger.warning(f"⚠️ Failed to create auth token for account orders subscription: {self.symbol} {err}")
                        else: 

                            subscribe_orders_msg = {
                                "type": "subscribe",
                                "channel": f"account_orders/{self.market_index}/{self.account_index}",
                                "auth": auth_token
                            }
                            await ws.send(json.dumps(subscribe_orders_msg))
                            logger.info(f"✅ Subscribed to account orders with auth token: {self.symbol} (expires in 10 minutes)")
                    except Exception as e:
                        logger.exception(f"❌ Error creating auth token for account orders subscription: {self.symbol} {e}")

                    self._orderbook_watchdog_task = asyncio.create_task(
                        self._watch_orderbook_silence(ws)
                    )

                    # 接收消息
                    while True:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=1.0)  # 整条 WS 活性检查
                            self._consecutive_ws_timeouts = 0
                            data = json.loads(msg)
                            self._last_any_ws_message_ts = time.time()
                            
                            if data.get("type") == "ping":
                                await ws.send(json.dumps({"type": "pong"}))
                                continue
                            
                            await self._process_lighter_message(data)  # 处理消息
                            
                        except asyncio.TimeoutError:
                            self._consecutive_ws_timeouts += 1
                            logger.info(f"⚠️ Lighter WS 1s 无消息，继续监听... ({self.symbol})")  # 心跳检查
                            continue
                        except websockets.exceptions.ConnectionClosedError as e:
                            if self._ws_close_reason == "orderbook_silence_watchdog":
                                logger.info(
                                    f"ℹ️ Lighter WS 因 order_book 静默主动关闭，准备重连 ({self.symbol})"
                                )
                            else:
                                logger.warning(
                                    f"❌ Lighter WS 连接关闭 ({self.symbol})，code={e.code}, reason={e.reason}"
                                )
                            break  # 跳出内循环，重连外层
                        except websockets.exceptions.ConnectionClosed as e:
                            if self._ws_close_reason == "orderbook_silence_watchdog":
                                logger.info(
                                    f"ℹ️ Lighter WS 因 order_book 静默主动关闭，准备重连 ({self.symbol})"
                                )
                            else:
                                logger.exception(f"❌ Lighter WS 连接关闭 ({self.symbol}): {e}")
                            break  # 跳出内循环，重连外层
                        except asyncio.CancelledError:
                            logger.info(f"🔚 Lighter WS 任务被取消，退出循环 ({self.symbol})")
                            self._is_running = False
                            break  # 跳出内循环，重连外层
                    if self._orderbook_watchdog_task:
                        self._orderbook_watchdog_task.cancel()
                        try:
                            await self._orderbook_watchdog_task
                        except asyncio.CancelledError:
                            pass
                        self._orderbook_watchdog_task = None
                    self._ws_close_reason = None
            
            except websockets.exceptions.ConnectionClosed as e:
                logger.exception(f"❌ Lighter WebSocket 连接关闭 ({self.symbol}): {e}")
            except Exception as e:
                logger.exception(f"❌ Lighter WebSocket 异常 ({self.symbol}): {e}")
            if self._is_running:
                # 重连逻辑
                reconnect_count += 1
                wait_time = min(
                    self.reconnect_max_delay,
                    self.reconnect_base_delay * (2 ** (reconnect_count - 1))
                )
                cooldown_remain = self._watchdog_cooldown_until - time.time()
                if cooldown_remain > wait_time:
                    wait_time = cooldown_remain
                    logger.warning(
                        f"⚠️ Lighter order_book 连续静默重连过多，"
                        f"进入 {wait_time:.1f}s 冷却期后再重连 ({self.symbol})"
                    )
                logger.info(f"⏳ {wait_time}秒后重连 Lighter WebSocket... ({self.symbol})")
                try:
                    await asyncio.sleep(wait_time)
                except asyncio.CancelledError:
                    logger.info(f"🔚 Lighter WS 外层任务被取消，退出重连 ({self.symbol})")
                    break

    async def _watch_orderbook_silence(self, ws):
        """单独监控 order_book 频道静默，不依赖整条 WS 是否超时。"""
        try:
            while self._is_running and self.ws is ws:
                await asyncio.sleep(0.5)

                if not self.lighter_snapshot_loaded:
                    continue

                silent_anchor = self._last_orderbook_message_ts or self.lighter_last_update_ts
                if silent_anchor <= 0:
                    continue

                silent_for = time.time() - silent_anchor
                if silent_for < self.orderbook_silence_reconnect_seconds:
                    continue

                logger.warning(
                    f"⚠️ Lighter order_book 静默 {silent_for:.1f}s，"
                    f"主动重连 WebSocket（其他频道可能仍有消息）({self.symbol})"
                )
                self._record_watchdog_reconnect()
                self._ws_close_reason = "orderbook_silence_watchdog"
                await ws.close()
                break
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception(f"❌ order_book 静默监控失败 ({self.symbol}): {e}")

    def _record_watchdog_reconnect(self):
        """记录 watchdog 触发的重连，并在短时间内过于频繁时进入冷却。"""
        now = time.time()
        window_start = now - self.watchdog_burst_window_seconds
        self._watchdog_reconnect_times = [
            ts for ts in self._watchdog_reconnect_times
            if ts >= window_start
        ]
        self._watchdog_reconnect_times.append(now)
        if len(self._watchdog_reconnect_times) >= self.watchdog_burst_threshold:
            self._watchdog_cooldown_until = max(
                self._watchdog_cooldown_until,
                now + self.watchdog_cooldown_seconds
            )

    def _reset_lighter_orderbook_state(self):
        """重置本地订单簿缓存，确保重连后不会使用旧数据"""
        self.lighter_order_book = {"bids": {}, "asks": {}}
        self.lighter_best_bid = None
        self.lighter_best_ask = None
        self.lighter_snapshot_loaded = False
        self.lighter_last_update_ts = 0.0
        self._order_book_fingerprint = None
        self.lighter_last_notify_ts = 0.0
        self._last_orderbook_message_ts = 0.0
        self._last_any_ws_message_ts = 0.0
        self._consecutive_ws_timeouts = 0
        self.order_book_offset = None
        self.order_book_sequence_gap = False
        self._orderbook_cleanup_counter = 0
        self._lighter_user_stats_raw = {}
        self._lighter_user_stats_ts = 0.0
        self._lighter_user_stats_seen = False
        self._lighter_user_stats_parsed = {
            'leverage': None,
            'collateral': None,
            'portfolio_value': None,
        }
        self._lighter_market_stats_raw = {}
        self._lighter_market_stats_ts = 0.0
        self._lighter_market_stats_seen = False
        self._lighter_market_stats_parsed = {
            'mark_price': None,
            'index_price': None,
        }

    def _extract_order_book_offset(self, order_book: dict) -> Optional[int]:
        """提取订单簿 offset。"""
        if not isinstance(order_book, dict):
            return None
        offset = order_book.get("offset")
        if offset is None:
            return None
        try:
            return int(offset)
        except (TypeError, ValueError):
            return None

    def _validate_order_book_offset(self, new_offset: int) -> bool:
        """校验订单簿 offset 连续性。"""
        if self.order_book_offset is None:
            self.order_book_offset = new_offset
            self.order_book_sequence_gap = False
            return True

        expected_offset = self.order_book_offset + 1
        if new_offset >= expected_offset:
            self.order_book_offset = new_offset
            self.order_book_sequence_gap = False
            return True

        logger.warning(
            f"⚠️ [{self.symbol}] Lighter 订单簿 offset 断档: "
            f"expected={expected_offset}, got={new_offset}"
        )
        self.order_book_sequence_gap = True
        return False

    async def _request_fresh_snapshot(self):
        """在订单簿断档或完整性异常后，请求新的快照。"""
        if not self.ws or self.market_index is None:
            return
        try:
            unsubscribe_msg = {
                "type": "unsubscribe",
                "channel": f"order_book/{self.market_index}"
            }
            subscribe_msg = {
                "type": "subscribe",
                "channel": f"order_book/{self.market_index}"
            }
            await self.ws.send(json.dumps(unsubscribe_msg))
            await asyncio.sleep(0.2)
            await self.ws.send(json.dumps(subscribe_msg))
            logger.warning(f"⚠️ [{self.symbol}] 已请求新的 Lighter 订单簿快照")
        except Exception as e:
            logger.exception(f"❌ 请求新的 Lighter 快照失败 ({self.symbol}): {e}")

    def _validate_order_book_integrity(self) -> bool:
        """校验订单簿内部一致性。"""
        try:
            if not self.lighter_order_book["bids"] or not self.lighter_order_book["asks"]:
                return True
            best_bid = max(self.lighter_order_book["bids"].keys())
            best_ask = min(self.lighter_order_book["asks"].keys())
            if best_bid >= best_ask:
                logger.warning(
                    f"⚠️ [{self.symbol}] Lighter 订单簿异常: best_bid={best_bid}, best_ask={best_ask}"
                )
                return False
            return True
        except Exception as e:
            logger.exception(f"❌ 校验 Lighter 订单簿一致性失败 ({self.symbol}): {e}")
            return False

    def _cleanup_order_book_levels(self):
        """裁剪订单簿深度，避免长期运行后本地订单簿持续膨胀。"""
        max_levels = self.orderbook_cleanup_levels
        if max_levels <= 0:
            return

        bids = self.lighter_order_book["bids"]
        asks = self.lighter_order_book["asks"]

        if len(bids) > max_levels:
            top_bids = sorted(bids.items(), key=lambda item: item[0], reverse=True)[:max_levels]
            bids.clear()
            bids.update(top_bids)

        if len(asks) > max_levels:
            top_asks = sorted(asks.items(), key=lambda item: item[0])[:max_levels]
            asks.clear()
            asks.update(top_asks)

    def get_runtime_diagnostics(self) -> Dict[str, Any]:
        """返回 Lighter WS/订单簿的运行时快照，供进程级诊断汇总。"""
        now = time.time()
        return {
            'timestamp': now,
            'snapshot_loaded': self.lighter_snapshot_loaded,
            'any_ws_gap_ms': (now - self._last_any_ws_message_ts) * 1000 if self._last_any_ws_message_ts > 0 else None,
            'order_book_gap_ms': (now - self._last_orderbook_message_ts) * 1000 if self._last_orderbook_message_ts > 0 else None,
            'last_update_gap_ms': (now - self.lighter_last_update_ts) * 1000 if self.lighter_last_update_ts > 0 else None,
            'last_notify_gap_ms': (now - self.lighter_last_notify_ts) * 1000 if self.lighter_last_notify_ts > 0 else None,
            'processing_delay_ms': max(
                0.0,
                (self.lighter_last_update_ts - self._last_orderbook_message_ts) * 1000
            ) if self.lighter_last_update_ts > 0 and self._last_orderbook_message_ts > 0 else None,
            'bids_levels': len(self.lighter_order_book["bids"]),
            'asks_levels': len(self.lighter_order_book["asks"]),
            'consecutive_ws_timeouts': self._consecutive_ws_timeouts,
            'watchdog_cooldown_remaining_ms': max(0.0, (self._watchdog_cooldown_until - now) * 1000),
            'message_count': self.message_count,
        }
    
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
        if msg_type in ["subscribed/order_book", "snapshot"]:
            self._last_orderbook_message_ts = time.time()
            logger.info(f"📸 收到 Lighter 快照消息: {self.symbol}")
            await self._handle_lighter_snapshot(data)

        elif msg_type == "update/order_book":
            order_book = data.get("order_book", {})
            if isinstance(order_book, dict):
                self._last_orderbook_message_ts = time.time()
            # ✅ 如果是第一次收到，当作快照处理
            if not self.lighter_snapshot_loaded:
                logger.info(f"📸 收到 Lighter 初始订单簿（当作快照）: {self.symbol}")
                await self._handle_lighter_snapshot(data)
            else:
                # ✅ 后续消息当作增量更新
                await self._handle_lighter_update(data)

        elif msg_type in ["update/account_orders"]:
            logger.debug(f"📨 收到订单更新消息: {data}")
            orders = data.get("orders", {}).get(str(self.market_index), [])
            for order_data in orders:
                # logger.info(f"---------order-data---------{order_data}")
                # 调用订单更新 handler
                self._on_order_update(order_data)

        elif msg_type in ["subscribed/user_stats", "update/user_stats", "snapshot/user_stats"] or channel.startswith("user_stats"):
            logger.info(f"📊 收到 Lighter user_stats 消息: {self.symbol} type={msg_type}")
            self._handle_lighter_user_stats(data)

        elif channel.startswith("market_stats") or msg_type in [
            "subscribed/market_stats",
            "update/market_stats",
            "snapshot/market_stats",
        ]:
            # logger.debug(f"📊 收到 Lighter market_stats 消息: {self.symbol} type={msg_type}")
            self._handle_lighter_market_stats(data)
                
        else:
            # 未知消息类型
            if self.message_count <= 5:
                logger.debug(f"⏭️ 跳过消息类型: {msg_type}")

    def _handle_lighter_user_stats(self, data: dict):
        """处理 Lighter user_stats 消息并缓存关键风控字段"""
        try:
            # 常见结构：{"type":"update/user_stats","stats":{...}}
            user_stats = data.get("stats")
            if not isinstance(user_stats, dict):
                # 兼容其它可能字段名
                for key in ("user_stats", "stats", "data", "payload"):
                    candidate = data.get(key)
                    if isinstance(candidate, dict):
                        user_stats = candidate
                        break

            # 仍未命中时，尝试将顶层当作 payload（排除通用元字段）
            if not isinstance(user_stats, dict):
                fallback = {
                    k: v for k, v in data.items()
                    if k not in {"type", "channel", "ts", "timestamp"}
                }
                if fallback:
                    user_stats = fallback

            if not isinstance(user_stats, dict) or not user_stats:
                logger.debug(f"⏭️ user_stats 消息无有效 payload: {data}")
                return

            leverage = self._extract_nested_decimal(
                user_stats,
                ("leverage",),
                ("total_stats", "leverage"),
                ("cross_stats", "leverage"),
            )
            collateral = self._extract_nested_decimal(
                user_stats,
                ("collateral",),
                ("balance",),
                ("total_stats", "collateral"),
            )
            portfolio_value = self._extract_nested_decimal(
                user_stats,
                ("portfolio_value",),
                ("total_stats", "portfolio_value"),
            )

            self._lighter_user_stats_raw = user_stats
            self._lighter_user_stats_ts = time.time()
            self._lighter_user_stats_parsed = {
                'leverage': leverage,
                'collateral': collateral,
                'portfolio_value': portfolio_value,
            }

            if not self._lighter_user_stats_seen:
                self._lighter_user_stats_seen = True
                logger.info(
                    "✅ 已收到 Lighter user_stats: "
                    f"leverage={leverage}, collateral={collateral}, portfolio_value={portfolio_value}"
                )
            logger.debug(
                "📊 Lighter user_stats 更新: "
                f"leverage={leverage}, collateral={collateral}, portfolio_value={portfolio_value}"
            )
        except Exception as e:
            logger.exception(f"❌ 处理 Lighter user_stats 失败: {e}")

    def get_lighter_user_stats(self) -> Optional[dict]:
        """获取最近一次 user_stats 缓存（供风控模块读取）"""
        if not self._lighter_user_stats_seen:
            return None
        return {
            'timestamp': self._lighter_user_stats_ts,
            'raw': dict(self._lighter_user_stats_raw) if isinstance(self._lighter_user_stats_raw, dict) else {},
            'parsed': dict(self._lighter_user_stats_parsed),
        }

    def _handle_lighter_market_stats(self, data: dict):
        """处理 Lighter market_stats 消息并缓存标记价"""
        try:
            market_stats = self._extract_market_stats_payload(data)
            if not isinstance(market_stats, dict) or not market_stats:
                logger.debug(f"⏭️ market_stats 消息无有效 payload: {data}")
                return

            mark_price = self._extract_nested_decimal(
                market_stats,
                ("mark_price",),
            )
            index_price = self._extract_nested_decimal(
                market_stats,
                ("index_price",),
            )

            self._lighter_market_stats_raw = market_stats
            self._lighter_market_stats_ts = time.time()
            self._lighter_market_stats_parsed = {
                'mark_price': mark_price,
                'index_price': index_price,
            }

            if not self._lighter_market_stats_seen:
                self._lighter_market_stats_seen = True
                logger.info(
                    "✅ 已收到 Lighter market_stats: "
                    f"mark_price={mark_price}, index_price={index_price}"
                )
            # logger.debug(
            #     "📊 Lighter market_stats 更新: "
            #     f"mark_price={mark_price}, index_price={index_price}"
            # )
        except Exception as e:
            logger.exception(f"❌ 处理 Lighter market_stats 失败: {e}")

    def get_lighter_market_stats(self) -> Optional[dict]:
        """获取最近一次 market_stats 缓存（供风控模块读取）"""
        if not self._lighter_market_stats_seen:
            return None
        return {
            'timestamp': self._lighter_market_stats_ts,
            'raw': dict(self._lighter_market_stats_raw) if isinstance(self._lighter_market_stats_raw, dict) else {},
            'parsed': dict(self._lighter_market_stats_parsed),
        }

    def _extract_market_stats_payload(self, data: dict) -> Any:
        candidates = [
            data.get("market_stats"),
            data.get("stats"),
            data.get("data"),
            data.get("payload"),
        ]
        for candidate in candidates:
            if isinstance(candidate, dict):
                return candidate
        fallback = {
            k: v for k, v in data.items()
            if k not in {"type", "channel", "ts", "timestamp"}
        }
        return fallback

    @staticmethod
    def _extract_nested_value(data: dict, *paths: tuple[str, ...]):
        for path in paths:
            current = data
            ok = True
            for key in path:
                if not isinstance(current, dict) or key not in current:
                    ok = False
                    break
                current = current[key]
            if ok:
                return current
        return None

    @classmethod
    def _extract_nested_decimal(cls, data: dict, *paths: tuple[str, ...]) -> Optional[Decimal]:
        value = cls._extract_nested_value(data, *paths)
        return cls._safe_decimal(value)

    @staticmethod
    def _safe_decimal(value: Any) -> Optional[Decimal]:
        if value is None:
            return None
        try:
            return Decimal(str(value))
        except Exception:
            return None

    async def _handle_lighter_snapshot(self, data: dict):
        """处理 Lighter 快照消息"""
        try:
            async with self.lighter_order_book_lock:
                # ✅ 清空订单簿
                self.lighter_order_book = {"bids": {}, "asks": {}}
                
                # ✅ 数据在 order_book 字段内
                order_book = data.get("order_book", {})
                offset = self._extract_order_book_offset(order_book)
                
                bids = order_book.get("bids", [])
                asks = order_book.get("asks", [])
                
                logger.info(
                    f"📸 Lighter 快照数据: {self.symbol}\n"
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
                self._orderbook_cleanup_counter += 1
                if self._orderbook_cleanup_counter >= self.orderbook_cleanup_interval:
                    self._cleanup_order_book_levels()
                    self._orderbook_cleanup_counter = 0
                self._update_lighter_best_prices()
                
                self.lighter_snapshot_loaded = True
                self.order_book_offset = offset
                self.order_book_sequence_gap = False
                
                logger.info(
                    f"✅ Lighter 快照加载完成: {self.symbol}\n"
                    f"   {len(self.lighter_order_book['bids'])} bids\n"
                    f"   {len(self.lighter_order_book['asks'])} asks\n"
                    f"   Best Bid: ${self.lighter_best_bid}\n"
                    f"   Best Ask: ${self.lighter_best_ask}"
                )
                if offset is not None:
                    logger.info(f"📌 Lighter 初始订单簿 offset: {offset}")
                
                # 通知回调
                await self._notify_orderbook_update_if_changed()
        
        except Exception as e:
            logger.exception(f"❌ 处理 Lighter 快照失败: {e}")
    
    async def _handle_lighter_update(self, data: dict):
        """处理 Lighter 增量更新消息"""
        if not self.lighter_snapshot_loaded:
            return
        
        try:
            request_snapshot = False
            async with self.lighter_order_book_lock:
                # ✅ 数据在 order_book 字段内
                order_book = data.get("order_book", {})
                offset = self._extract_order_book_offset(order_book)

                if offset is None:
                    logger.warning(f"⚠️ Lighter 订单簿更新缺少 offset，准备重新请求快照 ({self.symbol})")
                    request_snapshot = True
                elif not self._validate_order_book_offset(offset):
                    request_snapshot = self.order_book_sequence_gap

                if not request_snapshot:
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
                    self._orderbook_cleanup_counter += 1
                    if self._orderbook_cleanup_counter >= self.orderbook_cleanup_interval:
                        self._cleanup_order_book_levels()
                        self._orderbook_cleanup_counter = 0
                    self._update_lighter_best_prices()
                    if not self._validate_order_book_integrity():
                        request_snapshot = True
                    else:
                        # 通知回调（仅在订单簿有变化时）
                        await self._notify_orderbook_update_if_changed()

            if request_snapshot:
                await self._request_fresh_snapshot()
        
        except Exception as e:
            logger.exception(f"❌ 处理 Lighter 更新失败: {e}")
    
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
        """通知订单簿更新（不检查内容变化的内部版本）"""
        if self._orderbook_callback and not self.lighter_best_bid or not self.lighter_best_ask:
            logger.warning(
                f"⚠️ 订单簿数据不完整: {self.symbol}\n"
                f"   Best Bid: {self.lighter_best_bid}\n"
                f"   Best Ask: {self.lighter_best_ask}"
            )
            return
        
        # 格式化为标准订单簿格式
        bid_size = float(self.lighter_order_book["bids"].get(self.lighter_best_bid, 0))
        ask_size = float(self.lighter_order_book["asks"].get(self.lighter_best_ask, 0))

        ts = self.lighter_last_update_ts or time.time()
        orderbook_message_ts = self._last_orderbook_message_ts or 0.0
        processing_delay_ms = 0.0
        if orderbook_message_ts > 0:
            processing_delay_ms = max(0.0, (ts - orderbook_message_ts) * 1000)
        self._orderbook = {
            'bids': [[float(self.lighter_best_bid), bid_size]],
            'asks': [[float(self.lighter_best_ask), ask_size]],
            'timestamp': ts,
            'orderbook_message_ts': orderbook_message_ts,
            'processing_delay_ms': processing_delay_ms,
            'poll_duration_ms': 0,  # WebSocket 无延迟
            'mark_price': self._lighter_market_stats_parsed.get('mark_price')
        }
        self.client.order_book = {
                    'bids': dict(self.lighter_order_book['bids']),
                    'asks': dict(self.lighter_order_book['asks'])
                }
        self.client.best_bid = self.lighter_best_bid
        self.client.best_ask = self.lighter_best_ask
        # logger.debug("✅ Order book synced to Client")
        # logger.debug(
        #     f"📗 Lighter 订单簿更新:\n"
        #     f"   Bid: ${self.lighter_best_bid} x {bid_size}\n"
        #     f"   Ask: ${self.lighter_best_ask} x {ask_size}"
        #     f"   时间戳 {ts:.3f}"
        # )
        
        # 触发回调
        if self._orderbook_callback:
            await self._orderbook_callback(self._orderbook)
            self.lighter_last_notify_ts = ts

    async def _notify_orderbook_update_if_changed(self):
        """
        仅当订单簿内容发生变化时才触发回调，并记录真正的事件时间
        """
        try:
            fingerprint = self._make_orderbook_fingerprint()
            # 取消息自带时间（若有），否则用当前时间
            msg_ts = time.time()
            
            self.lighter_last_update_ts = float(msg_ts)

            if fingerprint == self._order_book_fingerprint:
                # 内容未变化时也要把最新事件时间往上游传，避免被 200ms 级别的 stale 阈值误判
                heartbeat_gap = 0.1  # 秒
                if (
                    self.lighter_last_notify_ts == 0.0
                    or (self.lighter_last_update_ts - self.lighter_last_notify_ts) >= heartbeat_gap
                ):
                    await self._notify_orderbook_update()
                return

            self._order_book_fingerprint = fingerprint
            await self._notify_orderbook_update()
        except Exception as e:
            logger.exception(f"❌ 通知订单簿更新失败: {e}")

    def _make_orderbook_fingerprint(self) -> int:
        """
        生成订单簿内容指纹，用于检测内容是否变化。
        仅比较前 N 档摘要，降低整本订单簿排序带来的长期运行开销。
        """
        top_levels = self.orderbook_fingerprint_levels
        bids_tuple = tuple(
            (str(price), str(size))
            for price, size in sorted(
                self.lighter_order_book["bids"].items(),
                key=lambda item: item[0],
                reverse=True
            )[:top_levels]
        )
        asks_tuple = tuple(
            (str(price), str(size))
            for price, size in sorted(
                self.lighter_order_book["asks"].items(),
                key=lambda item: item[0]
            )[:top_levels]
        )
        return hash((bids_tuple, asks_tuple))

    async def _get_depth_price(self, side: str, quantity: Decimal) -> Optional[Decimal]:
        """
        根据目标数量选择“最多吃到第 N 档”的价格。
        - side=BUY：从卖盘向上累计
        - side=SELL：从买盘向下累计
        """
        try:
            async with self.lighter_order_book_lock:
                if side == 'BUY':
                    levels = list(self.lighter_order_book["asks"].items())
                    levels.sort(key=lambda x: x[0])  # 低价优先
                else:
                    levels = list(self.lighter_order_book["bids"].items())
                    levels.sort(key=lambda x: x[0], reverse=True)  # 高价优先

            if not levels:
                return None

            max_levels = self.depth_price_max_levels
            if max_levels is not None and max_levels > 0:
                levels = levels[:max_levels]

            cumulative = Decimal('0')
            for price, size in levels:
                size_val = Decimal(str(size))
                if size_val <= 0:
                    continue
                cumulative += size_val
                if cumulative >= quantity:
                    return Decimal(str(price))

            # 若前 N 档不足，返回第 N 档价格（最多吃到该档位）
            return Decimal(str(levels[-1][0]))
        except Exception as e:
            logger.warning(f"⚠️ 获取盘口定价失败: {e}")
            return None

    def _next_client_order_index(self) -> int:
        """
        生成 48 位范围内的高唯一 client_order_index。
        规则：`((now_ms - epoch_ms) << seq_bits) | seq`。
        """
        now_ms = int(time.time() * 1000)

        # 避免同毫秒内生成重复索引，强制递增。
        if now_ms <= self._last_client_order_ms:
            now_ms = self._last_client_order_ms
            self._client_order_seq += 1
            # 极端并发保护：同毫秒序号超过上限时，推进到下一毫秒，避免复用。
            if self._client_order_seq > self._client_order_seq_max:
                now_ms += 1
                self._client_order_seq = 0
        else:
            self._client_order_seq = 0

        self._last_client_order_ms = now_ms
        delta_ms = now_ms - self._client_order_epoch_ms
        if delta_ms < 0:
            delta_ms = 0

        if delta_ms > self._client_order_max_delta_ms:
            raise ValueError(
                f"client_order_index delta_ms 超范围: {delta_ms} > {self._client_order_max_delta_ms}"
            )

        client_order_index = (delta_ms << self._client_order_seq_bits) | self._client_order_seq
        if client_order_index > self._client_order_index_max:
            raise ValueError(
                f"client_order_index 超过上限: {client_order_index} > {self._client_order_index_max}"
            )
        return client_order_index

    def _on_order_update(self, order_update: dict):
        """处理 WebSocket 订单更新（同步回调）"""
        try:
            raw_client_order_index = order_update.get('client_order_index')
            if raw_client_order_index is None:
                logger.debug(f"⏭️ 跳过无 client_order_index 的更新")
                return
            try:
                client_order_index = int(raw_client_order_index)
            except (TypeError, ValueError):
                logger.warning(f"⚠️ 跳过非法 client_order_index: {raw_client_order_index}")
                return

            real_order_id = order_update.get('order_id')
            status = str(order_update.get('status', '')).upper()
            side = "short" if order_update.get("is_ask") else "long"
            filled_base_amount = Decimal(str(order_update.get('filled_base_amount', '0')))
            if filled_base_amount == 0:
                price = Decimal(str(order_update.get('price', '0')))
            else:
                filled_quote_amount = Decimal(str(order_update.get('filled_quote_amount', '0')))
                price = (filled_quote_amount / filled_base_amount) if filled_base_amount > 0 else Decimal('0')
            size = Decimal(str(order_update.get('base_size', '0')))
            order_type = order_update.get('type', 'OPEN')  # 字段无效
            contract_id = self.client.config.contract_id

            # 某些终态更新可能不再携带成交量/成交价，避免把已有有效值覆盖为 0
            previous_data = self._order_status_data.get(client_order_index)
            if previous_data:
                previous_filled_size = Decimal(str(previous_data.get('filled_size', '0')))
                if filled_base_amount <= 0 and previous_filled_size > 0:
                    filled_base_amount = previous_filled_size
                previous_price = Decimal(str(previous_data.get('price', '0')))
                if price <= 0 and previous_price > 0:
                    price = previous_price

            data = {
                'order_id': real_order_id,
                'client_order_index': client_order_index,
                'status': status,
                'side': side,
                'order_type': order_type,
                'size': size,
                'price': price,
                'contract_id': contract_id,
                'filled_size': filled_base_amount
            }
            # 无论是否还在等待，都缓存，避免“晚到回报”丢失
            self._order_status_data[client_order_index] = data

            future = self._order_status_futures.pop(client_order_index, None)
            if future and not future.done():
                future.set_result(data)
                logger.debug(f"✅ 订单状态 Future 已完成: {client_order_index} -> {status}")

            # ✅ 日志（节流）
            order_id = order_update.get('order_id')
            raw_status = order_update.get('status')
            raw_filled_base_amount = order_update.get('filled_base_amount', 0)
            raw_filled_quote_amount = order_update.get('filled_quote_amount', 0)
            logger.info(
                f"📨 收到订单更新: client_idx={client_order_index}, order_id={order_id}, status={raw_status}, "
                f"filled_base_amount={raw_filled_base_amount}, filled_quote_amount={raw_filled_quote_amount}, "
                f"calc_filled_size={filled_base_amount}, calc_price={price}"
            )

        except Exception as e:
            logger.exception(f"❌ 处理订单更新失败: {e}")

    async def _wait_for_order_status(self, client_order_index: int, timeout: float = 1.5) -> dict:
        """等待订单状态（使用 Future）"""
        cached = self._order_status_data.pop(client_order_index, None)
        if cached:
            return cached

        future = self._order_status_futures.get(client_order_index)
        if not future:
            raise ValueError(f"No future for client_order_index: {client_order_index}")
        
        try:
            status_data = await asyncio.wait_for(future, timeout=timeout)
            return status_data
        except asyncio.TimeoutError:
            logger.warning(f"⏰ 订单状态超时 ({self.symbol}, client_idx={client_order_index})")
            # 清理
            self._order_status_futures.pop(client_order_index, None)
            raise
        except Exception as e:
            logger.exception(f"❌ 等待订单状态异常 ({self.symbol}): {e}")
            self._order_status_futures.pop(client_order_index, None)
            raise

    async def _wait_for_late_order_status(
        self,
        client_order_index: int,
        timeout: float = 3.0,
        poll_interval: float = 0.1
    ) -> Optional[dict]:
        """等待晚到的订单状态回报（用于 WS 超时后的短暂兜底）"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            cached = self._order_status_data.pop(client_order_index, None)
            if cached:
                return cached
            await asyncio.sleep(poll_interval)
        return None

    async def _wait_until_terminal_order_status(
        self,
        client_order_index: int,
        status_data: dict,
        timeout: float = 1.2,
        poll_interval: float = 0.005
    ) -> dict:
        """
        对 IN-PROGRESS 做短暂追加等待，尽量拿到终态（FILLED/CANCELED）。
        Lighter 当前无 NEW 状态，这里只处理 IN-PROGRESS。
        """
        latest = status_data
        deadline = time.time() + timeout

        while str(latest.get('status', '')).upper() == 'IN-PROGRESS' and time.time() < deadline:
            remain = deadline - time.time()
            late_status_data = await self._wait_for_late_order_status(
                client_order_index=client_order_index,
                timeout=min(0.2, max(0.0, remain)),
                poll_interval=poll_interval
            )
            if not late_status_data:
                break

            # 晚到状态缺少成交字段时，沿用上一条有效值
            prev_filled_size = Decimal(str(latest.get('filled_size', '0')))
            late_filled_size = Decimal(str(late_status_data.get('filled_size', '0')))
            if late_filled_size <= 0 and prev_filled_size > 0:
                late_status_data['filled_size'] = prev_filled_size

            prev_price = Decimal(str(latest.get('price', '0')))
            late_price = Decimal(str(late_status_data.get('price', '0')))
            if late_price <= 0 and prev_price > 0:
                late_status_data['price'] = prev_price

            latest = late_status_data

        return latest

    async def place_open_order(self,
        side: str,
        quantity: Decimal,
        price: Optional[Decimal] = None,
        retry_mode: str = 'opportunistic',
        quote_id: Optional[str] = None,
        slippage: Optional[Decimal] = None,
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
        
        注意：Lighter 使用 IOC 订单，天然就是"激进"的，
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
            client_order_index = self._next_client_order_index()

            # 防御性清理：理论上不应命中；若命中说明出现了 client_order_index 复用风险。
            stale_data = self._order_status_data.pop(client_order_index, None)
            stale_future = self._order_status_futures.pop(client_order_index, None)
            if stale_future and not stale_future.done():
                stale_future.cancel()
            if stale_data is not None or stale_future is not None:
                logger.warning(f"⚠️ 下单前发现并清理同 key 残留状态: client_idx={client_order_index}")

            self._order_status_futures[client_order_index] = future
            slippage = slippage if slippage is not None else self.slippage
            logger.info(f"Placing market order with slippage: {slippage}")

            depth_price = await self._get_depth_price(side_upper, quantity)

            # 计算最大滑点价格上限/下限
            max_slip = slippage or Decimal('0')
            base_price = Decimal(str(price)) if price is not None else None
            if base_price is None:
                base_price = self.lighter_best_ask if side_upper == 'BUY' else self.lighter_best_bid

            if base_price is not None:
                if side_upper == 'BUY':
                    cap_price = base_price * (Decimal('1') + Decimal(str(max_slip)))
                else:
                    cap_price = base_price * (Decimal('1') - Decimal(str(max_slip)))
            else:
                cap_price = None

            # 定价规则：
            # 1) 首次下单：用“盘口档位价”和“最大滑点价”做夹逼，尽量贴近盘口
            # 2) 重试下单：直接放宽到最大滑点价
            if retry_mode == 'aggressive':
                order_price = cap_price
                logger.info(
                    f"💡 最大滑点定价: side={side_upper}, qty={quantity}, "
                    f"slip={max_slip}, price={order_price}"
                )
            else:
                if depth_price is not None and cap_price is not None:
                    if side_upper == 'BUY':
                        order_price = min(depth_price, cap_price)
                    else:
                        order_price = max(depth_price, cap_price)
                    logger.info(
                        f"💡 盘口/滑点夹逼定价: side={side_upper}, qty={quantity}, "
                        f"max_levels={self.depth_price_max_levels}, depth_price={depth_price}, "
                        f"cap_price={cap_price}, price={order_price}"
                    )
                elif depth_price is not None:
                    order_price = depth_price
                else:
                    order_price = cap_price

            if order_price is None:
                logger.error(f"❌ 无法获取有效下单价格: side={side_upper}, qty={quantity}")
                return {
                    'success': False,
                    'order_id': None,
                    'error': 'Order price unavailable'
                }
            logger.info(f"📤 {self.exchange_name} 下市价单: {side_upper} {quantity} @ {order_price}")
            # logger.info(
            #     f"📤 {self.exchange_name} 下单:\n"
            #     f"   市场: {self.market_index}\n"
            #     f"   方向: {side_upper}\n"
            #     f"   原始数量: {quantity} (type: {type(quantity)})\n"
            #     f"   滑点: {slippage}\n"
            #     f"   价格: {order_price}\n"
            #     f"   base_amount_multiplier: {self.client.base_amount_multiplier}\n"
            #     f"   price_multiplier: {self.client.price_multiplier}"
            # )
            # 计算 base_amount
            base_amount_decimal = Decimal(str(quantity)) * Decimal(str(self.client.base_amount_multiplier))
            base_amount = int(base_amount_decimal)
            
            # 计算 price
            price_decimal = Decimal(str(order_price)) * Decimal(str(self.client.price_multiplier))
            price_int = int(price_decimal)
            
            # logger.info(
            #     f"📋 计算后的订单参数:\n"
            #     f"   base_amount (decimal): {base_amount_decimal}\n"
            #     f"   base_amount (int): {base_amount}\n"
            #     f"   price (decimal): {price_decimal}\n"
            #     f"   price (int): {price_int}"
            # )
            
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
                'time_in_force': self.client.lighter_client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                'reduce_only': False,
                'trigger_price': 0,
                'order_expiry': 0,
            }
            
            # logger.info(
            #     f"📋 Lighter 订单参数:\n"
            #     f"   market_index: {order_params['market_index']}\n"
            #     f"   client_order_index: {order_params['client_order_index']}\n"
            #     f"   base_amount: {order_params['base_amount']}\n"
            #     f"   price: {order_params['price']}\n"
            #     f"   is_ask: {order_params['is_ask']}"
            # )
            
            # ✅ 签名订单
            tx_info, tx_hash, error = await self.client.lighter_client.create_order(**order_params)
            
            if error is not None:
                logger.exception(f"❌ 创建订单失败: {error}")
                return {
                    'success': False,
                    'order_id': None,
                    'error': f'Create order error: {error}'
                }
            
            # ✅ 发送交易
            # tx_hash = await self.client.lighter_client.send_tx(
            #     tx_type=self.client.lighter_client.TX_TYPE_CREATE_ORDER,
            #     tx_info=tx_info
            # )
            
            if tx_hash is None:
                logger.error("❌ send_tx 返回 None")
                return {
                    'success': False,
                    'order_id': None,
                    'error': 'Order submission returned None'
                }
            wait_start_time = time.time()
            place_duration = (wait_start_time - order_start_time) * 1000

            logger.info(f"✅ {self.exchange_name} 下单完成: tx_hash={tx_hash}, 下单耗时:{place_duration:.2f}ms")

            # ✅ 等待订单状态（WebSocket 更新）
            logger.info(f"⏳ 开始等待订单状态: client_idx={client_order_index}")
            order_info = {
                    'success': False,
                    'order_id': '',
                    'error': None,
                    'filled_price': None,
                    'filled_quantity': None,
                    'partial_fill': False,
                    'timestamp': None,
                    'place_duration_ms': place_duration,
                    'execution_duration_ms': None,
                }
            canceled_statuses = [
                'CANCELED', 'CANCELED-NOT-ENOUGH-LIQUIDITY', 'CANCELED-POSITION_NOT_ALLOWED',
                'CANCELED-MARGIN-NOT-ALLOWED', 'CANCELED-TOO-MUCH-SLIPPAGE', 'CANCELED-SELF-TRADE',
                'CANCELED-EXPIRED', 'CANCELED-OCO', 'CANCELED-CHILD', 'CANCELED-LIQUIDATION'
            ]

            # 下单成功后，判断订单是否成交
            try:
                status_data = await self._wait_for_order_status(client_order_index, timeout=1.5)
                status_data = await self._wait_until_terminal_order_status(
                    client_order_index=client_order_index,
                    status_data=status_data,
                    timeout=1.2,
                    poll_interval=0.005
                )
                status = str(status_data.get('status', '')).upper()
                real_order_id = status_data.get('order_id')
                filled_size_from_ws = Decimal(status_data.get('filled_size', '0'))
                price_from_ws = status_data.get('price', order_price)
                
                logger.info(f"{self.exchange_name} 订单状态: client_idx={client_order_index} -> {status} (order_id={real_order_id})")
                wait_end_time = time.time()
                wait_duration = (wait_end_time - wait_start_time) * 1000
                logger.info(f"⏱️ {self.exchange_name} 等待状态耗时: {wait_duration:.2f} ms, 状态: {status}")
                total_duration = (wait_end_time - order_start_time) * 1000
                logger.info(f"⏱️ {self.exchange_name} 下单总耗时: {total_duration:.2f} ms")

                order_info['order_id'] = real_order_id
                order_info['filled_price'] = price_from_ws
                order_info['filled_quantity'] = filled_size_from_ws
                order_info['timestamp'] = time.time()
                order_info['execution_duration_ms'] = wait_duration
                
                if filled_size_from_ws > 0 and filled_size_from_ws < quantity:
                    order_info['partial_fill'] = True
                    
                    logger.warning(f"订单 ID: {real_order_id}出现部分成交, 成交数量{filled_size_from_ws} / {quantity}, 成交价格${price_from_ws}")

                if status in canceled_statuses:
                    msg = f"✅ 订单被取消: {real_order_id}, 订单状态{status}"
                    order_info['error'] = msg
                    logger.info(msg)
                
                elif status in ['FILLED']:                    
                    logger.info(f"✅ Lighter 市价单成交: {filled_size_from_ws} @${price_from_ws}({real_order_id})")
                    order_info['success'] = True
                elif status == 'IN-PROGRESS':
                    order_info['unknown_status'] = True
                    order_info['retryable'] = False
                    order_info['error'] = (
                        f'订单状态仍为 IN-PROGRESS(client_index={client_order_index})，'
                        f'已禁止自动重试以避免重复下单'
                    )
                    logger.warning(f"⚠️ {order_info['error']}")
                    # 未知状态
                else:
                    logger.warning(f"⚠️ 未知订单状态: {status}")
                    order_info['error'] = f'⚠️ 未知订单状态: {status}'

                return order_info
            except asyncio.TimeoutError:
                logger.warning(f"⏰ 订单状态超时 (client_idx={client_order_index})，进入延迟确认窗口")
                logger.info(f"⏱️ {self.exchange_name} 从下单到超时共耗时: {(time.time() - order_start_time) * 1000:.2f} ms")

                order_info['timestamp'] = time.time()
                order_info['execution_duration_ms'] = (order_info['timestamp'] - wait_start_time) * 1000

                late_status_data = await self._wait_for_late_order_status(
                    client_order_index=client_order_index,
                    timeout=3.0,
                    poll_interval=0.1
                )

                if late_status_data:
                    late_status_data = await self._wait_until_terminal_order_status(
                        client_order_index=client_order_index,
                        status_data=late_status_data,
                        timeout=1.2,
                        poll_interval=0.005
                    )
                    status = str(late_status_data.get('status', '')).upper()
                    real_order_id = late_status_data.get('order_id')
                    filled_size_from_ws = Decimal(late_status_data.get('filled_size', '0'))
                    price_from_ws = late_status_data.get('price', order_price)
                    order_info['order_id'] = real_order_id
                    order_info['filled_price'] = price_from_ws
                    order_info['filled_quantity'] = filled_size_from_ws
                    if filled_size_from_ws > 0 and filled_size_from_ws < quantity:
                        order_info['partial_fill'] = True
                    if status in ['FILLED']:
                        order_info['success'] = True
                        order_info['error'] = 'Timeout but confirmed by late websocket update'
                    elif status in canceled_statuses:
                        order_info['success'] = False
                        order_info['error'] = f'Order {status} (late websocket update)'
                    elif status == 'IN-PROGRESS':
                        order_info['success'] = False
                        order_info['unknown_status'] = True
                        order_info['retryable'] = False
                        order_info['error'] = (
                            f'订单状态仍为 IN-PROGRESS(client_index={client_order_index})，'
                            f'已禁止自动重试以避免重复下单'
                        )
                    else:
                        order_info['success'] = False
                        order_info['error'] = f'Unknown status {status} (late websocket update)'
                else:
                    # 关键：未知状态不应被当作“可重试失败”，否则容易重复下单
                    order_info['success'] = False
                    order_info['order_id'] = str(client_order_index)
                    order_info['filled_price'] = Decimal('0')
                    order_info['filled_quantity'] = Decimal('0')
                    order_info['unknown_status'] = True
                    order_info['retryable'] = False
                    order_info['error'] = (
                        f'订单状态未知(client_index={client_order_index})，'
                        f'已禁止自动重试以避免重复下单'
                    )
                return order_info
   
            except Exception as wait_e:
                logger.exception(f"❌ 等待状态异常: {wait_e}")
                logger.info(f"⏱️ {self.exchange_name} 从下单到报错共耗时: {(time.time() - order_start_time) * 1000:.2f} ms")
                order_info['timestamp'] = time.time()
                order_info['execution_duration_ms'] = (order_info['timestamp'] - wait_start_time) * 1000
                order_info['success'] = False
                order_info['error'] = f'ws超时错误{wait_e}，client_index={client_order_index}'
                
        except lighter.exceptions.ApiException as le:
            logger.info(f"⏱️ {self.exchange_name} 从下单到报错共耗时: {(time.time() - order_start_time) * 1000:.2f} ms")
            raise
                
        except Exception as e:
            logger.exception(f"❌ {self.exchange_name} 下单失败: {e}")
            logger.info(f"⏱️ {self.exchange_name} 从下单到报错共耗时: {(time.time() - order_start_time) * 1000:.2f} ms")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'order_id': None,
                'error': str(e)
            }

    async def get_latest_orderbook(self, quantity: Optional[Decimal]) -> Optional[Dict]:
        """获取最新订单簿"""
        orderbook = self._orderbook or await self.client.get_orderbook()
        if not isinstance(orderbook, dict):
            return orderbook
        merged_orderbook = dict(orderbook)
        if merged_orderbook.get('mark_price') is None:
            merged_orderbook['mark_price'] = self._lighter_market_stats_parsed.get('mark_price')
        return merged_orderbook
    
    async def get_position(self, symbol: str) -> Optional[dict]:
        try :
            position_snapshot = None
            if hasattr(self.client, 'get_position_snapshot'):
                try:
                    position_snapshot = await self.client.get_position_snapshot()
                except Exception:
                    position_snapshot = None

            if isinstance(position_snapshot, dict):
                position_size = Decimal(str(position_snapshot.get('position', '0')))
                sign = int(position_snapshot.get('sign', 1) or 1)
                if position_size != 0:
                    logger.info(
                        f"📊 lighter 持仓:  {'+' if sign == 1 else '-'}{position_size} {position_snapshot.get('symbol', symbol)} @ {position_snapshot.get('avg_entry_price')}"
                    )
                    return {
                        'symbol': symbol,
                        'side': 'long' if sign == 1 else 'short',
                        'size': position_size,
                        'entry_price': position_snapshot.get('avg_entry_price'),
                        'unrealized_pnl': position_snapshot.get('unrealized_pnl'),
                        'liquidation_price': position_snapshot.get('liquidation_price'),
                    }

            position = await self.client.get_position_info()

            if position and Decimal(position.position) != 0:
                logger.info(
                    f"📊 lighter 持仓:  {'+' if position.sign == 1 else '-'}{position.position} {position.symbol} @ {position.avg_entry_price}"
                )
                return {
                    'symbol': symbol,
                    'side': 'long' if position.sign == 1 else 'short',
                    'size': Decimal(position.position),
                    'entry_price': position.avg_entry_price,
                    'unrealized_pnl': position.unrealized_pnl,
                    'liquidation_price': getattr(position, 'liquidation_price', None),
                    }
            else:
                logger.info(f"📊 {self.exchange_name} 无持仓: {symbol}")
                return {
                    'symbol': symbol,
                    'side': 'neutral',
                    'size': 0,
                    'entry_price': '--',
                    'unrealized_pnl': 0,
                    'liquidation_price': None,
                }
            
        except Exception as e:
            logger.exception(f"❌ {self.exchange_name} 获取持仓失败: {e}", exc_info=True)
            return None
    async def get_trade_volume(self) -> Decimal:
        """
        获取当前交易量（复用 client 方法）
        
        Returns:
            Decimal: 交易量
        """
        try:
            return Decimal('0')
        except Exception as e:
            logger.exception(f"❌ lighter 获取交易量失败: {e}", exc_info=True)
            return Decimal('0')
        
    async def get_balance(self) -> Decimal:
        """
        获取账户交易股权余额（复用 client 方法）
        
        Returns:
            Decimal: 余额
        """
        try:
            balance_info = await self.client.get_portfolio()
            return Decimal(balance_info.get('balance', '0'))
        except Exception as e:
            logger.exception(f"❌ lighter 获取余额失败: {e}", exc_info=True)
            return Decimal('0')
