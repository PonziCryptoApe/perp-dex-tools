"""
对冲套利监控器
监控 Extended 和 Lighter 价差，执行对冲套利策略

策略逻辑:
1. 开仓信号: (ext_bid - lighter_ask) / avg_bid > 0.05%
   - Extended 开空 (卖出)
   - Lighter 开多 (买入)

2. 平仓信号: 价差 < 0%
   - Extended 平空 (买入)
   - Lighter 平多 (卖出)

用法:
    python arbitrage/hedge_monitor.py --symbol BTC --quantity 0.01
    python arbitrage/hedge_monitor.py --symbol ETH --quantity 0.1 --open-threshold 0.08
"""

import asyncio
import argparse
import logging
import sys
import time
from pathlib import Path
from decimal import Decimal
from datetime import datetime
from typing import Optional, Dict, Tuple
from dataclasses import dataclass
from x10.perpetual.orders import TimeInForce, OrderSide

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))
from  exchanges.base import OrderResult, OrderInfo, query_retry

from exchanges.extended import ExtendedClient
from exchanges.lighter import LighterClient
from helpers.lark_bot import LarkBot
from helpers.util import Config
import os
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    # level=logging.INFO,
    level=os.getenv("LOG_LEVEL", "INFO").upper(),  # 默认 INFO
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True
)
logger = logging.getLogger(__name__)
# from helpers.logger import get_logger
# logger = get_logger(__name__, log_file='hedge_monitor.log')

@dataclass
class Position:
    """持仓信息"""
    symbol: str
    quantity: Decimal
    ext_entry_price: Decimal  # Extended 开仓价格
    lighter_entry_price: Decimal  # Lighter 开仓价格
    open_time: datetime
    open_spread_pct: float  # 开仓时的价差百分比
    
    def __str__(self):
        return (
            f"Position({self.symbol}, "
            f"qty={self.quantity}, "
            f"ext={self.ext_entry_price}, "
            f"lighter={self.lighter_entry_price}, "
            f"spread={self.open_spread_pct:.4f}%)"
        )


# class SimpleConfig:
#     """简单配置对象"""
#     def __init__(self, ticker: str, quantity: Decimal):
#         self.ticker = ticker
#         self.contract_id = f"{ticker}-USD"
#         self.tick_size = Decimal('0.1')
#         self.quantity = quantity
#         self.close_order_side = 'sell'
#         self.leverage = 1
#         self.order_type = 'market'
#         self.size = quantity


class HedgeMonitor:
    """对冲套利监控器"""
    
    def __init__(
        self,
        symbol: str,
        quantity: Decimal,
        open_threshold_pct: float = 0.06,
        close_threshold_pct: float = 0.0,
        check_interval: float = 1.0,
        lark_token: Optional[str] = None
    ):
        """
        Args:
            symbol: 交易币种 (如 BTC, ETH)
            quantity: 开仓数量
            open_threshold_pct: 开仓阈值（百分比）
            close_threshold_pct: 平仓阈值（百分比）
            check_interval: 检查间隔（秒）
            lark_token: 飞书 Bot Token
        """
        self.symbol = symbol
        self.quantity = quantity
        self.open_threshold_pct = open_threshold_pct
        self.close_threshold_pct = close_threshold_pct
        self.check_interval = check_interval
        
        # 交易所客户端
        self.extended_client: Optional[ExtendedClient] = None
        self.lighter_client: Optional[LighterClient] = None
        self.extended_contract_id = None
        
        # 持仓状态
        self.position: Optional[Position] = None
        self.is_running = False
        self.stop_flag = False
        
        # 飞书通知
        self.lark_bot: Optional[LarkBot] = None
        if lark_token:
            self.lark_bot = LarkBot(lark_token)
        self.buy_notified = False
        self.sell_notified = False
        
        # Lighter 订单簿数据
        self.lighter_market_index = None
        self.lighter_order_book = {"bids": {}, "asks": {}}
        self.lighter_best_bid = None
        self.lighter_best_ask = None
        self.lighter_order_book_ready = False
        self.lighter_snapshot_loaded = False
        self.lighter_order_book_lock = asyncio.Lock()
        self.lighter_ws_task = None
    def _to_float(self, v, default=0.0):
        try:
            if v is None:
                return default
            return float(v)
        except (ValueError, TypeError):
            return default
        
    async def initialize(self):
        """初始化交易所连接"""
        try:
            config_dict = {
                'exchange': 'extended',
                'ticker': self.symbol,
                'quantity': self.quantity,
                'iterations': 1,  # hedge_monitor 不需要迭代
                'tick_size': Decimal('0.1'),  # 初始值，会被覆盖
                'contract_id': '',  # 空字符串，会被覆盖
                # 'side': 'sell',  # hedge 策略开仓方向
                'take_profit': 0,
                'close_order_side': 'sell',
                'order_type': 'market',
            }
            config = Config(config_dict)
            
            # ========== Extended 初始化 ==========
            logger.info(f"🔌 初始化 Extended ({self.symbol})...")
            
            # 1. 创建 client(可增加一个更新config的方法，以便后续更新tick_size等)
            self.extended_client = ExtendedClient(config)
            
            # 2. 获取合约信息（在 connect 之前调用）
            logger.info("📋 获取 Extended 合约信息...")
            ext_contract_id, ext_tick_size = await self.extended_client.get_contract_attributes()
            # print(f"Extended 合约信息: contract_id={ext_contract_id}, tick_size={ext_tick_size}")
            self.extended_contract_id = ext_contract_id
            self.extended_client.config.contract_id = ext_contract_id
            self.extended_client.config.tick_size = ext_tick_size
            print(f"Extended 合约信息: contract_id={self.extended_client.config.contract_id}, tick_size={self.extended_client.config.tick_size}")
            # return
            # 3. 验证最小下单量
            if hasattr(self.extended_client, 'min_order_size'):
                min_size = self.extended_client.min_order_size
                if self.quantity < min_size:
                    raise ValueError(f"下单量不足: {self.quantity} < {min_size}")
            
            logger.info(
                f"✅ Extended 合约信息:\n"
                f"   contract_id: {ext_contract_id}\n"
                f"   tick_size: {ext_tick_size}\n"
                f"   min_order_size: {getattr(self.extended_client, 'min_order_size', 'N/A')}"
            )
            
            # 4. 连接 WebSocket
            logger.info("🔌 连接 Extended WebSocket...")
            await self.extended_client.connect()
            
            # 5. 等待 WebSocket 启动
            logger.info("⏳ 等待 Extended WebSocket 预热（3秒）...")
            await asyncio.sleep(3)
            
            # 6. 等待订单簿就绪
            logger.info("🔍 等待 Extended 订单簿数据...")
            if not await self._wait_for_extended_orderbook(max_wait=30):
                raise Exception("Extended 订单簿初始化超时")
            logger.info("✅ Extended 已就绪")
            
            # 初始化 Lighter
            logger.info(f"🔌 连接 Lighter ({self.symbol})...")
            self.lighter_client = LighterClient(config)
            await self.lighter_client.connect()
            
            # ✅ 获取 Lighter 合约信息
            contract_id, tick_size = await self.lighter_client.get_contract_attributes()
            self.lighter_client.config.contract_id = contract_id
            self.lighter_client.config.tick_size = tick_size
            
            self.lighter_market_index = await self._get_lighter_market_index()
            logger.info(f"✅ Lighter 已连接 (market_id: {self.lighter_market_index})")
            
            # 启动 Lighter WebSocket
            logger.info("📡 启动 Lighter WebSocket...")
            self.lighter_ws_task = asyncio.create_task(self._handle_lighter_ws())
            
            logger.info("⏳ 等待 Lighter 订单簿数据...")
            if not await self._wait_for_lighter_orderbook():
                raise Exception("Lighter 订单簿初始化超时")
            logger.info("✅ Lighter 已就绪")
            
            logger.info("🎯 所有交易所连接完成")
            
        except Exception as e:
            logger.error(f"❌ 初始化失败: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    async def _wait_for_extended_orderbook(self, max_wait: int = 30):
        """等待 Extended 订单簿就绪"""
        start = time.time()
        attempt = 0
        last_error = None

        while time.time() - start < max_wait:
            attempt += 1
            try:
                bid, ask, _, bid_size, ask_size = await self.extended_client.fetch_bbo_prices_extended(
                    self.extended_client.config.contract_id
                )
                print(f"Extended 订单簿数据: bid={bid}, ask={ask}, bid_size={bid_size}, ask_size={ask_size}")
                # if bid > 0 and ask > 0 and bid_size > 0 and ask_size > 0:
                #     elapsed = time.time() - start
                #     self.extended_client.config.contract_id
                #     logger.info(
                #         f"✅ Extended 订单簿就绪 ({elapsed:.1f}s, 第 {attempt} 次尝试) "
                #         f"Bid: ${bid:.2f}, Ask: ${ask:.2f}"
                #     )
                #     return True
                # bid = self._to_float(bid)
                # ask = self._to_float(ask)
                # bid_size = self._to_float(bid_size)
                # ask_size = self._to_float(ask_size)

                if bid and ask:
                    elapsed = time.time() - start
                    logger.info(
                        f"✅ Extended 订单簿就绪 ({elapsed:.1f}s, 第 {attempt} 次尝试) "
                        f"Bid: ${bid:.2f}, Ask: ${ask:.2f}"
                    )
                    return True
                else:
                    logger.debug(
                        f"尝试 {attempt}: 数据不完整 - "
                        f"bid={bid}, ask={ask}, bid_size={bid_size}, ask_size={ask_size}"
                    )
        
            except Exception as e:
                last_error = str(e)
                if "orderbook is None" in last_error:
                    logger.debug(f"尝试 {attempt}: Extended 订单簿未初始化")
                else:
                    logger.debug(f"尝试 {attempt}: {last_error}")
            
            await asyncio.sleep(1)
    
        logger.error(
            f"❌ Extended 订单簿未就绪 (超时 {max_wait}s, 共 {attempt} 次尝试)\n"
            f"   最后错误: {last_error}"
        )
        return False
    async def _get_lighter_market_index(self) -> int:
        """获取 Lighter market index"""
        import requests
        
        url = "https://mainnet.zklighter.elliot.ai/api/v1/orderBooks"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            for market in data.get("order_books", []):
                if market["symbol"] == self.symbol:
                    return market["market_id"]
            
            raise Exception(f"未找到 {self.symbol} 的市场")
        except Exception as e:
            logger(f"获取 market index 失败: {e}")
            raise
    
    async def _handle_lighter_ws(self):
        """处理 Lighter WebSocket（订单簿更新）"""
        import websockets
        import json
        
        url = "wss://mainnet.zklighter.elliot.ai/stream"
        reconnect_count = 0
        
        while not self.stop_flag:
            try:
                logger.info("🔌 连接 Lighter WebSocket...")
                await self._reset_lighter_orderbook()
                
                async with websockets.connect(url) as ws:
                    subscribe_msg = {
                        "type": "subscribe",
                        "channel": f"order_book/{self.lighter_market_index}"
                    }
                    await ws.send(json.dumps(subscribe_msg))
                    logger.info(f"📡 已订阅: order_book/{self.lighter_market_index}")
                    
                    reconnect_count = 0
                    
                    while not self.stop_flag:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=30)
                            data = json.loads(msg)
                            msg_type = data.get("type")
                            
                            async with self.lighter_order_book_lock:
                                if msg_type == "subscribed/order_book":
                                    await self._handle_lighter_snapshot(data)
                                elif msg_type == "update/order_book":
                                    if self.lighter_snapshot_loaded:
                                        await self._handle_lighter_update(data)
                                elif msg_type == "ping":
                                    await ws.send(json.dumps({"type": "pong"}))
                        
                        except asyncio.TimeoutError:
                            logger.warning("⏰ WebSocket 超时")
                            break
                        except Exception as e:
                            logger.error(f"处理消息错误: {e}")
                            break
            
            except Exception as e:
                logger.error(f"WebSocket 错误: {e}")
            
            if not self.stop_flag:
                reconnect_count += 1
                wait_time = min(5, reconnect_count)
                logger.info(f"⏳ {wait_time}秒后重连...")
                await asyncio.sleep(wait_time)
    
    async def _reset_lighter_orderbook(self):
        """重置订单簿"""
        async with self.lighter_order_book_lock:
            self.lighter_order_book["bids"].clear()
            self.lighter_order_book["asks"].clear()
            self.lighter_snapshot_loaded = False
            self.lighter_best_bid = None
            self.lighter_best_ask = None
            self.lighter_order_book_ready = False
    
    def _parse_order_entry(self, entry) -> Optional[Tuple[Decimal, Decimal]]:
        """解析订单条目"""
        try:
            if isinstance(entry, dict):
                return (Decimal(str(entry['price'])), Decimal(str(entry['size'])))
            elif isinstance(entry, list) and len(entry) >= 2:
                return (Decimal(str(entry[0])), Decimal(str(entry[1])))
            return None
        except Exception as e:
            logger.debug(f"解析订单失败: {e}")
            return None
    
    async def _handle_lighter_snapshot(self, data):
        """处理订单簿快照"""
        try:
            order_book = data.get("order_book", {})
            
            self.lighter_order_book["bids"].clear()
            self.lighter_order_book["asks"].clear()
            
            for bid in order_book.get("bids", []):
                parsed = self._parse_order_entry(bid)
                if parsed:
                    price, size = parsed
                    if size > 0:
                        self.lighter_order_book["bids"][price] = size
            
            for ask in order_book.get("asks", []):
                parsed = self._parse_order_entry(ask)
                if parsed:
                    price, size = parsed
                    if size > 0:
                        self.lighter_order_book["asks"][price] = size
            
            self._update_lighter_best_levels()
            self.lighter_snapshot_loaded = True
            self.lighter_order_book_ready = True
            
            logger.info(f"✅ 订单簿快照加载: {len(self.lighter_order_book['bids'])} bids, "
                       f"{len(self.lighter_order_book['asks'])} asks")
        
        except Exception as e:
            logger.error(f"处理快照错误: {e}")
    
    async def _handle_lighter_update(self, data):
        """处理订单簿更新"""
        try:
            order_book = data.get("order_book", {})
            
            for bid in order_book.get("bids", []):
                parsed = self._parse_order_entry(bid)
                if parsed:
                    price, size = parsed
                    if size > 0:
                        self.lighter_order_book["bids"][price] = size
                    else:
                        self.lighter_order_book["bids"].pop(price, None)
            
            for ask in order_book.get("asks", []):
                parsed = self._parse_order_entry(ask)
                if parsed:
                    price, size = parsed
                    if size > 0:
                        self.lighter_order_book["asks"][price] = size
                    else:
                        self.lighter_order_book["asks"].pop(price, None)
            
            self._update_lighter_best_levels()
        
        except Exception as e:
            logger.error(f"处理更新错误: {e}")
    
    def _update_lighter_best_levels(self):
        """更新最佳买卖价"""
        if self.lighter_order_book["bids"]:
            self.lighter_best_bid = max(self.lighter_order_book["bids"].keys())
        else:
            self.lighter_best_bid = None
        
        if self.lighter_order_book["asks"]:
            self.lighter_best_ask = min(self.lighter_order_book["asks"].keys())
        else:
            self.lighter_best_ask = None
    
    async def _wait_for_lighter_orderbook(self, max_wait: int = 30):
        """等待订单簿就绪"""
        start = time.time()
        while time.time() - start < max_wait:
            if self.lighter_order_book_ready and self.lighter_best_bid and self.lighter_best_ask:
                logger.info(f"✅ Lighter 订单簿就绪 ({time.time() - start:.1f}s)")
                return True
            await asyncio.sleep(1)
        
        logger.warning(f"⚠️ Lighter 订单簿未就绪")
        return False
    
    async def fetch_prices(self) -> Optional[Dict]:
        """获取当前价格"""
        try:
            # Extended BBO
            ext_bid, ext_ask, _, ext_bid_size, ext_ask_size = \
                await self.extended_client.fetch_bbo_prices_extended(
                    self.extended_client.config.contract_id
                )
            
            # ✅ 统一转换为 float
            ext_bid = float(ext_bid) if ext_bid is not None else 0.0
            ext_ask = float(ext_ask) if ext_ask is not None else 0.0
            ext_bid_size = float(ext_bid_size) if ext_bid_size is not None else 0.0
            ext_ask_size = float(ext_ask_size) if ext_ask_size is not None else 0.0
            
            if ext_bid <= 0 or ext_ask <= 0:
                return None
            
            # Lighter BBO
            async with self.lighter_order_book_lock:
                if not self.lighter_best_bid or not self.lighter_best_ask:
                    return None
                
                lighter_bid = float(self.lighter_best_bid)
                lighter_ask = float(self.lighter_best_ask)
                lighter_bid_size = float(self.lighter_order_book["bids"].get(self.lighter_best_bid, 0))
                lighter_ask_size = float(self.lighter_order_book["asks"].get(self.lighter_best_ask, 0))
            
            return {
                'ext_bid': ext_bid,
                'ext_ask': ext_ask,
                'ext_bid_size': ext_bid_size,
                'ext_ask_size': ext_ask_size,
                'lighter_bid': lighter_bid,
                'lighter_ask': lighter_ask,
                'lighter_bid_size': lighter_bid_size,
                'lighter_ask_size': lighter_ask_size,
            }
        
        except Exception as e:
            logger.error(f"获取价格失败: {e}")
            return None
    
    def calculate_spread_open(self, prices: Dict) -> Tuple[float, float]:
        """
        计算开仓价差

        Returns:
            (spread_value, spread_pct)
            spread_value: ext_bid - lighter_ask
            spread_pct: spread / avg_bid * 100
        """
        ext_bid = prices['ext_bid']
        lighter_ask = prices['lighter_ask']
        lighter_bid = prices['lighter_bid']
        ext_ask = prices["ext_ask"]
        
        spread_value = ext_bid - lighter_ask
        avg_mid = (ext_bid + ext_ask + lighter_ask + lighter_bid) / 4
        spread_pct = (spread_value / avg_mid) * 100

        return spread_value, spread_pct
    
    def calculate_spread_close(self, prices: Dict) -> Tuple[float, float]:
        """
        计算平仓价差

        Returns:
            (spread_value, spread_pct)
            spread_value: ext_bid - lighter_ask
            spread_pct: spread / avg_bid * 100
        """
        ext_bid = prices['ext_bid']
        lighter_ask = prices['lighter_ask']
        lighter_bid = prices['lighter_bid']
        ext_ask = prices["ext_ask"]
        
        spread_value = lighter_bid - ext_ask
        avg_mid = (ext_bid + ext_ask + lighter_ask + lighter_bid) / 4
        spread_pct = (spread_value / avg_mid) * 100

        return spread_value, spread_pct

    def check_depth(self, prices: Dict) -> bool:
        """
        检查订单簿深度是否足够
        
        Returns:
            True: 深度足够
            False: 深度不足
        """
        min_size = float(self.quantity)
        
        ext_bid_size = prices['ext_bid_size']
        lighter_ask_size = prices['lighter_ask_size']
        
        if ext_bid_size < min_size:
            logger.warning(f"⚠️ Extended bid 深度不足: {ext_bid_size} < {min_size}")
            return False
        
        if lighter_ask_size < min_size:
            logger.warning(f"⚠️ Lighter ask 深度不足: {lighter_ask_size} < {min_size}")
            return False
        
        return True
    
    async def open_position(self, prices: Dict, spread_pct: float):
        """
        开仓：Extended 开空 + Lighter 开多
        """
        try:
            logger.info(f"🔓 开始开仓...")
            logger.info(f"   Extended Bid: ${prices['ext_bid']:.2f}")
            logger.info(f"   Lighter Ask: ${prices['lighter_ask']:.2f}")
            logger.info(f"   价差: {spread_pct:.4f}%")
            
            # Extended 开空 (卖出)
            logger.info(f"📤 Extended 开空 {self.quantity}...")
            logger.info(f"   合约: {self.extended_client.config.contract_id}")
            ext_result = await self.place_extended_market_order(
                self.extended_contract_id,
                self.quantity,
                prices,
                'sell'
            )
            
            if not ext_result.success:
                logger.error(f"❌ Extended 开空失败: {ext_result.error_message}")
                return False
            
            ext_price = ext_result.price
            logger.info(f"✅ Extended 开空成功: {ext_price}")
            
            # Lighter 开多 (买入)
            logger.info(f"📥 Lighter 开多 {self.quantity}...")
            logger.info(f"   合约: {self.lighter_client.config.contract_id}")
            lighter_result = await self.place_lighter_market_order(
                self.lighter_client.config.contract_id,
                self.quantity,
                prices,
                'buy'
            )
            logger.info("Lighter_result.success:", lighter_result.success)
            if not lighter_result.success:
                logger.error(f"❌ Lighter 开多失败: {lighter_result.error_message}")
                # TODO: 回滚 Extended 订单
                logger.warning("⚠️ 需要手动平仓 Extended 空单！")
                return False
            
            lighter_price = lighter_result.price
            logger.info(f"✅ Lighter 开多成功: {lighter_price}")
            
            # 记录持仓
            self.position = Position(
                symbol=self.symbol,
                quantity=self.quantity,
                ext_entry_price=ext_price,
                lighter_entry_price=lighter_price,
                open_time=datetime.now(),
                open_spread_pct=spread_pct
            )
            
            logger.info(f"🎉 开仓完成: {self.position}")
            
            # 发送通知
            await self._send_open_notification(prices, spread_pct)
            
            return True
        
        except Exception as e:
            logger.error(f"❌ 开仓失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    async def close_position(self, prices: Dict, spread_pct: float):
        """
        平仓：Extended 平空 + Lighter 平多
        """
        try:
            logger.info(f"🔒 开始平仓...")
            logger.info(f"   当前价差: {spread_pct:.4f}%")
            
            # Extended 平空 (买入)
            logger.info(f"📥 Extended 平空 {self.quantity}...")
            ext_result = await self.place_extended_market_order(
                self.extended_contract_id,
                self.quantity,
                prices,
                'buy'
            )
            
            if not ext_result.success:
                logger.error(f"❌ Extended 平空失败: {ext_result.error_message}")
                return False
            
            ext_close_price = ext_result.price
            logger.info(f"✅ Extended 平空成功: {ext_close_price}")
            
            # Lighter 平多 (卖出)
            logger.info(f"📤 Lighter 平多 {self.quantity}...")
            lighter_result = await self.place_lighter_market_order(
                self.lighter_client.config.contract_id,
                self.quantity,
                prices,
                'sell'
            )
            
            if not lighter_result.success:
                logger.error(f"❌ Lighter 平多失败: {lighter_result.error_message}")
                logger.warning("⚠️ 需要手动平仓 Lighter 多单！")
                return False
            
            lighter_close_price = lighter_result.price
            logger.info(f"✅ Lighter 平多成功: {lighter_close_price}")
            
            # 计算盈亏
            ext_pnl = (self.position.ext_entry_price - ext_close_price) * self.quantity
            lighter_pnl = (lighter_close_price - self.position.lighter_entry_price) * self.quantity
            total_pnl = ext_pnl + lighter_pnl
            
            logger.info(f"📊 平仓盈亏:")
            logger.info(f"   Extended: ${ext_pnl:.2f}")
            logger.info(f"   Lighter: ${lighter_pnl:.2f}")
            logger.info(f"   总计: ${total_pnl:.2f}")
            
            # 发送通知
            await self._send_close_notification(
                prices, spread_pct, ext_close_price, lighter_close_price, total_pnl
            )
            
            # 清空持仓
            self.position = None
            
            return True
        
        except Exception as e:
            logger.error(f"❌ 平仓失败: {e}")
            import traceback
            traceback.print_exc()
            return False

    async def place_extended_market_order(self, contract_id: str, quantity: Decimal, prices: Dict, side: str) -> OrderResult:
        """下市价单"""
        # BUY or SELL
        side = side.upper()
        # 或者可以加减tick_size
        order_price = prices['ext_ask'] if side == 'BUY' else prices['ext_bid']
        # rounded_price = self.round_to_tick(order_price)
        if side == 'BUY':
            order_price = Decimal(str(prices['ext_ask']))
        else:
            order_price = Decimal(str(prices['ext_bid']))
        
        logger.info(
            f"📤 Extended 下单:\n"
            f"   合约: {contract_id}\n"
            f"   方向: {side}\n"
            f"   数量: {quantity}\n"
            f"   价格: {order_price}"
        )
        try:
            order_result = await self.extended_client.perpetual_trading_client.place_order(
                market_name=contract_id,
                amount_of_synthetic=quantity,
                price=order_price,
                side=OrderSide.BUY if side == 'BUY' else OrderSide.SELL,
                time_in_force=TimeInForce.IOC,
                post_only=False,  # Ensure TAKER orders
            )

            if not order_result or not order_result.data or order_result.status != 'OK':
                error_msg = getattr(order_result, 'message', 'Unknown error')
                logger.error(f"❌ 下单失败: {error_msg}")
                return OrderResult(success=False, error_message=f'Failed to place order: {error_msg}')

            # Extract order ID from response
            order_id = order_result.data.id

            if not order_id:
                return OrderResult(success=False, error_message='No order ID in response')
            logger.info(f"🚀 订单已发送: order_id={order_id}")

            # Check order status after a short delay to see if it was rejected
            await asyncio.sleep(0.01)

            order_info = await self.extended_client.get_order_info(order_id)
            if not order_info:
                # 无法获取订单信息，假设成功
                logger.warning(f"⚠️ 无法获取订单状态，假设已成交")
                return OrderResult(
                    success=True,
                    order_id=order_id,
                    side=side,
                    size=quantity,
                    price=order_price,
                    status='ASSUMED_FILLED'
                )
            # ✅ 检查订单状态（统一使用字符串比较）
            status = str(order_info.status).upper()
        
            
            if status in ['CANCELED', 'REJECTED']:
                    return OrderResult(success=False, error_message=f'Order rejected or canceled: {status}')
            if status in ['NEW', 'OPEN', 'PARTIALLY_FILLED', 'FILLED']:
                    # Order successfully placed
                    return OrderResult(
                        success=True,
                        order_id=order_id,
                        side=side,
                        size=quantity,
                        price=order_price,
                        status=status
                    )
            return OrderResult(success=False, error_message=f'Unknown order status: {status}')
        except Exception as e:
            logger.error(f"下单失败: {e}")
            return OrderResult({
                'success': False,
                'error_message': str(e)
            })

    async def place_lighter_market_order(self, contract_id: str, quantity: Decimal, prices: Dict, side: str) -> OrderResult:
        """下市价单"""
        side_upper = side.upper()
        
        # 计算订单价格
        if side_upper == 'BUY':
            order_price = Decimal(str(prices['lighter_ask']))
        else:
            order_price = Decimal(str(prices['lighter_bid']))
        
        logger.info(
            f"📤 Lighter 下单:\n"
            f"   市场: {contract_id}\n"
            f"   方向: {side_upper}\n"
            f"   数量: {quantity}\n"
            f"   价格: {order_price}"
        )
        # ✅ 验证必要属性
        if not hasattr(self.lighter_client, 'base_amount_multiplier'):
            logger.error("❌ lighter_client 缺少 base_amount_multiplier")
            return OrderResult(
                success=False,
                error_message='lighter_client.base_amount_multiplier not initialized'
            )
        
        if not hasattr(self.lighter_client, 'price_multiplier'):
            logger.error("❌ lighter_client 缺少 price_multiplier")
            return OrderResult(
                success=False,
                error_message='lighter_client.price_multiplier not initialized'
            )
        
        if not hasattr(self.lighter_client, 'lighter_client'):
            logger.error("❌ lighter_client 缺少 lighter_client (SignerClient)")
            return OrderResult(
                success=False,
                error_message='lighter_client.lighter_client not initialized'
            )
        
        # ✅ 确保 contract_id 是整数
        try:
            market_index = int(contract_id)
        except (ValueError, TypeError):
            logger.error(f"❌ 无效的 market_index: {contract_id}")
            return OrderResult(
                success=False,
                error_message=f'Invalid market_index: {contract_id}'
            )
        
        order_params = {
            'market_index': market_index,
            'client_order_index': int(time.time() * 1000) % 1000000,
            'base_amount': int(quantity * self.lighter_client.base_amount_multiplier),  # assuming 6 decimals
            'price': int(order_price * self.lighter_client.price_multiplier),  # assuming 2 decimals
            'is_ask': side_upper == 'SELL',
            'order_type': self.lighter_client.lighter_client.ORDER_TYPE_LIMIT,
            'time_in_force': self.lighter_client.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
            'reduce_only': False,
            'trigger_price': 0,
        }

        logger.info(
            f"📋 Lighter 订单参数:\n"
            f"   market_index: {order_params['market_index']}\n"
            f"   client_order_index: {order_params['client_order_index']}\n"
            f"   base_amount: {order_params['base_amount']}\n"
            f"   price: {order_params['price']}\n"
            f"   is_ask: {order_params['is_ask']}\n"
            f"   order_type: {order_params['order_type']}\n"
            f"   time_in_force: {order_params['time_in_force']}"
        )
        try:
            tx_info, error = self.lighter_client.lighter_client.sign_create_order(
                **order_params
            )
            if error is not None:
                raise Exception(f"Sign error: {error}")

            # Prepare the form data
            tx_hash = await self.lighter_client.lighter_client.send_tx(
                tx_type=self.lighter_client.lighter_client.TX_TYPE_CREATE_ORDER,
                tx_info=tx_info
            )
            logger.info(f"🚀 Lighter limit order sent: {side_upper} {quantity}")
            # timestamp2 = time.perf_counter()
            # self.logger.info(f"⏱️ Lighter order placement time: {(timestamp2 - timestamp1) * 1000:.2f} ms")
            # await self.monitor_lighter_order(client_order_index)
            # logger.info(f"🚀 订单已发送，等待结果...{order_result}")
             # ✅ 处理返回结果
            if tx_hash is None:
                logger.error("❌ _submit_order_with_retry 返回 None")
                return OrderResult(
                    success=False,
                    error_message='Order submission returned None'
                )
            # order_result = await self.lighter_client.lighter_client.(
            #     market_index=market_index,
            #     client_order_index=order_params['client_order_index']
            # )
            # if isinstance(order_result, OrderResult):
            #     return order_result # 直接返回 OrderResult
            else:
                logger.error(f"🚀 订单已发送: tx_hash={tx_hash}")
                return OrderResult(
                    success=True,
                    order_id=tx_hash,
                    side=side_upper,
                    size=quantity,
                    price=order_price,
                    status='SUBMITTED'
                )
        except Exception as e:
            logger.error(f"下单失败: {e}")
            return OrderResult({
                'success': False,
                'error_message': str(e)
            })

    async def _send_open_notification(self, prices: Dict, spread_pct: float):
        """发送开仓通知"""
        if not self.lark_bot:
            return
        try:
            msg = (
                f"🔓 开仓通知（extended卖 lighter买）\n"
                f"━━━━━━━━━━━━━━━\n"
                f"币种: {self.symbol}\n"
                f"数量: {self.quantity}\n"
                f"━━━━━━━━━━━━━━━\n"
                f"Extended:\n"
                f"  开空价格: ${self.position.ext_entry_price:.2f}\n"
                f"  Bid: ${prices['ext_bid']:.2f}\n"
                f"  Bid Size: {prices['ext_bid_size']}\n"
                f"━━━━━━━━━━━━━━━\n"
                f"Lighter:\n"
                f"  开多价格: ${self.position.lighter_entry_price:.2f}\n"
                f"  Ask: ${prices['lighter_ask']:.2f}\n"
                f"  Ask Size: {prices['lighter_ask_size']}\n"
                f"━━━━━━━━━━━━━━━\n"
                f"价差: {spread_pct:.4f}%\n"
                f"时间: {self.position.open_time.strftime('%Y-%m-%d %H:%M:%S')}"
            )
            await self.lark_bot.send_text(msg)
            logger.info("📨 开仓通知已发送")
        except Exception as e:
            logger.error(f"发送通知失败: {e}")
    
    async def _send_close_notification(
        self, 
        prices: Dict, 
        spread_pct: float,
        ext_close_price: Decimal,
        lighter_close_price: Decimal,
        total_pnl: Decimal
    ):
        """发送平仓通知"""
        if not self.lark_bot:
            return
        
        try:
            duration = datetime.now() - self.position.open_time
            duration_str = str(duration).split('.')[0]  # 去掉微秒
            
            pnl_emoji = "📈" if total_pnl > 0 else "📉"
            
            msg = (
                f"🔒 平仓通知（extended买 lighter卖）\n"
                f"━━━━━━━━━━━━━━━\n"
                f"币种: {self.symbol}\n"
                f"数量: {self.quantity}\n"
                f"持仓时长: {duration_str}\n"
                f"━━━━━━━━━━━━━━━\n"
                f"Extended:\n"
                f"  开仓: ${self.position.ext_entry_price:.2f}\n"
                f"  平仓: ${ext_close_price:.2f}\n"
                f"  ext Ask: ${prices['ext_ask']:.2f}\n"
                f"  ext Ask Size: {prices['ext_ask_size']}\n"
                f"  盈亏: ${(self.position.ext_entry_price - ext_close_price) * self.quantity:.2f}\n"
                f"━━━━━━━━━━━━━━━\n"
                f"Lighter:\n"
                f"  开仓: ${self.position.lighter_entry_price:.2f}\n"
                f"  平仓: ${lighter_close_price:.2f}\n"
                f"  lighter Bid: ${prices['lighter_bid']:.2f}\n"
                f"  lighter Bid Size: {prices['lighter_bid_size']}\n"
                f"  盈亏: ${(lighter_close_price - self.position.lighter_entry_price) * self.quantity:.2f}\n"
                f"━━━━━━━━━━━━━━━\n"
                f"{pnl_emoji} 总盈亏: ${total_pnl:.2f}\n"
                f"平仓价差: {spread_pct:.4f}%\n"
                f"开仓价差: {self.position.open_spread_pct:.4f}%"
            )
            await self.lark_bot.send_text(msg)
            logger.info("📨 平仓通知已发送")
        except Exception as e:
            logger.error(f"发送通知失败: {e}")
    
    async def monitor_loop(self):
        """主监控循环"""
        logger.info("🎯 开始监控价差...")
        logger.info(f"   开仓阈值: {self.open_threshold_pct}%")
        logger.info(f"   平仓阈值: {self.close_threshold_pct}%")
        logger.info(f"   检查间隔: {self.check_interval}s")
        
        self.is_running = True
        
        try:
            while self.is_running:
                try:
                    prices = await self.fetch_prices()
                    
                    if not prices:
                        await asyncio.sleep(self.check_interval)
                        continue
                    
                    spread_value, spread_pct = self.calculate_spread_open(prices)
                    spread_value_close, spread_pct_close = self.calculate_spread_close(prices)
                    
                    # 显示当前状态
                    status = "🟢 持仓中" if self.position else "🔵 空仓"
                    if self.position:
                        logger.info(
                            f"{status} | "
                            f"LgtBid: ${prices['lighter_bid']:.2f} | "
                            f"ExtAsk: ${prices['ext_ask']:.2f} | "
                            f"开仓价差: {self.position.open_spread_pct:.4f}% | "
                            f"当前价差: {spread_pct_close:.4f}%"
                        )
                    else:
                        logger.info(
                            f"{status} | "
                            f"ExtBid: ${prices['ext_bid']:.2f} | "
                            f"LgtAsk: ${prices['lighter_ask']:.2f} | "
                            f"价差: {spread_pct:.4f}%"
                        )
                    
                    # 检查开仓信号
                    if not self.position and spread_pct > self.open_threshold_pct:
                        logger.debug("检测到开仓信号，检查订单簿深度...")
                        logger.debug("spread_pct: {:.4f}%, open_threshold: {:.4f}%".format(
                            spread_pct, self.open_threshold_pct
                        ))
                        if self.check_depth(prices) and self.buy_notified is False:
                            logger.info(f"🚨 检测到开仓信号！价差 {spread_pct:.4f}% > {self.open_threshold_pct}%")
                            # 增加一个标志位，如果开仓失败等待下一次机会
                            res = await self.open_position(prices, spread_pct)
                            await self._send_open_notification(prices, spread_pct)
                            if res:
                                self.buy_notified = True
                                self.sell_notified = False

                    
                    # 检查平仓信号
                    elif self.position and spread_pct_close > self.close_threshold_pct and self.sell_notified is False and self.buy_notified is True:
                        logger.info(f"🚨 检测到平仓信号！价差 {spread_pct_close:.4f}% > {self.close_threshold_pct}%")
                        res = await self.close_position(prices, spread_pct)
                        if res:
                            self.sell_notified = True
                            self.buy_notified = False
                            ext_close_price = Decimal(prices['ext_ask'])
                            lighter_close_price = Decimal(prices['lighter_bid'])
                            ext_pnl = (self.position.ext_entry_price - ext_close_price) * self.quantity
                            lighter_pnl = (lighter_close_price - self.position.lighter_entry_price) * self.quantity
                            total_pnl = ext_pnl + lighter_pnl
                            await self._send_close_notification(
                                prices, spread_pct,
                                ext_close_price=ext_close_price,
                                lighter_close_price=lighter_close_price,
                                total_pnl=total_pnl
                            )
                        # self.sell_notified = True
                        # self.buy_notified = False
                
                except Exception as e:
                    logger.error(f"监控循环错误: {e}")
                    import traceback
                    traceback.print_exc()
                
                await asyncio.sleep(self.check_interval)
        
        except KeyboardInterrupt:
            logger.info("\n⏸️ 收到停止信号...")
        finally:
            await self.cleanup()
    
    async def cleanup(self):
        """清理资源"""
        logger.info("🧹 清理资源...")
        self.stop_flag = True
        self.is_running = False
        
        if self.lighter_ws_task:
            self.lighter_ws_task.cancel()
            try:
                await self.lighter_ws_task
            except asyncio.CancelledError:
                pass
        
        if self.extended_client:
            try:
                await self.extended_client.disconnect()
            except Exception as e:
                logger.error(f"断开 Extended 失败: {e}")
        
        if self.lighter_client:
            try:
                await self.lighter_client.disconnect()
            except Exception as e:
                logger.error(f"断开 Lighter 失败: {e}")
        
        if self.lark_bot:
            try:
                await self.lark_bot.close()
            except Exception as e:
                logger.error(f"关闭 Lark Bot 失败: {e}")
        
        logger.info("✅ 清理完成")
    
    async def run(self):
        """启动监控"""
        try:
            await self.initialize()
            await self.monitor_loop()
        except Exception as e:
            logger.error(f"运行失败: {e}")
            import traceback
            traceback.print_exc()
        finally:
            await self.cleanup()


async def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='对冲套利监控器',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python arbitrage/hedge_monitor.py --symbol BTC --quantity 0.01
  python arbitrage/hedge_monitor.py --symbol ETH --quantity 0.1 --open-threshold 0.08
        """
    )
    
    parser.add_argument('--symbol', '-s', type=str, required=True,
                       help='交易币种 (如 BTC, ETH, SOL)')
    parser.add_argument('--quantity', '-q', type=str, required=True,
                       help='开仓数量 (如 0.01)')
    parser.add_argument('--open-threshold', type=float, default=0.05,
                       help='开仓阈值百分比 (默认 0.06%%)')
    parser.add_argument('--close-threshold', type=float, default=0.0,
                       help='平仓阈值百分比 (默认 0.0%%)')
    parser.add_argument('--check-interval', type=float, default=1.0,
                       help='检查间隔秒数 (默认 1.0s)')
    parser.add_argument('--env-file', type=str, default=None,
                       help='环境变量文件路径 (可选)')

    args = parser.parse_args()
    if args.env_file:
        load_dotenv(args.env_file)
    # 获取飞书 Token
    lark_token = os.getenv('LARK_TOKEN')
    if not lark_token:
        logger.warning("⚠️ 未设置 LARK_TOKEN，将不发送通知")
    
    # 创建监控器
    monitor = HedgeMonitor(
        symbol=args.symbol,
        quantity=Decimal(args.quantity),
        open_threshold_pct=args.open_threshold,
        close_threshold_pct=args.close_threshold,
        check_interval=args.check_interval,
        lark_token=lark_token
    )
    
    # 运行
    await monitor.run()


if __name__ == '__main__':
    asyncio.run(main())