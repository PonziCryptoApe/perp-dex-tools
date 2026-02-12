"""价格监控服务"""

import asyncio
import logging
import time
from typing import Optional, Callable, List
from decimal import Decimal
from ..models.prices import PriceSnapshot
from ..exchanges.base import ExchangeAdapter

logger = logging.getLogger(__name__)

class PriceMonitorService:
    """
    价格监控服务
    
    职责:
    1. 连接交易所 WebSocket
    2. 实时更新订单簿
    3. 计算价差
    4. 触发回调通知策略层
    
    支持任意交易所组合（通过适配器模式）
    """
    
    def __init__(
        self,
        symbol: str,
        exchange_a: ExchangeAdapter,
        exchange_b: ExchangeAdapter,
        min_depth_multiplier: float = 1.0,  # 最小深度倍数
        trigger_exchange: str = 'exchange_b'  # 触发信号的交易所
    ):
        """
        Args:
            symbol: 交易币种
            exchange_a: 交易所 A 适配器（开空）
            exchange_b: 交易所 B 适配器（开多）
            min_depth_multiplier: 最小深度倍数（相对于交易数量）
        """
        self.symbol = symbol
        self.exchange_a = exchange_a
        self.exchange_b = exchange_b
        self.min_depth_multiplier = min_depth_multiplier
        self.trigger_exchange = trigger_exchange
        
        # ✅ 订阅者列表
        self._subscribers: List[Callable] = []
        
        # 订单簿缓存
        self.orderbook_a: Optional[dict] = None
        self.orderbook_b: Optional[dict] = None
        
        # 订单簿更新统计
        self.orderbook_a_updates = 0
        self.orderbook_b_updates = 0
        self.last_orderbook_a_time = 0.0
        self.last_orderbook_b_time = 0.0
        
        # 回调限流（避免过于频繁触发）
        self.last_callback_time = 0.0
        self.min_callback_interval = 0.1  # 最小回调间隔（秒）
        
        # 状态
        self._running = False
        self.suppress_health_logs = False  # 等待结束阶段可关闭健康日志
        
        logger.info(
            f"🔧 初始化价格监控:\n"
            f"   Symbol: {symbol}\n"
            f"   Exchange A: {exchange_a.exchange_name}\n"
            f"   Exchange B: {exchange_b.exchange_name}\n"
            f"   Min Depth Multiplier: {min_depth_multiplier}x\n"
            f"   Trigger Exchange: {trigger_exchange}"
        )
    
    def subscribe(self, callback: Callable):
        """
        订阅价格更新
        
        Args:
            callback: 回调函数 async def callback(prices: PriceSnapshot)
        """
        if callback not in self._subscribers:
            self._subscribers.append(callback)
            logger.info(f"✅ 添加订阅者: {callback.__name__}")
    
    def unsubscribe(self, callback: Callable):
        """取消订阅"""
        if callback in self._subscribers:
            self._subscribers.remove(callback)
            logger.info(f"✅ 移除订阅者: {callback.__name__}")
    
    async def start(self):
        """启动监控"""
        logger.info(f"🚀 启动价格监控: {self.symbol}")
        
        try:
            # 连接交易所
            logger.info(f"🔌 连接 {self.exchange_a.exchange_name}...")
            await self.exchange_a.connect()
            
            logger.info(f"🔌 连接 {self.exchange_b.exchange_name}...")
            await self.exchange_b.connect()
            
            # 订阅订单簿
            logger.info(f"📡 订阅 {self.exchange_a.exchange_name} 订单簿...")
            await self.exchange_a.subscribe_orderbook(self._on_orderbook_a_update)
            
            logger.info(f"📡 订阅 {self.exchange_b.exchange_name} 订单簿...")
            await self.exchange_b.subscribe_orderbook(self._on_orderbook_b_update)
            
            # 等待订单簿就绪
            logger.info("⏳ 等待订单簿数据就绪...")
            if not await self._wait_for_orderbook():
                raise TimeoutError("订单簿初始化超时")
            
            logger.info(f"✅ {self.symbol} 价格监控已就绪")
            self._running = True
            
            # 启动监控任务
            asyncio.create_task(self._monitor_orderbook_health())
        
        except Exception as e:
            logger.error(f"❌ 启动价格监控失败: {e}")
            raise
    
    async def stop(self):
        """停止监控"""
        logger.info(f"⏹️ 停止价格监控: {self.symbol}")
        self._running = False
        
        await self.exchange_a.disconnect()
        await self.exchange_b.disconnect()
    
    def get_latest_prices(self) -> Optional[PriceSnapshot]:
        """
        获取最新价格（带深度信息）
        
        Returns:
            PriceSnapshot 或 None（数据未就绪）
        """
        if not self.orderbook_a or not self.orderbook_b:
            return None
        
        try:
            bids_a = self.orderbook_a.get('bids', [])
            asks_a = self.orderbook_a.get('asks', [])
            mark_a = self.orderbook_a.get('mark_price', None)
            bids_b = self.orderbook_b.get('bids', [])
            asks_b = self.orderbook_b.get('asks', [])
            mark_b = self.orderbook_b.get('mark_price', None)
            timestamp_a = self.orderbook_a.get('timestamp', time.time())
            timestamp_b = self.orderbook_b.get('timestamp', time.time())
            
            if not (bids_a and asks_a and bids_b and asks_b):
                return None
            
            # ✅ 提取价格和深度
            exchange_a_bid = Decimal(str(bids_a[0][0]))
            exchange_a_ask = Decimal(str(asks_a[0][0]))
            exchange_b_bid = Decimal(str(bids_b[0][0]))
            exchange_b_ask = Decimal(str(asks_b[0][0]))
            exchange_a_mark = Decimal(str(mark_a)) if mark_a is not None else None
            exchange_b_mark = Decimal(str(mark_b)) if mark_b is not None else None

            # ✅ 提取深度（如果有）
            exchange_a_bid_size = Decimal(str(bids_a[0][1])) if len(bids_a[0]) > 1 else Decimal('0')
            exchange_a_ask_size = Decimal(str(asks_a[0][1])) if len(asks_a[0]) > 1 else Decimal('0')
            exchange_b_bid_size = Decimal(str(bids_b[0][1])) if len(bids_b[0]) > 1 else Decimal('0')
            exchange_b_ask_size = Decimal(str(asks_b[0][1])) if len(asks_b[0]) > 1 else Decimal('0')
            
            snapshot = PriceSnapshot(
                symbol=self.symbol,
                exchange_a_bid=exchange_a_bid,
                exchange_a_ask=exchange_a_ask,
                exchange_a_mark=exchange_a_mark,
                exchange_b_bid=exchange_b_bid,
                exchange_b_ask=exchange_b_ask,
                exchange_b_mark=exchange_b_mark,
                exchange_a_name=self.exchange_a.exchange_name,
                exchange_b_name=self.exchange_b.exchange_name,
                exchange_a_quote_id=getattr(self.exchange_a, '_quote_id', None),
                exchange_b_quote_id=getattr(self.exchange_b, '_quote_id', None),
                exchange_a_timestamp=timestamp_a,
                exchange_b_timestamp=timestamp_b
            )
            
            # ✅ 添加深度信息（扩展属性）
            snapshot.exchange_a_bid_size = exchange_a_bid_size
            snapshot.exchange_a_ask_size = exchange_a_ask_size
            snapshot.exchange_b_bid_size = exchange_b_bid_size
            snapshot.exchange_b_ask_size = exchange_b_ask_size
            
            return snapshot
        
        except Exception as e:
            logger.error(f"❌ 获取价格失败: {e}")
            return None
    
    def check_depth(self, prices: PriceSnapshot, quantity: Decimal) -> tuple[bool, str]:
        """
        检查订单簿深度是否足够
        
        Args:
            prices: 价格快照
            quantity: 交易数量
        
        Returns:
            (is_sufficient, error_message)
        """
        min_size = float(quantity) * self.min_depth_multiplier
        
        # 检查 Exchange A Bid 深度（开空时需要）
        if hasattr(prices, 'exchange_a_bid_size'):
            exchange_a_bid_size = float(prices.exchange_a_bid_size)
            if exchange_a_bid_size < min_size:
                msg = (
                    f"⚠️ {self.exchange_a.exchange_name} Bid 深度不足: "
                    f"{exchange_a_bid_size:.4f} < {min_size:.4f}"
                )
                logger.warning(msg)
                return False, msg
        
        # 检查 Exchange B Ask 深度（开多时需要）
        if hasattr(prices, 'exchange_b_ask_size'):
            exchange_b_ask_size = float(prices.exchange_b_ask_size)
            if exchange_b_ask_size < min_size:
                msg = (
                    f"⚠️ {self.exchange_b.exchange_name} Ask 深度不足: "
                    f"{exchange_b_ask_size:.4f} < {min_size:.4f}"
                )
                logger.warning(msg)
                return False, msg
        
        return True, ""
    
    def get_orderbook_stats(self) -> dict:
        """获取订单簿统计信息"""
        return {
            'exchange_a': {
                'name': self.exchange_a.exchange_name,
                'updates': self.orderbook_a_updates,
                'last_update': self.last_orderbook_a_time,
                'age': time.time() - self.last_orderbook_a_time if self.last_orderbook_a_time > 0 else 0,
                'ready': self.orderbook_a is not None
            },
            'exchange_b': {
                'name': self.exchange_b.exchange_name,
                'updates': self.orderbook_b_updates,
                'last_update': self.last_orderbook_b_time,
                'age': time.time() - self.last_orderbook_b_time if self.last_orderbook_b_time > 0 else 0,
                'ready': self.orderbook_b is not None
            }
        }
    
    async def _on_orderbook_a_update(self, orderbook: dict):
        """交易所 A 订单簿更新"""
        self.orderbook_a = orderbook
        self.orderbook_a_updates += 1
        self.last_orderbook_a_time = orderbook.get('timestamp', time.time())  # 使用订单簿自带时间，避免旧数据被当作新数据
        
        # 记录详细日志（仅在 DEBUG 模式）
        if logger.isEnabledFor(logging.DEBUG):
            bids = orderbook.get('bids', [])
            asks = orderbook.get('asks', [])
            # if bids and asks:
                # logger.debug(
                #     f"📘 {self.exchange_a.exchange_name} 订单簿更新 #{self.orderbook_a_updates}:\n"
                #     f"   Bid: ${bids[0][0]:.2f} x {bids[0][1] if len(bids[0]) > 1 else 'N/A'}\n"
                #     f"   Ask: ${asks[0][0]:.2f} x {asks[0][1] if len(asks[0]) > 1 else 'N/A'}"
                # )
        if self.trigger_exchange == 'exchange_a':
            await self._notify_price_update()
    
    async def _on_orderbook_b_update(self, orderbook: dict):
        """交易所 B 订单簿更新"""
        self.orderbook_b = orderbook
        self.orderbook_b_updates += 1
        self.last_orderbook_b_time = orderbook.get('timestamp', time.time())  # 使用订单簿自带时间，避免旧数据被当作新数据
        
        # 记录详细日志（仅在 DEBUG 模式）
        if logger.isEnabledFor(logging.DEBUG):
            bids = orderbook.get('bids', [])
            asks = orderbook.get('asks', [])
            # if bids and asks:
            #     logger.debug(
            #         f"📗 {self.exchange_b.exchange_name} 订单簿更新 #{self.orderbook_b_updates}:\n"
            #         f"   Bid: ${bids[0][0]:.2f} x {bids[0][1] if len(bids[0]) > 1 else 'N/A'}\n"
            #         f"   Ask: ${asks[0][0]:.2f} x {asks[0][1] if len(asks[0]) > 1 else 'N/A'}"
            #     )
        if self.trigger_exchange == 'exchange_b':
            await self._notify_price_update()

    async def _notify_price_update(self):
        """通知所有订阅者（带限流）"""
        if not self._subscribers:
            return
        
        # ✅ 检查订单簿是否都已就绪
        if not self.orderbook_a or not self.orderbook_b:
            logger.info("⏳ 等待两个交易所的订单簿数据...")
            return
        
        # ✅ 限流：避免过于频繁触发回调
        # current_time = time.time()
        # if current_time - self.last_callback_time < self.min_callback_interval:
        #     return
        
        # self.last_callback_time = current_time
        
        prices = self.get_latest_prices()
        if prices:
            # ✅ 通知所有订阅者
            for callback in self._subscribers:
                try:
                    await callback(prices)
                except Exception as e:
                    logger.exception(
                        f"❌ [{self.symbol}] 价格更新回调失败 "
                        f"({callback.__name__}) | "
                        f"A={self.exchange_a.exchange_name}, "
                        f"B={self.exchange_b.exchange_name}: {e}"
                    )
    
    async def _wait_for_orderbook(self, timeout: float = 10.0) -> bool:
        """等待订单簿数据就绪"""
        start_time = time.time()
        attempt = 0
        
        while time.time() - start_time < timeout:
            attempt += 1
            if self.orderbook_a:
                logger.info(f"📘 {self.exchange_a.exchange_name} 订单簿已就绪")
                logger.info(f"   bids_a: {self.orderbook_a.get('bids', [])[:1]}, asks_a: {self.orderbook_a.get('asks', [])[:1]}")
            if self.orderbook_b:
                logger.info(f"📗 {self.exchange_b.exchange_name} 订单簿已就绪")
                logger.info(f"   bids_b: {self.orderbook_b.get('bids', [])[:1]}, asks_b: {self.orderbook_b.get('asks', [])[:1]}")

            if self.orderbook_a and self.orderbook_b:
                bids_a = self.orderbook_a.get('bids', [])
                asks_a = self.orderbook_a.get('asks', [])
                bids_b = self.orderbook_b.get('bids', [])
                asks_b = self.orderbook_b.get('asks', [])
                
                if bids_a and asks_a and bids_b and asks_b:
                    elapsed = time.time() - start_time
                    logger.info(
                        f"✅ 订单簿就绪 ({elapsed:.1f}s, 第 {attempt} 次尝试)\n"
                        f"   {self.exchange_a.exchange_name}: "
                        f"Bid ${bids_a[0][0]:.2f}, Ask ${asks_a[0][0]:.2f}\n"
                        f"   {self.exchange_b.exchange_name}: "
                        f"Bid ${bids_b[0][0]:.2f}, Ask ${asks_b[0][0]:.2f}"
                    )
                    return True
            
            if attempt % 10 == 0:
                logger.debug(
                    f"尝试 {attempt}: 等待订单簿数据... "
                    f"({self.exchange_a.exchange_name}: {self.orderbook_a is not None}, "
                    f"{self.exchange_b.exchange_name}: {self.orderbook_b is not None})"
                )
            
            await asyncio.sleep(0.1)
        
        logger.error(
            f"❌ 订单簿未就绪 (超时 {timeout}s, 共 {attempt} 次尝试)\n"
            f"   {self.exchange_a.exchange_name}: {self.orderbook_a is not None}\n"
            f"   {self.exchange_b.exchange_name}: {self.orderbook_b is not None}"
        )
        return False
    
    async def _monitor_orderbook_health(self):
        """监控订单簿健康状态"""
        check_interval = 5.0  # 每 5 秒检查一次
        max_age = 30.0  # 订单簿最大年龄（秒）
        
        while self._running:
            try:
                await asyncio.sleep(check_interval)
                
                current_time = time.time()
                # 结束等待阶段可关闭健康日志
                if self.suppress_health_logs:
                    continue
                
                # 检查 Exchange A
                if self.last_orderbook_a_time > 0:
                    age_a = current_time - self.last_orderbook_a_time
                    if age_a > max_age:
                        logger.warning(
                            f"⚠️ {self.exchange_a.exchange_name} 订单簿已 {age_a:.1f}s 未更新 "
                            f"(共 {self.orderbook_a_updates} 次更新)"
                        )
                
                # 检查 Exchange B
                if self.last_orderbook_b_time > 0:
                    age_b = current_time - self.last_orderbook_b_time
                    if age_b > max_age:
                        logger.warning(
                            f"⚠️ {self.exchange_b.exchange_name} 订单簿已 {age_b:.1f}s 未更新 "
                            f"(共 {self.orderbook_b_updates} 次更新)"
                        )
                
                # 定期输出统计信息（每分钟）
                if int(current_time) % 60 == 0:
                    stats = self.get_orderbook_stats()
                    logger.info(
                        f"📊 订单簿统计:\n"
                        f"   {stats['exchange_a']['name']}: "
                        f"{stats['exchange_a']['updates']} 次更新, "
                        f"年龄 {stats['exchange_a']['age']:.1f}s\n"
                        f"   {stats['exchange_b']['name']}: "
                        f"{stats['exchange_b']['updates']} 次更新, "
                        f"年龄 {stats['exchange_b']['age']:.1f}s"
                    )
                    stats['exchange_a']['updates'] = 0
                    stats['exchange_b']['updates'] = 0
            
            except Exception as e:
                logger.error(f"❌ 订单簿健康检查失败: {e}")
    
    def is_orderbook_stale(self, max_age: float = 10.0) -> tuple[bool, str]:
        """检查订单簿是否过时"""
        current_time = time.time()
        age_a = None
        age_b = None
        
        # 检查 Exchange A
        if self.last_orderbook_a_time > 0:
            age_a = current_time - self.last_orderbook_a_time
            if age_a > max_age:
                logger.warning(
                    f"⚠️ [{self.symbol}] 订单簿过时: "
                    f"{self.exchange_a.exchange_name} age_ms={age_a*1000:.0f}"
                )
                return True, f"{self.exchange_a.exchange_name} 订单簿已 {age_a:.1f}s 未更新"
        elif self.orderbook_a is None:
            logger.warning(
                f"⚠️ [{self.symbol}] 订单簿未初始化: "
                f"{self.exchange_a.exchange_name}"
            )
            return True, f"{self.exchange_a.exchange_name} 订单簿未初始化"
        
        # 检查 Exchange B
        if self.last_orderbook_b_time > 0:
            age_b = current_time - self.last_orderbook_b_time
            if age_b > max_age:
                logger.warning(
                    f"⚠️ [{self.symbol}] 订单簿过时: "
                    f"{self.exchange_b.exchange_name} age_ms={age_b*1000:.0f}"
                )
                return True, f"{self.exchange_b.exchange_name} 订单簿已 {age_b:.1f}s 未更新"
        elif self.orderbook_b is None:
            logger.warning(
                f"⚠️ [{self.symbol}] 订单簿未初始化: "
                f"{self.exchange_b.exchange_name}"
            )
            return True, f"{self.exchange_b.exchange_name} 订单簿未初始化"
        
        return False, ""
