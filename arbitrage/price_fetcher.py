import asyncio
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Tuple
from decimal import Decimal
import logging
import sys
import websockets
import threading

sys.path.insert(0, str(Path(__file__).parent.parent))

from arbitrage import config
from exchanges.extended import ExtendedClient
from exchanges.lighter import LighterClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PriceSnapshot:
    """价格快照数据类"""
    def __init__(
        self,
        timestamp: int,
        symbol: str,
        extended: Optional[Dict] = None,
        lighter: Optional[Dict] = None
    ):
        self.timestamp = timestamp
        self.symbol = symbol
        self.extended = extended
        self.lighter = lighter
        self.spread = None
        self.spread_percentage = None
        
        if extended and lighter:
            self._calculate_spread()
    
    def _calculate_spread(self):
        """计算价差"""
        spread1 = self.lighter['bid_price'] - self.extended['ask_price']
        spread2 = self.extended['bid_price'] - self.lighter['ask_price']
        
        self.spread = max(spread1, spread2)
        base_price = self.extended['ask_price'] if spread1 > spread2 else self.lighter['ask_price']
        self.spread_percentage = (self.spread / base_price) * 100
    
    def to_dict(self) -> dict:
        return {
            'timestamp': self.timestamp,
            'symbol': self.symbol,
            'extended': self.extended,
            'lighter': self.lighter,
            'spread': self.spread,
            'spread_percentage': self.spread_percentage
        }


class SimpleConfig:
    """简单配置对象"""
    def __init__(self, ticker: str):
        self.ticker = ticker
        self.contract_id = f"{ticker}-USD"
        self.tick_size = Decimal('0.01')
        self.quantity = Decimal('0.1')
        self.close_order_side = 'sell'
        self.leverage = 1
        self.order_type = 'market'
        self.size = Decimal('0.1')


class PriceFetcher:
    """价格拉取器"""
    
    def __init__(
        self,
        symbol: str = 'ETH',
        interval_seconds: int = 5,
        data_dir: str = None
    ):
        self.symbol = symbol
        self.interval_seconds = interval_seconds
        
        if data_dir is None:
            data_dir = config.DATA_CONFIG['data_dir']
        
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.is_running = False
        self.stop_flag = False
        
        self.extended_client = None
        self.extended_timestamp = None
        
        self.lighter_client = None
        self.lighter_market_index = None
        self.lighter_order_book = {"bids": {}, "asks": {}}
        self.lighter_timestamp = None
        self.lighter_best_bid = None
        self.lighter_best_ask = None
        self.lighter_order_book_ready = False
        self.lighter_order_book_offset = 0
        self.lighter_snapshot_loaded = False
        self.lighter_order_book_lock = asyncio.Lock()
        self.lighter_ws_task = None
        
        self.lighter_ws_msg_count = 0
        self.lighter_last_update_time = 0
        
        # 添加文件锁，防止并发写入冲突
        self.file_lock = threading.Lock()
        
    async def _initialize_clients(self):
        """初始化交易所客户端"""
        try:
            client_config = SimpleConfig(self.symbol)
            
            logger.info("正在连接 Extended...")
            self.extended_client = ExtendedClient(client_config)
            await self.extended_client.connect()
            await asyncio.sleep(3)
            logger.info("✅ Extended 已连接")
            
            logger.info("正在连接 Lighter...")
            self.lighter_client = LighterClient(client_config)
            await self.lighter_client.connect()
            
            contract_id, tick_size = await self.lighter_client.get_contract_attributes()
            self.lighter_client.config.contract_id = contract_id
            self.lighter_client.config.tick_size = tick_size
            
            self.lighter_market_index = await self._get_lighter_market_index()
            
            logger.info("✅ Lighter 已连接")
            logger.info(f"   合约ID: {contract_id}")
            logger.info(f"   Market Index: {self.lighter_market_index}")
            
            logger.info("⏳ 启动 Lighter WebSocket...")
            self.lighter_ws_task = asyncio.create_task(self._handle_lighter_ws())
            
            await self._wait_for_lighter_orderbook()
            
        except Exception as e:
            logger.error(f"❌ 客户端初始化失败: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    async def _get_lighter_market_index(self) -> int:
        """获取 Lighter market index"""
        import requests
        
        url = "https://mainnet.zklighter.elliot.ai/api/v1/orderBooks"
        headers = {"accept": "application/json"}
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            for market in data.get("order_books", []):
                if market["symbol"] == self.symbol:
                    market_id = market["market_id"]
                    logger.info(f"找到市场: {market['symbol']} -> market_id={market_id}")
                    return market_id
            
            raise Exception(f"未找到 {self.symbol} 的市场信息")
            
        except Exception as e:
            logger.error(f"获取 market index 失败: {e}")
            raise
    
    async def _handle_lighter_ws(self):
        """处理 Lighter WebSocket"""
        url = "wss://mainnet.zklighter.elliot.ai/stream"
        
        reconnect_count = 0
        
        while not self.stop_flag:
            try:
                logger.info(f"🔌 连接 Lighter WebSocket...")
                
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
                            self.lighter_ws_msg_count += 1
                            self.lighter_last_update_time = time.time()
                            
                            data = json.loads(msg)
                            msg_type = data.get("type")
                            
                            async with self.lighter_order_book_lock:
                                if msg_type == "subscribed/order_book":
                                    logger.info("📸 收到订单簿快照")
                                    await self._handle_lighter_snapshot(data)
                                
                                elif msg_type == "update/order_book":
                                    if not self.lighter_snapshot_loaded:
                                        continue
                                    await self._handle_lighter_update(data)
                                
                                elif msg_type == "ping":
                                    await ws.send(json.dumps({"type": "pong"}))
                        
                        except asyncio.TimeoutError:
                            logger.warning("⏰ WebSocket 接收超时")
                            break
                        except websockets.exceptions.ConnectionClosed:
                            logger.warning("⚠️ WebSocket 连接关闭")
                            break
                        except Exception as e:
                            logger.error(f"处理消息错误: {e}")
                            break
            
            except Exception as e:
                logger.error(f"WebSocket 连接错误: {e}")
            
            if not self.stop_flag:
                reconnect_count += 1
                wait_time = min(5, reconnect_count)
                logger.info(f"⏳ {wait_time}秒后重新连接...")
                await asyncio.sleep(wait_time)
    
    async def _reset_lighter_orderbook(self):
        """重置 Lighter 订单簿"""
        async with self.lighter_order_book_lock:
            self.lighter_order_book["bids"].clear()
            self.lighter_order_book["asks"].clear()
            self.lighter_order_book_offset = 0
            self.lighter_snapshot_loaded = False
            self.lighter_best_bid = None
            self.lighter_best_ask = None
            self.lighter_order_book_ready = False
    
    def _parse_order_entry(self, entry) -> Optional[Tuple[Decimal, Decimal]]:
        """解析订单条目，支持多种格式"""
        try:
            # 格式1: 字典 {'price': '...', 'size': '...'}
            if isinstance(entry, dict):
                price = Decimal(str(entry['price']))
                size = Decimal(str(entry['size']))
                return (price, size)
            
            # 格式2: 列表 [price, size]
            elif isinstance(entry, list) and len(entry) >= 2:
                price = Decimal(str(entry[0]))
                size = Decimal(str(entry[1]))
                return (price, size)
            
            return None
            
        except Exception as e:
            logger.debug(f"解析订单条目失败: {e}, 数据: {entry}")
            return None
    
    async def _handle_lighter_snapshot(self, data):
        """处理 Lighter 订单簿快照"""
        try:
            order_book = data.get("order_book", {})
            timestamp = data.get("timestamp", 0)

            if "offset" in order_book:
                self.lighter_order_book_offset = order_book["offset"]
            
            self.lighter_order_book["bids"].clear()
            self.lighter_order_book["asks"].clear()
            self.lighter_timestamp = timestamp
            logger.info(f"🕒 lighter快照时间戳: {self.lighter_timestamp}")

            # 处理 bids
            bids = order_book.get("bids", [])
            for bid in bids:
                parsed = self._parse_order_entry(bid)
                if parsed:
                    price, size = parsed
                    if size > 0:
                        self.lighter_order_book["bids"][price] = size
            
            # 处理 asks
            asks = order_book.get("asks", [])
            for ask in asks:
                parsed = self._parse_order_entry(ask)
                if parsed:
                    price, size = parsed
                    if size > 0:
                        self.lighter_order_book["asks"][price] = size
            
            self._update_lighter_best_levels()
            
            self.lighter_snapshot_loaded = True
            self.lighter_order_book_ready = True
            
            logger.info(f"✅ 订单簿快照加载完成: "
                       f"{len(self.lighter_order_book['bids'])} bids, "
                       f"{len(self.lighter_order_book['asks'])} asks")
            
            if self.lighter_best_bid and self.lighter_best_ask:
                logger.info(f"   最佳买价: {self.lighter_best_bid:.2f}")
                logger.info(f"   最佳卖价: {self.lighter_best_ask:.2f}")
            
        except Exception as e:
            logger.error(f"处理快照错误: {e}")
            import traceback
            traceback.print_exc()
    
    async def _handle_lighter_update(self, data):
        """处理 Lighter 订单簿更新"""
        try:
            order_book = data.get("order_book", {})
            timestamp = data.get("timestamp", 0)

            new_offset = order_book.get("offset")
            if new_offset and new_offset <= self.lighter_order_book_offset:
                return
            
            self.lighter_order_book_offset = new_offset or self.lighter_order_book_offset
            self.lighter_timestamp = timestamp

            # 更新 bids
            for bid in order_book.get("bids", []):
                parsed = self._parse_order_entry(bid)
                if parsed:
                    price, size = parsed
                    if size > 0:
                        self.lighter_order_book["bids"][price] = size
                    else:
                        self.lighter_order_book["bids"].pop(price, None)
            
            # 更新 asks
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
        """更新 Lighter 最佳买卖价"""
        try:
            if self.lighter_order_book["bids"]:
                self.lighter_best_bid = max(self.lighter_order_book["bids"].keys())
            else:
                self.lighter_best_bid = None
            
            if self.lighter_order_book["asks"]:
                self.lighter_best_ask = min(self.lighter_order_book["asks"].keys())
            else:
                self.lighter_best_ask = None
                
        except Exception as e:
            logger.error(f"更新最佳买卖价错误: {e}")
    
    async def _wait_for_lighter_orderbook(self, max_wait: int = 30):
        """等待 Lighter 订单簿准备就绪"""
        start_time = time.time()
        
        logger.info(f"⏳ 等待订单簿数据...")
        
        while time.time() - start_time < max_wait:
            if self.lighter_order_book_ready and self.lighter_best_bid and self.lighter_best_ask:
                elapsed = time.time() - start_time
                logger.info(f"✅ Lighter 订单簿已就绪 (耗时 {elapsed:.1f}秒)")
                return True
            
            await asyncio.sleep(1)
        
        logger.warning(f"⚠️ Lighter 订单簿在 {max_wait} 秒内未就绪")
        return False
    
    async def fetch_extended_price(self) -> Optional[Dict]:
        """获取 Extended 价格"""
        try:
            if not self.extended_client:
                return None
            
            best_bid, best_ask, ts = await self.extended_client.fetch_bbo_prices(
                self.extended_client.config.contract_id
            )
            
            if best_bid <= 0 or best_ask <= 0:
                return None
            logger.info(f"Fetched Extended prices: bid={best_bid}, ask={best_ask}, ts={ts}")
            return {
                'bid_price': float(best_bid),
                'ask_price': float(best_ask),
                'mid_price': float((best_bid + best_ask) / 2),
                'timestamp': ts,
                'bid_size': 0,
                'ask_size': 0
            }
            
        except Exception as e:
            logger.debug(f'获取 Extended 价格失败: {e}')
            return None
    
    async def fetch_lighter_price(self) -> Optional[Dict]:
        """获取 Lighter 价格"""
        try:
            if not self.lighter_order_book_ready:
                return None
            
            async with self.lighter_order_book_lock:
                if not self.lighter_best_bid or not self.lighter_best_ask:
                    return None
                
                bids_list = sorted(self.lighter_order_book["bids"].items(), reverse=True)[:5]
                asks_list = sorted(self.lighter_order_book["asks"].items())[:5]
                
                order_book = {
                    'bids': [[float(p), float(s)] for p, s in bids_list],
                    'asks': [[float(p), float(s)] for p, s in asks_list]
                }
                
                return {
                    'bid_price': float(self.lighter_best_bid),
                    'ask_price': float(self.lighter_best_ask),
                    'mid_price': float((self.lighter_best_bid + self.lighter_best_ask) / 2),
                    'bid_size': float(self.lighter_order_book["bids"].get(self.lighter_best_bid, 0)),
                    'ask_size': float(self.lighter_order_book["asks"].get(self.lighter_best_ask, 0)),
                    'timestamp': self.lighter_timestamp if self.lighter_timestamp is not None else '',
                    'order_book': order_book
                }
            
        except Exception as e:
            logger.error(f'获取 Lighter 价格失败: {e}')
            return None
    
    async def fetch_price_snapshot(self) -> PriceSnapshot:
        """获取完整的价格快照"""
        timestamp = int(time.time() * 1000)
        
        extended_task = self.fetch_extended_price()
        lighter_task = self.fetch_lighter_price()
        
        extended_price, lighter_price = await asyncio.gather(
            extended_task,
            lighter_task,
            return_exceptions=True
        )
        
        if isinstance(extended_price, Exception):
            logger.error(f'Extended 价格获取错误: {extended_price}')
            extended_price = None
        
        if isinstance(lighter_price, Exception):
            logger.error(f'Lighter 价格获取错误: {lighter_price}')
            lighter_price = None
        
        return PriceSnapshot(
            timestamp=timestamp,
            symbol=self.symbol,
            extended=extended_price,
            lighter=lighter_price
        )
    
    def save_snapshot(self, snapshot: PriceSnapshot):
        """保存快照到文件 - 使用文件锁防止并发冲突"""
        try:
            with self.file_lock:
                date = datetime.now().strftime('%Y-%m-%d')
                
                # 保存 JSONL 格式
                jsonl_file = self.data_dir / f'prices_{self.symbol}_{date}.jsonl'
                with open(jsonl_file, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(snapshot.to_dict(), default=str) + '\n')
                
                # 保存 CSV 格式
                csv_file = self.data_dir / f'prices_{self.symbol}_{date}.csv'
                
                # 检查文件是否存在，如果不存在则写入表头
                file_exists = csv_file.exists()
                
                with open(csv_file, 'a', encoding='utf-8') as f:
                    if not file_exists:
                        # 完整的表头
                        headers = [
                            'timestamp',
                            'symbol',
                            'ext_ts',
                            'lighter_ts',
                            'ext_bid',
                            'ext_ask',
                            'ext_mid',
                            'ext_bid_size',
                            'ext_ask_size',
                            'lighter_bid',
                            'lighter_ask',
                            'lighter_mid',
                            'lighter_bid_size',
                            'lighter_ask_size',
                            'spread',
                            'spread_percentage'
                        ]
                        f.write(','.join(headers) + '\n')
                    
                    # 写入数据
                    ext = snapshot.extended or {}
                    lighter = snapshot.lighter or {}
                    
                    row = [
                        snapshot.timestamp,
                        snapshot.symbol,
                        ext.get('timestamp', ''),
                        lighter.get('timestamp', ''),
                        ext.get('bid_price', ''),
                        ext.get('ask_price', ''),
                        ext.get('mid_price', ''),
                        ext.get('bid_size', ''),
                        ext.get('ask_size', ''),
                        lighter.get('bid_price', ''),
                        lighter.get('ask_price', ''),
                        lighter.get('mid_price', ''),
                        lighter.get('bid_size', ''),
                        lighter.get('ask_size', ''),
                        snapshot.spread if snapshot.spread is not None else '',
                        snapshot.spread_percentage if snapshot.spread_percentage is not None else ''
                    ]
                    f.write(','.join(map(str, row)) + '\n')
                    
        except Exception as e:
            logger.error(f"保存快照错误: {e}")
            import traceback
            traceback.print_exc()
    
    def display_snapshot(self, snapshot: PriceSnapshot):
        """在终端显示快照"""
        print('\033[2J\033[H')
        print('=' * 80)
        print(f'时间: {datetime.fromtimestamp(snapshot.timestamp / 1000).strftime("%Y-%m-%d %H:%M:%S")}')
        print(f'交易对: {snapshot.symbol}-USD-PERP')
        
        status_line = "状态: "
        if self.extended_client:
            status_line += "Extended ✅  "
        else:
            status_line += "Extended ❌  "
        
        if self.lighter_order_book_ready and self.lighter_best_bid and self.lighter_best_ask:
            status_line += f"Lighter ✅"
        else:
            status_line += f"Lighter ⏳"
        
        print(status_line)
        print('=' * 80)
        
        if snapshot.extended:
            ext = snapshot.extended
            print(f'\n📊 Extended:')
            print(f'  买一: ${ext["bid_price"]:.2f}')
            print(f'  卖一: ${ext["ask_price"]:.2f}')
            print(f'  中间价: ${ext["mid_price"]:.2f}')
            print(f'  价差: ${ext["ask_price"] - ext["bid_price"]:.2f} ({(ext["ask_price"] - ext["bid_price"]) / ext["mid_price"] * 100:.3f}%)')
        else:
            print('\n📊 Extended: 数据获取失败')
        
        if snapshot.lighter:
            lighter = snapshot.lighter
            print(f'\n📊 Lighter:')
            print(f'  买一: ${lighter["bid_price"]:.2f} (数量: {lighter["bid_size"]:.4f})')
            print(f'  卖一: ${lighter["ask_price"]:.2f} (数量: {lighter["ask_size"]:.4f})')
            print(f'  中间价: ${lighter["mid_price"]:.2f}')
            print(f'  价差: ${lighter["ask_price"] - lighter["bid_price"]:.2f} ({(lighter["ask_price"] - lighter["bid_price"]) / lighter["mid_price"] * 100:.3f}%)')
            
            if lighter.get('order_book'):
                ob = lighter['order_book']
                if ob.get('bids'):
                    print(f'\n  📖 买单深度:')
                    for p, s in ob["bids"][:3]:
                        print(f'     ${p:.2f} × {s:.4f}')
                if ob.get('asks'):
                    print(f'\n  📖 卖单深度:')
                    for p, s in ob["asks"][:3]:
                        print(f'     ${p:.2f} × {s:.4f}')
        else:
            print('\n📊 Lighter: 数据获取失败 ⚠️')
        
        if snapshot.spread is not None:
            print(f'\n💰 套利机会分析:')
            print(f'  绝对价差: ${snapshot.spread:.2f}')
            print(f'  相对价差: {snapshot.spread_percentage:.4f}%')
            
            threshold = 0.05
            
            if snapshot.spread_percentage > threshold:
                print(f'\n  🔥 发现套利机会！')
                
                if snapshot.extended and snapshot.lighter:
                    spread1 = snapshot.lighter['bid_price'] - snapshot.extended['ask_price']
                    spread2 = snapshot.extended['bid_price'] - snapshot.lighter['ask_price']
                    
                    if spread1 > spread2:
                        profit_bps = (spread1 / snapshot.extended['ask_price']) * 10000
                        print(f'  📈 策略: Extended 买入 → Lighter 卖出')
                        print(f'     Extended Ask: ${snapshot.extended["ask_price"]:.2f}')
                        print(f'     Lighter Bid:  ${snapshot.lighter["bid_price"]:.2f}')
                        print(f'     预期利润: ${spread1:.2f} ({profit_bps:.1f} bps)')
                    else:
                        profit_bps = (spread2 / snapshot.lighter['ask_price']) * 10000
                        print(f'  📉 策略: Lighter 买入 → Extended 卖出')
                        print(f'     Lighter Ask:  ${snapshot.lighter["ask_price"]:.2f}')
                        print(f'     Extended Bid: ${snapshot.extended["bid_price"]:.2f}')
                        print(f'     预期利润: ${spread2:.2f} ({profit_bps:.1f} bps)')
            else:
                print(f'  ℹ️  当前价差较小 (阈值: {threshold}%)')
        
        print('=' * 80)
        print(f'下次更新: {self.interval_seconds}秒后\n')
    
    async def start(self):
        """开始监控"""
        if self.is_running:
            return
        
        logger.info(f'🚀 开始监控 {self.symbol} 价格')
        logger.info(f'📁 数据保存路径: {self.data_dir}')
        logger.info(f'⏱️  更新间隔: {self.interval_seconds}秒\n')
        
        self.stop_flag = False
        await self._initialize_clients()
        
        self.is_running = True
        
        try:
            while self.is_running:
                try:
                    snapshot = await self.fetch_price_snapshot()
                    self.save_snapshot(snapshot)
                    self.display_snapshot(snapshot)
                except Exception as e:
                    logger.error(f'获取价格错误: {e}')
                    import traceback
                    traceback.print_exc()
                
                await asyncio.sleep(self.interval_seconds)
        
        except KeyboardInterrupt:
            logger.info('\n正在停止监控...')
            self.is_running = False
        finally:
            await self.cleanup()
    
    async def cleanup(self):
        """清理资源"""
        logger.info("正在清理资源...")
        self.stop_flag = True
        
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
                logger.error(f"断开 Extended 连接错误: {e}")
        
        if self.lighter_client:
            try:
                await self.lighter_client.disconnect()
            except Exception as e:
                logger.error(f"断开 Lighter 连接错误: {e}")
        
        await asyncio.sleep(1)
        logger.info("✅ 资源清理完成")
    
    def stop(self):
        self.is_running = False


async def main():
    fetcher = PriceFetcher(
        symbol='BTC',
        interval_seconds=5
    )
    
    await fetcher.start()


if __name__ == '__main__':
    asyncio.run(main())