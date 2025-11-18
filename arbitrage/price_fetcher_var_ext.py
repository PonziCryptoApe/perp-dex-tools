import asyncio
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict
from decimal import Decimal
import logging
import sys
import threading

sys.path.insert(0, str(Path(__file__).parent.parent))

from arbitrage import config
from exchanges.extended import ExtendedClient
from exchanges.variational import VariationalClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ✅ 添加全局共享的 Variational 客户端管理器
class VariationalClientManager:
    """全局 Variational 客户端管理器 - 单例模式"""
    _instance = None
    _lock = asyncio.Lock()
    _shared_client = None
    _ref_count = 0
    
    @classmethod
    async def get_client(cls, config) -> VariationalClient:
        """获取共享的 Variational 客户端"""
        async with cls._lock:
            if cls._shared_client is None:
                logger.info("🔐 创建共享 Variational 客户端...")
                cls._shared_client = VariationalClient(config)
                await cls._shared_client.connect()
                logger.info("✅ 共享 Variational 客户端已连接")
            
            cls._ref_count += 1
            logger.info(f"📊 Variational 客户端引用计数: {cls._ref_count}")
            return cls._shared_client
    
    @classmethod
    async def release_client(cls):
        """释放客户端引用"""
        async with cls._lock:
            cls._ref_count -= 1
            logger.info(f"📊 Variational 客户端引用计数: {cls._ref_count}")
            
            # 当所有引用都释放时，关闭客户端
            if cls._ref_count <= 0 and cls._shared_client is not None:
                logger.info("🔒 关闭共享 Variational 客户端...")
                await cls._shared_client.disconnect()
                cls._shared_client = None
                logger.info("✅ 共享 Variational 客户端已关闭")


class PriceSnapshot:
    """价格快照数据类"""
    def __init__(
        self,
        timestamp: int,
        symbol: str,
        extended: Optional[Dict] = None,
        variational: Optional[Dict] = None
    ):
        self.timestamp = timestamp
        self.symbol = symbol
        self.extended = extended
        self.variational = variational
        self.spread = None
        self.spread_percentage = None
        
        if extended and variational:
            self._calculate_spread()
    
    def _calculate_spread(self):
        """计算价差"""
        spread1 = self.variational['bid_price'] - self.extended['ask_price']
        spread2 = self.extended['bid_price'] - self.variational['ask_price']
        
        self.spread = max(spread1, spread2)
        base_price = self.extended['ask_price'] if spread1 > spread2 else self.variational['ask_price']
        self.spread_percentage = (self.spread / base_price) * 100
    
    def to_dict(self) -> dict:
        return {
            'timestamp': self.timestamp,
            'symbol': self.symbol,
            'extended': self.extended,
            'variational': self.variational,
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
    """价格拉取器 - Extended & Variational"""
    # ✅ 添加币种默认数量配置
    SYMBOL_QUANTITY_CONFIG = {
        'BTC': Decimal('0.005'),
        'ETH': Decimal('0.2'),
        'SOL': Decimal('4'),
        'BNB': Decimal('0.5'),
        'HYPE': Decimal('12'),
        # 添加更多币种...
    }

    DEFAULT_QUANTITY = Decimal('0.003')
    def __init__(
        self,
        symbol: str = 'ETH',
        interval_seconds: int = 5,
        data_dir: str = None,
        quantity: Decimal = None
    ):
        self.symbol = symbol
        self.interval_seconds = interval_seconds
        
        if data_dir is None:
            data_dir = config.DATA_CONFIG['data_dir']
        
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.is_running = False

        # 设置默认数量
        if quantity is not None:
            self.quantity = quantity
        else:
            self.quantity = self.SYMBOL_QUANTITY_CONFIG.get(symbol, self.DEFAULT_QUANTITY)

        self.stop_flag = False
        
        # Extended 客户端
        self.extended_client = None
        
        # Variational 客户端
        self.variational_client = None
        self.variational_contract_id = None
        
        # 文件锁，防止并发写入冲突
        self.file_lock = threading.Lock()
        
    async def _initialize_clients(self):
        """初始化交易所客户端"""
        try:
            client_config = SimpleConfig(self.symbol)
            
            # 初始化 Extended
            logger.info(f"[{self.symbol}] 正在连接 Extended...")
            self.extended_client = ExtendedClient(client_config)
            await self.extended_client.connect()
            await asyncio.sleep(1)
            logger.info(f"[{self.symbol}] ✅ Extended 已连接")

            # 初始化 Variational
            logger.info(f"[{self.symbol}] 正在获取 Variational 客户端...")
            self.variational_client = await VariationalClientManager.get_client(client_config)
            # logger.info(f"✅ Variational 已连接")

            # 获取合约信息
            self.variational_contract_id, _ = await self.variational_client.get_contract_attributes(self.symbol)
            logger.info(f"[{self.symbol}] ✅ Variational 已连接")
            logger.info(f"[{self.symbol}]    合约ID: {self.variational_contract_id}")
            
        except Exception as e:
            logger.error(f"[{self.symbol}] ❌ 客户端初始化失败: {e}")
            import traceback
            traceback.print_exc()
            # ✅ 清理已初始化的客户端
            if self.extended_client:
                try:
                    await self.extended_client.disconnect()
                except:
                    pass
            raise
    
    async def fetch_extended_price(self) -> Optional[Dict]:
        """获取 Extended 价格"""
        try:
            if not self.extended_client:
                return None

            best_bid, best_ask, ts, bid_size, ask_size = await self.extended_client.fetch_bbo_prices_extended(
                self.extended_client.config.contract_id
            )
            
            if best_bid <= 0 or best_ask <= 0:
                return None
            
            # ✅ 添加时间戳统一处理（之前缺失）
            if ts < 10**10:  # 秒级时间戳
                ts_ms = int(ts * 1000)
            else:  # 已经是毫秒
                ts_ms = int(ts)

            return {
                'bid_price': float(best_bid),
                'ask_price': float(best_ask),
                'mid_price': float((best_bid + best_ask) / 2),
                'timestamp': ts_ms,
                'bid_size': float(bid_size),
                'ask_size': float(ask_size)
            }
            
        except Exception as e:
            logger.debug(f'获取 Extended 价格失败: {e}')
            return None
    
    async def fetch_variational_price(self) -> Optional[Dict]:
        """获取 Variational 价格（带请求时长检测）"""
        try:
            if not self.variational_client:
                return None
            
            start_time = time.perf_counter()  # 记录开始时间
            request_start_ms = int(time.time() * 1000)  # ✅ 绝对时间（毫秒）

            quantity = self.quantity
            print(f"[{self.symbol}] 使用查询数量: {quantity}")
            # 设置超时
            try:
                quote_data = await asyncio.wait_for(
                    self.variational_client._fetch_indicative_quote(quantity, self.variational_contract_id),
                    timeout=2.0  # 2秒超时
                )
            except asyncio.TimeoutError:
                logger.warning('Variational 请求超时(>2s)，跳过')
                return None
            
            request_duration = time.perf_counter() - start_time  # 计算耗时
            
            if not quote_data or 'bid' not in quote_data or 'ask' not in quote_data:
                return None
            
            bid_price = Decimal(str(quote_data['bid']))
            ask_price = Decimal(str(quote_data['ask']))
            
            # 如果请求耗时过长，标记为"过期"
            if request_duration > 0.5:
                logger.warning(f'Variational 请求耗时 {request_duration:.2f}s，数据可能过期')
            
            return {
                'bid_price': float(bid_price),
                'ask_price': float(ask_price),
                'mid_price': float((bid_price + ask_price) / 2),
                'timestamp': request_start_ms,  # ✅ 使用请求开始时间
                'request_duration': request_duration,  # 记录耗时
                'is_stale': request_duration > 0.5  # 是否过期
            }
            
        except Exception as e:
            logger.debug(f'获取 Variational 价格失败: {e}')
            return None
    
    async def fetch_price_snapshot(self) -> PriceSnapshot:
        """获取完整的价格快照"""
        timestamp = int(time.time() * 1000)
        
        extended_task = self.fetch_extended_price()
        variational_task = self.fetch_variational_price()
        
        extended_price, variational_price = await asyncio.gather(
            extended_task,
            variational_task,
            return_exceptions=True
        )
        
        if isinstance(extended_price, Exception):
            logger.error(f'Extended 价格获取错误: {extended_price}')
            extended_price = None
        
        if isinstance(variational_price, Exception):
            logger.error(f'Variational 价格获取错误: {variational_price}')
            variational_price = None
        
        return PriceSnapshot(
            timestamp=timestamp,
            symbol=self.symbol,
            extended=extended_price,
            variational=variational_price
        )
    
    def save_snapshot(self, snapshot: PriceSnapshot):
        """保存快照到文件"""
        try:
            with self.file_lock:
                date = datetime.now().strftime('%Y-%m-%d')
                
                # 保存 JSONL 格式（包含完整订单簿数据）
                jsonl_file = self.data_dir / f'prices_{self.symbol}_var_ext_{date}.jsonl'
                with open(jsonl_file, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(snapshot.to_dict(), default=str) + '\n')
                
                # 保存 CSV 格式（基础价格数据）
                csv_file = self.data_dir / f'prices_{self.symbol}_var_ext_{date}.csv'
                
                file_exists = csv_file.exists()
                
                with open(csv_file, 'a', encoding='utf-8') as f:
                    if not file_exists:
                        headers = [
                            'timestamp',
                            'symbol',
                            'ext_ts',
                            'var_ts',
                            'ext_bid',
                            'ext_ask',
                            'ext_mid',
                            'ext_bid_size',
                            'ext_ask_size',
                            'var_bid',
                            'var_ask',
                            'var_mid',
                            'spread',
                            'spread_percentage'
                        ]
                        f.write(','.join(headers) + '\n')
                    
                    ext = snapshot.extended or {}
                    var = snapshot.variational or {}
                    
                    row = [
                        snapshot.timestamp,
                        snapshot.symbol,
                        ext.get('timestamp', ''),
                        var.get('timestamp', ''),
                        ext.get('bid_price', ''),
                        ext.get('ask_price', ''),
                        ext.get('mid_price', ''),
                        ext.get('bid_size', ''),
                        ext.get('ask_size', ''),
                        var.get('bid_price', ''),
                        var.get('ask_price', ''),
                        var.get('mid_price', ''),
                        # var.get('bid_size', ''),
                        # var.get('ask_size', ''),
                        snapshot.spread if snapshot.spread is not None else '',
                        snapshot.spread_percentage if snapshot.spread_percentage is not None else ''
                    ]
                    f.write(','.join(map(str, row)) + '\n')
                    
        except Exception as e:
            logger.error(f"保存快照错误: {e}")
            import traceback
            traceback.print_exc()
    
    # def display_snapshot(self, snapshot: PriceSnapshot):
    #     """在终端显示快照"""
    #     print('\033[2J\033[H')
    #     print('=' * 80)
    #     print(f'时间: {datetime.fromtimestamp(snapshot.timestamp / 1000).strftime("%Y-%m-%d %H:%M:%S")}')
    #     print(f'交易对: {snapshot.symbol}-USD-PERP')
        
    #     status_line = "状态: "
    #     if self.extended_client:
    #         status_line += "Extended ✅  "
    #     else:
    #         status_line += "Extended ❌  "
        
    #     if self.variational_client:
    #         status_line += "Variational ✅"
    #     else:
    #         status_line += "Variational ❌"
        
    #     print(status_line)
    #     print('=' * 80)
        
    #     # Extended 数据
    #     if snapshot.extended:
    #         ext = snapshot.extended
    #         print(f'\n📊 Extended:')
    #         print(f'  买一: ${ext["bid_price"]:.2f} (数量: {ext["bid_size"]:.4f})')
    #         print(f'  卖一: ${ext["ask_price"]:.2f} (数量: {ext["ask_size"]:.4f})')
    #         print(f'  中间价: ${ext["mid_price"]:.2f}')
    #         print(f'  价差: ${ext["ask_price"] - ext["bid_price"]:.2f} ({(ext["ask_price"] - ext["bid_price"]) / ext["mid_price"] * 100:.3f}%)')
    #         print(f'  时间戳: {ext["timestamp"]}')
    #     else:
    #         print('\n📊 Extended: 数据获取失败')
        
    #     # Variational 数据
    #     if snapshot.variational:
    #         var = snapshot.variational
    #         print(f'\n📊 Variational:')
    #         print(f'  买一: ${var["bid_price"]:.2f}')
    #         print(f'  卖一: ${var["ask_price"]:.2f}')
    #         print(f'  中间价: ${var["mid_price"]:.2f}')
    #         print(f'  价差: ${var["ask_price"] - var["bid_price"]:.2f} ({(var["ask_price"] - var["bid_price"]) / var["mid_price"] * 100:.3f}%)')
            
    #         # ✅ 显示数据新鲜度
    #         if var.get('request_duration'):
    #             print(f'  请求耗时: {var["request_duration"]:.2f}s', end='')
    #             if var.get('is_stale'):
    #                 print(' ⚠️ 数据可能过期')
    #             else:
    #                 print(' ✅')
    #     else:
    #         print('\n📊 Variational: 数据获取失败 ⚠️')
        
    #     # 套利分析
    #     if snapshot.spread is not None:
    #         print(f'\n💰 套利机会分析:')
    #         print(f'  绝对价差: ${snapshot.spread:.2f}')
    #         print(f'  相对价差: {snapshot.spread_percentage:.4f}%')
            
    #         threshold = 0.05
            
    #         if snapshot.spread_percentage > threshold:
    #             print(f'\n  🔥 发现套利机会！')
                
    #             if snapshot.extended and snapshot.variational:
    #                 spread1 = snapshot.variational['bid_price'] - snapshot.extended['ask_price']
    #                 spread2 = snapshot.extended['bid_price'] - snapshot.variational['ask_price']
                    
    #                 if spread1 > spread2:
    #                     profit_bps = (spread1 / snapshot.extended['ask_price']) * 10000
    #                     print(f'  📈 策略: Extended 买入 → Variational 卖出')
    #                     print(f'     Extended Ask:    ${snapshot.extended["ask_price"]:.2f}')
    #                     print(f'     Variational Bid: ${snapshot.variational["bid_price"]:.2f}')
    #                     print(f'     预期利润: ${spread1:.2f} ({profit_bps:.1f} bps)')
    #                 else:
    #                     profit_bps = (spread2 / snapshot.variational['ask_price']) * 10000
    #                     print(f'  📉 策略: Variational 买入 → Extended 卖出')
    #                     print(f'     Variational Ask: ${snapshot.variational["ask_price"]:.2f}')
    #                     print(f'     Extended Bid:    ${snapshot.extended["bid_price"]:.2f}')
    #                     print(f'     预期利润: ${spread2:.2f} ({profit_bps:.1f} bps)')
    #         else:
    #             print(f'  ℹ️  当前价差较小 (阈值: {threshold}%)')
        
    #     print('=' * 80)
    #     print(f'下次更新: {self.interval_seconds}秒后\n')
    def display_snapshot_s(self, snapshot: PriceSnapshot):
        """简化版显示（多币种时避免屏幕混乱）"""
        print(f"[{self.symbol}] 快照更新 | "
                   f"Ext: {snapshot.extended['mid_price'] if snapshot.extended else 'N/A'} | "
                   f"Var: {snapshot.variational['mid_price'] if snapshot.variational else 'N/A'} ")

    def display_snapshot(self, snapshot: PriceSnapshot):
        """在终端显示快照 - 详细版"""
        import os
        
        # 清屏
        print('\033[2J\033[H')
        
        # 获取终端宽度
        terminal_width = os.get_terminal_size().columns if hasattr(os, 'get_terminal_size') else 100
        
        # 标题栏
        print('═' * terminal_width)
        title = f'  套利监控面板 - {snapshot.symbol}-USD-PERP  '
        print(f'{title:^{terminal_width}}')
        print(f'  快照时间: {datetime.fromtimestamp(snapshot.timestamp / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}  '.center(terminal_width))
        print('═' * terminal_width)
        
        # 连接状态
        status_parts = []
        if self.extended_client:
            status_parts.append('Extended ✅')
        else:
            status_parts.append('Extended ❌')
        
        if self.variational_client:
            status_parts.append('Variational ✅')
        else:
            status_parts.append('Variational ❌')
        
        print(f'\n状态: {" | ".join(status_parts)}\n')
        
        # ==================== Extended 数据 ====================
        print('┌' + '─' * (terminal_width - 2) + '┐')
        print(f'│ 📊 Extended 交易所{" " * (terminal_width - 17)}│')
        print('├' + '─' * (terminal_width - 2) + '┤')
        
        if snapshot.extended:
            ext = snapshot.extended
            
            # 基础价格信息
            print(f'│ {"价格信息":<{terminal_width - 3}}│')
            print(f'│   买一价格: ${ext["bid_price"]:>10.2f}    数量: {ext["bid_size"]:>8.4f}{" " * (terminal_width - 51)}│')
            print(f'│   卖一价格: ${ext["ask_price"]:>10.2f}    数量: {ext["ask_size"]:>8.4f}{" " * (terminal_width - 51)}│')
            print(f'│   中间价格: ${ext["mid_price"]:>10.2f}{" " * (terminal_width - 30)}│')
        
            # 数据时效性
            data_age = (snapshot.timestamp - ext["timestamp"]) / 1000
            freshness = "🟢 新鲜" if data_age < 0.1 else "🟡 一般" if data_age < 0.5 else "🔴 过期"
            print(f'│   数据时间: {ext["timestamp"]:<15}  延迟: {data_age:.3f}s  {freshness}{" " * (terminal_width - 62)}│')
            
            # 订单簿深度 (如果有)
            if ext.get('order_book'):
                print(f'│ {"订单簿深度":<{terminal_width - 3}}│')
                ob = ext['order_book']
                
                # 显示前3档
                print(f'│   {"买盘":<15} {"价格":<15} {"数量":<15}{" " * (terminal_width - 49)}│')
                for i, (price, size) in enumerate(ob.get('bids', [])[:3], 1):
                    print(f'│   [{i}] {" " * 10} ${price:<13.2f} {size:<13.4f}{" " * (terminal_width - 48)}│')
                
                print(f'│   {"卖盘":<15} {"价格":<15} {"数量":<15}{" " * (terminal_width - 49)}│')
                for i, (price, size) in enumerate(ob.get('asks', [])[:3], 1):
                    print(f'│   [{i}] {" " * 10} ${price:<13.2f} {size:<13.4f}{" " * (terminal_width - 48)}│')
        else:
            print(f'│ ❌ 数据获取失败{" " * (terminal_width - 14)}│')
        
        print('└' + '─' * (terminal_width - 2) + '┘')
        
        # ==================== Variational 数据 ====================
        print('\n┌' + '─' * (terminal_width - 2) + '┐')
        print(f'│ 📊 Variational 交易所{" " * (terminal_width - 20)}│')
        print('├' + '─' * (terminal_width - 2) + '┤')
        
        if snapshot.variational:
            var = snapshot.variational
            
            # 基础价格信息
            print(f'│ {"价格信息":<{terminal_width - 3}}│')
            print(f'│   买一价格: ${var["bid_price"]:>10.2f}    数量: {var.get("bid_size", 0):>8.4f}{" " * (terminal_width - 51)}│')
            print(f'│   卖一价格: ${var["ask_price"]:>10.2f}    数量: {var.get("ask_size", 0):>8.4f}{" " * (terminal_width - 51)}│')
            print(f'│   中间价格: ${var["mid_price"]:>10.2f}{" " * (terminal_width - 30)}│')
            
            # 价差分析
            var_spread = var["ask_price"] - var["bid_price"]
            var_spread_pct = (var_spread / var["mid_price"]) * 100
            print(f'│   买卖价差: ${var_spread:>10.2f}    ({var_spread_pct:>6.3f}%){" " * (terminal_width - 48)}│')
            
            # 请求性能
            if var.get('request_duration'):
                duration = var['request_duration']
                is_stale = var.get('is_stale', False)
                perf_status = "⚠️  慢" if is_stale else "✅ 快"
                print(f'│   请求耗时: {duration:>6.3f}s  {perf_status}{" " * (terminal_width - 30)}│')
            
            # 数据时效性
            if 'timestamp' in var:
                data_age = (snapshot.timestamp - var["timestamp"]) / 1000
                freshness = "🟢 新鲜" if data_age < 0.5 else "🟡 一般" if data_age < 1.0 else "🔴 过期"
                print(f'│   数据时间: {var["timestamp"]:<15}  延迟: {data_age:.3f}s  {freshness}{" " * (terminal_width - 62)}│')
            
            # # 订单簿深度 (如果有)
            # if var.get('order_book'):
            #     print(f'│ {"订单簿深度":<{terminal_width - 3}}│')
            #     ob = var['order_book']
                
            #     # 显示前3档
            #     print(f'│   {"买盘":<15} {"价格":<15} {"数量":<15}{" " * (terminal_width - 49)}│')
            #     for i, (price, size) in enumerate(ob.get('bids', [])[:3], 1):
            #         print(f'│   [{i}] {" " * 10} ${price:<13.2f} {size:<13.4f}{" " * (terminal_width - 48)}│')
                
            #     print(f'│   {"卖盘":<15} {"价格":<15} {"数量":<15}{" " * (terminal_width - 49)}│')
            #     for i, (price, size) in enumerate(ob.get('asks', [])[:3], 1):
            #         print(f'│   [{i}] {" " * 10} ${price:<13.2f} {size:<13.4f}{" " * (terminal_width - 48)}│')
        else:
            print(f'│ ❌ 数据获取失败{" " * (terminal_width - 14)}│')
        
        print('└' + '─' * (terminal_width - 2) + '┘')
        
        # ==================== 套利分析 ====================
        # if snapshot.spread is not None and snapshot.extended and snapshot.variational:
        #     print('\n┌' + '─' * (terminal_width - 2) + '┐')
        #     print(f'│ 💰 套利机会分析{" " * (terminal_width - 15)}│')
        #     print('├' + '─' * (terminal_width - 2) + '┤')
            
        #     ext = snapshot.extended
        #     var = snapshot.variational
            
        #     # 计算两个方向的价差
        #     spread1 = var['bid_price'] - ext['ask_price']  # Ext买 -> Var卖
        #     spread2 = ext['bid_price'] - var['ask_price']  # Var买 -> Ext卖
            
        #     # 基础信息
        #     print(f'│ 价差统计{" " * (terminal_width - 10)}│')
        #     print(f'│   绝对价差: ${abs(snapshot.spread):>10.2f}{" " * (terminal_width - 30)}│')
        #     print(f'│   相对价差: {abs(snapshot.spread_percentage):>9.4f}%{" " * (terminal_width - 30)}│')
            
        #     # 时间同步检查
        #     time_diff = abs(ext.get('timestamp', 0) - var.get('timestamp', 0)) / 1000
        #     sync_status = "✅ 同步" if time_diff < 0.5 else "⚠️  延迟" if time_diff < 1.0 else "❌ 失步"
        #     print(f'│   时间差异: {time_diff:>8.3f}s  {sync_status}{" " * (terminal_width - 34)}│')
            
        #     print('│' + ' ' * (terminal_width - 2) + '│')
            
        #     # 方向1: Extended买 -> Variational卖
        #     print(f'│ 方向1: Extended 买入 → Variational 卖出{" " * (terminal_width - 39)}│')
        #     print(f'│   买入成本 (Extended):  ${ext["ask_price"]:>10.2f}{" " * (terminal_width - 38)}│')
        #     print(f'│   卖出收入 (Variational): ${var["bid_price"]:>10.2f}{" " * (terminal_width - 41)}│')
        #     print(f'│   理论利润: ${spread1:>10.2f}', end='')
            
        #     if spread1 > 0:
        #         profit_pct1 = (spread1 / ext["ask_price"]) * 100
        #         profit_bps1 = profit_pct1 * 100
        #         print(f'  ({profit_pct1:>6.3f}% / {profit_bps1:>7.1f} bps) 🟢{" " * (terminal_width - 68)}│')
        #     else:
        #         print(f'  ❌ 无利润{" " * (terminal_width - 32)}│')
            
        #     print('│' + ' ' * (terminal_width - 2) + '│')
            
        #     # 方向2: Variational买 -> Extended卖
        #     print(f'│ 方向2: Variational 买入 → Extended 卖出{" " * (terminal_width - 39)}│')
        #     print(f'│   买入成本 (Variational): ${var["ask_price"]:>10.2f}{" " * (terminal_width - 41)}│')
        #     print(f'│   卖出收入 (Extended):  ${ext["bid_price"]:>10.2f}{" " * (terminal_width - 38)}│')
        #     print(f'│   理论利润: ${spread2:>10.2f}', end='')
            
        #     if spread2 > 0:
        #         profit_pct2 = (spread2 / var["ask_price"]) * 100
        #         profit_bps2 = profit_pct2 * 100
        #         print(f'  ({profit_pct2:>6.3f}% / {profit_bps2:>7.1f} bps) 🟢{" " * (terminal_width - 68)}│')
        #     else:
        #         print(f'  ❌ 无利润{" " * (terminal_width - 32)}│')
            
        #     print('│' + ' ' * (terminal_width - 2) + '│')
            
        #     # 套利建议
        #     threshold = 0.05  # 5个基点
        #     max_spread = max(spread1, spread2)
            
        #     if max_spread > 0:
        #         best_profit_pct = max(
        #             (spread1 / ext["ask_price"]) * 100 if spread1 > 0 else 0,
        #             (spread2 / var["ask_price"]) * 100 if spread2 > 0 else 0
        #         )
                
        #         if best_profit_pct > threshold:
        #             print(f'│ 🔥 交易建议{" " * (terminal_width - 11)}│')
                    
        #             if spread1 > spread2:
        #                 print(f'│   推荐策略: Extended 买入 → Variational 卖出{" " * (terminal_width - 42)}│')
        #                 print(f'│   预期收益: {best_profit_pct:.3f}% ({best_profit_pct * 100:.1f} bps){" " * (terminal_width - 44)}│')
        #             else:
        #                 print(f'│   推荐策略: Variational 买入 → Extended 卖出{" " * (terminal_width - 42)}│')
        #                 print(f'│   预期收益: {best_profit_pct:.3f}% ({best_profit_pct * 100:.1f} bps){" " * (terminal_width - 44)}│')
                    
        #             # 风险提示
        #             if time_diff > 0.5:
        #                 print(f'│   ⚠️  风险提示:{" " * (terminal_width - 15)}│')
        #                 print(f'│      - 数据时间差较大 ({time_diff:.2f}s)，价格可能已变化{" " * (terminal_width - 48)}│')
        #             if var.get('is_stale'):
        #                 print(f'│   ⚠️  风险提示:{" " * (terminal_width - 15)}│')
        #                 print(f'│      - Variational 数据获取较慢，可能不够实时{" " * (terminal_width - 44)}│')
                    
        #             # 数量建议
        #             min_size = min(ext.get('bid_size', 0), ext.get('ask_size', 0), 
        #                           var.get('bid_size', 0), var.get('ask_size', 0))
        #             if min_size > 0:
        #                 print(f'│   建议数量: ≤ {min_size:.4f} (受限于最小档位){" " * (terminal_width - 42)}│')
        #         else:
        #             print(f'│ ℹ️  当前价差较小{" " * (terminal_width - 14)}│')
        #             print(f'│   最大收益: {best_profit_pct:.3f}% (阈值: {threshold}%){" " * (terminal_width - 38)}│')
        #     else:
        #         print(f'│ ❌ 无套利机会 (两个方向均无正价差){" " * (terminal_width - 34)}│')
            
        #     print('└' + '─' * (terminal_width - 2) + '┘')
        
        # elif snapshot.spread is None:
        #     print('\n┌' + '─' * (terminal_width - 2) + '┐')
        #     print(f'│ 💰 套利分析: 数据不完整，无法计算{" " * (terminal_width - 31)}│')
        #     print('└' + '─' * (terminal_width - 2) + '┘')
        
        # 底部信息栏
        print('\n' + '═' * terminal_width)
        footer = f'下次更新: {self.interval_seconds}秒后 | 数据保存: {self.data_dir}'
        print(f'{footer:^{terminal_width}}')
        print('═' * terminal_width + '\n')

    async def start(self):
        """开始监控"""
        if self.is_running:
            return
        
        logger.info(f'🚀 开始监控 {self.symbol} 价格 (Extended & Variational)')
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
                    self.display_snapshot_s(snapshot)
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
        logger.info(f"[{self.symbol}] 正在清理资源...")
        self.stop_flag = True
        
        # Extended 独立关闭
        if self.extended_client:
            try:
                await self.extended_client.disconnect()
            except Exception as e:
                logger.error(f"[{self.symbol}] 断开 Extended 连接错误: {e}")
        
        # ✅ Variational 释放引用（而不是直接 disconnect）
        if self.variational_client:
            try:
                await VariationalClientManager.release_client()  # ✅ 使用管理器释放
            except Exception as e:
                logger.error(f"[{self.symbol}] 释放 Variational 客户端错误: {e}")
        
        await asyncio.sleep(1)
        logger.info(f"[{self.symbol}] ✅ 资源清理完成")

    def stop(self):
        """停止监控"""
        self.is_running = False


async def main():
    fetcher = PriceFetcher(
        symbol='BTC',
        interval_seconds=5
    )
    
    await fetcher.start()


if __name__ == '__main__':
    asyncio.run(main())
