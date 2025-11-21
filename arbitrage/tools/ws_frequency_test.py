"""WebSocket 推送频率测试工具"""

import asyncio
import logging
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
from decimal import Decimal

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from exchanges.variational import VariationalClient
from exchanges.extended import ExtendedClient
from exchanges.lighter import LighterClient
from helpers.util import Config
from dotenv import load_dotenv

# ========== 日志配置 ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# ========== 统计收集器 ==========

class FrequencyStats:
    """推送频率统计"""
    
    def __init__(self, name: str):
        self.name = name
        self.update_count = 0
        self.first_update_time = None
        self.last_update_time = None
        self.update_timestamps = []
        self.intervals = []  # 推送间隔（秒）
        
    def record_update(self):
        """记录一次更新"""
        now = time.time()
        
        if self.first_update_time is None:
            self.first_update_time = now
        else:
            # 计算间隔
            interval = now - self.last_update_time
            self.intervals.append(interval)
        
        self.last_update_time = now
        self.update_timestamps.append(now)
        self.update_count += 1
    
    def get_stats(self):
        """获取统计数据"""
        if self.update_count == 0:
            return {
                'name': self.name,
                'count': 0,
                'duration': 0,
                'avg_frequency': 0,
                'min_interval': 0,
                'max_interval': 0,
                'avg_interval': 0
            }
        
        duration = self.last_update_time - self.first_update_time if self.first_update_time else 0
        avg_frequency = self.update_count / duration if duration > 0 else 0
        
        stats = {
            'name': self.name,
            'count': self.update_count,
            'duration': duration,
            'avg_frequency': avg_frequency,  # 每秒推送次数
        }
        
        if self.intervals:
            stats['min_interval'] = min(self.intervals)
            stats['max_interval'] = max(self.intervals)
            stats['avg_interval'] = sum(self.intervals) / len(self.intervals)
        else:
            stats['min_interval'] = 0
            stats['max_interval'] = 0
            stats['avg_interval'] = 0
        
        return stats
    
    def print_stats(self):
        """打印统计结果"""
        stats = self.get_stats()
        
        print(f"\n{'='*60}")
        print(f"📊 {stats['name']} - WebSocket 推送统计")
        print(f"{'='*60}")
        print(f"总推送次数: {stats['count']}")
        print(f"测试时长:   {stats['duration']:.2f} 秒 ({stats['duration']/60:.2f} 分钟)")
        print(f"平均频率:   {stats['avg_frequency']:.2f} 次/秒")
        
        if stats['count'] > 1:
            print(f"推送间隔:")
            print(f"  最小: {stats['min_interval']*1000:.2f} ms")
            print(f"  最大: {stats['max_interval']*1000:.2f} ms")
            print(f"  平均: {stats['avg_interval']*1000:.2f} ms")
        
        print(f"{'='*60}\n")

# ========== 交易所测试器 ==========

class VariationalFrequencyTest:
    """Variational WebSocket 频率测试"""
    
    def __init__(self, symbol: str = 'ETH'):
        self.symbol = symbol
        self.client = None
        self.stats = FrequencyStats(f'Variational-{symbol}')
        self.is_running = False
    
    async def start(self, duration_seconds: int = 300):
        """启动测试"""
        logger.info(f"🚀 开始测试 Variational WebSocket 推送频率 ({duration_seconds}秒)")
        
        # 初始化客户端
        config = Config({
            'exchange': 'variational',
            'ticker': self.symbol,
        })
        
        self.client = VariationalClient(config)
        await self.client.connect()
        
        # ✅ Variational 使用 subscribe_price_updates
        self.client.subscribe_price_updates(self._on_price_update)
        self.client.subscribe_position_updates(self._on_position_update)
        
        self.is_running = True
        start_time = time.time()
        
        logger.info(f"✅ 已连接，开始统计...")
        
        # 运行指定时长
        while self.is_running and (time.time() - start_time) < duration_seconds:
            await asyncio.sleep(1)
            
            # 每 30 秒打印一次中间结果
            elapsed = time.time() - start_time
            if int(elapsed) % 30 == 0 and int(elapsed) > 0:
                count = self.stats.update_count
                freq = count / elapsed if elapsed > 0 else 0
                logger.info(f"⏱️ {elapsed:.0f}s - 推送次数: {count}, 频率: {freq:.2f}/s")
        
        # 打印最终统计
        self.stats.print_stats()
        
        # 断开连接
        await self.client.disconnect()
        logger.info("✅ 测试完成")
    
    def _on_price_update(self, data):
        """价格更新回调"""
        self.stats.record_update()
        
        # 记录第一次推送的详细信息
        if self.stats.update_count == 1:
            logger.info(f"📊 首次推送数据: {data}")
    
    def _on_position_update(self, data):
        """持仓更新回调（不统计，仅记录）"""
        logger.debug(f"持仓更新: {data}")

class ExtendedFrequencyTest:
    """Extended WebSocket 频率测试"""
    
    def __init__(self, symbol: str = 'ETH'):
        self.symbol = symbol
        self.client = None
        self.stats = FrequencyStats(f'Extended-{symbol}')
        self.is_running = False
        self._monitoring_task = None
    
    async def start(self, duration_seconds: int = 300):
        """启动测试"""
        logger.info(f"🚀 开始测试 Extended WebSocket 推送频率 ({duration_seconds}秒)")
        
        # 初始化客户端
        config = Config({
            'exchange': 'extended',
            'ticker': self.symbol,
            'contract_id': f'{self.symbol}-USD',
        })
        
        self.client = ExtendedClient(config)
        await self.client.connect()
        
        # ✅ 等待 WebSocket 接收初始数据
        await asyncio.sleep(2)
        
        if not self.client.orderbook:
            logger.warning("⚠️ WebSocket 未接收到初始订单簿数据")
        else:
            logger.info(f"✅ 初始订单簿: {self.client.orderbook}")
        
        self.is_running = True
        start_time = time.time()
        
        logger.info(f"✅ 已连接，开始统计...")
        
        # ✅ 使用内部 orderbook 监控
        self._monitoring_task = asyncio.create_task(self._monitor_prices())
        
        # 运行指定时长
        try:
            while self.is_running and (time.time() - start_time) < duration_seconds:
                await asyncio.sleep(1)
                
                # 每 30 秒打印一次中间结果
                elapsed = time.time() - start_time
                if int(elapsed) % 30 == 0 and int(elapsed) > 0:
                    count = self.stats.update_count
                    freq = count / elapsed if elapsed > 0 else 0
                    logger.info(f"⏱️ {elapsed:.0f}s - 推送次数: {count}, 频率: {freq:.2f}/s")
        
        finally:
            self.is_running = False
            
            # 停止监控任务
            if self._monitoring_task:
                self._monitoring_task.cancel()
                try:
                    await self._monitoring_task
                except asyncio.CancelledError:
                    pass
        
        # 打印最终统计
        self.stats.print_stats()
        
        # 断开连接
        await self.client.disconnect()
        logger.info("✅ 测试完成")
    
    async def _monitor_prices(self):
        """监控价格更新（读取 client.orderbook）"""
        last_orderbook = None
        consecutive_errors = 0
        
        try:
            while self.is_running:
                try:
                    # ✅ 直接读取 client.orderbook
                    current_orderbook = self.client.orderbook
                    
                    if current_orderbook:
                        # ✅ 检查是否有更新（比较时间戳或内容）
                        current_ts = current_orderbook.get('ts', 0)
                        last_ts = last_orderbook.get('ts', 0) if last_orderbook else 0
                        
                        if current_ts != last_ts:
                            # ✅ 有新的推送
                            self.stats.record_update()
                            consecutive_errors = 0
                            
                            # 记录第一次推送
                            if self.stats.update_count == 1:
                                bid = current_orderbook.get('bid', [])
                                ask = current_orderbook.get('ask', [])
                                logger.info(
                                    f"📊 首次推送数据:\n"
                                    f"   timestamp: {current_ts}\n"
                                    f"   bid: {bid[0] if bid else 'N/A'}\n"
                                    f"   ask: {ask[0] if ask else 'N/A'}"
                                )
                            
                            last_orderbook = current_orderbook.copy()
                    else:
                        consecutive_errors += 1
                        if consecutive_errors == 1:
                            logger.warning("⚠️ client.orderbook 为 None")
                    
                    # ✅ 短暂延迟（10ms 检测一次）
                    await asyncio.sleep(0.01)
                
                except Exception as e:
                    consecutive_errors += 1
                    if consecutive_errors <= 3:
                        logger.warning(f"⚠️ 监控异常 ({consecutive_errors}): {e}")
                    
                    if consecutive_errors >= 10:
                        logger.error(f"❌ 连续异常 {consecutive_errors} 次:")
                        import traceback
                        traceback.print_exc()
                        consecutive_errors = 0
                    
                    await asyncio.sleep(1)
        
        except asyncio.CancelledError:
            logger.debug("价格监控任务已取消")
class LighterFrequencyTest:
    """Lighter WebSocket 频率测试"""
    
    def __init__(self, symbol: str = 'ETH'):
        self.symbol = symbol
        self.client = None
        self.stats = FrequencyStats(f'Lighter-{symbol}')
        self.is_running = False
        self._monitoring_task = None
    
    async def start(self, duration_seconds: int = 300):
        """启动测试"""
        logger.info(f"🚀 开始测试 Lighter WebSocket 推送频率 ({duration_seconds}秒)")
        
        # 初始化客户端
        config = Config({
            'exchange': 'lighter',
            'ticker': self.symbol,
        })
        
        self.client = LighterClient(config)
        await self.client.connect()
        
        # 获取合约信息
        contract_id, tick_size = await self.client.get_contract_attributes()
        logger.info(f"✅ 合约信息: {contract_id}, tick_size: {tick_size}")
        
        self.is_running = True
        start_time = time.time()
        
        logger.info(f"✅ 已连接，开始统计...")
        
        # ✅ Lighter 使用轮询方式获取价格
        self._monitoring_task = asyncio.create_task(self._monitor_prices())
        
        # 运行指定时长
        try:
            while self.is_running and (time.time() - start_time) < duration_seconds:
                await asyncio.sleep(1)
                
                # 每 30 秒打印一次中间结果
                elapsed = time.time() - start_time
                if int(elapsed) % 30 == 0 and int(elapsed) > 0:
                    count = self.stats.update_count
                    freq = count / elapsed if elapsed > 0 else 0
                    logger.info(f"⏱️ {elapsed:.0f}s - 推送次数: {count}, 频率: {freq:.2f}/s")
        
        finally:
            self.is_running = False
            
            # 停止监控任务
            if self._monitoring_task:
                self._monitoring_task.cancel()
                try:
                    await self._monitoring_task
                except asyncio.CancelledError:
                    pass
        
        # 打印最终统计
        self.stats.print_stats()
        
        # 断开连接
        await self.client.disconnect()
        logger.info("✅ 测试完成")
    
    async def _monitor_prices(self):
        """监控价格更新（轮询模式）"""
        last_price = None
        
        try:
            while self.is_running:
                try:
                    # ✅ 获取最新价格
                    orderbook = await self.client.get_orderbook()
                    
                    if orderbook and 'bids' in orderbook and 'asks' in orderbook:
                        bids = orderbook['bids']
                        asks = orderbook['asks']
                        
                        if bids and asks:
                            current_price = {
                                'bid': float(bids[0][0]),
                                'ask': float(asks[0][0])
                            }
                            
                            # ✅ 检测到价格变化才计数
                            if current_price != last_price:
                                self.stats.record_update()
                                
                                # 记录第一次推送
                                if self.stats.update_count == 1:
                                    logger.info(f"📊 首次价格数据: {current_price}")
                                
                                last_price = current_price
                    
                    # ✅ 短暂延迟
                    await asyncio.sleep(0.1)
                
                except Exception as e:
                    logger.debug(f"获取价格失败: {e}")
                    await asyncio.sleep(1)
        
        except asyncio.CancelledError:
            logger.debug("价格监控任务已取消")


# ========== 多交易所对比测试 ==========

async def test_all_exchanges(symbol: str = 'ETH', duration_seconds: int = 300):
    """测试所有交易所的 WebSocket 推送频率"""
    logger.info(f"\n{'='*60}")
    logger.info(f"🔬 开始测试所有交易所 WebSocket 推送频率")
    logger.info(f"{'='*60}")
    logger.info(f"交易对: {symbol}")
    logger.info(f"测试时长: {duration_seconds} 秒 ({duration_seconds/60:.1f} 分钟)")
    logger.info(f"{'='*60}\n")
    
    # 创建测试器
    tests = [
        VariationalFrequencyTest(symbol),
        ExtendedFrequencyTest(symbol),
        LighterFrequencyTest(symbol),
    ]
    
    # 并发运行所有测试
    await asyncio.gather(*[test.start(duration_seconds) for test in tests])
    
    # 打印对比总结
    print(f"\n{'='*60}")
    print(f"📊 测试总结 - {symbol}")
    print(f"{'='*60}")
    print(f"{'交易所':<15} {'推送次数':<12} {'平均频率':<15} {'平均间隔'}")
    print(f"{'-'*60}")
    
    for test in tests:
        stats = test.stats.get_stats()
        avg_interval_str = f"{stats['avg_interval']*1000:.2f} ms" if stats['avg_interval'] > 0 else "N/A"
        print(
            f"{stats['name']:<15} "
            f"{stats['count']:<12} "
            f"{stats['avg_frequency']:<15.2f} "
            f"{avg_interval_str}"
        )
    
    print(f"{'='*60}\n")


# ========== 主函数 ==========

async def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='WebSocket 推送频率测试工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 测试所有交易所（5 分钟）
  python arbitrage/tools/ws_frequency_test.py
  
  # 测试单个交易所
  python arbitrage/tools/ws_frequency_test.py --exchange variational
  
  # 指定环境变量文件
  python arbitrage/tools/ws_frequency_test.py --env-file .env.prod
  
  # 自定义测试时长（10 分钟）
  python arbitrage/tools/ws_frequency_test.py --duration 600
  
  # 测试 BTC
  python arbitrage/tools/ws_frequency_test.py --symbol BTC
        """
    )
    
    parser.add_argument('--exchange', type=str, 
                       choices=['variational', 'extended', 'lighter', 'all'],
                       default='all', 
                       help='要测试的交易所（默认：all）')
    parser.add_argument('--symbol', type=str, 
                       default='ETH', 
                       help='交易对（默认：ETH）')
    parser.add_argument('--duration', type=int, 
                       default=300, 
                       help='测试时长（秒），默认 300 秒（5 分钟）')
    parser.add_argument('--env-file', type=str, 
                       default=None,
                       help='环境变量文件路径（默认：.env）')
    
    args = parser.parse_args()
    
    # ✅ 加载环境变量
    if args.env_file:
        env_path = Path(args.env_file)
        if not env_path.exists():
            logger.error(f"❌ 环境变量文件不存在: {args.env_file}")
            return
        
        logger.info(f"📁 加载环境变量文件: {args.env_file}")
        load_dotenv(args.env_file)
    else:
        # 默认加载 .env
        default_env = Path(__file__).parent.parent.parent / '.env'
        if default_env.exists():
            logger.info(f"📁 加载默认环境变量文件: {default_env}")
            load_dotenv(default_env)
        else:
            logger.warning("⚠️ 未找到 .env 文件，使用系统环境变量")
            load_dotenv()
    
    try:
        if args.exchange == 'all':
            await test_all_exchanges(args.symbol, args.duration)
        elif args.exchange == 'variational':
            test = VariationalFrequencyTest(args.symbol)
            await test.start(args.duration)
        elif args.exchange == 'extended':
            test = ExtendedFrequencyTest(args.symbol)
            await test.start(args.duration)
        elif args.exchange == 'lighter':
            test = LighterFrequencyTest(args.symbol)
            await test.start(args.duration)
    
    except KeyboardInterrupt:
        logger.info("\n👋 测试被用户中断")
    except Exception as e:
        logger.error(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    asyncio.run(main())