"""Extended REST API 延迟测试工具"""

import asyncio
import logging
import sys
import time
import statistics
from pathlib import Path
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import List, Dict, Tuple, Optional

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from exchanges.extended import ExtendedClient
from helpers.util import Config
from dotenv import load_dotenv

# ========== 日志配置 ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# ========== 延迟统计类 ==========

class LatencyStats:
    """接口延迟统计"""
    
    def __init__(self, name: str):
        self.name = name
        self.latencies: List[float] = []  # 毫秒
        self.errors = 0
        self.successes = 0
    
    def record(self, latency_ms: float, success: bool = True):
        """记录一次调用"""
        if success:
            self.latencies.append(latency_ms)
            self.successes += 1
        else:
            self.errors += 1
    
    def get_stats(self) -> dict:
        """获取统计数据"""
        if not self.latencies:
            return {
                'name': self.name,
                'count': 0,
                'successes': self.successes,
                'errors': self.errors,
                'min': 0,
                'max': 0,
                'avg': 0,
                'median': 0,
                'p95': 0,
                'p99': 0
            }
        
        sorted_latencies = sorted(self.latencies)
        count = len(sorted_latencies)
        
        return {
            'name': self.name,
            'count': count,
            'successes': self.successes,
            'errors': self.errors,
            'min': min(sorted_latencies),
            'max': max(sorted_latencies),
            'avg': statistics.mean(sorted_latencies),
            'median': statistics.median(sorted_latencies),
            'p95': sorted_latencies[int(count * 0.95)] if count > 0 else 0,
            'p99': sorted_latencies[int(count * 0.99)] if count > 0 else 0
        }
    
    def print_stats(self):
        """打印统计结果"""
        stats = self.get_stats()
        
        print(f"\n{'='*70}")
        print(f"📊 {stats['name']}")
        print(f"{'='*70}")
        print(f"调用次数:   {stats['count']} 次")
        print(f"成功:       {stats['successes']} 次")
        print(f"失败:       {stats['errors']} 次")
        
        if stats['count'] > 0:
            print(f"\n延迟统计 (ms):")
            print(f"  最小值:   {stats['min']:.2f} ms")
            print(f"  最大值:   {stats['max']:.2f} ms")
            print(f"  平均值:   {stats['avg']:.2f} ms")
            print(f"  中位数:   {stats['median']:.2f} ms")
            print(f"  P95:      {stats['p95']:.2f} ms")
            print(f"  P99:      {stats['p99']:.2f} ms")
        
        print(f"{'='*70}\n")

# ========== Extended API 测试类 ==========

class ExtendedAPILatencyTest:
    """Extended REST API 延迟测试"""
    
    def __init__(self, symbol: str = 'ETH'):
        self.symbol = symbol
        self.contract_id = f"{symbol}-USD"
        self.client: ExtendedClient = None
        
        # ✅ WebSocket 订单推送监听
        self._order_push_times: Dict[str, float] = {}  # order_id -> 推送时间
        self._order_place_times: Dict[str, float] = {}  # order_id -> 下单时间
        
        # ✅ 各接口的统计
        self.stats = {
            'get_markets': LatencyStats('获取市场信息 (get_markets)'),
            'get_order_info': LatencyStats('获取订单信息 (get_order_info)'),
            'get_positions': LatencyStats('获取持仓 (get_positions)'),
            'get_open_orders': LatencyStats('获取活跃订单 (get_open_orders)'),
            'place_order_buy': LatencyStats('下市价买单 (place_order BUY)'),
            'place_order_sell': LatencyStats('下市价卖单 (place_order SELL)'),
            'cancel_order': LatencyStats('取消订单 (cancel_order)'),
            'round_trip': LatencyStats('往返延迟 (下单→成交确认)'),
            'ws_push_latency': LatencyStats('WebSocket 推送延迟 (下单→收到推送)')  # ✅ 新增
        }
    
    async def setup(self):
        """初始化客户端"""
        logger.info(f"🔧 初始化 Extended 客户端...")
        
        config = Config({
            'exchange': 'extended',
            'ticker': self.symbol,
            'contract_id': self.contract_id,
            'quantity': Decimal('0.01'),
            'open_order_side': 'buy',
            'close_order_side': 'sell',
        })
        
        self.client = ExtendedClient(config)
        await self.client.connect()
        
        # ✅ 设置 WebSocket 订单推送监听
        self._setup_order_push_handler()
        
        # ✅ 获取合约属性（tick_size 等）
        await self.client.get_contract_attributes()
        
        logger.info(f"✅ 客户端已初始化: {self.contract_id}")
        logger.info(f"   tick_size: {self.client.config.tick_size}")
        logger.info(f"   min_order_size: {self.client.min_order_size}")
    
    def _setup_order_push_handler(self):
        """设置 WebSocket 订单推送监听器"""
        
        def order_update_handler(order_data: dict):
            """监听订单推送"""
            order_id = order_data.get('order_id')
            status = order_data.get('status')
            
            if not order_id:
                return
            
            # ✅ 记录推送时间
            push_time = time.time()
            self._order_push_times[order_id] = push_time
            
            # ✅ 计算推送延迟
            if order_id in self._order_place_times:
                place_time = self._order_place_times[order_id]
                push_latency = (push_time - place_time) * 1000
                
                logger.info(
                    f"📨 WebSocket 推送: order_id={order_id}, status={status}, "
                    f"推送延迟={push_latency:.2f} ms"
                )
                
                # ✅ 记录统计（只记录 FILLED 状态）
                if status in ['FILLED', 'PARTIALLY_FILLED']:
                    self.stats['ws_push_latency'].record(push_latency, success=True)
            else:
                logger.debug(f"📨 WebSocket 推送: order_id={order_id}, status={status} (未追踪)")
        
        # ✅ 注册处理器
        self.client.setup_order_update_handler(order_update_handler)
        logger.info("✅ WebSocket 订单推送监听器已设置")
    
    async def cleanup(self):
        """清理资源"""
        if self.client:
            await self.client.disconnect()
        logger.info("✅ 客户端已断开")
    
    # ========== 1. 获取市场信息 ==========
    
    async def test_get_markets(self, count: int = 10):
        """测试获取市场信息接口"""
        logger.info(f"\n📡 测试 get_markets ({count} 次)...")
        
        for i in range(count):
            try:
                start = time.time()
                
                result = await self.client.perpetual_trading_client.markets_info.get_markets(
                    market_names=[self.contract_id]
                )
                
                end = time.time()
                latency = (end - start) * 1000
                
                success = (result and hasattr(result, 'data') and len(result.data) > 0)
                self.stats['get_markets'].record(latency, success)
                
                logger.debug(f"  [{i+1}/{count}] {latency:.2f} ms - {'✅' if success else '❌'}")
                
                await asyncio.sleep(0.1)
            
            except Exception as e:
                logger.error(f"  [{i+1}/{count}] ❌ 异常: {e}")
                self.stats['get_markets'].record(0, success=False)
                await asyncio.sleep(0.5)
    
    # ========== 2. 获取订单信息 ==========
    
    async def test_get_order_info(self, order_id: str):
        """测试获取订单信息接口（单次）"""
        try:
            start = time.time()
            
            order_info = await self.client.get_order_info(order_id)
            
            end = time.time()
            latency = (end - start) * 1000
            
            success = (order_info is not None)
            self.stats['get_order_info'].record(latency, success)
            
            logger.debug(f"  get_order_info: {latency:.2f} ms - {'✅' if success else '❌'}")
            
            return order_info
        
        except Exception as e:
            logger.error(f"  get_order_info 异常: {e}")
            self.stats['get_order_info'].record(0, success=False)
            return None
    
    # ========== 3. 获取持仓 ==========
    
    async def test_get_positions(self, count: int = 10):
        """测试获取持仓接口"""
        logger.info(f"\n📡 测试 get_positions ({count} 次)...")
        
        for i in range(count):
            try:
                start = time.time()
                
                result = await self.client.perpetual_trading_client.account.get_positions(
                    market_names=[self.contract_id]
                )
                
                end = time.time()
                latency = (end - start) * 1000
                
                success = (result and hasattr(result, 'data'))
                self.stats['get_positions'].record(latency, success)
                
                logger.debug(f"  [{i+1}/{count}] {latency:.2f} ms - {'✅' if success else '❌'}")
                
                await asyncio.sleep(0.1)
            
            except Exception as e:
                logger.error(f"  [{i+1}/{count}] ❌ 异常: {e}")
                self.stats['get_positions'].record(0, success=False)
                await asyncio.sleep(0.5)
    
    # ========== 4. 获取活跃订单 ==========
    
    async def test_get_open_orders(self, count: int = 10):
        """测试获取活跃订单接口"""
        logger.info(f"\n📡 测试 get_open_orders ({count} 次)...")
        
        for i in range(count):
            try:
                start = time.time()
                
                result = await self.client.perpetual_trading_client.account.get_open_orders(
                    market_names=[self.contract_id]
                )
                
                end = time.time()
                latency = (end - start) * 1000
                
                success = (result and hasattr(result, 'data'))
                self.stats['get_open_orders'].record(latency, success)
                
                logger.debug(f"  [{i+1}/{count}] {latency:.2f} ms - {'✅' if success else '❌'}")
                
                await asyncio.sleep(0.1)
            
            except Exception as e:
                logger.error(f"  [{i+1}/{count}] ❌ 异常: {e}")
                self.stats['get_open_orders'].record(0, success=False)
                await asyncio.sleep(0.5)
    
    # ========== 5. 下单接口（买/卖） ==========
    
    async def test_place_order(self, side: str = 'buy') -> Tuple[Optional[str], float]:
        """
        测试下单接口（单次）
        
        Returns:
            (order_id, latency_ms)
        """
        try:
            from x10.perpetual.orders import OrderSide, TimeInForce
            
            # ✅ 获取当前价格
            best_bid, best_ask, _ = await self.client.fetch_bbo_prices(self.contract_id)
            
            if best_bid <= 0 or best_ask <= 0:
                logger.error("❌ 无效的 bid/ask 价格")
                return None, 0
            
            # ✅ 计算订单价格（IOC 市价单）
            if side == 'buy':
                order_price = best_ask
                order_side = OrderSide.BUY
                stat_key = 'place_order_buy'
            else:
                order_price = best_bid
                order_side = OrderSide.SELL
                stat_key = 'place_order_sell'
            
            order_price = self.client.round_to_tick(order_price)
            quantity = Decimal('0.01')
            
            logger.info(f"  📤 下{side.upper()}单: {quantity} @ ${order_price}")
            
            # ✅ 记录下单时间
            place_start = time.time()
            
            order_result = await self.client.perpetual_trading_client.place_order(
                market_name=self.contract_id,
                amount_of_synthetic=quantity,
                price=order_price,
                side=order_side,
                time_in_force=TimeInForce.IOC,
                post_only=False,
                expire_time=datetime.now(tz=timezone.utc) + timedelta(days=1)
            )
            
            place_end = time.time()
            api_latency = (place_end - place_start) * 1000
            
            # ✅ 检查结果
            if not order_result or not hasattr(order_result, 'data') or not order_result.data:
                logger.error(f"  ❌ 下单失败: {getattr(order_result, 'message', 'Unknown')}")
                self.stats[stat_key].record(0, success=False)
                return None, 0
            
            order_id = order_result.data.id
            
            # ✅ 记录下单时间（用于计算推送延迟）
            self._order_place_times[order_id] = place_start
            
            logger.info(
                f"  ✅ 下单成功: {order_id}\n"
                f"     API 耗时: {api_latency:.2f} ms\n"
                f"     等待 WebSocket 推送..."
            )
            self.stats[stat_key].record(api_latency, success=True)
            
            # ✅ 等待 WebSocket 推送（最多 2 秒）
            await asyncio.sleep(2)
            
            # ✅ 检查是否收到推送
            if order_id in self._order_push_times:
                push_latency = (self._order_push_times[order_id] - place_start) * 1000
                logger.info(f"  📨 WebSocket 推送延迟: {push_latency:.2f} ms")
            else:
                logger.warning(f"  ⚠️ 未收到 WebSocket 推送（2秒超时）")
            
            return order_id, api_latency
        
        except Exception as e:
            logger.error(f"  ❌ 下单异常: {e}")
            import traceback
            traceback.print_exc()
            
            stat_key = 'place_order_buy' if side == 'buy' else 'place_order_sell'
            self.stats[stat_key].record(0, success=False)
            return None, 0
    
    # ========== 6. 往返延迟测试（下单→成交确认） ==========
    
    async def test_round_trip_latency(self, count: int = 5):
        """
        测试往返延迟：下单 → 等待成交 → 获取订单状态
        
        流程：
        1. 下买单（IOC 市价单，应立即成交）
        2. 等待 2 秒（等待 WebSocket 推送）
        3. 下卖单（平仓）
        4. 记录总耗时
        """
        logger.info(f"\n📡 测试往返延迟 (下单→成交确认) ({count} 次)...")
        
        for i in range(count):
            try:
                logger.info(f"\n  --- 第 {i+1}/{count} 轮 ---")
                
                # ✅ 1. 下买单
                logger.info("  📤 下买单...")
                buy_order_id, buy_latency = await self.test_place_order(side='buy')
                
                if not buy_order_id:
                    logger.error("  ❌ 买单失败，跳过本轮")
                    await asyncio.sleep(2)
                    continue
                
                # ✅ 2. 等待成交（2 秒）
                logger.info("  ⏱️ 等待 2 秒...")
                await asyncio.sleep(2)
                
                # ✅ 3. 确认买单已成交
                buy_info = await self.test_get_order_info(buy_order_id)
                
                if not buy_info or buy_info.status != 'FILLED':
                    logger.warning(f"  ⚠️ 买单未成交: {buy_info.status if buy_info else 'Unknown'}")
                else:
                    logger.info(f"  ✅ 买单已成交: {buy_order_id}")
                
                # ✅ 4. 下卖单（平仓）
                logger.info("  📤 下卖单...")
                sell_order_id, sell_latency = await self.test_place_order(side='sell')
                
                if not sell_order_id:
                    logger.error("  ❌ 卖单失败")
                    await asyncio.sleep(2)
                    continue
                
                # ✅ 5. 等待卖单成交
                await asyncio.sleep(2)
                sell_info = await self.test_get_order_info(sell_order_id)
                
                if not sell_info or sell_info.status != 'FILLED':
                    logger.warning(f"  ⚠️ 卖单未成交: {sell_info.status if sell_info else 'Unknown'}")
                else:
                    logger.info(f"  ✅ 卖单已成交: {sell_order_id}")
                
                # ✅ 6. 记录往返延迟（API 延迟）
                round_trip_time = buy_latency + sell_latency
                self.stats['round_trip'].record(round_trip_time, success=True)
                
                logger.info(f"  ⏱️ 往返延迟 (API): {round_trip_time:.2f} ms")
                
                # ✅ 等待下一轮
                await asyncio.sleep(3)
            
            except Exception as e:
                logger.error(f"  ❌ 第 {i+1} 轮异常: {e}")
                import traceback
                traceback.print_exc()
                
                self.stats['round_trip'].record(0, success=False)
                await asyncio.sleep(5)
    
    # ========== 打印所有统计 ==========
    
    def print_all_stats(self):
        """打印所有接口的统计结果"""
        print(f"\n\n{'='*70}")
        print(f"📊 Extended REST API 延迟测试报告 - {self.symbol}")
        print(f"{'='*70}\n")
        
        for key in [
            'get_markets',
            'get_order_info',
            'get_positions',
            'get_open_orders',
            'place_order_buy',
            'place_order_sell',
            'ws_push_latency',  # ✅ WebSocket 推送延迟
            'round_trip'
        ]:
            if self.stats[key].successes > 0 or self.stats[key].errors > 0:
                self.stats[key].print_stats()
        
        print(f"{'='*70}")
        print(f"✅ 测试完成")
        print(f"{'='*70}\n")

# ========== 主函数 ==========

async def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Extended REST API 延迟测试工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 完整测试（包含下单）
  python scripts/test_extended_api_latency.py --symbol ETH --full
  
  # 只测试查询接口（不下单）
  python scripts/test_extended_api_latency.py --symbol ETH --query-only
  
  # 自定义测试次数
  python scripts/test_extended_api_latency.py --symbol ETH --count 20
        """
    )
    
    parser.add_argument('--symbol', type=str, default='ETH', help='交易对（默认：ETH）')
    parser.add_argument('--count', type=int, default=10, help='每个接口测试次数（默认：10）')
    parser.add_argument('--full', action='store_true', help='完整测试（包含下单）')
    parser.add_argument('--query-only', action='store_true', help='只测试查询接口（不下单）')
    parser.add_argument('--env-file', type=str, default=None, help='环境变量文件路径')
    
    args = parser.parse_args()
    
    # ✅ 加载环境变量
    if args.env_file:
        logger.info(f"📁 加载环境变量: {args.env_file}")
        load_dotenv(args.env_file)
    else:
        load_dotenv()
    
    # ✅ 创建测试实例
    test = ExtendedAPILatencyTest(symbol=args.symbol)
    
    try:
        # ✅ 初始化
        await test.setup()
        
        # ✅ 测试查询接口
        await test.test_get_markets(count=args.count)
        await test.test_get_positions(count=args.count)
        await test.test_get_open_orders(count=args.count)
        
        # ✅ 测试下单接口
        if args.full:
            logger.info("\n🚀 开始完整测试（包含下单）...")
            await test.test_round_trip_latency(count=5)
        elif not args.query_only:
            logger.info("\n🚀 测试单次下单...")
            # 只测试一次买/卖
            await test.test_place_order(side='buy')
            await asyncio.sleep(2)
            await test.test_place_order(side='sell')
        
        # ✅ 打印统计
        test.print_all_stats()
    
    except KeyboardInterrupt:
        logger.info("\n👋 测试被用户中断")
    except Exception as e:
        logger.error(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # ✅ 清理
        await test.cleanup()


if __name__ == '__main__':
    asyncio.run(main())