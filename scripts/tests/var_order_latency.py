"""
测试 Variational 订单成交延迟
1. WebSocket 仓位推送延迟测试（独立）
2. RESTful API 历史订单查询延迟测试（独立）
"""

import asyncio
import time
from decimal import Decimal
import logging
from datetime import datetime

# 导入必要的模块
import sys
import os
from pathlib import Path

# ✅ 修正：添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent  # 从 scripts/tests/ 回到项目根目录
sys.path.insert(0, str(project_root))

from exchanges.variational import VariationalClient
from helpers.util import Config
from dotenv import load_dotenv

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class OrderLatencyTester:
    """订单延迟测试器"""
    
    def __init__(self, symbol: str = 'BTC'):
        """
        Args:
            symbol: 测试币种
        """
        self.symbol = symbol
        self.client = None
        
        # 测试配置
        self.test_quantity = Decimal('0.0001')  # 测试数量
        
        # 延迟统计
        self.ws_delays = []  # WebSocket 推送延迟
        self.api_delays = []  # API 查询延迟
        
        # WebSocket 回调状态
        self.current_order_id = None
        self.current_order_side = None  # 'buy' 或 'sell'
        self.order_place_time = None
        self.ws_received_time = None
        self.delay_recorded = False  # 防止重复记录延迟
        
        # 当前仓位状态
        self.current_positions = []
        
        # 历史订单列表（用于后续查询测试）
        self.completed_orders = []
    
    async def setup(self):
        """初始化客户端"""
        logger.info("🔧 初始化 Variational 客户端...")
        
        # ✅ 使用字典方式创建配置
        config_dict = {
            'ticker': self.symbol,
            'exchange': 'variational',
            'quantity': self.test_quantity,
            'contract_id': f'{self.symbol}-PERP',
        }
        config = Config(config_dict)
        self.client = VariationalClient(config)
        
        # 连接
        await self.client.connect()
        
        # 设置 WebSocket 回调
        self.client.setup_order_update_handler(self._on_position_update)
        
        logger.info("✅ 客户端初始化完成")
        
        # 等待 WebSocket 连接稳定
        await asyncio.sleep(3)
    
    async def cleanup(self):
        """清理资源"""
        if self.client:
            try:
                await self.client.disconnect()
            except Exception as e:
                logger.warning(f"⚠️ 断开连接时出错: {e}")
        logger.info("🧹 清理完成")
    
    def _on_position_update(self, positions):
        """WebSocket 仓位更新回调"""
        self.ws_received_time = time.time()
        self.current_positions = positions if positions else []
        
        # ✅ 只在测试期间记录延迟
        if self.order_place_time and self.current_order_side and not self.delay_recorded:
            # 判断目标仓位是否达成
            position_matched = False
            
            if self.current_order_side == 'buy':
                # 买单：期望仓位列表非空（有持仓）
                if len(self.current_positions) > 0:
                    position_matched = True
            
            elif self.current_order_side == 'sell':
                # 卖单：期望仓位列表为空（无持仓）
                if len(self.current_positions) == 0:
                    position_matched = True
            
            # 如果目标仓位达成，计算延迟
            if position_matched:
                delay_ms = (self.ws_received_time - self.order_place_time) * 1000
                
                logger.info(
                    f"📊 [WebSocket 延迟] {self.current_order_side.upper()} 订单:\n"
                    f"   订单 ID: {self.current_order_id}\n"
                    f"   ⏱️  延迟: {delay_ms:.2f} ms"
                )
                
                self.ws_delays.append(delay_ms)
                self.delay_recorded = True  # 防止重复记录延迟

    async def test_websocket_latency(self, num_tests: int = 2):
        """
        测试 WebSocket 推送延迟
        
        Args:
            num_tests: 测试次数（必须是偶数，因为买卖成对）
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"🧪 开始 WebSocket 延迟测试（共 {num_tests} 次）")
        logger.info(f"{'='*60}\n")
        
        # 确保测试次数是偶数
        if num_tests % 2 != 0:
            num_tests += 1
            logger.info(f"⚠️ 调整测试次数为 {num_tests}（买卖成对）")
        
        for i in range(num_tests):
            # 交替买入卖出：买-卖-买-卖...
            side = 'buy' if i % 2 == 0 else 'sell'
            
            logger.info(f"\n--- 第 {i + 1}/{num_tests} 次测试 ({side.upper()}) ---")
            
            try:
                # ========== 1. 获取报价 ==========
                quote_data = await self.client._fetch_indicative_quote(
                    qty=self.test_quantity,
                    contract_id=f"{self.symbol}-PERP"
                )
                
                if not quote_data or 'quote_id' not in quote_data:
                    logger.error("❌ 获取报价失败")
                    continue
                
                quote_id = quote_data['quote_id']
                price = Decimal(str(quote_data.get('bid' if side == 'sell' else 'ask', '0')))
                
                logger.info(f"📊 报价: ${price}")
                
                # ========== 2. 下市价单 ==========
                # 重置状态
                self.delay_recorded = False
                self.order_place_time = None
                self.current_order_side = None
                self.current_order_id = None
                
                # 设置新值
                self.current_order_side = side
                self.order_place_time = time.time()
                
                result = await self.client._place_market_order(
                    quote_id=quote_id,
                    side=side,
                    max_slippage=0.0005
                )
                
                if not result.success:
                    logger.error(f"❌ 下单失败: {result.error_message}")
                    continue
                
                self.current_order_id = result.order_id
                logger.info(f"✅ 订单已发送: {self.current_order_id}")
                
                # 保存订单 ID 用于后续查询测试
                self.completed_orders.append({
                    'order_id': self.current_order_id,
                    'place_time': self.order_place_time,
                    'side': side
                })
                
                # ========== 3. 等待 WebSocket 推送（通过回调处理） ==========
                # ✅ 简单等待，让 WebSocket 回调自动记录延迟
                await asyncio.sleep(3)  # 等待 3 秒让 WebSocket 推送完成
                
                # 检查是否记录了延迟
                if not self.delay_recorded:
                    logger.warning(
                        f"⚠️ 3 秒内未收到目标仓位推送\n"
                        f"   当前仓位: {len(self.current_positions)} 个\n"
                        f"   期望: {'非空' if side == 'buy' else '空'}"
                    )
                
            except Exception as e:
                logger.error(f"❌ 测试异常: {e}", exc_info=True)
            
            # 等待一段时间再进行下一次测试
            if i < num_tests - 1:
                logger.info("⏳ 等待 1 秒后进行下一次测试...\n")
                await asyncio.sleep(1)
        
        # 打印 WebSocket 延迟统计
        self._print_ws_summary()
    
    async def test_api_query_latency(self, num_tests: int = 6):
        """
        测试历史订单 API 查询延迟
        实时下单后立即轮询查询，测试 API 延迟
        
        Args:
            num_tests: 测试次数（必须是偶数，因为买卖成对）
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"🧪 开始历史订单查询延迟测试（共 {num_tests} 次）")
        logger.info(f"{'='*60}\n")
        
        # 确保测试次数是偶数
        if num_tests % 2 != 0:
            num_tests += 1
            logger.info(f"⚠️ 调整测试次数为 {num_tests}（买卖成对）")
        
        for i in range(num_tests):
            # 交替买入卖出：买-卖-买-卖...
            side = 'buy' if i % 2 == 0 else 'sell'
            
            logger.info(f"\n--- 第 {i + 1}/{num_tests} 次测试 ({side.upper()}) ---")
            
            try:
                # ========== 1. 获取报价 ==========
                quote_data = await self.client._fetch_indicative_quote(
                    qty=self.test_quantity,
                    contract_id=f"{self.symbol}-PERP"
                )
                
                if not quote_data or 'quote_id' not in quote_data:
                    logger.error("❌ 获取报价失败")
                    continue
                
                quote_id = quote_data['quote_id']
                price = Decimal(str(quote_data.get('bid' if side == 'sell' else 'ask', '0')))
                
                logger.info(f"📊 报价: ${price}")
                
                # ========== 2. 下市价单 ==========
                order_place_time = time.time()
                
                result = await self.client._place_market_order(
                    quote_id=quote_id,
                    side=side,
                    max_slippage=0.0005
                )
                
                if not result.success:
                    logger.error(f"❌ 下单失败: {result.error_message}")
                    continue
                
                order_id = result.order_id
                logger.info(f"✅ 订单已发送: {order_id}")
                
                # ========== 3. 立即开始轮询查询 ==========
                logger.info("🔍 开始轮询查询历史订单...")
                
                max_attempts = 50  # 最多查询 50 次
                found = False
                
                for attempt in range(1, max_attempts + 1):
                    query_start = time.time()
                    
                    try:
                        # 查询历史订单
                        history_data = await self.client.get_orders_history(
                            limit=50,
                            offset=0,
                            rfq_id=order_id
                        )
                        
                        query_end = time.time()
                        
                        # 检查是否查到
                        if history_data and 'result' in history_data and history_data['result']:
                            # ✅ 查到了！计算延迟
                            delay_from_order_ms = (query_end - order_place_time) * 1000
                            query_time_ms = (query_end - query_start) * 1000
                            
                            order_data = history_data['result'][0]
                            
                            logger.info(
                                f"✅ 第 {attempt} 次查询成功:\n"
                                f"   订单 ID: {order_id}\n"
                                f"   订单状态: {order_data.get('status')}\n"
                                f"   成交价: ${order_data.get('price', '0')}\n"
                                f"   成交量: {order_data.get('qty', '0')}\n"
                                f"   单次查询耗时: {query_time_ms:.2f} ms\n"
                                f"   ⏱️  延迟（下单 → 查到）: {delay_from_order_ms:.2f} ms"
                            )
                            
                            self.api_delays.append(delay_from_order_ms)
                            found = True
                            break
                        else:
                            # 未查到，继续轮询
                            if attempt % 10 == 0:  # 每 10 次打印一次
                                elapsed = (time.time() - order_place_time) * 1000
                                logger.info(f"⏳ 第 {attempt} 次查询未找到（已耗时 {elapsed:.0f} ms）...")
                            
                            await asyncio.sleep(0.1)  # 每 100ms 查询一次
                    
                    except Exception as e:
                        logger.error(f"❌ 第 {attempt} 次查询异常: {e}")
                        await asyncio.sleep(0.1)
                
                if not found:
                    total_elapsed = (time.time() - order_place_time) * 1000
                    logger.error(
                        f"❌ {max_attempts} 次查询均未找到订单 {order_id}\n"
                        f"   总耗时: {total_elapsed:.0f} ms"
                    )
                
            except Exception as e:
                logger.error(f"❌ 测试异常: {e}", exc_info=True)
            
            # 等待一段时间再进行下一次测试
            if i < num_tests - 1:
                logger.info("⏳ 等待 1 秒后进行下一次测试...\n")
                await asyncio.sleep(1)
        
        # 打印 API 查询延迟统计
        self._print_api_summary()
    def _print_ws_summary(self):
        """打印 WebSocket 延迟统计"""
        logger.info(f"\n{'='*60}")
        logger.info("📊 WebSocket 延迟测试统计")
        logger.info(f"{'='*60}\n")
        
        if self.ws_delays:
            avg_delay = sum(self.ws_delays) / len(self.ws_delays)
            
            logger.info(
                f"📡 WebSocket 推送延迟:\n"
                f"   样本数: {len(self.ws_delays)}\n"
                f"   平均: {avg_delay:.2f} ms\n"
                f"   最小: {min(self.ws_delays):.2f} ms\n"
                f"   最大: {max(self.ws_delays):.2f} ms\n"
                f"   详细数据: {[f'{d:.2f}' for d in self.ws_delays]}"
            )
        else:
            logger.warning("⚠️ 无 WebSocket 延迟数据")
        
        logger.info(f"\n{'='*60}\n")
    
    def _print_api_summary(self):
        """打印 API 查询延迟统计"""
        logger.info(f"\n{'='*60}")
        logger.info("📊 历史订单查询延迟统计")
        logger.info(f"{'='*60}\n")
        
        if self.api_delays:
            avg_delay = sum(self.api_delays) / len(self.api_delays)
            
            logger.info(
                f"🔍 历史订单查询延迟:\n"
                f"   样本数: {len(self.api_delays)}\n"
                f"   平均: {avg_delay:.2f} ms\n"
                f"   最小: {min(self.api_delays):.2f} ms\n"
                f"   最大: {max(self.api_delays):.2f} ms\n"
                f"   详细数据: {[f'{d:.2f}' for d in self.api_delays]}"
            )
        else:
            logger.warning("⚠️ 无 API 查询延迟数据")
        
        logger.info(f"\n{'='*60}\n")


async def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Variational 订单延迟测试')
    parser.add_argument('--symbol', '-s', type=str, default='BTC',
                       help='测试币种（默认：BTC）')
    parser.add_argument('--ws-tests', type=int, default=6,
                       help='WebSocket 测试次数（默认：6，自动调整为偶数）')
    parser.add_argument('--api-tests', type=int, default=6,
                       help='API 查询测试次数（默认：6，自动调整为偶数）')  # ✅ 新增：API 独立测试次数
    parser.add_argument('--quantity', '-q', type=str, default='0.0001',
                       help='测试数量（默认：0.0001）')
    parser.add_argument('--test-type', choices=['ws', 'api', 'both'], default='both',
                       help='测试类型: ws=WebSocket, api=API查询, both=两者都测（默认：both）')
    parser.add_argument('--env-file', type=str, default=None,
                       help='指定 .env 文件路径（默认使用项目根目录的env文件）')
    
    args = parser.parse_args()
    
    tester = OrderLatencyTester(symbol=args.symbol)
    tester.test_quantity = Decimal(args.quantity)
    load_dotenv(args.env_file) 
    
    try:
        await tester.setup()
        
        # 根据参数选择测试类型
        if args.test_type in ['ws', 'both']:
            await tester.test_websocket_latency(num_tests=args.ws_tests)
        
        if args.test_type in ['api', 'both']:
            # ✅ 修改：API 测试现在是独立的，直接调用
            await tester.test_api_query_latency(num_tests=args.api_tests)
    
    finally:
        await tester.cleanup()


if __name__ == '__main__':
    asyncio.run(main())