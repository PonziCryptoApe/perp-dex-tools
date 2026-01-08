"""
测试 Lighter 交易所延迟的脚本。
此脚本用于测量：
1. 下单延迟：从调用 place_limit_order 到返回 OrderResult 的时间。
2. 撮合延迟：从下单到订单完全撮合 (FILLED) 的时间（使用 WebSocket 更新监控）。
3. 取消订单延迟：从调用 cancel_order 到订单状态变为 CANCELED 的时间（假设 lighter.py 有 cancel_order 方法；如果没有，需要实现）。

注意：
- 需要设置环境变量：API_KEY_PRIVATE_KEY, LIGHTER_ACCOUNT_INDEX, LIGHTER_API_KEY_INDEX。
- 需要安装 lighter SDK：pip install lighter（但在你的环境中已假设可用）。
- 替换 config 中的 ticker 和 contract_id 为实际值（如 'BTC-USD'）。
- 运行前确保 lighter.py 在同一目录或 import 路径中。
- 此脚本是异步的，使用 asyncio.run() 运行。
- 风险警告：测试订单可能导致实际交易损失，请在测试网或小额测试。
"""

import argparse
import asyncio
import json
from pathlib import Path
import sys
import time
import os
from decimal import Decimal
from typing import Dict, Any

from dotenv import load_dotenv
import requests

sys.path.insert(0, str(Path(__file__).parent.parent))
from helpers.util import Config

# 假设 lighter.py 已导入 BaseExchangeClient 和其他依赖
from exchanges.lighter import LighterClient  # 调整路径为你的实际位置
from helpers.logger import TradingLogger  # 如果需要，调整

# 示例配置
CONFIG = {
    'ticker': 'BTC-USD',  # 替换为你的交易对
    'contract_id': 1,     # 替换为实际 contract_id
    'exchange': 'lighter',
    'quantity': Decimal('0.001'),
    'open_order_side': 'buy',
    'close_order_side': 'sell',
}

async def test_order_placement_latency(client: LighterClient, quantity: Decimal, price: Decimal, side: str, iterations: int = 5):
    """
    测试下单延迟。
    :param client: LighterClient 实例
    :param quantity: 订单数量
    :param price: 订单价格
    :param side: 'buy' 或 'sell'
    :param iterations: 测试迭代次数
    :return: 平均延迟
    """
    latencies = []
    for i in range(iterations):
        start_time = time.time()
        result = await client.place_limit_order(CONFIG['contract_id'], quantity, price, side)
        end_time = time.time()
        latency = end_time - start_time
        latencies.append(latency)
        print(f"迭代 {i+1}: 下单延迟 = {latency:.4f} 秒, 成功: {result.success}")
        
        # 如果失败，跳过后续
        if not result.success:
            print(f"下单失败: {result.error_message}")
            continue
        
        # 立即取消订单以避免实际持仓（假设有 cancel_order）
        await asyncio.sleep(1)  # 短暂等待订单确认
        cancel_start = time.time()
        cancel_result = await client.cancel_order(result.order_id)  # 假设方法存在
        cancel_end = time.time()
        cancel_latency = cancel_end - cancel_start
        print(f"快速取消延迟 = {cancel_latency:.4f} 秒")
        
        await asyncio.sleep(2)  # 避免速率限制
    
    avg_latency = sum(latencies) / len(latencies)
    print(f"平均下单延迟: {avg_latency:.4f} 秒")
    return avg_latency

async def test_matching_latency(client: LighterClient, quantity: Decimal, side: str, max_wait: float = 30.0):
    """
    测试撮合延迟：下单到完全撮合。
    使用 client.current_order 监控状态（依赖 WebSocket 更新）。
    :param client: LighterClient 实例
    :param quantity: 订单数量
    :param price: 使用市场价附近的价格以增加撮合机会
    :param side: 'buy' 或 'sell'
    :param max_wait: 最大等待时间 (秒)
    :return: 撮合时间
    """
    # 获取当前最佳价格
    # best_bid, best_ask = await client.fetch_bbo_prices(CONFIG['contract_id'])
    best_bid, best_ask = await _fetch_bbo_prices()
 
    if side == 'buy':
        test_price = best_bid * Decimal('1.005')  # 略高于 bid 以增加撮合几率
    else:
        test_price = best_ask * Decimal('0.995')  # 略低于 ask
    start_time = time.time()
    result = await client.place_limit_order(CONFIG['contract_id'], quantity, test_price, side)
    placement_end = time.time()
    print(f"下单完成，放置延迟: {placement_end - start_time:.4f} 秒")
    
    if not result.success:
        print(f"下单失败，无法测试撮合: {result.error_message}")
        return None
    
    # 等待撮合，使用 current_order 监控
    wait_start = time.time()
    while time.time() - wait_start < max_wait:
        await asyncio.sleep(0.01)
        print('client.current_order', client.current_order)
        if client.current_order and client.current_order.status == 'FILLED':
            match_time = time.time() - start_time
            print(f"订单撮合完成，总撮合延迟: {match_time:.4f} 秒 (放置: {placement_end - start_time:.4f} 秒, 匹配: {match_time - (placement_end - start_time):.4f} 秒)")
            return match_time
        elif client.current_order and client.current_order.status in ['CANCELED', 'REJECTED']:
            print("订单被取消或拒绝，无法撮合")
            return None
    
    print("撮合超时")
    return None

async def test_cancellation_latency(client: LighterClient, quantity: Decimal, side: str, iterations: int = 5):
    """
    测试取消订单延迟。
    先下单，然后立即取消，并监控状态变化。
    :param client: LighterClient 实例
    :param quantity: 订单数量
    :param price: 订单价格
    :param side: 'buy' 或 'sell'
    :param iterations: 测试迭代次数
    :return: 平均取消延迟
    """
    latencies = []
    for i in range(iterations):
        best_bid, best_ask = await _fetch_bbo_prices()

        if side == 'buy':
            test_price = best_bid * Decimal('0.985')  # 略高于 bid 以增加撮合几率
        else:
            test_price = best_ask * Decimal('1.015')  # 略低于 ask
        # 下单
        order_result = await client.place_limit_order(CONFIG['contract_id'], quantity, test_price, side)
        if not order_result.success:
            print(f"迭代 {i+1}: 下单失败，跳过")
            continue
        
        await asyncio.sleep(1)  # 等待订单 OPEN
        
        # 取消
        cancel_start = time.time()
        cancel_result = await client.cancel_order(order_result.order_id)  # 假设方法存在
        cancel_send_end = time.time()
        print(f"迭代 {i+1}: 取消发送延迟: {cancel_send_end - cancel_start:.4f} 秒")
        
        if not cancel_result.success:
            print(f"取消失败: {cancel_result.error_message}")
            continue
        
        # 监控取消确认
        confirm_start = time.time()
        while time.time() - confirm_start < 10.0:  # 最大等待 10 秒
            await asyncio.sleep(0.1)
            if client.current_order and client.current_order.status == 'CANCELED':
                total_cancel_latency = time.time() - cancel_start
                latencies.append(total_cancel_latency)
                print(f"取消确认，总延迟: {total_cancel_latency:.4f} 秒")
                break
        else:
            print("取消确认超时")
        
        await asyncio.sleep(2)
    
    if latencies:
        avg_latency = sum(latencies) / len(latencies)
        print(f"平均取消延迟: {avg_latency:.4f} 秒")
        return avg_latency
    return None

async def _fetch_bbo_prices():
        url = "https://mainnet.zklighter.elliot.ai/api/v1/orderBookOrders?market_id=1&limit=1"

        headers = {"accept": "application/json"}

        response = requests.get(url, headers=headers)
        result = json.loads(response.text)
        # print(response.text)
        return Decimal(result.get('bids')[0]['price']), Decimal(result.get('asks')[0]['price'])
async def main():
    """主函数：运行所有测试。"""
    # 设置环境变量（请在实际运行前设置）
    # os.environ['API_KEY_PRIVATE_KEY'] = 'your_private_key'
    # os.environ['LIGHTER_ACCOUNT_INDEX'] = '0'
    # os.environ['LIGHTER_API_KEY_INDEX'] = '0'
    parser = argparse.ArgumentParser(
        description='Lighter REST API 延迟测试工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 完整测试（包含下单）
  python scripts/test_lighter_api_latency.py --symbol ETH --full
  
  # 只测试查询接口（不下单）
  python scripts/test_lighter_api_latency.py --symbol ETH --query-only
  
  # 自定义测试次数
  python scripts/test_lighter_api_latency.py --symbol ETH --count 20
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
        print(f"📁 加载环境变量: {args.env_file}")
        load_dotenv(args.env_file)
    else:
        load_dotenv()
    # 初始化客户端
    client = LighterClient(Config(CONFIG))
    try:
        await client.connect()
        print("连接到 Lighter 成功")
        market_id, base_mult, price_mult = await client._get_market_config('BTC')
        client.base_amount_multiplier = base_mult
        client.price_multiplier = price_mult
        print(f"市场配置: ID={market_id}, Base Mult={base_mult}, Price Mult={price_mult}")
        # 测试参数（小额测试，避免大额损失）
        test_quantity = Decimal('0.001')  # 小数量
        
        # 2. 测试撮合延迟（注意：撮合取决于市场流动性，可能不总是成功）
        print("\n=== 测试撮合延迟 ===")
        for i in range(5):
            await test_matching_latency(client, test_quantity, 'buy')
            await asyncio.sleep(2)
            await test_matching_latency(client, test_quantity, 'sell')
            await asyncio.sleep(2)

        # 3. 测试取消订单延迟
        print("\n=== 测试取消订单延迟 ===")
        await test_cancellation_latency(client, test_quantity, 'buy', 5)
        
    except Exception as e:
        print(f"测试出错: {e}")
    finally:
        await client.disconnect()
        print("断开连接")

if __name__ == "__main__":
    asyncio.run(main())