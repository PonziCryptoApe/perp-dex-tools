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
import traceback
from typing import Dict, Any

from dotenv import load_dotenv
import requests

sys.path.insert(0, str(Path(__file__).parent.parent))
from helpers.util import Config

# 假设 lighter.py 已导入 BaseExchangeClient 和其他依赖
from exchanges.lighter import LighterClient  # 调整路径为你的实际位置
from helpers.logger import TradingLogger  # 如果需要，调整
import lighter


# 示例配置
CONFIG = {
    'ticker': 'BTC-USD',  # 替换为你的交易对
    'contract_id': 1,     # 替换为实际 contract_id
    'exchange': 'lighter',
    'quantity': Decimal('0.001'),
    'open_order_side': 'buy',
    'close_order_side': 'sell',
}

async def test_place_429(client: LighterClient, quantity: Decimal, side: str, best_bid, best_ask):
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
 
    if side == 'buy':
        test_price = best_bid * Decimal('1.02')  # 略高于 bid 以增加撮合几率
    else:
        test_price = best_ask * Decimal('0.98')  # 略低于 ask
    start_time = time.time()
    result = await client.place_limit_order(CONFIG['contract_id'], quantity, test_price, side)
    print(f"下单结果: {result}")
    placement_end = time.time()
    if not result.success:
        print("下单失败")
        raise Exception("下单失败")
    print(f"下单价格: {test_price}, 数量: {quantity}, 方向: {side}")
    print(f"下单完成，放置延迟: {placement_end - start_time:.4f} 秒")
    
    return None

async def _fetch_bbo_prices():
        url = "https://mainnet.zklighter.elliot.ai/api/v1/orderBookOrders?market_id=1&limit=1"

        headers = {"accept": "application/json"}

        response = requests.get(url, headers=headers)
        result = json.loads(response.text)
        # print(response.text)
        return Decimal(result.get('bids')[0]['price']).quantize(Decimal('0.1')), Decimal(result.get('asks')[0]['price']).quantize(Decimal('0.1'))

async def main():
    """主函数：运行所有测试。"""
    # 设置环境变量（请在实际运行前设置）
    # os.environ['API_KEY_PRIVATE_KEY'] = 'your_private_key'
    # os.environ['LIGHTER_ACCOUNT_INDEX'] = '0'
    # os.environ['LIGHTER_API_KEY_INDEX'] = '0'
    parser = argparse.ArgumentParser(
        description='Lighter REST API 测试工具',
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
    
    parser.add_argument('--count', type=int, default=10, help='每个接口测试次数（默认：10）')
    parser.add_argument('--place-order', action='store_true', help='只测试下单接口')
    parser.add_argument('--account', action='store_true', help='只测试仓位接口')
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
        print("====余额信息====")
        balance = await client.get_portfolio()
        print(f"账户余额: {balance}")

        # return
        market_id, base_mult, price_mult = await client._get_market_config('BTC')
        client.base_amount_multiplier = base_mult
        client.price_multiplier = price_mult
        print(f"市场配置: ID={market_id}, Base Mult={base_mult}, Price Mult={price_mult}")
        # 测试参数（小额测试，避免大额损失）
        test_quantity = Decimal('0.0002')  # 小数量
        
        # 2. 测试撮合延迟（注意：撮合取决于市场流动性，可能不总是成功）
        print("\n=== 测试429 ===")
        if args.place_order:
            print("仅测试下单接口")
            # 获取当前最佳价格
            best_bid, best_ask = await _fetch_bbo_prices()
            start_time = time.time()
            for i in range(args.count):
                client.current_order = None  # 清空残留
                await test_place_429(client, test_quantity, 'buy', best_bid, best_ask)
                client.current_order = None  # 清空残留
                await test_place_429(client, test_quantity, 'sell', best_bid, best_ask)
            print(f"总测试时间: {time.time() - start_time:.2f} 秒")

        if args.account:
            print("仅测试仓位接口")
            start_time = time.time()
            for i in range(args.count):
                account_api = lighter.AccountApi(client.api_client)

                # Get account info
                account_data = await account_api.account(by="index", value=str(client.account_index))

                if not account_data or not account_data.accounts:
                    raise ValueError("Failed to get positions")

                position_value = None
                positions = account_data.accounts[0].positions
                for position in positions:
                    if position.market_id == market_id:
                        position_value = Decimal(position.position)
                print(f"仓位查询 {i+1}/{args.count}: {position_value}")
            print(f"总测试时间: {time.time() - start_time:.2f} 秒")
    except Exception as e:
        error_time = time.time()
        traceback.print_exc()
        print(f"测试出错: {e} (持续时间: {error_time - start_time:.2f} 秒)")
    finally:
        await client.disconnect()
        print("断开连接")

if __name__ == "__main__":
    asyncio.run(main())