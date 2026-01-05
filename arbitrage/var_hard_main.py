"""对冲套利监控器 - 主入口"""

import asyncio
import argparse
import logging
import sys
import os
from pathlib import Path
from decimal import Decimal
import time
from dotenv import load_dotenv
from datetime import datetime
from logging.handlers import RotatingFileHandler

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from arbitrage.strategies.var_hard_strategy import VarHardStrategy  # ✅ 新增导入

# from arbitrage.config.loader import load_pair_config, list_all_pairs, list_enabled_pairs
from arbitrage.exchanges.extended_adapter import ExtendedAdapter
from arbitrage.exchanges.lighter_adapter import LighterAdapter
from arbitrage.exchanges.variational_adapter import VariationalAdapter  # ✅ 新增
from arbitrage.exchanges.nado_adapter import NadoAdapter  # ✅ 新增
from arbitrage.utils.logger import setup_logging
from arbitrage.utils.trade_logger import TradeLogger
from exchanges.extended import ExtendedClient
from exchanges.lighter import LighterClient
from exchanges.variational import VariationalClient  # ✅ 新增
from exchanges.nado import NadoClient  # ✅ 新增
from helpers.lark_bot import LarkBot
from helpers.util import Config, beijing_to_timestamp

# 配置日志
# logging.basicConfig(
#     level=os.getenv("LOG_LEVEL", "INFO").upper(),
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
#     handlers=[logging.StreamHandler(sys.stdout)],
#     force=True
# )
logger = logging.getLogger(__name__)

# ========== 交易所适配器工厂 ==========

EXCHANGE_ADAPTERS = {
    'extended': ExtendedAdapter,
    'lighter': LighterAdapter,
    'variational': VariationalAdapter,
    'nado': NadoAdapter,  # ✅ 新增
}

EXCHANGE_CLIENTS = {
    'extended': ExtendedClient,
    'lighter': LighterClient,
    'variational': VariationalClient,  # ✅ 新增
    'nado': NadoClient,  # ✅ 新增
}

async def create_exchange_adapter(
    exchange_name: str,
    symbol: str,
    quantity: Decimal = None,
    config_override: dict = None
):
    """创建交易所适配器"""
    if exchange_name not in EXCHANGE_CLIENTS:
        raise ValueError(
            f"不支持的交易所: {exchange_name}\n"
            f"支持的交易所: {', '.join(EXCHANGE_CLIENTS.keys())}"
        )
    
    logger.info(f"🔧 初始化 {exchange_name.upper()} 适配器...")

    # ========== 1. 创建基础配置 ==========
    config_dict = {
        'exchange': exchange_name,
        'ticker': symbol,
        'quantity': quantity,
    }
    
    # ========== 2. 交易所特定配置 ==========
    if exchange_name == 'lighter':
        config_dict.update({
            'iterations': 1,
            'tick_size': Decimal('0.01'),
            'contract_id': '',
            'side': 'buy',
            'take_profit': 0,
            'close_order_side': 'sell',
        })
    
    elif exchange_name == 'variational':
        # Variational 特定配置
        config_dict.update({
            'polling_interval': config_override.get('polling_interval', 1.0) if config_override else 1.0,
            'query_quantity': quantity,  # 使用交易数量作为查询数量
        })
    elif exchange_name == 'extended':
        # ✅ 创建完整配置
        config_dict = {
            'exchange': 'extended',
            'ticker': symbol,
            'quantity': quantity,
            'contract_id': f'{symbol}-USD',  # ✅ 添加 contract_id
            # 套利模式的默认值
            'take_profit_percentage': None,
            'stop_loss_percentage': None,
            'close_order_side': None,  # 套利模式不需要此字段
        }
    # 应用配置覆盖
    if config_override:
        config_dict.update(config_override)

    # ========== 3. 创建客户端 ==========
    config = Config(config_dict)
    client_class = EXCHANGE_CLIENTS[exchange_name]
    client = client_class(config)
    # 连接客户端
    await client.connect()
    logger.info(f"✅ {exchange_name.upper()} 客户端已连接")
    
    # ========== 4. 获取合约信息 ==========
    if exchange_name == 'lighter':
        logger.info(f"🔍 获取 Lighter 合约信息...")
        contract_id, tick_size = await client.get_contract_attributes()
        
        logger.info(
            f"✅ 获取到 Lighter 合约信息:\n"
            f"   contract_id: {contract_id}\n"
            f"   tick_size: {tick_size}"
        )
        
        client.config.contract_id = contract_id
        client.config.tick_size = tick_size
        
        logger.info(
            f"✅ Lighter 合约信息已设置:\n"
            f"   contract_id: {client.config.contract_id}\n"
            f"   tick_size: {client.config.tick_size}"
        )
        
        if client.config.contract_id is None or client.config.contract_id == '':
            raise ValueError(
                f"Lighter contract_id 设置失败: {client.config.contract_id}"
            )
        
        logger.info(f"✅ Lighter contract_id 验证通过: {client.config.contract_id}")

    elif exchange_name == 'variational':
        # Variational 需要获取合约信息
        try:
            logger.info(f"🔍 获取 Variational 合约信息...")
            contract_id, tick_size = await client.get_contract_attributes()
            
            if not contract_id:
                raise ValueError("Variational contract_id 获取失败")
            
            client.config.contract_id = contract_id
            client.config.tick_size = tick_size
            
            logger.info(
                f"✅ Variational 合约信息:\n"
                f"   contract_id: {contract_id}\n"
                f"   tick_size: {tick_size}"
            )
        except Exception as e:
            logger.error(f"❌ 获取 Variational 合约信息失败: {e}")
            raise
    elif exchange_name == 'extended':
        # Extended 需要获取合约信息
        try:
            logger.info(f"🔍 获取 Extended 合约信息...")
            contract_id, tick_size = await client.get_contract_attributes()

            if not contract_id:
                raise ValueError("Extended contract_id 获取失败")
            logger.info(f"✅ 获取到 Extended 合约信息: contract_id={contract_id}, tick_size={tick_size}")
            client.config.contract_id = contract_id
            client.config.tick_size = tick_size

            logger.info(
                f"✅ Extended 合约信息:\n"
                f"   contract_id: {client.config.contract_id}\n"
                f"   tick_size: {client.config.tick_size}"
            )
        except Exception as e:
            logger.error(f"❌ 获取 Extended 合约信息失败: {e}")
            raise

    elif exchange_name == 'nado':
        # Nado 需要获取合约信息
        try:
            logger.info(f"🔍 获取 Nado 合约信息...")
            contract_id, tick_size = await client.get_contract_attributes()

            if not contract_id:
                raise ValueError("Nado contract_id 获取失败")
            logger.info(f"✅ 获取到 Nado 合约信息: contract_id={contract_id}, tick_size={tick_size}")
            client.config.contract_id = contract_id
            client.config.tick_size = tick_size

            logger.info(
                f"✅ Nado 合约信息:\n"
                f"   contract_id: {client.config.contract_id}\n"
                f"   tick_size: {client.config.tick_size}"
            )
        except Exception as e:
            logger.error(f"❌ 获取 Nado 合约信息失败: {e}")
            raise
        
    # ========== 5. 创建适配器 ==========
    adapter_class = EXCHANGE_ADAPTERS[exchange_name]
    
    # 为适配器准备配置
    adapter_config = {}
    
    if exchange_name == 'variational':
        adapter_config = {
            'polling_interval': config_dict.get('polling_interval', 1.0),
            'query_quantity': quantity
        }
    
    adapter = adapter_class(symbol, client, config=adapter_config)
    
    logger.info(f"✅ {exchange_name.upper()} 适配器创建成功: {adapter.exchange_name}")
    
    return adapter

# ========== 主函数 ==========

async def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='对冲套利监控器（支持任意交易所组合）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 运行 硬刷模式 (BTC)
  python arbitrage/var_hard_main.py  --symbol BTC --quantity-range 0.0001 0.0003 --env-file .env.var.ext.main1 --spread-threshold 0.0042 --poll-interval 0.3 --data-dir data/hard-var-new
        """
    )
      
    parser.add_argument('--symbol', '-s', type=str,
                       help='交易币种（硬刷策略必需，如 BTC、ETH）')
    parser.add_argument('--spread-threshold', type=float, default=0.0026,
                       help='硬刷策略点差阈值（默认：0.0026%%）')
    parser.add_argument('--cooldown', type=float, default=5.0,
                       help='硬刷策略冷却时间（秒，默认：5）')
    parser.add_argument('--poll-interval', type=float, default=0.1,
                       help='硬刷策略轮询间隔（秒，默认：0.1）')
    parser.add_argument('--data-dir', type=str, default=None,
                       help='硬刷策略数据目录（默认：data/var_hard）')
    parser.add_argument('--max-lifetime-volume', '-mlv', type=float, default=float('inf'),
                       help='硬刷策略最大终生交易量，超过会自动退出。默认: %(default)s"')
    parser.add_argument('--quantity-range', nargs=2, type=Decimal, default=[Decimal('0.0011'), Decimal('0.0033')],
                        help="硬刷策略数量范围，最小值和最大值（例如：0.001 0.005）。默认: %(default)s")
    parser.add_argument('--cooldown-range', nargs=2, type=float, default=[3.0, 6.0],
                        help="硬刷策略冷却时间范围，最小值和最大值（例如：3.0 6.0）。默认: %(default)s")
    parser.add_argument('--env-file', type=str, default=None,
                       help='环境变量文件路径')
    
    parser.add_argument('--end-time', type=str, default=None,
                       help='指定策略结束时间，格式为 YYYY-MM-DD HH:MM:SS（北京时间）')
    args = parser.parse_args()
    # 加载环境变量
    if args.env_file:
        load_dotenv(args.env_file)
    else:
        load_dotenv()

    lark_bot = None
    lark_token = os.getenv('LARK_TOKEN')
    if lark_token:
        lark_bot = LarkBot(lark_token)
        logger.info("✅ 飞书通知已启用")
    else:
        logger.warning("⚠️ 未设置 LARK_TOKEN，飞书通知已禁用")
     # 检查必要参数
    # if not args.pair:
    #     parser.error("需要指定 --pair 参数，或使用 --list-pairs 查看可用交易对")
    
    # Step 1: 加载交易对配置
    # try:
    #     config = load_pair_config(args.pair)
    #     logger.info(f"📋 加载配置成功: {args.pair}")
    # except Exception as e:
    #     logger.error(f"❌ 加载配置失败: {e}")
    #     return
    
    # ✅ Step 1.5: 设置日志系统（在加载配置后）
    # logger.info(f"📋 加载配置成功: {args.pair}")
    # ========== ✅ 新增：硬刷策略 ==========
    # 检查必要参数
    if not args.symbol:
        parser.error("硬刷策略需要指定 --symbol 参数（如 BTC、ETH）")
    
    if not args.quantity_range:
        parser.error("硬刷策略需要指定 --quantity-range 参数")
    
    # 加载环境变量
    if args.env_file:
        load_dotenv(args.env_file)
    else:
        load_dotenv()
    
    # 设置日志
    log_dir = Path(__file__).parent.parent / "logs/var_hard"
    setup_logging(f"var_hard_{args.symbol}", log_dir)
    
    logger.info(
        f"\n"
        f"{'='*60}\n"
        f"🚀 启动 Variational 硬刷策略\n"
        f"{'='*60}\n"
        f"  币种:           {args.symbol}\n"
        f"  数量范围:    [{str(args.quantity_range[0])}, {str(args.quantity_range[1])}]\n"
        f"  点差阈值:       {args.spread_threshold}%\n"
        f"  冷却时间范围:       [{str(args.cooldown_range[0])}, {str(args.cooldown_range[1])}]\n"
        f"  最大终身交易量:       {args.max_lifetime_volume}\n"
        f"  运行截止时间:         { args.end_time }"
        f"  轮询间隔:       {args.poll_interval}s\n"
        # f"  监控模式:       {'是' if args.monitor_only else '否'}\n"
        f"  数据目录:       {args.data_dir or 'data/var_hard'}\n"
        f"{'='*60}\n"
    )
    
    # 创建 Variational 适配器
    try:
        logger.info("🔌 初始化 Variational 适配器...")
        
        exchange = await create_exchange_adapter(
            exchange_name='variational',
            symbol=args.symbol,
            # quantity=Decimal(args.quantity),
            config_override={'polling_interval': args.poll_interval}
        )
        
        logger.info("✅ Variational 适配器初始化成功")
    
    except Exception as e:
        logger.error(f"❌ 适配器初始化失败: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # 创建硬刷策略
    data_dir = Path(args.data_dir) if args.data_dir else None
    
    
    strategy = VarHardStrategy(
        symbol=args.symbol,
        exchange=exchange,
        quantity_range=args.quantity_range,
        spread_threshold=Decimal(str(args.spread_threshold)),
        max_slippage=Decimal('0.0005'),
        max_lifetime_volume=args.max_lifetime_volume,
        cooldown_range=args.cooldown_range,
        poll_interval=args.poll_interval,
        data_dir=data_dir,
        lark_bot=lark_bot,
    )
    
    logger.info("✅ 硬刷策略创建成功\n")
    
    # 启动策略
    try:
        await strategy.start()
        
        mode_text = "交易模式"
        print(
            f"\n"
            f"╔════════════════════════════════════════════════════════════╗\n"
            f"║  Variational 硬刷策略运行中 - {mode_text}                    ║\n"
            f"╠════════════════════════════════════════════════════════════╣\n"
            f"║  币种:   {args.symbol:^10s}                                      ║\n"
            f"║  数量范围:   [{str(args.quantity_range[0])}, {str(args.quantity_range[1])}]                         ║\n"
            f"║  点差阈值: {args.spread_threshold:^6.6f}%                                        ║\n"
            f"║  最大终身交易量: {args.max_lifetime_volume}                                   ║\n"
            f"╠════════════════════════════════════════════════════════════╣\n"
            f"║  按 Ctrl+C 停止                                            ║\n"
            f"╚════════════════════════════════════════════════════════════╝\n"
        )
        
        # 保持运行
        if args.end_time:
            logger.info(f"⏰ 策略运行至北京时间 {args.end_time}自动停止")
            end_timestamp = beijing_to_timestamp(args.end_time)

            while end_timestamp - time.time() > 0:
                await asyncio.sleep(1)

            await strategy.stop()
        else:
            while True:
                await asyncio.sleep(1)
    
    except KeyboardInterrupt:
        logger.info("\n👋 收到停止信号")
    
    except Exception as e:
        logger.error(f"❌ 策略运行异常: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        logger.info("🧹 清理资源...")
        await strategy.stop()
        await exchange.disconnect()
        logger.info("✅ 程序已退出")
    
    return  # ✅ 硬刷策略运行完毕，直接返回
    # ====================================== 

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 程序被用户中断")
    except Exception as e:
        logger.error(f"❌ 程序异常退出: {e}")
        import traceback
        traceback.print_exc()