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

from arbitrage.strategies.hedge_strategy import HedgeStrategy
from arbitrage.config.loader import load_pair_config, list_all_pairs, list_enabled_pairs
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
    slippage: Decimal = None,
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
    adapter_config = {
        'slippage': slippage
    }
    
    if exchange_name == 'variational':
        adapter_config = {
            'polling_interval': config_dict.get('polling_interval', 1.0),
            'query_quantity': quantity,
            'slippage': slippage
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
  # 运行 Extended + Lighter (BTC)
  python arbitrage/main.py --pair extended_lighter_btc
  
  # 运行 Extended + Lighter (ETH)
  python arbitrage/main.py --pair extended_lighter_eth

  # 运行 Variational + Extended (ETH)
  python arbitrage/main.py --pair variational_extended_eth
  
  
  # 覆盖配置参数
  python arbitrage/main.py --pair extended_lighter_btc --quantity 0.02 --open-threshold 0.08
  
  # 列出所有可用的交易对
  python arbitrage/main.py --list-pairs
  
  # 只监控，不下单
  python arbitrage/main.py --pair extended_lighter_btc --monitor-only
        """
    )
    
    parser.add_argument('--pair', '-p', type=str,
                       help='交易对 ID (如 extended_lighter_btc)')
    parser.add_argument('--list-pairs', action='store_true',
                       help='列出所有可用的交易对')
    parser.add_argument('--quantity', '-q', type=str, default=None,
                       help='开仓数量（可选，覆盖配置）')
    parser.add_argument('--quantity-precision', '-qp', type=str, default=None, help='下单的数量精度，低于该精度下单会失败')
    parser.add_argument('--open-threshold', '-ot', type=float, default=None,
                       help='开仓阈值（可选，覆盖配置）')
    parser.add_argument('--close-threshold', '-ct', type=float, default=None,
                       help='平仓阈值（可选，覆盖配置）')
    parser.add_argument('--min-total-threshold', '-mtt', type=float, default=None, help='最小的阈值和')
    parser.add_argument('--sample-size', type=int, help='价差样本数最终的值')
    parser.add_argument('--env-file', type=str, default=None,
                       help='环境变量文件路径')
    parser.add_argument('--monitor-only', action='store_true',
                       help='只监控，不下单')
    parser.add_argument('--end-time', type=str, default=None,
                       help='指定策略结束时间，格式为 YYYY-MM-DD HH:MM:SS（北京时间）')
    parser.add_argument('--min-depth-quantity', type=float, default=None, help='最小深度值')
    parser.add_argument('--max-position', type=float, default=None, help='最大仓位，如果不传则使用配置文件中的')
    parser.add_argument('--direction-reverse', type=bool, default=False, help='是表示正向滑点方向才下单，默认先负滑点方向才下单')
    parser.add_argument('--cooldown-seconds', type=str, default='5', help='下单冷却时间，默认5秒，只通过控制台传参')
    parser.add_argument('--exchange-a-slippage', type=float, default=None, help='交易所A的滑点')
    parser.add_argument('--exchange-b-slippage', type=float, default=None, help='交易所B的滑点')
    parser.add_argument('--max-std-multiplier', type=float, default=4.0, help='标准差的最大系数')
    parser.add_argument('--min-std-multiplier', type=float, default=0.0, help='标准差的最小系数')
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

    # ======================================
    # 列出所有交易对
    if args.list_pairs:
        print("\n📋 所有可用的交易对:\n")
        all_pairs = list_all_pairs()
        enabled_pairs = list_enabled_pairs()
        
        for pair_id in all_pairs:
            status = "✅ 已启用" if pair_id in enabled_pairs else "❌ 未启用"
            print(f"  {pair_id:30s} {status}")
        
        print(f"\n共 {len(all_pairs)} 个交易对，其中 {len(enabled_pairs)} 个已启用\n")
        return
    
    # 检查必要参数
    if not args.pair:
        parser.error("需要指定 --pair 参数，或使用 --list-pairs 查看可用交易对")
    
    # Step 1: 加载交易对配置
    try:
        config = load_pair_config(args.pair)
        logger.info(f"📋 加载配置成功: {args.pair}")
    except Exception as e:
        logger.error(f"❌ 加载配置失败: {e}")
        return
    
    # ✅ Step 1.5: 设置日志系统（在加载配置后）
    log_dir = Path(__file__).parent.parent / "logs/arbitrage"
    setup_logging(config.symbol, log_dir)
    logger.info(f"📋 加载配置成功: {args.pair}")
    
    # ✅ Step 1.6: 创建交易日志记录器
    trade_logger = TradeLogger(config.symbol, log_dir)

    # 命令行参数覆盖配置
    quantity = Decimal(str(args.quantity)) if args.quantity else Decimal(str(config.quantity))
    quantity_precision = Decimal(str(args.quantity_precision)) if args.quantity_precision is not None else Decimal(str(config.quantity_precision))
    open_threshold = args.open_threshold if args.open_threshold is not None else config.open_threshold
    close_threshold = args.close_threshold if args.close_threshold is not None else config.close_threshold
    monitor_only = args.monitor_only  # ✅ 获取 monitor_only 参数
    if args.min_depth_quantity:
        min_depth_quantity = Decimal(str(args.min_depth_quantity))
    elif hasattr(config, 'min_depth_quantity') and config.min_depth_quantity is not None:
        min_depth_quantity = Decimal(str(config.min_depth_quantity))
    else:
        min_depth_quantity = quantity
    
    # 读取累计模式配置
    accumulate_mode = config.accumulate_mode
    max_position = Decimal(str(args.max_position)) if args.max_position is not None else Decimal(str(config.max_position))
    direction_reverse = args.direction_reverse
    dynamic_threshold = config.dynamic_threshold if hasattr(config, 'dynamic_threshold') else False

    if dynamic_threshold:
        if args.min_total_threshold is not None:
            dynamic_threshold["min_total_threshold"] = float(args.min_total_threshold)
        if args.sample_size is not None:
            dynamic_threshold["sample_size"] = int(args.sample_size)
        
        dynamic_threshold['min_samples'] = dynamic_threshold.get('sample_size', 1000)

        dynamic_threshold['max_std_multiplier'] = float(args.max_std_multiplier)
        dynamic_threshold['min_std_multiplier'] = float(args.min_std_multiplier)
    cooldown_seconds = float(args.cooldown_seconds) if args.cooldown_seconds else 5
    # 设置滑点
    if args.exchange_a_slippage is not None:
        exchange_a_slippage = Decimal(str(args.exchange_a_slippage))
    if args.exchange_b_slippage is not None:
        exchange_b_slippage = Decimal(str(args.exchange_b_slippage))

    logger.info(
        f"\n"
        f"{'='*60}\n"
        f"🚀 启动参数\n"
        f"{'='*60}\n"
        f"  交易对 ID:    {args.pair}\n"
        f"  币种:         {config.symbol}\n"
        f"  交易所 A:     {config.exchange_a} (开空)\n"
        f"  交易所 B:     {config.exchange_b} (开多)\n"
        f"  数量:         {quantity}\n"
        f"  数量精度:     {quantity_precision}\n"
        f"  开仓阈值:     {open_threshold}%\n"
        f"  平仓阈值:     {close_threshold}%\n"
        f"  最小深度:     {min_depth_quantity}\n"
        f"  最小阈值和:   {dynamic_threshold["min_total_threshold"]}\n"
        f"  样本数:       {dynamic_threshold["sample_size"]}\n"
        f"  最大标准差系数: {dynamic_threshold["max_std_multiplier"]}\n"
        f"  最小标准差系数: {dynamic_threshold["min_std_multiplier"]}\n"
        f"  监控模式:     {'是' if monitor_only else '否'}\n"  # ✅ 显示监控模式
        f"  累计模式:     {'启用' if accumulate_mode else '禁用'}\n"
        f"  最大持仓:     {max_position}\n"
        f"  负向滑点方向下单: { '是' if not direction_reverse else '否'}\n"
        f"  动态阈值:     {'启用' if dynamic_threshold.get('enabled', False) else '禁用'}\n"  # ✅ 新增
        f"  冷却时间:     { cooldown_seconds }s\n"
        f"  交易所 A 滑点: {exchange_a_slippage or '--'}\n"
        f"  交易所 B 滑点: {exchange_b_slippage or '--'}\n"
        f"{'='*60}\n"
    )
    
    # Step 2: 创建交易所适配器
    logger.info("🔌 初始化交易所适配器...")
    
    try:
        # 准备配置覆盖
        config_override_a = {}
        config_override_b = {}
        
        # Variational 特定配置
        if config.exchange_a == 'variational' and hasattr(config, 'variational_config'):
            config_override_a = config.variational_config
        
        if config.exchange_b == 'variational' and hasattr(config, 'variational_config'):
            config_override_b = config.variational_config

        symbol_a = config.symbol
        symbol_b = config.symbol
        if config.exchange_a == 'variational' and config.symbol == 'LIT':
            symbol_a = 'LIGHTER'
        if config.exchange_b == 'variational' and config.symbol == 'LIT':
            symbol_b = 'LIGHTER'
        
        # 创建适配器
        exchange_a = await create_exchange_adapter(
            config.exchange_a,
            symbol_a,
            quantity,
            exchange_a_slippage,
            config_override_a
        )
        
        exchange_b = await create_exchange_adapter(
            config.exchange_b,
            symbol_b,
            quantity,
            exchange_b_slippage,
            config_override_b
        )
        
        logger.info(
            f"\n✅ 适配器初始化完成:\n"
            f"   Exchange A: {exchange_a.exchange_name}\n"
            f"   Exchange B: {exchange_b.exchange_name}\n"
        )
    
    except Exception as e:
        logger.error(f"❌ 适配器初始化失败: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Step 3: 初始化飞书机器人
    
    
    # Step 4: 创建策略
    strategy = HedgeStrategy(
        symbol=config.symbol,
        symbol_a=symbol_a,
        symbol_b=symbol_b,
        quantity=quantity,
        quantity_precision=quantity_precision,
        open_threshold_pct=open_threshold,
        close_threshold_pct=close_threshold,
        exchange_a=exchange_a,
        exchange_b=exchange_b,
        lark_bot=lark_bot,
        monitor_only=monitor_only,  # ✅ 传递 monitor_only 参数
        trade_logger=trade_logger,  # ✅ 传递交易日志记录器
        min_depth_quantity=min_depth_quantity,  # ✅ 传递最小深度数量
        accumulate_mode=accumulate_mode,
        max_position=max_position,
        direction_reverse=direction_reverse,
        dynamic_threshold=dynamic_threshold,  # ✅ 传递动态阈值配置
        cooldown_seconds=cooldown_seconds,
        end_time=args.end_time,
    )
    logger.info("✅ 策略创建成功\n")
    # ========== ✅ 新增：Step 4.5 启动时同步仓位 ==========
    if accumulate_mode:
        logger.info("🔄 累计模式：正在从交易所同步仓位...")
        try:
            synced_qty = await strategy.position_manager.sync_from_exchanges(
                exchange_a=exchange_a,
                exchange_b=exchange_b,
                symbol_a=symbol_a,
                symbol_b=symbol_b
            )
            
            if synced_qty is not None and synced_qty != 0:
                logger.warning(
                    f"⚠️ 检测到未平仓位: {synced_qty:+.4f}\n"
                    f"   已同步到本地，策略将继续运行"
                )
            else:
                logger.info("✅ 无持仓，从空仓开始")
        
        except Exception as e:
            logger.error(f"❌ 同步仓位失败: {e}")
            logger.warning("⚠️ 将从本地默认值（0）开始运行")
    # ========== 新增部分结束 ==========
    # Step 5: 启动策略
    try:
        await strategy.start()
        
        mode_text = "监控模式" if monitor_only else "交易模式"
        print(
            f"\n"
            f"╔════════════════════════════════════════════════════════════╗\n"
            f"║  策略运行中 - {mode_text:^40s} ║\n"
            f"╠════════════════════════════════════════════════════════════╣\n"
            f"║  交易对: {config.exchange_a.upper():^10s} ⇄ {config.exchange_b.upper():^10s}                          ║\n"
            f"║  币种:   {config.symbol:^10s}                                        ║\n"
            f"║  数量:   {str(quantity):^10s}                                        ║\n"
            f"╠════════════════════════════════════════════════════════════╣\n"
            f"║  按 Ctrl+C 停止                                            ║\n"
            f"╚════════════════════════════════════════════════════════════╝\n"
        )
        
        # 保持运行
        # if args.end_time:
        #     logger.info(f"⏰ 策略运行至北京时间 {args.end_time}自动停止")
        #     end_timestamp = beijing_to_timestamp(args.end_time)

        #     while end_timestamp - time.time() > 0:
        #         await asyncio.sleep(1)

        #     await strategy.stop()
        # else:
        #     while True:
        #         await asyncio.sleep(1)
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
        logger.info("✅ 程序已退出")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 程序被用户中断")
    except Exception as e:
        logger.error(f"❌ 程序异常退出: {e}")
        import traceback
        traceback.print_exc()