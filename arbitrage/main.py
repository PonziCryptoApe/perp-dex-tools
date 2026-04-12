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
from arbitrage.services.signal_execution_service import SignalExecutionService
from arbitrage.services.signal_mailbox import SignalMailbox
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
    config_override: dict = None,
    lighter_reconnect_base_delay: float = 0.3,
    lighter_reconnect_max_delay: float = 10.0
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
    if exchange_name == 'lighter':
        adapter_config.update({
            'reconnect_base_delay': lighter_reconnect_base_delay,
            'reconnect_max_delay': lighter_reconnect_max_delay
        })
    
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
    parser.add_argument('--signal-mode', choices=['legacy', 'quantile', 'stat_arb'], default=None, help='信号逻辑模式（默认读取配置）')
    parser.add_argument('--signal-quantile', type=float, default=None, help='分位数门槛（如 0.6 表示 P60）')
    parser.add_argument('--signal-sample-size', type=int, default=None, help='分位数样本数（冷启动阈值）')
    parser.add_argument('--signal-min-edge-pct', type=float, default=None, help='分位数模式最小安全边际（%）')
    parser.add_argument('--signal-min-abs-spread-pct', type=float, default=None, help='分位数模式绝对净价差底线（%）')
    parser.add_argument('--signal-stat-arb-enabled', choices=['on', 'off'], default=None, help='统计套利模式总开关（默认读取配置）')
    parser.add_argument('--signal-stat-arb-baseline-adjustment', choices=['on', 'off'], default=None, help='统计套利是否启用盘口点差基线修正（默认读取配置）')
    parser.add_argument('--signal-stat-arb-baseline-ratio', type=float, default=None, help='统计套利盘口基线修正比例（默认 0.5）')
    parser.add_argument('--signal-stat-arb-medium-window-seconds', type=int, default=None, help='统计套利中期窗口秒数（默认 1800）')
    parser.add_argument('--signal-stat-arb-long-window-seconds', type=int, default=None, help='统计套利长期窗口秒数（默认 3600）')
    parser.add_argument('--signal-stat-arb-medium-min-samples', type=int, default=None, help='统计套利中期窗口最小样本数（默认 120）')
    parser.add_argument('--signal-stat-arb-long-min-samples', type=int, default=None, help='统计套利长期窗口最小样本数（默认 240）')
    parser.add_argument('--signal-stat-arb-medium-weight', type=float, default=None, help='统计套利 30 分钟分数权重（默认 0.4）')
    parser.add_argument('--signal-stat-arb-long-weight', type=float, default=None, help='统计套利 60 分钟分数权重（默认 0.6）')
    parser.add_argument('--signal-stat-arb-entry-threshold', type=float, default=None, help='统计套利最终开仓分数阈值（默认 2.8）')
    parser.add_argument('--signal-stat-arb-exit-threshold', type=float, default=None, help='统计套利回归平仓分数阈值（默认 0.8）')
    parser.add_argument('--signal-stat-arb-exit-spread-floor-pct', type=float, default=None, help='统计套利回归平仓时退出方向真实可执行价差底线（默认 -0.01）')
    parser.add_argument('--signal-stat-arb-min-score-gap', type=float, default=None, help='统计套利两个方向最小分数差（默认 0.5）')
    parser.add_argument('--signal-stat-arb-min-mad-pct', type=float, default=None, help='统计套利 MAD 下限（默认 0.003）')
    parser.add_argument('--signal-stat-arb-quality-log-interval-seconds', type=float, default=None, help='统计套利回归质量日志输出间隔（秒，默认 15）')
    parser.add_argument('--signal-stat-arb-require-same-sign', choices=['on', 'off'], default=None, help='统计套利是否要求 30m/60m 中枢方向一致（默认读取配置）')
    parser.add_argument('--signal-stat-arb-block-regime', choices=['on', 'off'], default=None, help='统计套利是否在怀疑 regime 变化时阻断开仓（默认读取配置）')
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
    parser.add_argument('--lighter-reconnect-base-delay', type=float, default=0.3, help='Lighter WS 重连基础等待秒数（默认0.3）')
    parser.add_argument('--lighter-reconnect-max-delay', type=float, default=10.0, help='Lighter WS 重连最大等待秒数（默认10）')
    parser.add_argument('--max-signal-delay-ms', type=int, default=200, help='交易所信号延迟总阈值（默认200ms）')
    parser.add_argument('--max-signal-delay-ms-a', type=int, default=None, help='交易所 A 信号延迟阈值（ms，不传则使用 --max-signal-delay-ms）')
    parser.add_argument('--max-signal-delay-ms-b', type=int, default=None, help='交易所 B 信号延迟阈值（ms，不传则使用 --max-signal-delay-ms）')
    parser.add_argument('--max-std-multiplier', type=float, default=4.0, help='标准差的最大系数')
    parser.add_argument('--min-std-multiplier', type=float, default=0.0, help='标准差的最小系数')
    parser.add_argument('--edge-filter', choices=['on', 'off'], default=None, help='边际二次过滤开关（默认读取配置，配置缺失时为 off）')
    parser.add_argument('--min-edge-bps', type=float, default=None, help='边际二次过滤最小安全边际（bps），默认 0.8')
    parser.add_argument('--edge-base-cost-bps', type=float, default=None, help='边际二次过滤基础成本估计（bps），默认 3.0')
    parser.add_argument('--edge-fee-bps', type=float, default=None, help='边际二次过滤手续费估计（bps），默认 0.0')
    parser.add_argument('--edge-latency-bps-per-100ms', type=float, default=None, help='边际二次过滤延迟风险系数（每 100ms 增加 bps），默认 0.0')
    parser.add_argument('--edge-latency-free-ms', type=float, default=None, help='边际二次过滤延迟免惩罚阈值（ms），默认 120')
    parser.add_argument('--risk-poll-interval-seconds', type=float, default=None, help='风控后台轮询间隔（秒），不传则使用当前配置')
    parser.add_argument('--risk-stale-after-seconds', type=float, default=None, help='风控快照过期判定阈值（秒），不传则使用当前配置')
    parser.add_argument('--risk-reduce-position-ratio', type=float, default=None, help='REDUCE 档目标仓位比例，不传则使用当前配置')
    parser.add_argument('--risk-stop-position-ratio', type=float, default=None, help='STOP 档目标仓位比例，不传则使用当前配置')
    parser.add_argument('--risk-reduce-cooldown-seconds', type=float, default=None, help='风控减仓冷却时间（秒），不传则使用当前配置')
    parser.add_argument('--risk-liq-distance-warn', type=float, default=None, help='清算距离 WARN 阈值，不传则使用当前配置')
    parser.add_argument('--risk-liq-distance-warn-recover', type=float, default=None, help='清算距离 WARN 恢复阈值，不传则使用当前配置')
    parser.add_argument('--risk-liq-distance-reduce', type=float, default=None, help='清算距离 REDUCE 阈值，不传则使用当前配置')
    parser.add_argument('--risk-liq-distance-stop', type=float, default=None, help='清算距离 STOP 阈值，不传则使用当前配置')
    args = parser.parse_args()
    if args.lighter_reconnect_base_delay <= 0:
        parser.error("--lighter-reconnect-base-delay 必须大于 0")
    if args.lighter_reconnect_max_delay <= 0:
        parser.error("--lighter-reconnect-max-delay 必须大于 0")
    if args.lighter_reconnect_max_delay < args.lighter_reconnect_base_delay:
        parser.error("--lighter-reconnect-max-delay 不能小于 --lighter-reconnect-base-delay")
    if args.max_signal_delay_ms <= 0:
        parser.error("--max-signal-delay-ms 必须大于 0")
    if args.max_signal_delay_ms_a is not None and args.max_signal_delay_ms_a <= 0:
        parser.error("--max-signal-delay-ms-a 必须大于 0")
    if args.max_signal_delay_ms_b is not None and args.max_signal_delay_ms_b <= 0:
        parser.error("--max-signal-delay-ms-b 必须大于 0")
    if args.signal_stat_arb_baseline_ratio is not None and args.signal_stat_arb_baseline_ratio < 0:
        parser.error("--signal-stat-arb-baseline-ratio 不能小于 0")
    if args.signal_stat_arb_medium_window_seconds is not None and args.signal_stat_arb_medium_window_seconds <= 0:
        parser.error("--signal-stat-arb-medium-window-seconds 必须大于 0")
    if args.signal_stat_arb_long_window_seconds is not None and args.signal_stat_arb_long_window_seconds <= 0:
        parser.error("--signal-stat-arb-long-window-seconds 必须大于 0")
    if args.signal_stat_arb_medium_min_samples is not None and args.signal_stat_arb_medium_min_samples <= 0:
        parser.error("--signal-stat-arb-medium-min-samples 必须大于 0")
    if args.signal_stat_arb_long_min_samples is not None and args.signal_stat_arb_long_min_samples <= 0:
        parser.error("--signal-stat-arb-long-min-samples 必须大于 0")
    if args.signal_stat_arb_entry_threshold is not None and args.signal_stat_arb_entry_threshold <= 0:
        parser.error("--signal-stat-arb-entry-threshold 必须大于 0")
    if args.signal_stat_arb_exit_threshold is not None and args.signal_stat_arb_exit_threshold < 0:
        parser.error("--signal-stat-arb-exit-threshold 不能小于 0")
    if args.signal_stat_arb_exit_spread_floor_pct is not None and args.signal_stat_arb_exit_spread_floor_pct > 100:
        parser.error("--signal-stat-arb-exit-spread-floor-pct 不能大于 100")
    if args.signal_stat_arb_min_score_gap is not None and args.signal_stat_arb_min_score_gap < 0:
        parser.error("--signal-stat-arb-min-score-gap 不能小于 0")
    if args.signal_stat_arb_min_mad_pct is not None and args.signal_stat_arb_min_mad_pct <= 0:
        parser.error("--signal-stat-arb-min-mad-pct 必须大于 0")
    if args.signal_stat_arb_quality_log_interval_seconds is not None and args.signal_stat_arb_quality_log_interval_seconds <= 0:
        parser.error("--signal-stat-arb-quality-log-interval-seconds 必须大于 0")
    if args.min_edge_bps is not None and args.min_edge_bps < 0:
        parser.error("--min-edge-bps 不能小于 0")
    if args.edge_base_cost_bps is not None and args.edge_base_cost_bps < 0:
        parser.error("--edge-base-cost-bps 不能小于 0")
    if args.edge_fee_bps is not None and args.edge_fee_bps < 0:
        parser.error("--edge-fee-bps 不能小于 0")
    if args.edge_latency_bps_per_100ms is not None and args.edge_latency_bps_per_100ms < 0:
        parser.error("--edge-latency-bps-per-100ms 不能小于 0")
    if args.edge_latency_free_ms is not None and args.edge_latency_free_ms < 0:
        parser.error("--edge-latency-free-ms 不能小于 0")
    if args.risk_poll_interval_seconds is not None and args.risk_poll_interval_seconds <= 0:
        parser.error("--risk-poll-interval-seconds 必须大于 0")
    if args.risk_stale_after_seconds is not None and args.risk_stale_after_seconds <= 0:
        parser.error("--risk-stale-after-seconds 必须大于 0")
    if args.risk_reduce_position_ratio is not None and not 0 <= args.risk_reduce_position_ratio <= 1:
        parser.error("--risk-reduce-position-ratio 必须在 [0, 1] 区间内")
    if args.risk_stop_position_ratio is not None and not 0 <= args.risk_stop_position_ratio <= 1:
        parser.error("--risk-stop-position-ratio 必须在 [0, 1] 区间内")
    if args.risk_reduce_cooldown_seconds is not None and args.risk_reduce_cooldown_seconds < 0:
        parser.error("--risk-reduce-cooldown-seconds 不能小于 0")
    if args.risk_liq_distance_warn is not None and args.risk_liq_distance_warn < 0:
        parser.error("--risk-liq-distance-warn 不能小于 0")
    if args.risk_liq_distance_warn_recover is not None and args.risk_liq_distance_warn_recover < 0:
        parser.error("--risk-liq-distance-warn-recover 不能小于 0")
    if args.risk_liq_distance_reduce is not None and args.risk_liq_distance_reduce < 0:
        parser.error("--risk-liq-distance-reduce 不能小于 0")
    if args.risk_liq_distance_stop is not None and args.risk_liq_distance_stop < 0:
        parser.error("--risk-liq-distance-stop 不能小于 0")
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
    signal_logic = config.signal_logic if hasattr(config, 'signal_logic') else {}
    if not isinstance(signal_logic, dict):
        signal_logic = {}
    edge_filter_config = config.edge_filter if hasattr(config, 'edge_filter') and isinstance(config.edge_filter, dict) else {}
    risk_control_config = dict(config.risk_control) if hasattr(config, 'risk_control') and isinstance(config.risk_control, dict) else {}

    edge_filter_enabled = edge_filter_config.get('enabled', False)
    if args.edge_filter is not None:
        edge_filter_enabled = (args.edge_filter == 'on')

    min_edge_bps = float(args.min_edge_bps) if args.min_edge_bps is not None else float(edge_filter_config.get('min_edge_bps', 0.8))
    edge_base_cost_bps = float(args.edge_base_cost_bps) if args.edge_base_cost_bps is not None else float(edge_filter_config.get('base_cost_bps', 3.0))
    edge_fee_bps = float(args.edge_fee_bps) if args.edge_fee_bps is not None else float(edge_filter_config.get('fee_bps', 0.0))
    edge_latency_bps_per_100ms = (
        float(args.edge_latency_bps_per_100ms)
        if args.edge_latency_bps_per_100ms is not None
        else float(edge_filter_config.get('latency_bps_per_100ms', 0.0))
    )
    edge_latency_free_ms = (
        float(args.edge_latency_free_ms)
        if args.edge_latency_free_ms is not None
        else float(edge_filter_config.get('latency_free_ms', 120.0))
    )

    if args.signal_mode is not None:
        signal_logic['mode'] = args.signal_mode
    if args.signal_quantile is not None:
        signal_logic['quantile'] = float(args.signal_quantile)
    if args.signal_sample_size is not None:
        signal_logic['sample_size'] = int(args.signal_sample_size)
        # 冷启动最小样本默认与样本数一致
        signal_logic['min_samples'] = int(args.signal_sample_size)
    if args.signal_min_edge_pct is not None:
        signal_logic['min_edge_pct'] = float(args.signal_min_edge_pct)
    if args.signal_min_abs_spread_pct is not None:
        signal_logic['min_abs_spread_pct'] = float(args.signal_min_abs_spread_pct)
    stat_arb_logic = signal_logic.get('stat_arb', {})
    if not isinstance(stat_arb_logic, dict):
        stat_arb_logic = {}
    if args.signal_stat_arb_enabled is not None:
        stat_arb_logic['enabled'] = (args.signal_stat_arb_enabled == 'on')
    if args.signal_stat_arb_baseline_adjustment is not None:
        stat_arb_logic['baseline_adjustment'] = (args.signal_stat_arb_baseline_adjustment == 'on')
    if args.signal_stat_arb_baseline_ratio is not None:
        stat_arb_logic['baseline_ratio'] = float(args.signal_stat_arb_baseline_ratio)
    if args.signal_stat_arb_medium_window_seconds is not None:
        stat_arb_logic['medium_window_seconds'] = int(args.signal_stat_arb_medium_window_seconds)
    if args.signal_stat_arb_long_window_seconds is not None:
        stat_arb_logic['long_window_seconds'] = int(args.signal_stat_arb_long_window_seconds)
    if args.signal_stat_arb_medium_min_samples is not None:
        stat_arb_logic['medium_min_samples'] = int(args.signal_stat_arb_medium_min_samples)
    if args.signal_stat_arb_long_min_samples is not None:
        stat_arb_logic['long_min_samples'] = int(args.signal_stat_arb_long_min_samples)
    if args.signal_stat_arb_medium_weight is not None:
        stat_arb_logic['medium_weight'] = float(args.signal_stat_arb_medium_weight)
    if args.signal_stat_arb_long_weight is not None:
        stat_arb_logic['long_weight'] = float(args.signal_stat_arb_long_weight)
    if args.signal_stat_arb_entry_threshold is not None:
        stat_arb_logic['entry_threshold'] = float(args.signal_stat_arb_entry_threshold)
    if args.signal_stat_arb_exit_threshold is not None:
        stat_arb_logic['exit_threshold'] = float(args.signal_stat_arb_exit_threshold)
    if args.signal_stat_arb_exit_spread_floor_pct is not None:
        stat_arb_logic['exit_spread_floor_pct'] = float(args.signal_stat_arb_exit_spread_floor_pct)
    if args.signal_stat_arb_min_score_gap is not None:
        stat_arb_logic['min_score_gap'] = float(args.signal_stat_arb_min_score_gap)
    if args.signal_stat_arb_min_mad_pct is not None:
        stat_arb_logic['min_mad_pct'] = float(args.signal_stat_arb_min_mad_pct)
    if args.signal_stat_arb_quality_log_interval_seconds is not None:
        stat_arb_logic['quality_log_interval_seconds'] = float(args.signal_stat_arb_quality_log_interval_seconds)
    if args.signal_stat_arb_require_same_sign is not None:
        stat_arb_logic['require_same_sign_for_medium_long'] = (args.signal_stat_arb_require_same_sign == 'on')
    if args.signal_stat_arb_block_regime is not None:
        stat_arb_logic['block_when_regime_suspected'] = (args.signal_stat_arb_block_regime == 'on')
    signal_logic['stat_arb'] = stat_arb_logic

    if dynamic_threshold:
        if args.min_total_threshold is not None:
            dynamic_threshold["min_total_threshold"] = float(args.min_total_threshold)
        if args.sample_size is not None:
            dynamic_threshold["sample_size"] = int(args.sample_size)
        
        dynamic_threshold['min_samples'] = dynamic_threshold.get('sample_size', 1000)

        dynamic_threshold['max_std_multiplier'] = float(args.max_std_multiplier)
        dynamic_threshold['min_std_multiplier'] = float(args.min_std_multiplier)
    cooldown_seconds = float(args.cooldown_seconds) if args.cooldown_seconds else 5
    exchange_a_slippage = None
    exchange_b_slippage = None
    # 设置滑点
    if args.exchange_a_slippage is not None:
        exchange_a_slippage = Decimal(str(args.exchange_a_slippage))
    if args.exchange_b_slippage is not None:
        exchange_b_slippage = Decimal(str(args.exchange_b_slippage))
    dt_min_total_threshold = dynamic_threshold.get('min_total_threshold', '--') if isinstance(dynamic_threshold, dict) else '--'
    dt_sample_size = dynamic_threshold.get('sample_size', '--') if isinstance(dynamic_threshold, dict) else '--'
    dt_max_std_multiplier = dynamic_threshold.get('max_std_multiplier', '--') if isinstance(dynamic_threshold, dict) else '--'
    dt_min_std_multiplier = dynamic_threshold.get('min_std_multiplier', '--') if isinstance(dynamic_threshold, dict) else '--'
    dt_enabled_text = '启用' if isinstance(dynamic_threshold, dict) and dynamic_threshold.get('enabled', False) else '禁用'
    signal_mode = str(signal_logic.get('mode', 'legacy')).lower()
    signal_quantile = float(signal_logic.get('quantile', 0.6))
    signal_sample_size = int(signal_logic.get('sample_size', 2000))
    signal_min_samples = int(signal_logic.get('min_samples', signal_sample_size))
    stat_arb_enabled = bool(stat_arb_logic.get('enabled', False))
    stat_arb_baseline_adjustment = bool(stat_arb_logic.get('baseline_adjustment', True))
    stat_arb_baseline_ratio = float(stat_arb_logic.get('baseline_ratio', 0.5))
    stat_arb_medium_window_seconds = int(stat_arb_logic.get('medium_window_seconds', 1800))
    stat_arb_long_window_seconds = int(stat_arb_logic.get('long_window_seconds', 3600))
    stat_arb_medium_min_samples = int(stat_arb_logic.get('medium_min_samples', 120))
    stat_arb_long_min_samples = int(stat_arb_logic.get('long_min_samples', 240))
    stat_arb_medium_weight = float(stat_arb_logic.get('medium_weight', 0.4))
    stat_arb_long_weight = float(stat_arb_logic.get('long_weight', 0.6))
    stat_arb_entry_threshold = float(stat_arb_logic.get('entry_threshold', 2.8))
    stat_arb_exit_threshold = float(stat_arb_logic.get('exit_threshold', 0.8))
    stat_arb_exit_spread_floor_pct = float(stat_arb_logic.get('exit_spread_floor_pct', -0.01))
    stat_arb_min_score_gap = float(stat_arb_logic.get('min_score_gap', 0.5))
    stat_arb_min_mad_pct = float(stat_arb_logic.get('min_mad_pct', 0.003))
    stat_arb_quality_log_interval = float(stat_arb_logic.get('quality_log_interval_seconds', 15.0))
    stat_arb_require_same_sign = bool(stat_arb_logic.get('require_same_sign_for_medium_long', True))
    stat_arb_block_regime = bool(stat_arb_logic.get('block_when_regime_suspected', True))
    risk_control_enabled = bool(risk_control_config.get('enabled', False))  # 风控模块总开关
    risk_poll_interval = float(args.risk_poll_interval_seconds) if args.risk_poll_interval_seconds is not None else float(risk_control_config.get('poll_interval_seconds', 1.0))  # 风控后台轮询间隔（秒）
    risk_stale_after = float(args.risk_stale_after_seconds) if args.risk_stale_after_seconds is not None else float(risk_control_config.get('stale_after_seconds', 3.0))  # 风控快照过期判定阈值（秒）
    risk_reduce_ratio = float(args.risk_reduce_position_ratio) if args.risk_reduce_position_ratio is not None else float(risk_control_config.get('reduce_position_ratio', 0.5))  # 进入 REDUCE 后目标仓位比例
    risk_stop_ratio = float(args.risk_stop_position_ratio) if args.risk_stop_position_ratio is not None else float(risk_control_config.get('stop_position_ratio', 0.0))  # 进入 STOP 后目标仓位比例
    risk_reduce_cooldown = float(args.risk_reduce_cooldown_seconds) if args.risk_reduce_cooldown_seconds is not None else float(risk_control_config.get('reduce_cooldown_seconds', 5.0))  # 两次风控主动减仓的最小间隔（秒）
    risk_liq_distance_warn = float(args.risk_liq_distance_warn) if args.risk_liq_distance_warn is not None else float(risk_control_config.get('liq_distance_warn', 0.20))  # 离清算价距离 WARN 阈值
    risk_liq_distance_warn_recover = float(args.risk_liq_distance_warn_recover) if args.risk_liq_distance_warn_recover is not None else float(risk_control_config.get('liq_distance_warn_recover', risk_control_config.get('liq_distance_warn', 0.20)))  # 离清算价距离 WARN 恢复阈值
    risk_liq_distance_reduce = float(args.risk_liq_distance_reduce) if args.risk_liq_distance_reduce is not None else float(risk_control_config.get('liq_distance_reduce', 0.10))  # 离清算价距离 REDUCE 阈值
    risk_liq_distance_stop = float(args.risk_liq_distance_stop) if args.risk_liq_distance_stop is not None else float(risk_control_config.get('liq_distance_stop', 0.05))  # 离清算价距离 STOP 阈值

    risk_control_config['poll_interval_seconds'] = risk_poll_interval
    risk_control_config['stale_after_seconds'] = risk_stale_after
    risk_control_config['reduce_position_ratio'] = risk_reduce_ratio
    risk_control_config['stop_position_ratio'] = risk_stop_ratio
    risk_control_config['reduce_cooldown_seconds'] = risk_reduce_cooldown
    risk_control_config['liq_distance_warn'] = risk_liq_distance_warn
    risk_control_config['liq_distance_warn_recover'] = risk_liq_distance_warn_recover
    risk_control_config['liq_distance_reduce'] = risk_liq_distance_reduce
    risk_control_config['liq_distance_stop'] = risk_liq_distance_stop

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
        f"  最小阈值和:   {dt_min_total_threshold}\n"
        f"  样本数:       {dt_sample_size}\n"
        f"  最大标准差系数: {dt_max_std_multiplier}\n"
        f"  最小标准差系数: {dt_min_std_multiplier}\n"
        f"  信号逻辑:     {'分位数' if signal_mode == 'quantile' else ('统计套利' if signal_mode == 'stat_arb' else '标准差')}\n"
        f"  分位数配置:   P{int(signal_quantile * 100)} | 样本{signal_sample_size} | 最小样本{signal_min_samples}\n"
        f"  统计套利开关: {'启用' if stat_arb_enabled else '禁用'}\n"
        f"  统计套利基线修正: {'启用' if stat_arb_baseline_adjustment else '禁用'} | ratio={stat_arb_baseline_ratio:.3f}\n"
        f"  统计套利窗口:  30m={stat_arb_medium_window_seconds}s / 60m={stat_arb_long_window_seconds}s\n"
        f"  统计套利样本:  30m={stat_arb_medium_min_samples} / 60m={stat_arb_long_min_samples}\n"
        f"  统计套利权重:  30m={stat_arb_medium_weight:.2f} / 60m={stat_arb_long_weight:.2f}\n"
        f"  统计套利阈值:  entry={stat_arb_entry_threshold:.3f} | exit={stat_arb_exit_threshold:.3f} | exit_floor={stat_arb_exit_spread_floor_pct:.4f}% | gap={stat_arb_min_score_gap:.3f} | MAD下限={stat_arb_min_mad_pct:.6f}\n"
        f"  质量日志间隔: {stat_arb_quality_log_interval:.1f}s\n"
        f"  统计套利过滤:  同向={ '是' if stat_arb_require_same_sign else '否'} | 阻断regime={ '是' if stat_arb_block_regime else '否'}\n"
        f"  监控模式:     {'是' if monitor_only else '否'}\n"  # ✅ 显示监控模式
        f"  累计模式:     {'启用' if accumulate_mode else '禁用'}\n"
        f"  最大持仓:     {max_position}\n"
        f"  负向滑点方向下单: { '是' if not direction_reverse else '否'}\n"
        f"  动态阈值:     {dt_enabled_text}\n"  # ✅ 新增
        f"  冷却时间:     { cooldown_seconds }s\n"
        f"  交易所 A 滑点: {exchange_a_slippage or '--'}\n"
        f"  交易所 B 滑点: {exchange_b_slippage or '--'}\n"
        f"  Lighter 重连基础等待: {args.lighter_reconnect_base_delay}s\n"
        f"  Lighter 重连最大等待: {args.lighter_reconnect_max_delay}s\n"
        f"  信号延迟A阈值: {args.max_signal_delay_ms_a if args.max_signal_delay_ms_a is not None else args.max_signal_delay_ms} ms\n"
        f"  信号延迟B阈值: {args.max_signal_delay_ms_b if args.max_signal_delay_ms_b is not None else args.max_signal_delay_ms} ms\n"
        f"  边际二次过滤: {'启用' if edge_filter_enabled else '禁用'}\n"
        f"  最小安全边际: {min_edge_bps:.2f} bps\n"
        f"  基础成本估计: {edge_base_cost_bps:.2f} bps\n"
        f"  手续费估计:   {edge_fee_bps:.2f} bps\n"
        f"  延迟风险系数: {edge_latency_bps_per_100ms:.2f} bps/100ms\n"
        f"  延迟免惩罚阈值: {edge_latency_free_ms:.0f} ms\n"
        f"  风控模块:     {'启用' if risk_control_enabled else '禁用'}\n"
        f"  风控轮询间隔: {risk_poll_interval:.2f}s\n"
        f"  风控过期阈值: {risk_stale_after:.2f}s\n"
        f"  风控减仓冷却: {risk_reduce_cooldown:.2f}s\n"
        f"  风控减仓目标: {risk_reduce_ratio:.2f}\n"
        f"  风控清仓目标: {risk_stop_ratio:.2f}\n"
        f"  清算距离阈值: WARN={risk_liq_distance_warn:.2%} WARN恢复={risk_liq_distance_warn_recover:.2%} REDUCE={risk_liq_distance_reduce:.2%} STOP={risk_liq_distance_stop:.2%}\n"
        f"{'='*60}\n"
    )

    if signal_mode == 'stat_arb' and not stat_arb_enabled:
        logger.error("❌ 当前 signal_mode=stat_arb，但 signal_logic.stat_arb.enabled=false，请先打开统计套利开关")
        return
    
    # Step 2: 创建交易所适配器
    logger.info("🔌 初始化交易所适配器...")
    
    try:
        # 准备配置覆盖
        config_override_a = {}
        config_override_b = {}
        
        # Variational 特定配置
        if config.exchange_a == 'variational' and hasattr(config, 'variational_config'):
            config_override_a = dict(config.variational_config)
        
        if config.exchange_b == 'variational' and hasattr(config, 'variational_config'):
            config_override_b = dict(config.variational_config)

        # Variational 请求诊断日志阈值默认跟随对应侧的信号延迟阈值，除非显式覆盖
        if config.exchange_a == 'variational' and 'request_timing_log_threshold_ms' not in config_override_a:
            config_override_a['request_timing_log_threshold_ms'] = float(
                args.max_signal_delay_ms_a if args.max_signal_delay_ms_a is not None else args.max_signal_delay_ms
            )
        if config.exchange_b == 'variational' and 'request_timing_log_threshold_ms' not in config_override_b:
            config_override_b['request_timing_log_threshold_ms'] = float(
                args.max_signal_delay_ms_b if args.max_signal_delay_ms_b is not None else args.max_signal_delay_ms
            )

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
            config_override_a,
            args.lighter_reconnect_base_delay,
            args.lighter_reconnect_max_delay
        )
        
        exchange_b = await create_exchange_adapter(
            config.exchange_b,
            symbol_b,
            quantity,
            exchange_b_slippage,
            config_override_b,
            args.lighter_reconnect_base_delay,
            args.lighter_reconnect_max_delay
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
        pair_id=args.pair,
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
        max_signal_delay_ms_a=args.max_signal_delay_ms_a if args.max_signal_delay_ms_a is not None else args.max_signal_delay_ms,
        max_signal_delay_ms_b=args.max_signal_delay_ms_b if args.max_signal_delay_ms_b is not None else args.max_signal_delay_ms,
        min_depth_quantity=min_depth_quantity,  # ✅ 传递最小深度数量
        accumulate_mode=accumulate_mode,
        max_position=max_position,
        direction_reverse=direction_reverse,
        dynamic_threshold=dynamic_threshold,  # ✅ 传递动态阈值配置
        signal_logic=signal_logic,  # ✅ 传递信号逻辑配置
        cooldown_seconds=cooldown_seconds,
        end_time=args.end_time,
        edge_filter_enabled=edge_filter_enabled,
        min_edge_bps=min_edge_bps,
        edge_base_cost_bps=edge_base_cost_bps,
        edge_fee_bps=edge_fee_bps,
        edge_latency_bps_per_100ms=edge_latency_bps_per_100ms,
        edge_latency_free_ms=edge_latency_free_ms,
        risk_control=risk_control_config,
        local_override_path=str(Path(__file__).parent / "config" / "overrides.local.yaml"),
    )
    signal_mailbox = SignalMailbox()
    signal_execution_service = SignalExecutionService(
        strategy=strategy,
        mailbox=signal_mailbox,
        execution_max_signal_age_ms=50.0,
    )
    strategy.set_signal_submitter(signal_execution_service.submit, signal_execution_service.clear)
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
            
            if synced_qty is None:
                logger.warning("⚠️ 仓位状态未知，保留本地默认值，不按空仓初始化")
            elif synced_qty != 0:
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
        await signal_execution_service.start()
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
        await signal_execution_service.stop()
        await strategy.stop()
        if lark_bot is not None:
            await lark_bot.close()
        await asyncio.sleep(0.1)
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
