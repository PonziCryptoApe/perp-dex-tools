"""
价格监控启动脚本 - Variational & Extended
"""

import asyncio
import argparse
from datetime import datetime, timedelta
import sys
import warnings
import logging
from pathlib import Path
import dotenv

dotenv.load_dotenv()

# 抑制 matplotlib 字体警告
warnings.filterwarnings('ignore', category=UserWarning, module='matplotlib')
logging.getLogger('matplotlib').setLevel(logging.ERROR)

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from arbitrage.price_fetcher_var_ext import PriceFetcher

async def run_multi_staggered(symbols: list[str], interval: int, data_dir: str):
    """错峰启动多个监控器"""
    fetchers = []
    
    # 计算每个币种的启动延迟
    total_interval = interval * 60  # 总间隔（秒）
    num_symbols = len(symbols)
    stagger_delay = total_interval / num_symbols  # 每个币种的延迟
    
    print(f"\n📊 错峰调度策略:")
    print(f"   总间隔: {interval} 分钟 ({total_interval} 秒)")
    print(f"   币种数量: {num_symbols}")
    print(f"   每个币种间隔: {stagger_delay:.1f} 秒\n")

    # ✅ 显示完整时间表
    print(f"\n⏰ 启动时间表:")
    for i, symbol in enumerate(symbols):
        delay = i * stagger_delay
        start_time = datetime.now() + timedelta(seconds=delay)
        print(f"   [{symbol}] {start_time.strftime('%H:%M:%S')} (延迟 {delay:.0f}s)")
    
    print()
    
    tasks = []
    
    for i, symbol in enumerate(symbols):
        delay = i * stagger_delay
        
        # print(f"⏰ [{symbol}] 将在 {delay:.1f} 秒后启动")
        
        # 创建延迟启动的任务
        async def delayed_start(sym, d):
            if d > 0:
                print(f"   [{sym}] 等待 {d:.1f} 秒...")
                await asyncio.sleep(d)
            
            print(f"🚀 [{sym}] 开始监控")
            fetcher = PriceFetcher(
                symbol=sym, 
                interval_seconds=total_interval,  # ✅ 使用总间隔作为轮询周期
                data_dir=data_dir
            )
            fetchers.append(fetcher)
            await fetcher.start()
        
        tasks.append(delayed_start(symbol, delay))
    
    # 并发运行所有任务
    await asyncio.gather(*tasks)

async def main():
    parser = argparse.ArgumentParser(description='启动价格监控 (Variational & Extended)')
    parser.add_argument('--symbols', type=str,
                        help='多个基础符号，逗号分隔，例如: BTC,ETH')
    parser.add_argument('--symbol', type=str, help='单一符号 (与 --symbols 二选一)')
    parser.add_argument('--interval', type=int, default=5, 
                        help='总循环间隔（分钟），默认5分钟。多币种时会平均分配')
    parser.add_argument('--data-dir', type=str, default='data/arbitrage')
    parser.add_argument('--env-file', type=str)
    args = parser.parse_args()

    # 环境
    if args.env_file:
        env_path = Path(args.env_file)
        if not env_path.exists():
            print(f"Env file not find: {env_path.resolve()}")
            sys.exit(1)
        dotenv.load_dotenv(args.env_file)

    if args.symbols:
        symbols = [s.upper() for s in args.symbols.split(',') if s.strip()]
    elif args.symbol:
        symbols = [args.symbol.upper()]
    else:
        print("必须提供 --symbol 或 --symbols")
        sys.exit(1)
    
    print(f"""
╔════════════════════════════════════════════════════════════╗
║     套利价格监控系统 v1.0 (Variational & Extended)        ║
║                                                            ║
║  启动符号: {str(symbols):46s}║
║  循环周期: {args.interval} 分钟{' ' * 42}║
║  数据目录: {args.data_dir:43s} ║
║                                                            ║
║  按 Ctrl+C 停止运行                                        ║
╚════════════════════════════════════════════════════════════╝
    """)
    
    try:
        if len(symbols) == 1:
            # 单币种：使用原逻辑（秒级间隔）
            fetcher = PriceFetcher(
                symbol=symbols[0], 
                interval_seconds=args.interval * 60,  # 转换为秒
                data_dir=args.data_dir
            )
            await fetcher.start()
        else:
            # 多币种：错峰启动
            await run_multi_staggered(symbols, args.interval, args.data_dir)
    
    except KeyboardInterrupt:
        print("\n\n程序已停止")


if __name__ == '__main__':
    asyncio.run(main())