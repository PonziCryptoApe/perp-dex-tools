"""
价格监控启动脚本
"""

import asyncio
import argparse
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

from arbitrage.price_fetcher import PriceFetcher

async def run_multi(symbols: list[str], interval: int, data_dir: str):
    fetchers = [
        PriceFetcher(symbol=s.strip(), interval_seconds=interval, data_dir=data_dir)
        for s in symbols
    ]
    # 并发启动
    await asyncio.gather(*(f.start() for f in fetchers))

async def main():
    parser = argparse.ArgumentParser(description='启动价格监控')
    parser.add_argument('--symbols', type=str,
                        help='多个基础符号，逗号分隔，例如: BTC,ETH')
    parser.add_argument('--symbol', type=str, help='单一符号 (与 --symbols 二选一)')
    parser.add_argument('--interval', type=int, default=5)
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
║          套利价格监控系统 v1.0                             ║
║                                                            ║
║  启动符号: {symbols}║
║  更新间隔: {args.interval} 秒{' ' * 44}║
║  数据目录: {args.data_dir:43s} ║
║                                                            ║
║  按 Ctrl+C 停止运行                                        ║
╚════════════════════════════════════════════════════════════╝
    """)
    
    try:
        if len(symbols) == 1:
            fetcher = PriceFetcher(symbol=symbols[0], interval_seconds=args.interval, data_dir=args.data_dir)
            await fetcher.start()
        else:
            await run_multi(symbols, args.interval, args.data_dir)
    
    except KeyboardInterrupt:
        print("\n\n程序已停止")
        fetcher.stop()


if __name__ == '__main__':
    asyncio.run(main())