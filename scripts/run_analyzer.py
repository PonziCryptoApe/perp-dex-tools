"""
数据分析脚本

用法:
    python scripts/run_analyzer.py --symbol BTC-USD-PERP --date 2025-11-06
    python scripts/run_analyzer.py --symbol BTC-USD-PERP --latest  # 分析最新数据
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from arbitrage.data_analyzer import DataAnalyzer
from arbitrage.config import DATA_CONFIG


def main():
    parser = argparse.ArgumentParser(description='分析套利数据')
    parser.add_argument(
        '--symbol',
        type=str,
        default='BTC-USD-PERP',
        help='交易对符号'
    )
    parser.add_argument(
        '--date',
        type=str,
        help='分析日期 (格式: YYYY-MM-DD)'
    )
    parser.add_argument(
        '--latest',
        action='store_true',
        help='分析今天的数据'
    )
    parser.add_argument(
        '--data-dir',
        type=str,
        default=DATA_CONFIG['data_dir'],
        help='数据目录'
    )
    
    args = parser.parse_args()
    
    # 确定要分析的日期
    if args.latest or not args.date:
        date = datetime.now().strftime('%Y-%m-%d')
    else:
        date = args.date
    
    print(f'\n正在分析 {args.symbol} 在 {date} 的数据...\n')
    
    analyzer = DataAnalyzer(data_dir=args.data_dir)
    
    try:
        stats = analyzer.generate_report(args.symbol, date)
        
        print(f'\n✅ 分析完成！')
        print(f'图表已保存到: output/charts/')
        
    except FileNotFoundError:
        print(f'\n❌ 找不到 {date} 的数据文件')
        print(f'数据文件应该在: {args.data_dir}/prices_{args.symbol}_{date}.csv')
        print('\n请先运行 scripts/run_fetcher.py 收集数据')
    except Exception as e:
        print(f'\n❌ 分析失败: {e}')
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()