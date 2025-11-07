import pandas as pd
import numpy as np
from matplotlib import font_manager
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple
import json

# 确认字体是否被 Matplotlib 识别
for f in font_manager.findSystemFonts():
    if 'NotoSansCJK' in f or 'Noto' in f:
        pass  # 可打印调试
# 设置中文字体
plt.rcParams['font.sans-serif'] = ['PingFang SC', 'SimHei', 'Noto Sans CJK SC']
plt.rcParams['axes.unicode_minus'] = False

sns.set_style('whitegrid')


class DataAnalyzer:
    """数据分析器"""
    
    def __init__(self, data_dir: str = 'data/arbitrage'):
        self.data_dir = Path(data_dir)
    
    def load_data(self, symbol: str, date: str) -> pd.DataFrame:
        """
        加载指定日期的数据
        
        Args:
            symbol: 交易对符号
            date: 日期，格式 YYYY-MM-DD
        
        Returns:
            DataFrame
        """
        csv_file = self.data_dir / f'prices_{symbol}_{date}.csv'
        
        if not csv_file.exists():
            raise FileNotFoundError(f'数据文件不存在: {csv_file}')
        
        df = pd.read_csv(csv_file)
        
        # 转换时间戳
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('datetime', inplace=True)
        
        return df
    
    def load_jsonl_data(self, symbol: str, date: str) -> List[Dict]:
        """加载 JSONL 格式的数据"""
        jsonl_file = self.data_dir / f'prices_{symbol}_{date}.jsonl'
        
        if not jsonl_file.exists():
            raise FileNotFoundError(f'数据文件不存在: {jsonl_file}')
        
        data = []
        with open(jsonl_file, 'r') as f:
            for line in f:
                data.append(json.loads(line))
        
        return data
    
    def basic_statistics(self, df: pd.DataFrame) -> Dict:
        """基础统计分析"""
        stats = {
            'total_snapshots': len(df),
            'time_range': {
                'start': df.index.min(),
                'end': df.index.max(),
                'duration_hours': (df.index.max() - df.index.min()).total_seconds() / 3600
            },
            'spread_stats': {
                'mean': df['spread_percentage'].mean(),
                'median': df['spread_percentage'].median(),
                'std': df['spread_percentage'].std(),
                'min': df['spread_percentage'].min(),
                'max': df['spread_percentage'].max(),
                'q25': df['spread_percentage'].quantile(0.25),
                'q75': df['spread_percentage'].quantile(0.75)
            },
            'opportunities': {
                'count_0.1': len(df[df['spread_percentage'] > 0.1]),
                'count_0.2': len(df[df['spread_percentage'] > 0.2]),
                'count_0.5': len(df[df['spread_percentage'] > 0.5]),
                'percentage_0.1': len(df[df['spread_percentage'] > 0.1]) / len(df) * 100
            },
            'extended_stats': {
                'avg_bid': df['ext_bid'].mean(),
                'avg_ask': df['ext_ask'].mean(),
                'avg_spread': (df['ext_ask'] - df['ext_bid']).mean(),
                'spread_bps': (df['ext_ask'] - df['ext_bid']).mean() / df['ext_mid'].mean() * 10000
            },
            'lighter_stats': {
                'avg_bid': df['lighter_bid'].mean(),
                'avg_ask': df['lighter_ask'].mean(),
                'avg_spread': (df['lighter_ask'] - df['lighter_bid']).mean(),
                'spread_bps': (df['lighter_ask'] - df['lighter_bid']).mean() / df['lighter_mid'].mean() * 10000
            }
        }
        
        return stats
    
    def hourly_analysis(self, df: pd.DataFrame) -> pd.DataFrame:
        """按小时分析"""
        df['hour'] = df.index.hour
        
        hourly = df.groupby('hour').agg({
            'spread_percentage': ['mean', 'max', 'min', 'std', 'count'],
            'spread': ['mean', 'max'],
            'ext_bid': 'mean',
            'lighter_ask': 'mean'
        }).round(4)
        
        # 计算每小时的套利机会数量
        opportunities = df[df['spread_percentage'] > 0.1].groupby('hour').size()
        hourly['opportunities'] = opportunities
        hourly['opportunities'] = hourly['opportunities'].fillna(0).astype(int)
        
        return hourly
    
    def find_best_opportunities(self, df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
        """找出最佳套利机会"""
        opportunities = df[df['spread_percentage'] > 0.1].copy()
        opportunities = opportunities.sort_values('spread_percentage', ascending=False)
        
        return opportunities.head(top_n)[[
            'ext_bid', 'ext_ask', 'lighter_bid', 'lighter_ask',
            'spread', 'spread_percentage'
        ]]
    
    def plot_analysis(self, df: pd.DataFrame, output_file: str = 'arbitrage_analysis.png'):
        """生成分析图表"""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
        # 1. 价格对比
        ax1 = axes[0, 0]
        ax1.plot(df.index, df['ext_ask'], label='Extended Ask', alpha=0.7, linewidth=1)
        ax1.plot(df.index, df['lighter_bid'], label='Lighter Bid', alpha=0.7, linewidth=1)
        ax1.fill_between(df.index, df['ext_ask'], df['lighter_bid'], 
                         where=(df['lighter_bid'] > df['ext_ask']), 
                         alpha=0.3, color='green', label='Positive Spread')
        ax1.set_title('价格对比 (Extended Ask vs Lighter Bid)', fontsize=12, fontweight='bold')
        ax1.set_xlabel('时间')
        ax1.set_ylabel('价格')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # 2. 价差百分比时序图
        ax2 = axes[0, 1]
        ax2.plot(df.index, df['spread_percentage'], linewidth=1, color='blue', alpha=0.7)
        ax2.axhline(y=0.1, color='red', linestyle='--', linewidth=2, label='阈值 0.1%')
        ax2.axhline(y=0.2, color='orange', linestyle='--', linewidth=2, label='阈值 0.2%')
        ax2.fill_between(df.index, 0, df['spread_percentage'], 
                         where=(df['spread_percentage'] > 0.1), 
                         alpha=0.3, color='green')
        ax2.set_title('价差百分比变化', fontsize=12, fontweight='bold')
        ax2.set_xlabel('时间')
        ax2.set_ylabel('价差 (%)')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        # 3. 价差分布直方图
        ax3 = axes[1, 0]
        ax3.hist(df['spread_percentage'], bins=50, edgecolor='black', alpha=0.7)
        ax3.axvline(x=0.1, color='red', linestyle='--', linewidth=2, label='阈值 0.1%')
        ax3.set_title('价差分布', fontsize=12, fontweight='bold')
        ax3.set_xlabel('价差 (%)')
        ax3.set_ylabel('频次')
        ax3.legend()
        ax3.grid(True, alpha=0.3, axis='y')
        
        # 4. 每小时套利机会统计（修复版）
        ax4 = axes[1, 1]
        df_temp = df.copy()
        df_temp['hour'] = df_temp.index.hour
        
        # 计算每小时的数据
        hourly_opps = df_temp[df_temp['spread_percentage'] > 0.1].groupby('hour').size()
        hourly_total = df_temp.groupby('hour').size()
        
        # 创建完整的24小时范围，填充缺失值为0
        all_hours = pd.Series(0, index=range(24))
        hourly_pct = (hourly_opps / hourly_total * 100).reindex(range(24), fill_value=0)
        
        # 绘制柱状图
        x = range(24)
        ax4.bar(x, hourly_pct.values, alpha=0.7, color='steelblue')
        ax4.set_title('每小时套利机会占比', fontsize=12, fontweight='bold')
        ax4.set_xlabel('小时')
        ax4.set_ylabel('套利机会占比 (%)')
        ax4.set_xticks(x)
        ax4.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        
        # 确保输出目录存在
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        plt.savefig(output_file, dpi=150, bbox_inches='tight')
        print(f'\n图表已保存到: {output_file}')
        plt.close()
    
    def compute_hedge_spreads(self, df: pd.DataFrame) -> pd.DataFrame:
            """计算两种对冲方向的价差(不考虑成本)"""
            df['spread_shortExt_longLight'] = df['ext_bid'] - df['lighter_ask']
            df['spread_longExt_shortLight'] = df['lighter_bid'] - df['ext_ask']
            # 标记即时可锁定正价差
            df['instant_opportunity_dir1'] = df['spread_shortExt_longLight'] > 0
            df['instant_opportunity_dir2'] = df['spread_longExt_shortLight'] > 0
            return df
    
    def hedge_spread_stats(self, df: pd.DataFrame) -> Dict:
        """输出对冲方向统计"""
        s1 = df['spread_shortExt_longLight']
        s2 = df['spread_longExt_shortLight']
        stats = {
            'dir1_mean': s1.mean(),
            'dir1_median': s1.median(),
            'dir1_std': s1.std(),
            'dir1_min': s1.min(),
            'dir1_max': s1.max(),
            'dir1_positive_ratio': (s1 > 0).mean() * 100,
            'dir2_mean': s2.mean(),
            'dir2_median': s2.median(),
            'dir2_std': s2.std(),
            'dir2_min': s2.min(),
            'dir2_max': s2.max(),
            'dir2_positive_ratio': (s2 > 0).mean() * 100
        }
        return stats
    
    def plot_hedge_spreads(self, df: pd.DataFrame, output_file: str):
        """绘制两方向对冲价差图"""
        import matplotlib.pyplot as plt
        plt.figure(figsize=(16,10))
        ax1 = plt.subplot(2,2,1)
        ax1.plot(df.index, df['spread_shortExt_longLight'], label='做空EXT+做多LGT', color='green')
        ax1.plot(df.index, df['spread_longExt_shortLight'], label='做多EXT+做空LGT', color='red')
        ax1.axhline(0, color='#666', linewidth=0.8)
        ax1.set_title('两方向价差时间序列')
        ax1.legend()
        ax2 = plt.subplot(2,2,2)
        ax2.hist(df['spread_shortExt_longLight'], bins=50, alpha=0.6, label='空EXT多LGT', color='green')
        ax2.hist(df['spread_longExt_shortLight'], bins=50, alpha=0.6, label='多EXT空LGT', color='red')
        ax2.set_title('价差分布对比'); ax2.legend()
        ax3 = plt.subplot(2,2,3)
        cumulative_dir1 = (df['spread_shortExt_longLight'].clip(lower=0)).cumsum()
        cumulative_dir2 = (df['spread_longExt_shortLight'].clip(lower=0)).cumsum()
        ax3.plot(df.index, cumulative_dir1, label='累积正价差(空EXT多LGT)', color='green')
        ax3.plot(df.index, cumulative_dir2, label='累积正价差(多EXT空LGT)', color='red')
        ax3.set_title('累积可锁定正价差'); ax3.legend()
        ax4 = plt.subplot(2,2,4)
        diff = df['spread_shortExt_longLight'] - df['spread_longExt_shortLight']
        ax4.plot(df.index, diff, color='purple')
        ax4.set_title('方向差值(Dir1 - Dir2)')
        plt.tight_layout()
        out = Path(output_file)
        out.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(output_file, dpi=150)
        plt.close()
        print(f'对冲价差图已生成: {output_file}')    

    def generate_report(self, symbol: str, date: str):
        """生成完整分析报告"""
        print('\n' + '=' * 80)
        print(f'套利数据分析报告 - {symbol}')
        print('=' * 80)
        
        # 加载数据
        df = self.load_data(symbol, date)
        df = self.load_data(symbol, date)
        df = self.compute_hedge_spreads(df)
        hedge_stats = self.hedge_spread_stats(df)
        print('\n🔀 对冲方向比较(不含成本):')
        print(f"  做空EXT+做多LGT: 均值={hedge_stats['dir1_mean']:.2f} 中位={hedge_stats['dir1_median']:.2f} "
                f"Std={hedge_stats['dir1_std']:.2f} 最小={hedge_stats['dir1_min']:.2f} 最大={hedge_stats['dir1_max']:.2f} "
                f"正值比例={hedge_stats['dir1_positive_ratio']:.2f}%")
        print(f"  做多EXT+做空LGT: 均值={hedge_stats['dir2_mean']:.2f} 中位={hedge_stats['dir2_median']:.2f} "
                f"Std={hedge_stats['dir2_std']:.2f} 最小={hedge_stats['dir2_min']:.2f} 最大={hedge_stats['dir2_max']:.2f} "
                f"正值比例={hedge_stats['dir2_positive_ratio']:.2f}%")
        preferred = '做空 Extended + 做多 Lighter' if hedge_stats['dir1_mean'] > 0 and hedge_stats['dir1_positive_ratio'] > 60 else '需进一步验证'
        print(f"\n✅ 推荐方向: {preferred}")
        
        # 基础统计
        stats = self.basic_statistics(df)
        
        print(f'\n📅 数据时间范围:')
        print(f"  开始: {stats['time_range']['start']}")
        print(f"  结束: {stats['time_range']['end']}")
        print(f"  时长: {stats['time_range']['duration_hours']:.2f} 小时")
        print(f"  总快照数: {stats['total_snapshots']}")
        
        print(f'\n💰 价差统计:')
        print(f"  平均价差: {stats['spread_stats']['mean']:.4f}%")
        print(f"  中位价差: {stats['spread_stats']['median']:.4f}%")
        print(f"  标准差: {stats['spread_stats']['std']:.4f}%")
        print(f"  最小价差: {stats['spread_stats']['min']:.4f}%")
        print(f"  最大价差: {stats['spread_stats']['max']:.4f}%")
        print(f"  25分位: {stats['spread_stats']['q25']:.4f}%")
        print(f"  75分位: {stats['spread_stats']['q75']:.4f}%")
        
        print(f'\n🎯 套利机会统计:')
        print(f"  价差 > 0.1%: {stats['opportunities']['count_0.1']} 次 ({stats['opportunities']['percentage_0.1']:.2f}%)")
        print(f"  价差 > 0.2%: {stats['opportunities']['count_0.2']} 次")
        print(f"  价差 > 0.5%: {stats['opportunities']['count_0.5']} 次")
        
        print(f'\n📈 Extended 交易所统计:')
        print(f"  平均买一价: {stats['extended_stats']['avg_bid']:.2f}")
        print(f"  平均卖一价: {stats['extended_stats']['avg_ask']:.2f}")
        print(f"  平均买卖价差: {stats['extended_stats']['avg_spread']:.4f}")
        print(f"  价差(基点): {stats['extended_stats']['spread_bps']:.2f} bps")
        
        print(f'\n📈 Lighter 交易所统计:')
        print(f"  平均买一价: {stats['lighter_stats']['avg_bid']:.2f}")
        print(f"  平均卖一价: {stats['lighter_stats']['avg_ask']:.2f}")
        print(f"  平均买卖价差: {stats['lighter_stats']['avg_spread']:.4f}")
        print(f"  价差(基点): {stats['lighter_stats']['spread_bps']:.2f} bps")
        
        # 每小时分析
        print(f'\n⏰ 每小时套利机会分析:')
        hourly = self.hourly_analysis(df)
        print(hourly.to_string())
        
        # 最佳机会
        print(f'\n🏆 Top 10 最佳套利机会:')
        best_opps = self.find_best_opportunities(df, top_n=10)
        print(best_opps.to_string())
        
        # 生成图表
        output_dir = Path('output/charts')
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f'arbitrage_analysis_{symbol}_{date}.png'
        self.plot_analysis(df, str(output_file))
        hedge_chart = Path('output/charts') / f'hedge_spread_{symbol}_{date}.png'
        self.plot_hedge_spreads(df, str(hedge_chart))
        print('\n' + '=' * 80)
        
        return stats


def main():
    """主函数"""
    analyzer = DataAnalyzer()
    
    # 使用今天的日期
    today = datetime.now().strftime('%Y-%m-%d')
    symbol = 'ETH-USD-PERP'
    
    try:
        analyzer.generate_report(symbol, today)
    except FileNotFoundError as e:
        print(f'错误: {e}')
        print('请先运行 price_fetcher.py 收集数据')
    except Exception as e:
        print(f'分析失败: {e}')
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()