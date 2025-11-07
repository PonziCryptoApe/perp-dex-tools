import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SpreadAnalyzer:
    """价差统计分析工具"""
    
    def __init__(
        self,
        csv_path: str,
        transaction_fee_bps: float = 4.0,  # 双边手续费 (basis points)
        gas_cost_usd: float = 10.0,        # Gas 成本
        min_profit_bps: float = 5.0        # 最小盈利要求 (bps)
    ):
        self.csv_path = Path(csv_path)
        self.transaction_fee_bps = transaction_fee_bps
        self.gas_cost_usd = gas_cost_usd
        self.min_profit_bps = min_profit_bps
        
        self.df = None
        self.stats = {}
        
    def load_data(self) -> pd.DataFrame:
        """加载和预处理数据"""
        logger.info(f"加载数据: {self.csv_path}")
        
        self.df = pd.read_csv(self.csv_path)
        
        # 转换时间戳
        self.df['datetime'] = pd.to_datetime(self.df['timestamp'], unit='ms')
        self.df['time'] = self.df['datetime'].dt.strftime('%H:%M:%S')
        
        # 计算价差 (basis points)
        self.df['spread_bps'] = (self.df['spread'] / self.df['ext_mid']) * 10000
        
        # 计算净利润 (扣除成本)
        self.df['net_profit_usd'] = self.df['spread'] - (
            self.df['ext_mid'] * self.transaction_fee_bps / 10000 + 
            self.gas_cost_usd
        )
        self.df['net_profit_bps'] = (self.df['net_profit_usd'] / self.df['ext_mid']) * 10000
        
        # 标记可盈利机会
        self.df['profitable'] = self.df['net_profit_bps'] > self.min_profit_bps
        
        logger.info(f"✅ 加载完成: {len(self.df)} 条记录")
        logger.info(f"   时间范围: {self.df['datetime'].min()} ~ {self.df['datetime'].max()}")
        
        return self.df
    
    def calculate_basic_stats(self) -> Dict:
        """计算基本统计量"""
        if self.df is None:
            self.load_data()
        
        logger.info("\n" + "="*80)
        logger.info("📊 基本统计分析")
        logger.info("="*80)
        
        stats = {
            'count': len(self.df),
            'duration_minutes': (self.df['datetime'].max() - self.df['datetime'].min()).total_seconds() / 60,
            
            # 价差统计 (USD)
            'spread_mean': self.df['spread'].mean(),
            'spread_std': self.df['spread'].std(),
            'spread_min': self.df['spread'].min(),
            'spread_max': self.df['spread'].max(),
            'spread_q25': self.df['spread'].quantile(0.25),
            'spread_q50': self.df['spread'].quantile(0.50),
            'spread_q75': self.df['spread'].quantile(0.75),
            'spread_q95': self.df['spread'].quantile(0.95),
            
            # 价差统计 (bps)
            'spread_bps_mean': self.df['spread_bps'].mean(),
            'spread_bps_std': self.df['spread_bps'].std(),
            'spread_bps_min': self.df['spread_bps'].min(),
            'spread_bps_max': self.df['spread_bps'].max(),
            
            # 净利润统计
            'net_profit_mean': self.df['net_profit_usd'].mean(),
            'net_profit_std': self.df['net_profit_usd'].std(),
            'net_profit_bps_mean': self.df['net_profit_bps'].mean(),
            'net_profit_bps_std': self.df['net_profit_bps'].std(),
            
            # 盈利机会
            'profitable_count': self.df['profitable'].sum(),
            'profitable_rate': self.df['profitable'].mean() * 100,
        }
        
        self.stats['basic'] = stats
        
        # 打印结果
        print(f"\n📈 数据概况:")
        print(f"   样本数量: {stats['count']}")
        print(f"   时间跨度: {stats['duration_minutes']:.1f} 分钟")
        print(f"   平均间隔: {stats['duration_minutes'] / stats['count'] * 60:.1f} 秒")
        
        print(f"\n💰 价差分析 (USD):")
        print(f"   均值: ${stats['spread_mean']:.2f}")
        print(f"   标准差: ${stats['spread_std']:.2f}")
        print(f"   范围: ${stats['spread_min']:.2f} ~ ${stats['spread_max']:.2f}")
        print(f"   分位数:")
        print(f"      25%: ${stats['spread_q25']:.2f}")
        print(f"      50%: ${stats['spread_q50']:.2f}")
        print(f"      75%: ${stats['spread_q75']:.2f}")
        print(f"      95%: ${stats['spread_q95']:.2f}")
        
        print(f"\n📊 价差分析 (bps):")
        print(f"   均值: {stats['spread_bps_mean']:.2f} bps")
        print(f"   标准差: {stats['spread_bps_std']:.2f} bps")
        print(f"   范围: {stats['spread_bps_min']:.2f} ~ {stats['spread_bps_max']:.2f} bps")
        
        print(f"\n💵 净利润分析 (扣除手续费 {self.transaction_fee_bps} bps + Gas ${self.gas_cost_usd}):")
        print(f"   均值: ${stats['net_profit_mean']:.2f} ({stats['net_profit_bps_mean']:.2f} bps)")
        print(f"   标准差: ${stats['net_profit_std']:.2f} ({stats['net_profit_bps_std']:.2f} bps)")
        
        print(f"\n🎯 盈利机会 (净利润 > {self.min_profit_bps} bps):")
        print(f"   数量: {stats['profitable_count']} / {stats['count']}")
        print(f"   占比: {stats['profitable_rate']:.2f}%")
        
        return stats
    
    def calculate_zscore(self) -> pd.DataFrame:
        """计算价差的 Z-Score"""
        if self.df is None:
            self.load_data()
        
        mean = self.df['spread'].mean()
        std = self.df['spread'].std()
        
        self.df['spread_zscore'] = (self.df['spread'] - mean) / std
        
        # 统计异常值
        extreme_high = (self.df['spread_zscore'] > 2).sum()
        extreme_low = (self.df['spread_zscore'] < -2).sum()
        
        logger.info(f"\n📊 Z-Score 分析:")
        logger.info(f"   Z > 2 (价差异常大): {extreme_high} 次")
        logger.info(f"   Z < -2 (价差异常小): {extreme_low} 次")
        
        return self.df
    
    def analyze_spread_persistence(self, threshold_bps: float = 40) -> Dict:
        """分析价差持续时间"""
        if self.df is None:
            self.load_data()
        
        logger.info(f"\n⏱️  价差持续性分析 (阈值: {threshold_bps} bps)")
        
        # 标记高价差时期
        self.df['high_spread'] = self.df['spread_bps'] > threshold_bps
        
        # 计算连续高价差的持续时间
        episodes = []
        in_episode = False
        start_idx = None
        
        for idx, row in self.df.iterrows():
            if row['high_spread'] and not in_episode:
                # 开始一个新周期
                in_episode = True
                start_idx = idx
            elif not row['high_spread'] and in_episode:
                # 结束周期
                end_idx = idx - 1
                duration = (
                    self.df.loc[end_idx, 'datetime'] - 
                    self.df.loc[start_idx, 'datetime']
                ).total_seconds()
                
                episodes.append({
                    'start_time': self.df.loc[start_idx, 'datetime'],
                    'end_time': self.df.loc[end_idx, 'datetime'],
                    'duration_seconds': duration,
                    'max_spread': self.df.loc[start_idx:end_idx, 'spread'].max(),
                    'avg_spread': self.df.loc[start_idx:end_idx, 'spread'].mean(),
                })
                
                in_episode = False
        
        if not episodes:
            logger.info("   未发现超过阈值的价差周期")
            return {}
        
        episodes_df = pd.DataFrame(episodes)
        
        stats = {
            'episode_count': len(episodes_df),
            'avg_duration': episodes_df['duration_seconds'].mean(),
            'max_duration': episodes_df['duration_seconds'].max(),
            'min_duration': episodes_df['duration_seconds'].min(),
            'avg_max_spread': episodes_df['max_spread'].mean(),
        }
        
        print(f"\n   发现 {stats['episode_count']} 个高价差周期")
        print(f"   平均持续: {stats['avg_duration']:.1f} 秒")
        print(f"   最长持续: {stats['max_duration']:.1f} 秒")
        print(f"   最短持续: {stats['min_duration']:.1f} 秒")
        print(f"   周期内平均最大价差: ${stats['avg_max_spread']:.2f}")
        
        self.stats['persistence'] = stats
        return stats
    
    def simulate_trading(
        self,
        entry_threshold_bps: float = 45,
        exit_threshold_bps: float = 30,
        max_position_time_minutes: int = 30
    ) -> Dict:
        """模拟交易回测"""
        if self.df is None:
            self.load_data()
        
        logger.info(f"\n🔄 交易模拟回测")
        logger.info(f"   入场阈值: {entry_threshold_bps} bps")
        logger.info(f"   出场阈值: {exit_threshold_bps} bps")
        logger.info(f"   最大持仓时间: {max_position_time_minutes} 分钟")
        
        trades = []
        in_position = False
        entry_idx = None
        entry_spread = None
        
        for idx, row in self.df.iterrows():
            if not in_position:
                # 检查入场条件
                if row['spread_bps'] > entry_threshold_bps:
                    in_position = True
                    entry_idx = idx
                    entry_spread = row['spread']
                    entry_time = row['datetime']
            else:
                # 检查出场条件
                exit_signal = False
                exit_reason = None
                
                # 条件1: 价差收敛
                if row['spread_bps'] < exit_threshold_bps:
                    exit_signal = True
                    exit_reason = 'spread_converged'
                
                # 条件2: 超时
                time_in_position = (row['datetime'] - entry_time).total_seconds() / 60
                if time_in_position > max_position_time_minutes:
                    exit_signal = True
                    exit_reason = 'timeout'
                
                if exit_signal or idx == len(self.df) - 1:
                    # 平仓
                    exit_spread = row['spread']
                    pnl_usd = entry_spread - exit_spread - (
                        row['ext_mid'] * self.transaction_fee_bps / 10000 * 2 +  # 开仓+平仓
                        self.gas_cost_usd * 2
                    )
                    pnl_bps = (pnl_usd / row['ext_mid']) * 10000
                    
                    trades.append({
                        'entry_time': entry_time,
                        'exit_time': row['datetime'],
                        'duration_minutes': (row['datetime'] - entry_time).total_seconds() / 60,
                        'entry_spread': entry_spread,
                        'exit_spread': exit_spread,
                        'pnl_usd': pnl_usd,
                        'pnl_bps': pnl_bps,
                        'exit_reason': exit_reason,
                    })
                    
                    in_position = False
        
        if not trades:
            logger.info("   ❌ 未触发任何交易")
            return {}
        
        trades_df = pd.DataFrame(trades)
        
        # 统计结果
        stats = {
            'total_trades': len(trades_df),
            'winning_trades': (trades_df['pnl_usd'] > 0).sum(),
            'losing_trades': (trades_df['pnl_usd'] < 0).sum(),
            'win_rate': (trades_df['pnl_usd'] > 0).mean() * 100,
            'total_pnl': trades_df['pnl_usd'].sum(),
            'avg_pnl': trades_df['pnl_usd'].mean(),
            'max_pnl': trades_df['pnl_usd'].max(),
            'min_pnl': trades_df['pnl_usd'].min(),
            'avg_duration': trades_df['duration_minutes'].mean(),
            'sharpe_ratio': trades_df['pnl_usd'].mean() / trades_df['pnl_usd'].std() if trades_df['pnl_usd'].std() > 0 else 0,
        }
        
        print(f"\n   交易次数: {stats['total_trades']}")
        print(f"   盈利交易: {stats['winning_trades']} ({stats['win_rate']:.1f}%)")
        print(f"   亏损交易: {stats['losing_trades']}")
        print(f"\n   累计盈亏: ${stats['total_pnl']:.2f}")
        print(f"   平均盈亏: ${stats['avg_pnl']:.2f}")
        print(f"   最大盈利: ${stats['max_pnl']:.2f}")
        print(f"   最大亏损: ${stats['min_pnl']:.2f}")
        print(f"   平均持仓: {stats['avg_duration']:.1f} 分钟")
        print(f"   夏普比率: {stats['sharpe_ratio']:.3f}")
        
        self.stats['simulation'] = stats
        self.trades_df = trades_df
        
        return stats
    
    def get_recommendations(self) -> Dict:
        """给出策略建议"""
        if not self.stats:
            logger.warning("请先运行分析方法")
            return {}
        
        basic = self.stats.get('basic', {})
        
        logger.info("\n" + "="*80)
        logger.info("💡 策略建议")
        logger.info("="*80)
        
        recommendations = {}
        
        # 1. 入场阈值建议
        spread_mean = basic.get('spread_bps_mean', 0)
        spread_std = basic.get('spread_bps_std', 0)
        
        entry_threshold = spread_mean + 1.5 * spread_std
        exit_threshold = spread_mean + 0.5 * spread_std
        
        recommendations['entry_threshold_bps'] = entry_threshold
        recommendations['exit_threshold_bps'] = exit_threshold
        
        print(f"\n1️⃣ 入场/出场阈值:")
        print(f"   建议入场: {entry_threshold:.1f} bps (均值 + 1.5σ)")
        print(f"   建议出场: {exit_threshold:.1f} bps (均值 + 0.5σ)")
        
        # 2. 盈利能力评估
        profitable_rate = basic.get('profitable_rate', 0)
        net_profit_mean = basic.get('net_profit_bps_mean', 0)
        
        print(f"\n2️⃣ 盈利能力:")
        if profitable_rate > 50:
            print(f"   ✅ 较好 - {profitable_rate:.1f}% 的时间可盈利")
            recommendations['verdict'] = 'good'
        elif profitable_rate > 20:
            print(f"   ⚠️  一般 - {profitable_rate:.1f}% 的时间可盈利")
            recommendations['verdict'] = 'moderate'
        else:
            print(f"   ❌ 较差 - 仅 {profitable_rate:.1f}% 的时间可盈利")
            recommendations['verdict'] = 'poor'
        
        print(f"   平均净利润: {net_profit_mean:.2f} bps")
        
        # 3. 风险提示
        print(f"\n3️⃣ 风险提示:")
        
        if spread_std / spread_mean > 0.3:
            print(f"   ⚠️  价差波动较大 (CV={spread_std/spread_mean:.2%})")
            recommendations['volatility'] = 'high'
        else:
            print(f"   ✅ 价差相对稳定 (CV={spread_std/spread_mean:.2%})")
            recommendations['volatility'] = 'low'
        
        # 4. 建议仓位
        if net_profit_mean > 10:
            position_size = "可以尝试 0.01 BTC"
        elif net_profit_mean > 5:
            position_size = "建议 0.005 BTC 小仓位测试"
        else:
            position_size = "建议使用最小单位 0.001 BTC"
        
        print(f"\n4️⃣ 建议仓位:")
        print(f"   {position_size}")
        recommendations['position_size'] = position_size
        
        return recommendations
    
    def export_summary(self, output_path: Optional[str] = None):
        """导出分析摘要"""
        if output_path is None:
            output_path = self.csv_path.parent / f"analysis_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("价差统计分析报告\n")
            f.write("="*80 + "\n\n")
            
            f.write(f"数据文件: {self.csv_path}\n")
            f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # 写入统计数据
            for section, data in self.stats.items():
                f.write(f"\n{section.upper()}:\n")
                f.write("-" * 40 + "\n")
                for key, value in data.items():
                    f.write(f"  {key}: {value}\n")
        
        logger.info(f"\n📄 分析报告已保存: {output_path}")


def main():
    """主函数 - 运行完整分析"""
    
    # 配置参数
    csv_path = "data/arbitrage/prices_BTC_2025-11-07.csv"
    
    analyzer = SpreadAnalyzer(
        csv_path=csv_path,
        transaction_fee_bps=4.0,  # 0.04% 双边手续费
        gas_cost_usd=10.0,        # Gas 成本
        min_profit_bps=5.0        # 最小5bps利润要求
    )
    
    # 1. 加载数据
    analyzer.load_data()
    
    # 2. 基本统计
    analyzer.calculate_basic_stats()
    
    # 3. Z-Score 分析
    analyzer.calculate_zscore()
    
    # 4. 持续性分析
    analyzer.analyze_spread_persistence(threshold_bps=40)
    
    # 5. 交易模拟
    analyzer.simulate_trading(
        entry_threshold_bps=45,
        exit_threshold_bps=30,
        max_position_time_minutes=30
    )
    
    # 6. 策略建议
    analyzer.get_recommendations()
    
    # 7. 导出报告
    analyzer.export_summary()
    
    print("\n" + "="*80)
    print("✅ 分析完成！")
    print("="*80)


if __name__ == '__main__':
    main()