"""交易日志记录器"""
import csv
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional


class TradeLogger:
    """交易日志记录器 - 记录交易到 CSV 文件"""
    
    def __init__(self, pair: str, log_dir: Path):
        """
        初始化交易日志记录器
        
        Args:
            pair: 交易对（如 ETH, BTC）
            log_dir: 日志目录路径
        """
        self.pair = pair
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # CSV 文件路径
        self.csv_filename = self.log_dir / f"arbitrage_{pair}_trades.csv"
        
        # 日志器
        self.logger = logging.getLogger(f"{__name__}.{pair}")
        
        # 初始化 CSV 文件
        self._initialize_csv()
        
        self.logger.info(f"📊 交易记录文件: {self.csv_filename}")
    
    def _initialize_csv(self):
        """初始化 CSV 文件（如果不存在，创建并写入表头）"""
        if not self.csv_filename.exists():
            with open(self.csv_filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'timestamp',           # 时间戳
                    'pair',                # 交易对
                    'exchange',            # 交易所
                    'side',                # 买卖方向
                    'price',               # 价格
                    'quantity',            # 数量
                    'order_id',            # 订单 ID
                    'position_type',       # 仓位类型（open/close）
                    'spread_pct',          # 价差百分比
                    'pnl_pct',             # 盈亏百分比
                    'notes'                # 备注
                ])
            self.logger.info(f"✅ 创建新的交易记录文件")
    
    def log_trade(
        self,
        exchange: str,
        side: str,
        price: str,
        quantity: str,
        order_id: str = '',
        position_type: str = '',
        spread_pct: str = '',
        pnl_pct: str = '',
        notes: str = ''
    ):
        """
        记录单笔交易到 CSV
        
        Args:
            exchange: 交易所名称
            side: 买卖方向（buy/sell）
            price: 成交价格
            quantity: 成交数量
            order_id: 订单 ID
            position_type: 仓位类型（open 开仓 / close 平仓）
            spread_pct: 价差百分比
            pnl_pct: 盈亏百分比
            notes: 备注信息
        """
        timestamp = datetime.utcnow().isoformat() + 'Z'
        
        try:
            with open(self.csv_filename, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    timestamp,
                    self.pair,
                    exchange,
                    side,
                    price,
                    quantity,
                    order_id,
                    position_type,
                    spread_pct,
                    pnl_pct,
                    notes
                ])
            
            self.logger.debug(
                f"📝 记录交易: {exchange} {side} {quantity} @ {price} ({notes})"
            )
        
        except Exception as e:
            self.logger.error(f"❌ 写入交易记录失败: {e}")
    
    def log_open_position(
        self,
        exchange_a_name: str,
        exchange_a_side: str,
        exchange_a_price: str,
        exchange_a_order_id: str,
        exchange_b_name: str,
        exchange_b_side: str,
        exchange_b_price: str,
        exchange_b_order_id: str,
        quantity: str,
        spread_pct: str
    ):
        """
        记录开仓（两条记录）
        
        Args:
            exchange_a_name: 交易所 A 名称
            exchange_a_side: 交易所 A 方向
            exchange_a_price: 交易所 A 价格
            exchange_a_order_id: 交易所 A 订单 ID
            exchange_b_name: 交易所 B 名称
            exchange_b_side: 交易所 B 方向
            exchange_b_price: 交易所 B 价格
            exchange_b_order_id: 交易所 B 订单 ID
            quantity: 数量
            spread_pct: 价差百分比
        """
        # 记录交易所 A
        self.log_trade(
            exchange=exchange_a_name,
            side=exchange_a_side,
            price=exchange_a_price,
            quantity=quantity,
            order_id=exchange_a_order_id,
            position_type='open',
            spread_pct=spread_pct,
            pnl_pct='0.0',
            notes=f'开仓-{exchange_a_name}-{exchange_a_side}'
        )
        
        # 记录交易所 B
        self.log_trade(
            exchange=exchange_b_name,
            side=exchange_b_side,
            price=exchange_b_price,
            quantity=quantity,
            order_id=exchange_b_order_id,
            position_type='open',
            spread_pct=spread_pct,
            pnl_pct='0.0',
            notes=f'开仓-{exchange_b_name}-{exchange_b_side}'
        )
        
        self.logger.info(
            f"✅ 开仓记录完成: {exchange_a_name}({exchange_a_side}) + "
            f"{exchange_b_name}({exchange_b_side}), 价差: {spread_pct}%"
        )
    
    def log_close_position(
        self,
        exchange_a_name: str,
        exchange_a_side: str,
        exchange_a_price: str,
        exchange_b_name: str,
        exchange_b_side: str,
        exchange_b_price: str,
        quantity: str,
        spread_pct: str,
        pnl_pct: str
    ):
        """
        记录平仓（两条记录）
        
        Args:
            exchange_a_name: 交易所 A 名称
            exchange_a_side: 交易所 A 方向
            exchange_a_price: 交易所 A 价格
            exchange_b_name: 交易所 B 名称
            exchange_b_side: 交易所 B 方向
            exchange_b_price: 交易所 B 价格
            quantity: 数量
            spread_pct: 价差百分比
            pnl_pct: 盈亏百分比
        """
        # 记录交易所 A
        self.log_trade(
            exchange=exchange_a_name,
            side=exchange_a_side,
            price=exchange_a_price,
            quantity=quantity,
            order_id='',
            position_type='close',
            spread_pct=spread_pct,
            pnl_pct=pnl_pct,
            notes=f'平仓-{exchange_a_name}-{exchange_a_side}'
        )
        
        # 记录交易所 B
        self.log_trade(
            exchange=exchange_b_name,
            side=exchange_b_side,
            price=exchange_b_price,
            quantity=quantity,
            order_id='',
            position_type='close',
            spread_pct=spread_pct,
            pnl_pct=pnl_pct,
            notes=f'平仓-{exchange_b_name}-{exchange_b_side}'
        )
        
        self.logger.info(
            f"✅ 平仓记录完成: 盈亏 {pnl_pct}%, 价差: {spread_pct}%"
        )