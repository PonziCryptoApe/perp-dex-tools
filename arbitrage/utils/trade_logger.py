"""交易日志记录器"""
import csv
import logging
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from decimal import Decimal
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
                    'signal_price',        # ✅ 新增：信号触发价格
                    'filled_price',        # ✅ 新增：实际成交价格
                    'slippage_pct',        # ✅ 新增：滑点百分比
                    'quantity',            # 数量
                    'order_id',            # 订单 ID
                    'position_type',       # 仓位类型（open/close）
                    'spread_pct',          # 价差百分比
                    'pnl_pct',             # 盈亏百分比
                    'notes',               # 备注
                    'signal_delay_ms',      # ✅ 可选：信号延迟时间
                    'place_duration_ms',   # ✅ 可选：下单耗时
                    'execution_duration_ms', # ✅ 可选：执行耗时
                    'attempt'              # ✅ 可选：尝试次数
                ])
            self.logger.info(f"✅ 创建新的交易记录文件")
    
    def log_trade(
        self,
        exchange: str,
        side: str,
        signal_price: Decimal,      # ✅ 新增：信号触发价格
        filled_price: Decimal,      # ✅ 新增：实际成交价格
        quantity: Decimal,
        order_id: str = '',
        position_type: str = '',
        spread_pct: Decimal = Decimal('0'),
        pnl_pct: Decimal = Decimal('0'),
        notes: str = '',
        signal_delay_ms: Optional[float] = None,
        place_duration_ms: Optional[float] = None,
        execution_duration_ms: Optional[float] = None,
        attempt: Optional[int] = None
    ):
        """
        记录单笔交易到 CSV
        
        Args:
            exchange: 交易所名称
            side: 买卖方向（buy/sell）
            signal_price: 信号触发价格
            filled_price: 实际成交价格
            quantity: 成交数量
            order_id: 订单 ID
            position_type: 仓位类型（open 开仓 / close 平仓）
            spread_pct: 价差百分比
            pnl_pct: 盈亏百分比
            notes: 备注信息
        """
        # 获取当前北京时间 (UTC+8, Asia/Shanghai 时区)
        beijing_tz = ZoneInfo("Asia/Shanghai")
        current_beijing_time = datetime.now(beijing_tz)
        timestamp = current_beijing_time.strftime("%Y-%m-%d %H:%M:%S %Z")
        # ✅ 计算滑点
        slippage_pct = Decimal('0')
        if signal_price and signal_price != Decimal('0'):
            if side.lower() == 'buy':
                slippage_pct = ((filled_price - signal_price) / signal_price) * 100
            elif side.lower() == 'sell':
                slippage_pct = ((signal_price - filled_price) / signal_price) * 100

        try:
            with open(self.csv_filename, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    timestamp,
                    self.pair,
                    exchange,
                    side,
                    float(signal_price),      # ✅ 信号价格
                    float(filled_price),      # ✅ 实际成交价
                    f"{slippage_pct:.6f}",    # ✅ 滑点百分比
                    float(quantity),
                    order_id,
                    position_type,
                    f"{spread_pct:.6f}",
                    f"{pnl_pct:.6f}",
                    notes,
                    signal_delay_ms, #✅ 可选：信号延迟时间
                    place_duration_ms,   # ✅ 可选：下单耗时
                    execution_duration_ms, # ✅ 可选：执行耗时
                    attempt              # ✅ 可选：尝试次数
                ])
            
            self.logger.debug(
                f"📝 记录交易: {exchange} {side} {quantity} @ ${filled_price} "
                f"(信号价: ${signal_price}, 滑点: {slippage_pct:+.4f}%, {notes})"
            )
        
        except Exception as e:
            self.logger.error(f"❌ 写入交易记录失败: {e}")
    
    def log_open_position(
        self,
        exchange_a_name: str,
        exchange_a_side: str,
        exchange_a_signal_price: Decimal,   # ✅ 新增：A 所信号价格
        exchange_a_filled_price: Decimal,   # ✅ 新增：A 所实际成交价
        exchange_a_order_id: str,
        exchange_b_name: str,
        exchange_b_side: str,
        exchange_b_signal_price: Decimal,   # ✅ 新增：B 所信号价格
        exchange_b_filled_price: Decimal,   # ✅ 新增：B 所实际成交价
        exchange_b_order_id: str,
        quantity: Decimal,
        spread_pct: Decimal,
        signal_delay_ms_a: float,
        signal_delay_ms_b: float,
        place_duration_a_ms: float,
        execution_duration_a_ms: float,
        attempt_a: int,
        place_duration_b_ms: float,
        execution_duration_b_ms: float,
        attempt_b: int,
    ):
        """
        记录开仓（两条记录）
        
        Args:
            exchange_a_name: 交易所 A 名称
            exchange_a_side: 交易所 A 方向
            exchange_a_signal_price: 交易所 A 信号价格
            exchange_a_filled_price: 交易所 A 实际成交价
            exchange_a_order_id: 交易所 A 订单 ID
            exchange_b_name: 交易所 B 名称
            exchange_b_side: 交易所 B 方向
            exchange_b_signal_price: 交易所 B 信号价格
            exchange_b_filled_price: 交易所 B 实际成交价
            exchange_b_order_id: 交易所 B 订单 ID
            quantity: 数量
            spread_pct: 价差百分比
        """
        # ✅ 记录交易所 A
        self.log_trade(
            exchange=exchange_a_name,
            side=exchange_a_side,
            signal_price=exchange_a_signal_price,
            filled_price=exchange_a_filled_price,
            quantity=quantity,
            order_id=exchange_a_order_id,
            position_type='open',
            spread_pct=spread_pct,
            pnl_pct=Decimal('0'),
            notes=f'开仓-{exchange_a_name}-{exchange_a_side}',
            signal_delay_ms=signal_delay_ms_a,
            place_duration_ms=place_duration_a_ms,
            execution_duration_ms=execution_duration_a_ms,
            attempt=attempt_a,
        )
        
        # ✅ 记录交易所 B
        self.log_trade(
            exchange=exchange_b_name,
            side=exchange_b_side,
            signal_price=exchange_b_signal_price,
            filled_price=exchange_b_filled_price,
            quantity=quantity,
            order_id=exchange_b_order_id,
            position_type='open',
            spread_pct=spread_pct,
            pnl_pct=Decimal('0'),
            notes=f'开仓-{exchange_b_name}-{exchange_b_side}',
            signal_delay_ms=signal_delay_ms_b,
            place_duration_ms=place_duration_b_ms,
            execution_duration_ms=execution_duration_b_ms,
            attempt=attempt_b,
        )
        
        # ✅ 计算总滑点
        slippage_a = -((exchange_a_filled_price - exchange_a_signal_price) / exchange_a_signal_price * 100)
        slippage_b = ((exchange_b_filled_price - exchange_b_signal_price) / exchange_b_signal_price * 100)
        total_slippage = slippage_a + slippage_b
        
        self.logger.info(
            f"✅ 开仓记录完成:\n"
            f"   {exchange_a_name}({exchange_a_side}): 信号价 ${exchange_a_signal_price} → 成交价 ${exchange_a_filled_price} (滑点: {slippage_a:+.4f}%)\n"
            f"   {exchange_b_name}({exchange_b_side}): 信号价 ${exchange_b_signal_price} → 成交价 ${exchange_b_filled_price} (滑点: {slippage_b:+.4f}%)\n"
            f"   价差: {spread_pct:.4f}%, 总滑点: {total_slippage:+.4f}%"
        )
    
    def log_close_position(
        self,
        exchange_a_name: str,
        exchange_a_side: str,
        exchange_a_signal_price: Decimal,   # ✅ 新增：A 所信号价格
        exchange_a_filled_price: Decimal,   # ✅ 新增：A 所实际成交价
        exchange_a_order_id: str,           # ✅ 新增：A 所订单 ID
        exchange_b_name: str,
        exchange_b_side: str,
        exchange_b_signal_price: Decimal,   # ✅ 新增：B 所信号价格
        exchange_b_filled_price: Decimal,   # ✅ 新增：B 所实际成交价
        exchange_b_order_id: str,           # ✅ 新增：B 所订单 ID
        quantity: Decimal,
        spread_pct: Decimal,
        pnl_pct: Decimal,
        signal_delay_ms_a: float = 0,
        signal_delay_ms_b: float = 0,
        place_duration_a_ms: float = 0,
        place_duration_b_ms: float = 0,
        execution_duration_a_ms: float = 0,
        execution_duration_b_ms: float = 0,
        attempt_a: int = 0,
        attempt_b: int = 0
    ):
        """
        记录反向开仓（两条记录）
        
        Args:
            exchange_a_name: 交易所 A 名称
            exchange_a_side: 交易所 A 方向
            exchange_a_signal_price: 交易所 A 信号价格
            exchange_a_filled_price: 交易所 A 实际成交价
            exchange_a_order_id: 交易所 A 订单 ID
            exchange_b_name: 交易所 B 名称
            exchange_b_side: 交易所 B 方向
            exchange_b_signal_price: 交易所 B 信号价格
            exchange_b_filled_price: 交易所 B 实际成交价
            exchange_b_order_id: 交易所 B 订单 ID
            quantity: 数量
            spread_pct: 价差百分比
            pnl_pct: 盈亏百分比
        """
        # ✅ 记录交易所 A
        self.log_trade(
            exchange=exchange_a_name,
            side=exchange_a_side,
            signal_price=exchange_a_signal_price,
            filled_price=exchange_a_filled_price,
            quantity=quantity,
            order_id=exchange_a_order_id,
            position_type='close',
            spread_pct=spread_pct,
            pnl_pct=pnl_pct,
            notes=f'反向开仓-{exchange_a_name}-{exchange_a_side}',
            signal_delay_ms=signal_delay_ms_a,
            place_duration_ms=place_duration_a_ms,
            execution_duration_ms=execution_duration_a_ms,
            attempt=attempt_a
        )
        
        # ✅ 记录交易所 B
        self.log_trade(
            exchange=exchange_b_name,
            side=exchange_b_side,
            signal_price=exchange_b_signal_price,
            filled_price=exchange_b_filled_price,
            quantity=quantity,
            order_id=exchange_b_order_id,
            position_type='close',
            spread_pct=spread_pct,
            pnl_pct=pnl_pct,
            notes=f'反向开仓-{exchange_b_name}-{exchange_b_side}',
            signal_delay_ms=signal_delay_ms_b,
            place_duration_ms=place_duration_b_ms,
            execution_duration_ms=execution_duration_b_ms,
            attempt=attempt_b
        )
        
        # ✅ 计算总滑点
        slippage_a = ((exchange_a_filled_price - exchange_a_signal_price) / exchange_a_signal_price * 100)
        slippage_b = -((exchange_b_filled_price - exchange_b_signal_price) / exchange_b_signal_price * 100)
        total_slippage = slippage_a + slippage_b
        
        self.logger.info(
            f"✅ 反向开仓记录完成:\n"
            f"   {exchange_a_name}({exchange_a_side}): 信号价 ${exchange_a_signal_price} → 成交价 ${exchange_a_filled_price} (滑点: {slippage_a:+.4f}%)\n"
            f"   {exchange_b_name}({exchange_b_side}): 信号价 ${exchange_b_signal_price} → 成交价 ${exchange_b_filled_price} (滑点: {slippage_b:+.4f}%)\n"
            f"   盈亏: {pnl_pct:.4f}%, 价差: {spread_pct:.4f}%, 总滑点: {total_slippage:+.4f}%"
        )