"""持仓数据模型"""

import time
from decimal import Decimal
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class Position:
    """套利持仓"""
    
    # ========== 基本信息 ==========
    symbol: str
    quantity: Decimal
    exchange_a_name: str
    exchange_b_name: str
    
    # ========== 开仓信息 ==========
    # ✅ 信号触发价格（理想价格）
    exchange_a_signal_entry_price: Decimal
    exchange_b_signal_entry_price: Decimal
    
    # ✅ 实际成交价格
    exchange_a_entry_price: Decimal
    exchange_b_entry_price: Decimal
    
    # ✅ 订单 ID
    exchange_a_order_id: str
    exchange_b_order_id: str
    
    # ✅ 开仓价差
    spread_pct: Decimal
    
    # ========== 平仓信息 ==========
    # ✅ 信号触发价格（理想价格）
    exchange_a_signal_exit_price: Optional[Decimal] = None
    exchange_b_signal_exit_price: Optional[Decimal] = None
    
    # ✅ 实际成交价格
    exchange_a_exit_price: Optional[Decimal] = None
    exchange_b_exit_price: Optional[Decimal] = None
    
    # ✅ 平仓订单 ID
    exchange_a_exit_order_id: Optional[str] = None
    exchange_b_exit_order_id: Optional[str] = None
    
    # ========== 时间信息 ==========
    entry_time: datetime = None
    exit_time: Optional[datetime] = None
    
    # ✅ 信号触发时间
    signal_entry_time: Optional[float] = None  # Unix timestamp
    signal_exit_time: Optional[float] = None
    
    # ========== 执行延迟 ==========
    # ✅ 开仓延迟（信号触发 → 订单成交）
    entry_execution_delay_ms: Optional[float] = None
    
    # ✅ 平仓延迟（信号触发 → 订单成交）
    exit_execution_delay_ms: Optional[float] = None

    place_duration_a_ms: Optional[float] = None
    place_duration_b_ms: Optional[float] = None
    execution_duration_a_ms: Optional[float] = None
    execution_duration_b_ms: Optional[float] = None
    attempt_a: Optional[int] = None
    attempt_b: Optional[int] = None
    
    def __post_init__(self):
        """初始化时间"""
        if self.entry_time is None:
            self.entry_time = datetime.now()
    
    # ========== 计算方法 ==========
    
    def calculate_pnl_pct(
        self,
        exchange_a_price: Optional[Decimal] = None,
        exchange_b_price: Optional[Decimal] = None
    ) -> Decimal:
        """
        计算盈亏百分比
        
        使用实际成交价计算
        """
        # ✅ 检查必要字段是否存在
        if not self.exchange_a_entry_price or not self.exchange_b_entry_price:
            # self.logger.warning(
            #     f"⚠️ 无法计算盈亏：缺少开仓价格\n"
            #     f"   Exchange A 开仓价: {self.exchange_a_entry_price}\n"
            #     f"   Exchange B 开仓价: {self.exchange_b_entry_price}"
            # )
            return Decimal('0')
        
        # ✅ 检查开仓价格是否为零
        if self.exchange_a_entry_price == Decimal('0') or self.exchange_b_entry_price == Decimal('0'):
            # self.logger.warning(
            #     f"⚠️ 无法计算盈亏：开仓价格为零\n"
            #     f"   Exchange A 开仓价: {self.exchange_a_entry_price}\n"
            #     f"   Exchange B 开仓价: {self.exchange_b_entry_price}"
            # )
            return Decimal('0')
        
        # 使用提供的价格或平仓价格
        current_price_a = exchange_a_price if exchange_a_price is not None else self.exchange_a_exit_price
        current_price_b = exchange_b_price if exchange_b_price is not None else self.exchange_b_exit_price
        
        # ✅ 检查当前价格是否存在
        if not current_price_a or not current_price_b:
            # self.logger.warning(
            #     f"⚠️ 无法计算盈亏：缺少当前价格\n"
            #     f"   Exchange A 当前价: {current_price_a}\n"
            #     f"   Exchange B 当前价: {current_price_b}"
            # )
            return Decimal('0')
        
        # ✅ 检查当前价格是否为零
        if current_price_a == Decimal('0') or current_price_b == Decimal('0'):
            # self.logger.warning(
            #     f"⚠️ 无法计算盈亏：当前价格为零\n"
            #     f"   Exchange A 当前价: {current_price_a}\n"
            #     f"   Exchange B 当前价: {current_price_b}"
            # )
            return Decimal('0')

        # ✅ 3. 验证当前价格
        if current_price_a is None or current_price_a <= Decimal('0'):
            # logger.warning(
            #     f"⚠️ Exchange A 当前价格无效: {current_price_a}\n"
            #     f"   传入价格: {exchange_a_price}\n"
            #     f"   平仓价格: {self.exchange_a_exit_price}"
            # )
            return Decimal('0')
        
        if current_price_b is None or current_price_b <= Decimal('0'):
            # logger.warning(
            #     f"⚠️ Exchange B 当前价格无效: {current_price_b}\n"
            #     f"   传入价格: {exchange_b_price}\n"
            #     f"   平仓价格: {self.exchange_b_exit_price}"
            # )
            return Decimal('0')
        
        # A 所开空，平仓时买入
        pnl_a = self.exchange_a_entry_price - current_price_a
        pnl_a_pct = (pnl_a / self.exchange_a_entry_price) * 100
        
        # B 所开多，平仓时卖出
        pnl_b = current_price_b - self.exchange_b_entry_price
        pnl_b_pct = (pnl_b / self.exchange_b_entry_price) * 100
        
        # 总盈亏
        total_pnl_pct = pnl_a_pct + pnl_b_pct
        
        return total_pnl_pct
    
    def calculate_slippage(self) -> dict:
        """
        计算滑点（信号价 vs 实际成交价）
        
        Returns:
            {
                'entry_a_slippage_pct': Decimal,  # A 所开仓滑点 %
                'entry_b_slippage_pct': Decimal,  # B 所开仓滑点 %
                'exit_a_slippage_pct': Decimal,   # A 所平仓滑点 %
                'exit_b_slippage_pct': Decimal,   # B 所平仓滑点 %
                'total_entry_slippage_pct': Decimal,  # 开仓总滑点 %
                'total_exit_slippage_pct': Decimal    # 平仓总滑点 %
            }
        """
        # ✅ 开仓滑点
        entry_a_slippage = -((self.exchange_a_entry_price - self.exchange_a_signal_entry_price) 
                           / self.exchange_a_signal_entry_price * 100)
        
        entry_b_slippage = ((self.exchange_b_entry_price - self.exchange_b_signal_entry_price) 
                           / self.exchange_b_signal_entry_price * 100)
        
        # ✅ 平仓滑点
        exit_a_slippage = Decimal('0')
        exit_b_slippage = Decimal('0')
        
        if self.exchange_a_exit_price and self.exchange_a_signal_exit_price:
            exit_a_slippage = ((self.exchange_a_exit_price - self.exchange_a_signal_exit_price) 
                              / self.exchange_a_signal_exit_price * 100)
        
        if self.exchange_b_exit_price and self.exchange_b_signal_exit_price:
            exit_b_slippage = -((self.exchange_b_exit_price - self.exchange_b_signal_exit_price) 
                              / self.exchange_b_signal_exit_price * 100)
        
        # ✅ 总滑点（开仓）
        # A 所开空：滑点越小越好（负数更好）
        # B 所开多：滑点越小越好（负数更好）
        total_entry_slippage = entry_a_slippage + entry_b_slippage
        
        # ✅ 总滑点（平仓）
        # A 所平空（买入）：滑点越小越好（负数更好）
        # B 所平多（卖出）：滑点越大越好（正数更好）
        total_exit_slippage = exit_a_slippage + exit_b_slippage
        
        return {
            'entry_a_slippage_pct': entry_a_slippage,
            'entry_b_slippage_pct': entry_b_slippage,
            'exit_a_slippage_pct': exit_a_slippage,
            'exit_b_slippage_pct': exit_b_slippage,
            'total_entry_slippage_pct': total_entry_slippage,
            'total_exit_slippage_pct': total_exit_slippage
        }
    
    def calculate_theoretical_pnl_pct(self) -> Decimal:
        """
        计算理论盈亏（基于信号触发价格）
        
        用于对比实际收益 vs 理论收益
        """
        if self.exchange_a_signal_exit_price is None or self.exchange_b_signal_exit_price is None:
            return Decimal('0')
        
        # A 所理论盈亏
        theoretical_pnl_a = self.exchange_a_signal_entry_price - self.exchange_a_signal_exit_price
        theoretical_pnl_a_pct = (theoretical_pnl_a / self.exchange_a_signal_entry_price) * 100
        
        # B 所理论盈亏
        theoretical_pnl_b = self.exchange_b_signal_exit_price - self.exchange_b_signal_entry_price
        theoretical_pnl_b_pct = (theoretical_pnl_b / self.exchange_b_signal_entry_price) * 100
        
        # 总理论盈亏
        total_theoretical_pnl_pct = theoretical_pnl_a_pct + theoretical_pnl_b_pct
        
        return total_theoretical_pnl_pct
    
    def get_execution_quality_report(self) -> dict:
        """
        生成执行质量报告
        
        Returns:
            {
                'theoretical_pnl_pct': Decimal,     # 理论盈亏 %
                'actual_pnl_pct': Decimal,          # 实际盈亏 %
                'pnl_loss_pct': Decimal,            # 盈亏损失 %（由于滑点）
                'entry_slippage': dict,             # 开仓滑点
                'exit_slippage': dict,              # 平仓滑点
                'entry_delay_ms': float,            # 开仓延迟（ms）
                'exit_delay_ms': float              # 平仓延迟（ms）
            }
        """
        slippage = self.calculate_slippage()
        theoretical_pnl = self.calculate_theoretical_pnl_pct()
        actual_pnl = self.calculate_pnl_pct(
            exchange_a_price=self.exchange_a_exit_price,
            exchange_b_price=self.exchange_b_exit_price
        )
        
        # ✅ 盈亏损失（由于滑点 + 延迟）
        pnl_loss = theoretical_pnl - actual_pnl
        
        return {
            'theoretical_pnl_pct': theoretical_pnl,
            'actual_pnl_pct': actual_pnl,
            'pnl_loss_pct': pnl_loss,
            'entry_slippage': {
                'exchange_a': slippage['entry_a_slippage_pct'],
                'exchange_b': slippage['entry_b_slippage_pct'],
                'total': slippage['total_entry_slippage_pct']
            },
            'exit_slippage': {
                'exchange_a': slippage['exit_a_slippage_pct'],
                'exchange_b': slippage['exit_b_slippage_pct'],
                'total': slippage['total_exit_slippage_pct']
            },
            'entry_delay_ms': self.entry_execution_delay_ms,
            'exit_delay_ms': self.exit_execution_delay_ms
        }
    
    def get_holding_duration(self) -> str:
        """获取持仓时长"""
        if self.exit_time:
            duration = self.exit_time - self.entry_time
        else:
            duration = datetime.now() - self.entry_time
        
        seconds = int(duration.total_seconds())
        minutes = seconds // 60
        hours = minutes // 60
        
        if hours > 0:
            return f"{hours}h {minutes % 60}m"
        elif minutes > 0:
            return f"{minutes}m {seconds % 60}s"
        else:
            return f"{seconds}s"