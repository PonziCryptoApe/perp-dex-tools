"""订单执行服务"""

import logging
from decimal import Decimal
from typing import Tuple, Optional
from ..models.position import Position
from ..exchanges.base import ExchangeAdapter

logger = logging.getLogger(__name__)

class OrderExecutor:
    """订单执行服务"""
    
    def __init__(
        self,
        exchange_a: ExchangeAdapter,
        exchange_b: ExchangeAdapter,
        quantity: Decimal
    ):
        """
        初始化订单执行器
        
        Args:
            exchange_a: 交易所 A（开空）
            exchange_b: 交易所 B（开多）
            quantity: 交易数量
        """
        self.exchange_a = exchange_a
        self.exchange_b = exchange_b
        self.quantity = quantity
        
        logger.info(
            f"📦 订单执行器已初始化:\n"
            f"   Exchange A: {exchange_a.exchange_name}\n"
            f"   Exchange B: {exchange_b.exchange_name}\n"
            f"   Quantity: {quantity}"
        )
    
    async def execute_open(
        self,
        exchange_a_price: Decimal,
        exchange_b_price: Decimal,
        spread_pct: Decimal,
        exchange_a_quote_id: Optional[str] = None,
        exchange_b_quote_id: Optional[str] = None
    ) -> Tuple[bool, Optional[Position]]:
        """
        执行开仓
        
        Args:
            exchange_a_price: Exchange A 价格（开空价格）
            exchange_b_price: Exchange B 价格（开多价格）
            spread_pct: 价差百分比
        
        Returns:
            (success: bool, position: Optional[Position])
        """
        logger.info(
            f"📤 执行开仓:\n"
            f"   {self.exchange_a.exchange_name} 开空 @ ${exchange_a_price}\n"
            f"   {self.exchange_b.exchange_name} 开多 @ ${exchange_b_price}"
        )
        
        try:
            # ✅ Exchange A 开空（卖出）
            order_a_result = await self.exchange_a.place_open_order(
                side='sell',
                quantity=self.quantity,
                price=exchange_a_price,
                retry_mode='opportunistic',
                quote_id=exchange_a_quote_id
            )
            
            if not order_a_result.get('success'):
                logger.error(
                    f"❌ {self.exchange_a.exchange_name} 开空失败: "
                    f"{order_a_result.get('error')}"
                )
                return False, None
            
            # ✅ Exchange B 开多（买入）
            order_b_result = await self.exchange_b.place_open_order(
                side='buy',
                quantity=self.quantity,
                price=exchange_b_price,
                retry_mode='aggressive',
                quote_id=exchange_b_quote_id
            )
            
            if not order_b_result.get('success'):
                logger.error(
                    f"❌ {self.exchange_b.exchange_name} 开多失败: "
                    f"{order_b_result.get('error')}"
                )
                
                # TODO: 回滚 Exchange A 的订单

                logger.warning(
                    f"⚠️ {self.exchange_a.exchange_name} 订单已成功，"
                    f"但 {self.exchange_b.exchange_name} 订单失败，需要手动处理！"
                )
                # ✅ 记录异常情况
                logger.critical(
                    f"\n"
                    f"{'='*60}\n"
                    f"🚨 严重错误：开仓失败（仓位不平衡）\n"
                    f"{'='*60}\n"
                    f"❌ {self.exchange_a.exchange_name} 订单已成交: {order_a_result.get('order_id')}\n"
                    f"❌ {self.exchange_b.exchange_name} 订单失败: {order_b_result.get('error')}\n"
                    f"\n"
                    f"⚠️ 需要手动处理以下仓位:\n"
                    f"   交易所: {self.exchange_a.exchange_name}\n"
                    f"   方向: 空头\n"
                    f"   数量: {self.quantity}\n"
                    f"   价格: ${exchange_a_price}\n"
                    f"   订单ID: {order_a_result.get('order_id')}\n"
                    f"\n"
                    f"{'='*60}\n"
                    f"程序将立即退出...\n"
                    f"{'='*60}\n"
                )
                
                # ✅ 抛出致命异常，触发程序退出
                raise RuntimeError(
                    f"开仓失败：{self.exchange_a.exchange_name} 已成交但 "
                    f"{self.exchange_b.exchange_name} 失败，需要手动处理仓位！"
                )
                # return False, None
            
            # ✅ 创建持仓记录
            position = Position(
                symbol=self.exchange_a.symbol,
                quantity=self.quantity,
                exchange_a_name=self.exchange_a.exchange_name,
                exchange_b_name=self.exchange_b.exchange_name,
                exchange_a_entry_price=exchange_a_price,
                exchange_b_entry_price=exchange_b_price,
                exchange_a_order_id=order_a_result.get('order_id', 'unknown'),
                exchange_b_order_id=order_b_result.get('order_id', 'unknown'),
                spread_pct=spread_pct
            )
            
            logger.info(
                f"✅ 开仓成功:\n"
                f"   {self.exchange_a.exchange_name} 订单: {position.exchange_a_order_id}\n"
                f"   {self.exchange_b.exchange_name} 订单: {position.exchange_b_order_id}\n"
                f"   价差: {spread_pct:.4f}%"
            )
            
            return True, position
        
        except Exception as e:
            logger.error(f"❌ 开仓执行异常: {e}")
            import traceback
            traceback.print_exc()
            return False, None
    
    async def execute_close(
        self,
        position: Position,
        exchange_a_price: Decimal,
        exchange_b_price: Decimal,
        exchange_a_quote_id: Optional[str] = None,
        exchange_b_quote_id: Optional[str] = None
    ) -> bool:
        """
        执行平仓
        
        Args:
            position: 持仓信息
            exchange_a_price: Exchange A 平仓价格（买入价格）
            exchange_b_price: Exchange B 平仓价格（卖出价格）
        
        Returns:
            success: bool
        """
        logger.info(
            f"📤 执行平仓:\n"
            f"   {self.exchange_a.exchange_name} 平空 @ ${exchange_a_price}\n"
            f"   {self.exchange_b.exchange_name} 平多 @ ${exchange_b_price}"
        )
        
        try:
            # ✅ Exchange A 平空（买入）
            order_a_result = await self.exchange_a.place_close_order(
                side='buy',
                quantity=self.quantity,
                price=exchange_a_price,
                retry_mode='opportunistic',
                quote_id=exchange_a_quote_id
            )
            
            if not order_a_result.get('success'):
                logger.error(
                    f"❌ {self.exchange_a.exchange_name} 平空失败: "
                    f"{order_a_result.get('error')}"
                )
                return False
            
            # ✅ Exchange B 平多（卖出）
            order_b_result = await self.exchange_b.place_close_order(
                side='sell',
                quantity=self.quantity,
                price=exchange_b_price,
                retry_mode='aggressive',
                quote_id=exchange_b_quote_id
            )
            
            if not order_b_result.get('success'):
                logger.error(
                    f"❌ {self.exchange_b.exchange_name} 平多失败: "
                    f"{order_b_result.get('error')}"
                )
                
                # TODO: 回滚 Exchange A 的订单
                logger.warning(
                    f"⚠️ {self.exchange_a.exchange_name} 订单已成功，"
                    f"但 {self.exchange_b.exchange_name} 订单失败，需要手动处理！"
                )
                logger.critical(
                    f"\n"
                    f"{'='*60}\n"
                    f"🚨 严重错误：平仓失败（仓位不平衡）\n"
                    f"{'='*60}\n"
                    f"✅ {self.exchange_a.exchange_name} 订单已成交: {order_a_result.get('order_id')}\n"
                    f"❌ {self.exchange_b.exchange_name} 订单失败: {order_b_result.get('error')}\n"
                    f"\n"
                    f"⚠️ 需要手动处理以下仓位:\n"
                    f"   原持仓: {self.exchange_b.exchange_name} 多头 {self.quantity}\n"
                    f"   现持仓: {self.exchange_b.exchange_name} 多头 {self.quantity}\n"
                    f"   {self.exchange_a.exchange_name} 已平仓\n"
                    f"\n"
                    f"{'='*60}\n"
                    f"程序将立即退出...\n"
                    f"{'='*60}\n"
                )
                
                # ✅ 抛出致命异常
                raise RuntimeError(
                    f"平仓失败：{self.exchange_a.exchange_name} 已平仓但 "
                    f"{self.exchange_b.exchange_name} 失败，需要手动处理仓位！"
                )
                # return False
            
            # ✅ 计算盈亏
            pnl_pct = position.calculate_pnl_pct(
                exchange_a_price=exchange_a_price,
                exchange_b_price=exchange_b_price
            )
            
            logger.info(
                f"✅ 平仓成功:\n"
                f"   {self.exchange_a.exchange_name} 订单: {order_a_result.get('order_id')}\n"
                f"   {self.exchange_b.exchange_name} 订单: {order_b_result.get('order_id')}\n"
                f"   盈亏: {pnl_pct:.4f}%\n"
                f"   持仓时长: {position.get_holding_duration()}"
            )
            
            return True
        
        except Exception as e:
            logger.error(f"❌ 平仓执行异常: {e}")
            import traceback
            traceback.print_exc()
            return False