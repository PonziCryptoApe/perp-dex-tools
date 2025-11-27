"""订单执行服务（并行 + 重试）"""

import asyncio
from datetime import datetime
import logging
from decimal import Decimal
import time
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
        quantity: Decimal,
        max_retries: int = 3,
        retry_delay: float = 0.3
    ):
        """
        初始化订单执行器
        
        Args:w
            exchange_a: 交易所 A（开空）
            exchange_b: 交易所 B（开多）
            quantity: 交易数量
            max_retries: 最大重试次数（默认 3）
            retry_delay: 重试延迟（秒，默认 0.3 秒）
        """
        self.exchange_a = exchange_a
        self.exchange_b = exchange_b
        self.quantity = quantity
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        logger.info(
            f"📦 订单执行器已初始化:\n"
            f"   Exchange A: {exchange_a.exchange_name}\n"
            f"   Exchange B: {exchange_b.exchange_name}\n"
            f"   Quantity: {quantity}\n"
            f"   Max Retries: {max_retries}\n"
            f"   Retry Delay: {retry_delay}s"
        )

    
    async def _balance_positions(
        self,
        target_quantity: Decimal,
        filled_qty_a: Decimal,
        filled_qty_b: Decimal,
        side_a: str,
        side_b: str,
        price_a: Decimal,
        price_b: Decimal,
        operation_type: str  # 'open' 或 'close'
    ) -> Tuple[Decimal, Decimal]:
        """
        平衡仓位（处理部分成交不匹配）
        
        Args:
            target_quantity: 目标数量
            filled_qty_a: Exchange A 实际成交量
            filled_qty_b: Exchange B 实际成交量
            side_a: Exchange A 方向
            side_b: Exchange B 方向
            price_a: Exchange A 价格
            price_b: Exchange B 价格
            operation_type: 操作类型
        
        Returns:
            (最终 Exchange A 数量, 最终 Exchange B 数量)
        """
        # ✅ 计算差异
        diff_a = target_quantity - filled_qty_a
        diff_b = target_quantity - filled_qty_b
        
        # ✅ 如果完全匹配，直接返回
        if diff_a == 0 and diff_b == 0:
            logger.info(f"✅ 仓位平衡，无需调整")
            return filled_qty_a, filled_qty_b
        
        logger.warning(
            f"⚠️ 检测到仓位不平衡:\n"
            f"   目标数量: {target_quantity}\n"
            f"   {self.exchange_a.exchange_name}: {filled_qty_a} (差异: {diff_a:+})\n"
            f"   {self.exchange_b.exchange_name}: {filled_qty_b} (差异: {diff_b:+})"
        )
        
        # ✅ 策略 1: 补齐未成交部分（优先）
        balanced_qty_a = filled_qty_a
        balanced_qty_b = filled_qty_b
        
        # ✅ Exchange A 需要补单
        if diff_a > 0:
            logger.info(f"🔄 补单 {self.exchange_a.exchange_name}: {diff_a}")
            
            try:
                result_a = await self._retry_place_order(
                    exchange=self.exchange_a,
                    side=side_a,
                    quantity=diff_a,
                    price=price_a,
                    order_type=operation_type,
                    retry_mode='aggressive'  # ✅ 激进模式，提高成交率
                )

                if result_a['success']:
                    balanced_qty_a += result_a.get('filled_quantity', Decimal('0'))
                    logger.info(
                        f"✅ 补单成功: {self.exchange_a.exchange_name} "
                        f"+{result_a['filled_quantity']} → 总计 {balanced_qty_a}"
                    )
                else:
                    logger.error(f"❌ 补单失败: {self.exchange_a.exchange_name}")
            
            except Exception as e:
                logger.error(f"❌ 补单异常: {self.exchange_a.exchange_name} - {e}")
        
        # ✅ Exchange B 需要补单
        if diff_b > 0:
            logger.info(f"🔄 补单 {self.exchange_b.exchange_name}: {diff_b}")
            
            try:
                result_b = await self._retry_place_order(
                    exchange=self.exchange_b,
                    side=side_b,
                    quantity=diff_b,
                    price=price_b,
                    order_type=operation_type,
                    retry_mode='aggressive'
                )

                if result_b['success']:
                    balanced_qty_b += result_b.get('filled_quantity', Decimal('0'))
                    logger.info(
                        f"✅ 补单成功: {self.exchange_b.exchange_name} "
                        f"+{result_b['filled_quantity']} → 总计 {balanced_qty_b}"
                    )
                else:
                    logger.error(f"❌ 补单失败: {self.exchange_b.exchange_name}")
            
            except Exception as e:
                logger.error(f"❌ 补单异常: {self.exchange_b.exchange_name} - {e}")
        
        # ✅ 策略 2: 如果补单后仍不匹配，平掉多余部分
        final_diff = balanced_qty_a - balanced_qty_b
        
        if abs(final_diff) > Decimal('0.001'):  # 容差 0.001
            logger.warning(
                f"⚠️ 补单后仍不平衡:\n"
                f"   {self.exchange_a.exchange_name}: {balanced_qty_a}\n"
                f"   {self.exchange_b.exchange_name}: {balanced_qty_b}\n"
                f"   差异: {final_diff:+}"
            )
            
            # ✅ 平掉多余部分
            if final_diff > 0:
                # Exchange A 多了，平掉多余部分
                excess = final_diff
                logger.info(f"🔄 平掉 {self.exchange_a.exchange_name} 多余部分: {excess}")
                
                # 反向操作（开仓 → 平仓，平仓 → 开仓）
                reverse_side = 'sell' if side_a == 'buy' else 'buy'
                
                try:
                    result_a = await self._retry_place_order(
                        exchange=self.exchange_a,
                        side=reverse_side,
                        quantity=excess,
                        price=price_a,
                        order_type='balance',  # ✅ 标记为平衡操作
                        retry_mode='aggressive'
                    )

                    if result_a['success']:
                        balanced_qty_a -= result_a.get('filled_quantity', Decimal('0'))
                        logger.info(
                            f"✅ 平仓成功: {self.exchange_a.exchange_name} "
                            f"-{result_a['filled_quantity']} → 剩余 {balanced_qty_a}"
                        )
                
                except Exception as e:
                    logger.error(f"❌ 平仓异常: {self.exchange_a.exchange_name} - {e}")
            
            elif final_diff < 0:
                # Exchange B 多了，平掉多余部分
                excess = abs(final_diff)
                logger.info(f"🔄 平掉 {self.exchange_b.exchange_name} 多余部分: {excess}")
                
                reverse_side = 'sell' if side_b == 'buy' else 'buy'
                
                try:
                    result_b = await self._retry_place_order(
                        exchange=self.exchange_b,
                        side=reverse_side,
                        quantity=excess,
                        price=price_b,
                        order_type='balance',
                        retry_mode='aggressive'
                    )

                    if result_b['success']:
                        balanced_qty_b -= result_b.get('filled_quantity', Decimal('0'))
                        logger.info(
                            f"✅ 平仓成功: {self.exchange_b.exchange_name} "
                            f"-{result_b['filled_quantity']} → 剩余 {balanced_qty_b}"
                        )
                
                except Exception as e:
                    logger.error(f"❌ 平仓异常: {self.exchange_b.exchange_name} - {e}")
        
        # ✅ 返回最终平衡后的数量
        logger.info(
            f"✅ 仓位平衡完成:\n"
            f"   {self.exchange_a.exchange_name}: {filled_qty_a} → {balanced_qty_a}\n"
            f"   {self.exchange_b.exchange_name}: {filled_qty_b} → {balanced_qty_b}\n"
            f"   最终差异: {abs(balanced_qty_a - balanced_qty_b)}"
        )
        
        return balanced_qty_a, balanced_qty_b
    
    async def _retry_place_order(
        self,
        exchange: ExchangeAdapter,
        order_type: str, # 'open' or 'close'
        side: str,
        quantity: Decimal,
        price: Decimal,
        retry_mode: str = 'opportunistic',
        quote_id: Optional[str] = None,
        max_retries: Optional[int] = None
    ) -> dict:
        """
        重试下单逻辑
        
        Args:
            exchange: 交易所适配器
            order_type: 订单类型（'open' 或 'close'）
            side: 订单方向（'buy' 或 'sell'）
            quantity: 订单数量
            price: 订单价格
            retry_mode: 重试模式
            quote_id: 报价 ID（可选）
            max_retries: 最大重试次数（可选）   
        Returns:
            {''success': bool, 'order_id': Optional[str], 'error': Optional[str]}
        """
        if max_retries is None:
            max_retries = self.max_retries
        # ✅ 保存初始价格
        initial_price = price
        current_quote_id = quote_id

        for attempt in range(1, max_retries + 1):
            try:
                if attempt > 1:
                    logger.info(
                        f"🔄 重试下单: {exchange.exchange_name} | "
                        f"类型: {order_type} | 方向: {side} | "
                        f"尝试次数: {attempt}/{max_retries}"
                    )
                    await asyncio.sleep(self.retry_delay)
                    # ✅ 从第 2 次重试开始，获取最新价格和 quote_id
                    try:
                        orderbook = exchange.get_latest_orderbook()
                        
                        if orderbook:
                            # ✅ 更新 quote_id（如果有）
                            if orderbook.get('quote_id'):
                                current_quote_id = orderbook['quote_id']
                                logger.info(f"💡 获取最新 quote_id: {current_quote_id[:8]}...")
                            
                            # ✅ 根据订单方向获取最优价格
                            if side.lower() == 'buy':
                                # 买入：使用卖一价（asks）
                                if orderbook.get('asks') and len(orderbook['asks']) > 0:
                                    new_price = Decimal(str(orderbook['asks'][0][0]))
                                    logger.info(
                                        f"💡 获取最新卖一价: ${initial_price} → ${new_price} "
                                        f"(变化: {((new_price - initial_price) / initial_price * 100):+.4f}%)"
                                    )
                                    price = new_price
                            else:
                                # 卖出：使用买一价（bids）
                                if orderbook.get('bids') and len(orderbook['bids']) > 0:
                                    new_price = Decimal(str(orderbook['bids'][0][0]))
                                    logger.info(
                                        f"💡 获取最新买一价: ${initial_price} → ${new_price} "
                                        f"(变化: {((new_price - initial_price) / initial_price * 100):+.4f}%)"
                                    )
                                    price = new_price
                        else:
                            logger.warning(f"⚠️ 无法获取最新订单簿，使用初始价格 ${initial_price}")
                    
                    except Exception as e:
                        logger.warning(f"⚠️ 获取最新价格失败: {e}，使用初始价格 ${initial_price}")
                # ✅ 从第 3 次重试开始，强制使用 aggressive 模式
                if attempt >= 3:
                    current_retry_mode = 'aggressive'
                    logger.info(f"💡 第 {attempt} 次重试，切换为 aggressive 模式")
                else:
                    current_retry_mode = retry_mode
                if order_type == 'open':
                    result = await exchange.place_open_order(
                        side=side,
                        quantity=quantity,
                        price=price,
                        retry_mode=current_retry_mode,
                        quote_id=current_quote_id
                    )
                else:  # 'close'
                    result = await exchange.place_close_order(
                        side=side,
                        quantity=quantity,
                        price=price,
                        retry_mode=current_retry_mode,
                        quote_id=current_quote_id
                    )
                # ✅ 检查部分成交
                if not result.get('success') and result.get('partial_fill'):
                    # ✅ 部分成交也返回（由上层处理）
                    logger.warning(
                        f"⚠️ 部分成交: {exchange.exchange_name} | "
                        f"已成交: {result.get('filled_quantity')} / {quantity}"
                    )
                    
                    return {
                        'success': True,  # ✅ 标记为成功（有成交）
                        'order_id': result.get('order_id'),
                        'filled_quantity': result.get('filled_quantity', Decimal('0')),
                        'filled_price': result.get('filled_price', price),
                        'error': None,
                        'partial_fill': True  # ✅ 传递部分成交标志
                    }
            
                if result.get('success'):
                    if attempt > 1:
                        logger.info(
                            f"✅ 下单成功: {exchange.exchange_name} | "
                            f"类型: {order_type} | 方向: {side} | "
                            f"尝试次数: {attempt}/{max_retries}"
                        )
                    return result
                else:
                    logger.warning(
                        f"⚠️ 下单失败: {exchange.exchange_name} | "
                        f"类型: {order_type} | 方向: {side} | "
                        f"尝试次数: {attempt}/{max_retries} | "
                        f"错误: {result.get('error')}"
                    )
            except Exception as e:
                logger.error(
                    f"❌ 下单异常: {exchange.exchange_name} | "
                    f"类型: {order_type} | 方向: {side} | "
                    f"尝试次数: {attempt}/{max_retries} | "
                    f"异常: {str(e)}"
                )
        logger.error(f"❌ 下单失败: {exchange.exchange_name} | "
                      f"类型: {order_type} | 方向: {side} | "
                      f"尝试次数: {attempt}/{max_retries} | "
                      f"错误: {result.get('error')}"
        )
        return {
            'success': False,
            'order_id': None,
            'filled_price': Decimal('0'),
            'filled_quantity': Decimal('0'),
            'error': 'Max retries exceeded'
        }

    async def execute_open(
        self,
        exchange_a_price: Decimal,
        exchange_b_price: Decimal,
        spread_pct: Decimal,
        exchange_a_quote_id: Optional[str] = None,
        exchange_b_quote_id: Optional[str] = None,
        signal_trigger_time: Optional[float] = None
    ) -> Tuple[bool, Optional[Position]]:
        """
        执行开仓
        
        Args:
            exchange_a_price: Exchange A 价格（开空价格）
            exchange_b_price: Exchange B 价格（开多价格）
            spread_pct: 价差百分比
        逻辑：
            1. 两所同时下单
            2. 如果都失败 → 跳过
            3. 如果都成功 → 创建持仓
            4. 如果一方失败 → 重试失败方
        
        Returns:
            (success: bool, position: Optional[Position])
        """
        # ✅ 记录开始执行时间
        execution_start_time = time.time()
        
        # ✅ 计算信号触发 → 开始执行的延迟
        if signal_trigger_time:
            signal_to_execution_delay = (execution_start_time - signal_trigger_time) * 1000
            logger.info(f"⏱️ 信号触发 → 开始执行: {signal_to_execution_delay:.2f} ms")
    
        logger.info(
            f"📤 执行开仓:\n"
            f"   {self.exchange_a.exchange_name} 开空 @ ${exchange_a_price}\n"
            f"   {self.exchange_b.exchange_name} 开多 @ ${exchange_b_price}"
        )
        
        try:
            # ✅ 1. 并行下单（首次尝试）
            logger.info("🚀 开始并行下单（首次尝试）...")

            task_a = asyncio.create_task(
                self.exchange_a.place_open_order(
                    side='sell',
                    quantity=self.quantity,
                    price=exchange_a_price,
                    retry_mode='opportunistic',
                    quote_id=exchange_a_quote_id
                )
            )
            
            task_b = asyncio.create_task(
                self.exchange_b.place_open_order(
                    side='buy',
                    quantity=self.quantity,
                    price=exchange_b_price,
                    retry_mode='aggressive',
                    quote_id=exchange_b_quote_id
                )
            )
            # ✅ Exchange A 开空（卖出）
            order_a_result, order_b_result = await asyncio.gather(task_a, task_b)

            success_a = order_a_result.get('success', False)
            success_b = order_b_result.get('success', False)

            # 情况 1️⃣: 两所都失败 → 跳过
            if not success_a and not success_b:
                logger.warning(
                    f"⚠️ 开仓失败（两所都失败）:\n"
                    f"   {self.exchange_a.exchange_name}: {order_a_result.get('error')}\n"
                    f"   {self.exchange_b.exchange_name}: {order_b_result.get('error')}\n"
                    f"   ⏱️ 耗时: {(time.time() - execution_start_time) * 1000:.2f} ms\n"
                    f"   🔄 等待下次机会..."
                )
                return False, None
            # 情况 2️⃣: 两所都成功 → 创建持仓
            if success_a and success_b:
                logger.info(
                    f"✅ 两所均下单成功:\n"
                    f"   {self.exchange_a.exchange_name} 订单: {order_a_result.get('order_id')}\n"
                    f"   {self.exchange_b.exchange_name} 订单: {order_b_result.get('order_id')}\n"
                    f"   ⏱️ 耗时: {(time.time() - execution_start_time) * 1000:.2f} ms"
                )

                # ✅ 3. 到这里两所都成功了，检查成交数量
                filled_qty_a = order_a_result.get('filled_quantity', self.quantity)
                filled_qty_b = order_b_result.get('filled_quantity', self.quantity)
                
                logger.info(
                    f"📊 初始成交结果:\n"
                    f"   {self.exchange_a.exchange_name}: {filled_qty_a} / {self.quantity}\n"
                    f"   {self.exchange_b.exchange_name}: {filled_qty_b} / {self.quantity}"
                )
                
                # ✅ 4. 平衡仓位（关键！）
                balanced_qty_a, balanced_qty_b = await self._balance_positions(
                    target_quantity=self.quantity,
                    filled_qty_a=filled_qty_a,
                    filled_qty_b=filled_qty_b,
                    side_a='sell',
                    side_b='buy',
                    price_a=exchange_a_price,
                    price_b=exchange_b_price,
                    operation_type='open'
                )
                
                # ✅ 5. 使用平衡后的数量（取较小值）
                final_quantity = min(balanced_qty_a, balanced_qty_b)
                
                if final_quantity == 0:
                    logger.error("❌ 平衡后仓位为 0")
                    return False, None
                
                actual_price_a = order_a_result.get('filled_price', exchange_a_price)
                actual_price_b = order_b_result.get('filled_price', exchange_b_price)

                execution_end_time = time.time()
                execution_delay_ms = (execution_end_time - execution_start_time) * 1000

                if signal_trigger_time:
                    total_delay_ms = (execution_end_time - signal_trigger_time) * 1000
                    logger.info(f"⏱️ 信号触发 → 完成开仓: {total_delay_ms:.2f} ms")
                else:
                    total_delay_ms = None
                    logger.info(f"⏱️ 完成开仓总耗时: {execution_delay_ms:.2f} ms")

                slippage_a = ((actual_price_a - exchange_a_price) / exchange_a_price * 100).quantize(Decimal('0.0001'))
                slippage_b = ((actual_price_b - exchange_b_price) / exchange_b_price * 100).quantize(Decimal('0.0001'))
                
                logger.info(
                    f"✅ 开仓成功:\n"
                    f"   {self.exchange_a.exchange_name}:\n"
                    f"      订单 ID: {order_a_result.get('order_id')}\n"
                    f"      信号价格: ${exchange_a_price}\n"
                    f"      成交价格: ${actual_price_a}\n"
                    f"      滑点: {slippage_a:+.4f}%\n"
                    f"   {self.exchange_b.exchange_name}:\n"
                    f"      订单 ID: {order_b_result.get('order_id')}\n"
                    f"      信号价格: ${exchange_b_price}\n"
                    f"      成交价格: ${actual_price_b}\n"
                    f"      滑点: {slippage_b:+.4f}%\n"
                    f"   ⏱️ 执行耗时: {execution_delay_ms:.2f} ms"
                )

                if total_delay_ms:
                    logger.info(f"   ⏱️ 信号 → 完成: {total_delay_ms:.2f} ms")
                
                position = Position(
                    symbol=self.exchange_a.symbol,
                    quantity=final_quantity,
                    exchange_a_name=self.exchange_a.exchange_name,
                    exchange_b_name=self.exchange_b.exchange_name,
                    # ✅ 信号触发价格
                    exchange_a_signal_entry_price=exchange_a_price,
                    exchange_b_signal_entry_price=exchange_b_price,
                    #✅ 实际成交价格
                    exchange_a_entry_price=actual_price_a,
                    exchange_b_entry_price=actual_price_b,

                    exchange_a_order_id=order_a_result.get('order_id', 'unknown'),
                    exchange_b_order_id=order_b_result.get('order_id', 'unknown'),
                    spread_pct=spread_pct,
                    signal_entry_time=signal_trigger_time,
                    entry_execution_delay_ms=total_delay_ms,
                )
                return True, position
            
            # 情况 3️⃣: A失败，B成功 → 重试A
            if not success_a and success_b:
                logger.warning(
                    f"⚠️ {self.exchange_b.exchange_name} 成功，"
                    f"⚠️ {self.exchange_a.exchange_name} 下单失败，"
                    f"正在重试...\n"
                    f"   错误: {order_a_result.get('error')}"
                )
                # ✅ 重试 Exchange A 下单
                retry_result_a = await self._retry_place_order(
                    exchange=self.exchange_a,
                    order_type='open',
                    side='sell',
                    quantity=self.quantity,
                    price=exchange_a_price,
                    retry_mode='aggressive',
                    quote_id=exchange_a_quote_id
                )
                if retry_result_a.get('success'):
                    # ✅ 获取实际成交价格
                    actual_price_a = retry_result_a.get('filled_price') or exchange_a_price
                    actual_price_b = order_b_result.get('filled_price') or exchange_b_price
                    
                    execution_end_time = time.time()
                    total_delay_ms = (execution_end_time - signal_trigger_time) * 1000 if signal_trigger_time else None
                    
                    logger.info(
                        f"✅ 开仓成功（A 所重试成功）:\n"
                        f"   {self.exchange_a.exchange_name}: {retry_result_a.get('order_id')}\n"
                        f"   {self.exchange_b.exchange_name}: {order_b_result.get('order_id')}\n"
                        f"   ⏱️ 总耗时: {(time.time() - execution_start_time) * 1000:.2f} ms"
                    )

                    position = Position(
                        symbol=self.exchange_a.symbol,
                        quantity=self.quantity,
                        exchange_a_name=self.exchange_a.exchange_name,
                        exchange_b_name=self.exchange_b.exchange_name,
                        # ✅ 信号触发价格
                        exchange_a_signal_entry_price=exchange_a_price,
                        exchange_b_signal_entry_price=exchange_b_price,
                        
                        # ✅ 实际成交价格
                        exchange_a_entry_price=actual_price_a,
                        exchange_b_entry_price=actual_price_b,
                        exchange_a_order_id=retry_result_a.get('order_id', 'unknown'),
                        exchange_b_order_id=order_b_result.get('order_id', 'unknown'),
                        spread_pct=spread_pct,
                        # ✅ 时间信息
                        signal_entry_time=signal_trigger_time,
                        entry_execution_delay_ms=total_delay_ms
                    )

                    return True, position
                else:
                    # ✅ A 所重试失败 → 需要平掉 B 所的仓位
                    logger.error(
                        f"❌ {self.exchange_a.exchange_name} 重试失败，"
                        f"需要平掉 {self.exchange_b.exchange_name} 的单边持仓"
                    )
                    
                    await self._emergency_close_b(
                        order_id=order_b_result.get('order_id'),
                        quantity=order_b_result.get('filled_quantity', self.quantity)
                    )
                    
                    return False, None
            # 情况 4️⃣: A成功，B失败 → 重试 B
            if success_a and not success_b:
                logger.warning(
                    f"⚠️ {self.exchange_a.exchange_name} 成功，"
                    f"{self.exchange_b.exchange_name} 失败 → 重试 {self.exchange_b.exchange_name}..."
                )

                retry_result_b = await self._retry_place_order(
                    exchange=self.exchange_b,
                    order_type='open',
                    side='buy',
                    quantity=self.quantity,
                    price=exchange_b_price,
                    retry_mode='aggressive',
                    quote_id=exchange_b_quote_id
                )
                if retry_result_b.get('success'):
                    # ✅ 获取实际成交价格
                    actual_price_a = order_a_result.get('filled_price') or exchange_a_price
                    actual_price_b = retry_result_b.get('filled_price') or exchange_b_price
                    
                    execution_end_time = time.time()
                    total_delay_ms = (execution_end_time - signal_trigger_time) * 1000 if signal_trigger_time else None
                    
                    logger.info(
                        f"✅ 开仓成功（B 所重试成功）:\n"
                        f"   {self.exchange_a.exchange_name}: {order_a_result.get('order_id')}\n"
                        f"   {self.exchange_b.exchange_name}: {retry_result_b.get('order_id')}\n"
                        f"   ⏱️ 总耗时: {(time.time() - execution_start_time) * 1000:.2f} ms"
                    )

                    position = Position(
                        symbol=self.exchange_a.symbol,
                        quantity=self.quantity,
                        exchange_a_name=self.exchange_a.exchange_name,
                        exchange_b_name=self.exchange_b.exchange_name,
                        # ✅ 信号触发价格
                        exchange_a_signal_entry_price=exchange_a_price,
                        exchange_b_signal_entry_price=exchange_b_price,
                        
                        # ✅ 实际成交价格
                        exchange_a_entry_price=actual_price_a,
                        exchange_b_entry_price=actual_price_b,
                        
                        exchange_a_order_id=order_a_result.get('order_id', 'unknown'),
                        exchange_b_order_id=retry_result_b.get('order_id', 'unknown'),
                        spread_pct=spread_pct,
                        
                        # ✅ 时间信息
                        signal_entry_time=signal_trigger_time,
                        entry_execution_delay_ms=total_delay_ms
                    )

                    return True, position
                else:
                    # ✅ B 所重试失败 → 需要平掉 A 所的仓位
                    logger.error(
                        f"❌ {self.exchange_b.exchange_name} 重试失败，"
                        f"需要平掉 {self.exchange_a.exchange_name} 的单边持仓"
                    )
                    
                    await self._emergency_close_a(
                        order_id=order_a_result.get('order_id'),
                        quantity=order_a_result.get('filled_quantity', self.quantity)
                    )
                    
                    return False, None
        except Exception as e:
            logger.critical(
                f"🚨 开仓执行异常: {str(e)}"
            )
            import traceback
            traceback.print_exc()
            return False, None
    
    async def execute_close(
        self,
        position: Position,
        exchange_a_price: Decimal,
        exchange_b_price: Decimal,
        exchange_a_quote_id: Optional[str] = None,
        exchange_b_quote_id: Optional[str] = None,
        signal_trigger_time: Optional[float] = None
    ) -> Tuple[bool, Optional[Position]]:
        """
        执行平仓
        
        Args:
            position: 持仓信息
            exchange_a_price: Exchange A 平仓价格（买入价格）
            exchange_b_price: Exchange B 平仓价格（卖出价格）
            exchange_a_quote_id: Exchange A 报价 ID
            exchange_b_quote_id: Exchange B 报价 ID
            signal_trigger_time: 信号触发时间
        逻辑：
            1. 两所同时下单
            2. 如果都成功 → 完成
            3. 如果一方失败 → 重试失败方（必须成功）
            4. 如果都失败 → 两所都重试
        Returns:
            (success: bool, position: Optional[Position])
        """
        # ✅ 记录开始执行时间
        execution_start_time = time.time()
        
        # ✅ 计算信号触发 → 开始执行的延迟
        if signal_trigger_time:
            signal_to_execution_delay = (execution_start_time - signal_trigger_time) * 1000
            logger.info(f"⏱️ 信号触发 → 开始执行: {signal_to_execution_delay:.2f} ms")
        
        logger.info(
            f"📤 执行平仓:\n"
            f"   {self.exchange_a.exchange_name} 平空 @ ${exchange_a_price}\n"
            f"   {self.exchange_b.exchange_name} 平多 @ ${exchange_b_price}"
        )
        
        try:
            # ✅ 1. 并行下单（首次尝试）
            logger.info("🚀 开始并行平仓（首次尝试）...")

            task_a = asyncio.create_task(
                self.exchange_a.place_close_order(
                    side='buy',
                    quantity=self.quantity,
                    price=exchange_a_price,
                    retry_mode='opportunistic',
                    quote_id=exchange_a_quote_id
                )
            )

            task_b = asyncio.create_task(
                self.exchange_b.place_close_order(
                    side='sell',
                    quantity=self.quantity,
                    price=exchange_b_price,
                    retry_mode='aggressive',
                    quote_id=exchange_b_quote_id
                )
            )

            # ✅ Exchange A 平空（买入）
            order_a_result, order_b_result = await asyncio.gather(task_a, task_b)

            success_a = order_a_result.get('success', False)
            success_b = order_b_result.get('success', False)
            
            # ✅ 2. 根据结果处理
            # 情况 1️⃣: 两所都失败 → 跳过
            if not success_a and not success_b:
                logger.warning(
                    f"⚠️ 平仓失败（两所都失败）→ 跳过...\n"
                    f"   {self.exchange_a.exchange_name}: {order_a_result.get('error')}\n"
                    f"   {self.exchange_b.exchange_name}: {order_b_result.get('error')}"
                )

                return False, None
            # 情况 2️⃣: 两所都成功 → 完成
            if success_a and success_b:

                # ✅ 3. 到这里两所都成功了，检查成交数量
                filled_qty_a = order_a_result.get('filled_quantity', position.quantity)
                filled_qty_b = order_b_result.get('filled_quantity', position.quantity)
                
                logger.info(
                    f"📊 初始平仓结果:\n"
                    f"   {self.exchange_a.exchange_name}: {filled_qty_a} / {position.quantity}\n"
                    f"   {self.exchange_b.exchange_name}: {filled_qty_b} / {position.quantity}"
                )
                
                # ✅ 4. 平衡仓位（关键！）
                balanced_qty_a, balanced_qty_b = await self._balance_positions(
                    target_quantity=position.quantity,
                    filled_qty_a=filled_qty_a,
                    filled_qty_b=filled_qty_b,
                    side_a='buy',
                    side_b='sell',
                    price_a=exchange_a_price,
                    price_b=exchange_b_price,
                    operation_type='close'
                )
                actual_price_a = order_a_result.get('filled_price')
                actual_price_b = order_b_result.get('filled_price')

                # 记录执行完成时间
                execution_end_time = time.time()

                # 更新Position
                position.exchange_a_signal_exit_price = exchange_a_price
                position.exchange_b_signal_exit_price = exchange_b_price

                position.exchange_a_exit_price = actual_price_a
                position.exchange_b_exit_price = actual_price_b

                position.exchange_a_exit_order_id = order_a_result.get('order_id')
                position.exchange_b_exit_order_id = order_b_result.get('order_id')

                position.exit_time = datetime.now()
                position.signal_exit_time = signal_trigger_time

                execution_delay_ms = (execution_end_time - execution_start_time) * 1000

                if signal_trigger_time:
                    total_delay_ms = (execution_end_time - signal_trigger_time) * 1000
                    logger.info(f"⏱️ 信号触发 → 完成平仓: {total_delay_ms:.2f} ms")
                    position.exit_execution_delay_ms = total_delay_ms
                else:
                    total_delay_ms = None

                quality_report = position.get_execution_quality_report()

                logger.info(
                    f"✅ 平仓成功:\n"
                    f"   {self.exchange_a.exchange_name}:\n"
                    f"      订单 ID: {order_a_result.get('order_id')}\n"
                    f"      信号价格: ${exchange_a_price}\n"
                    f"      成交价格: ${actual_price_a}\n"
                    f"      滑点: {quality_report['exit_slippage']['exchange_a']:+.4f}%\n"
                    f"   {self.exchange_b.exchange_name}:\n"
                    f"      订单 ID: {order_b_result.get('order_id')}\n"
                    f"      信号价格: ${exchange_b_price}\n"
                    f"      成交价格: ${actual_price_b}\n"
                    f"      滑点: {quality_report['exit_slippage']['exchange_b']:+.4f}%\n"
                    f"\n"
                    f"   📊 执行质量分析:\n"
                    f"      理论盈亏: {quality_report['theoretical_pnl_pct']:+.4f}%\n"
                    f"      实际盈亏: {quality_report['actual_pnl_pct']:+.4f}%\n"
                    f"      盈亏损失: {quality_report['pnl_loss_pct']:+.4f}% (由于滑点)\n"
                    f"      开仓滑点: {quality_report['entry_slippage']['total']:+.4f}%\n"
                    f"      平仓滑点: {quality_report['exit_slippage']['total']:+.4f}%\n"
                    f"      开仓延迟: {quality_report['entry_delay_ms']:.2f} ms\n"
                    f"      平仓延迟: {quality_report['exit_delay_ms']:.2f} ms\n"
                    f"   持仓时长: {position.get_holding_duration()}"
                )

                return True, position
            # 情况 3️⃣: A失败，B成功 → 重试A
            if not success_a and success_b:
                logger.warning(
                    f"⚠️ {self.exchange_b.exchange_name} 成功，"
                    f"⚠️ {self.exchange_a.exchange_name} 下单失败，"
                    f"正在重试...\n"
                    f"   错误: {order_a_result.get('error')}"
                )
                
                # ✅ 重试 Exchange A 下单
                retry_result_a = await self._retry_place_order(
                    exchange=self.exchange_a,
                    order_type='close',
                    side='buy',
                    quantity=self.quantity,
                    price=exchange_a_price,
                    retry_mode='opportunistic',
                    quote_id=exchange_a_quote_id,
                    max_retries=self.max_retries + 2  # 增加重试次数
                )

                if retry_result_a.get('success'):
                    # ✅ 获取实际成交价格
                    actual_price_a = retry_result_a.get('filled_price')
                    actual_price_b = order_b_result.get('filled_price')
                    
                    execution_end_time = time.time()
                    execution_delay_ms = (execution_end_time - execution_start_time) * 1000
                    
                    if signal_trigger_time:
                        total_delay_ms = (execution_end_time - signal_trigger_time) * 1000
                    else:
                        total_delay_ms = None
                    
                    # ✅ 更新 Position 对象
                    position.exchange_a_signal_exit_price = exchange_a_price
                    position.exchange_b_signal_exit_price = exchange_b_price
                    
                    position.exchange_a_exit_price = actual_price_a
                    position.exchange_b_exit_price = actual_price_b
                    
                    position.exchange_a_filled_exit_price = actual_price_a
                    position.exchange_b_filled_exit_price = actual_price_b
                    
                    position.exchange_a_exit_order_id = retry_result_a.get('order_id')
                    position.exchange_b_exit_order_id = order_b_result.get('order_id')
                    
                    position.exit_time = datetime.now()
                    position.signal_exit_time = signal_trigger_time
                    position.exit_execution_delay_ms = total_delay_ms
                    
                    logger.info(
                        f"✅ 平仓成功（A 所重试成功）:\n"
                        f"   {self.exchange_a.exchange_name}: {retry_result_a.get('order_id')}\n"
                        f"   {self.exchange_b.exchange_name}: {order_b_result.get('order_id')}\n"
                        f"   ⏱️ 总耗时: {execution_delay_ms:.2f} ms"
                    )
                    
                    return True, position
                else:
                    logger.error(
                        f"❌ {self.exchange_a.exchange_name} 重试失败，"
                        f"需要手动处理仓位！"
                    )
                    return False, None

            # 情况 4️⃣: A成功，B失败 → 重试 B
            if success_a and not success_b:
                logger.warning(
                    f"⚠️ {self.exchange_a.exchange_name} 成功，"
                    f"{self.exchange_b.exchange_name} 失败 → 重试 {self.exchange_b.exchange_name}..."
                )
                
                retry_result_b = await self._retry_place_order(
                    exchange=self.exchange_b,
                    order_type='close',
                    side='sell',
                    quantity=self.quantity,
                    price=exchange_b_price,
                    retry_mode='aggressive',
                    quote_id=exchange_b_quote_id,
                    max_retries=5
                )
                
                if retry_result_b.get('success'):
                    # ✅ 获取实际成交价格
                    actual_price_a = order_a_result.get('filled_price')
                    actual_price_b = retry_result_b.get('filled_price')
                    
                    execution_end_time = time.time()
                    execution_delay_ms = (execution_end_time - execution_start_time) * 1000
                    
                    if signal_trigger_time:
                        total_delay_ms = (execution_end_time - signal_trigger_time) * 1000
                    else:
                        total_delay_ms = None
                    
                    # ✅ 更新 Position 对象
                    position.exchange_a_signal_exit_price = exchange_a_price
                    position.exchange_b_signal_exit_price = exchange_b_price
                    
                    position.exchange_a_exit_price = actual_price_a
                    position.exchange_b_exit_price = actual_price_b
                    
                    position.exchange_a_filled_exit_price = actual_price_a
                    position.exchange_b_filled_exit_price = actual_price_b
                    
                    position.exchange_a_exit_order_id = order_a_result.get('order_id')
                    position.exchange_b_exit_order_id = retry_result_b.get('order_id')
                    
                    position.exit_time = datetime.now()
                    position.signal_exit_time = signal_trigger_time
                    position.exit_execution_delay_ms = total_delay_ms
                    
                    logger.info(
                        f"✅ 平仓成功（B 所重试成功）:\n"
                        f"   {self.exchange_a.exchange_name}: {order_a_result.get('order_id')}\n"
                        f"   {self.exchange_b.exchange_name}: {retry_result_b.get('order_id')}\n"
                        f"   ⏱️ 总耗时: {execution_delay_ms:.2f} ms"
                    )
                    
                    return True, position
                else:
                    logger.critical(
                        f"🚨 {self.exchange_b.exchange_name} 平仓失败（重试后仍失败），"
                        f"需要手动处理！"
                    )
                    return False, None
        except Exception as e:
            logger.critical(
                f"🚨 平仓执行异常: {str(e)}"
            )
            import traceback
            traceback.print_exc()
            return False, None
        
    # ✅ 紧急平仓方法
    async def _emergency_close_a(self, order_id: str, quantity: Decimal):
        """紧急平仓 A 所（单边持仓风险处理）"""
        try:
            logger.warning(f"🚨 紧急平仓 {self.exchange_a.exchange_name}: {order_id}")
            
            # ✅ 获取最新价格
            orderbook = self.exchange_a.get_latest_orderbook()
            if not orderbook or not orderbook.get('asks'):
                logger.error("❌ 无法获取价格，紧急平仓失败")
                return
            
            close_price = Decimal(str(orderbook['asks'][0][0]))
            
            # ✅ 下平仓单（买入平空）
            result = await self._retry_place_order(
                exchange=self.exchange_a,
                order_type='close',
                side='buy',
                quantity=quantity,
                price=close_price,
                retry_mode='aggressive',
                quote_id=orderbook.get('quote_id'),
                max_retries=5
            )
            
            if result.get('success'):
                logger.info(f"✅ 紧急平仓成功: {result.get('order_id')}")
            else:
                logger.critical(f"🚨 紧急平仓失败，需要手动处理！")
        
        except Exception as e:
            logger.error(f"❌ 紧急平仓异常: {e}")
    
    async def _emergency_close_b(self, order_id: str, quantity: Decimal):
        """紧急平仓 B 所（单边持仓风险处理）"""
        try:
            logger.warning(f"🚨 紧急平仓 {self.exchange_b.exchange_name}: {order_id}")
            
            # ✅ 获取最新价格
            orderbook = self.exchange_b.get_latest_orderbook()
            if not orderbook or not orderbook.get('bids'):
                logger.error("❌ 无法获取价格，紧急平仓失败")
                return
            quote_id = orderbook.get('quote_id')
            close_price = Decimal(str(orderbook['bids'][0][0]))
            
            # ✅ 下平仓单（卖出平多）
            result = await self._retry_place_order(
                exchange=self.exchange_b,
                order_type='close',
                side='sell',
                quantity=quantity,
                price=close_price,
                retry_mode='aggressive',
                quote_id=quote_id,
                max_retries=5
            )
            
            if result.get('success'):
                logger.info(f"✅ 紧急平仓成功: {result.get('order_id')}")
            else:
                logger.critical(f"🚨 紧急平仓失败，需要手动处理！")
        
        except Exception as e:
            logger.error(f"❌ 紧急平仓异常: {e}")