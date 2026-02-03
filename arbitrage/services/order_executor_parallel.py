"""订单执行服务（并行 + 重试）"""

import asyncio
from datetime import datetime
import json
import logging
from decimal import Decimal
import os
import time
from typing import Tuple, Optional

from aiolimiter import AsyncLimiter
import lighter

from ..models.position import Position
from ..exchanges.base import ExchangeAdapter
from helpers.lark_bot import LarkBot

logger = logging.getLogger(__name__)

class OrderExecutor:
    """订单执行服务"""
    
    def __init__(
        self,
        exchange_a: ExchangeAdapter,
        exchange_b: ExchangeAdapter,
        quantity: Decimal,
        quantity_precision: Decimal,
        max_retries: int = 5,
        retry_delay: float = 0.3,
        order_limiter_a: Optional[AsyncLimiter] = None,
        order_limiter_b: Optional[AsyncLimiter] = None
    ):
        """
        初始化订单执行器
        
        Args:
            exchange_a: 交易所 A（开空）
            exchange_b: 交易所 B（开多）
            quantity: 交易数量
            max_retries: 最大重试次数（默认 5）
            retry_delay: 重试延迟（秒，默认 0.3 秒）
        """
        self.exchange_a = exchange_a
        self.exchange_b = exchange_b
        self.quantity = quantity
        self.quantity_precision = quantity_precision
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.order_limiter_a = order_limiter_a
        self.order_limiter_b = order_limiter_b
        self.sleep_interval = 30
        self.sleep_interval_enhance = 61
        self.sleep_retries = 0
        self.lark_token = os.getenv("LARK_TOKEN_SERIOUS")
        self.lark_index_text = f'【{os.getenv("ENV_INDEX")}】' if os.getenv("ENV_INDEX", None) else ''
        if self.lark_token:
            self.lark_bot = LarkBot(self.lark_token)
        else:
            self.lark_bot = None

        logger.info(
            f"📦 订单执行器已初始化:\n"
            f"   Exchange A: {exchange_a.exchange_name}\n"
            f"   Exchange B: {exchange_b.exchange_name}\n"
            f"   Quantity: {quantity}\n"
            f"   Quantity Precision: {quantity_precision}\n"
            f"   Max Retries: {max_retries}\n"
            f"   Retry Delay: {retry_delay}s"
        )

    def _normalize_quantity(self, quantity: Decimal, exchange_name: str = None) -> Decimal:
        """标准化数量精度"""
        # ✅ 使用配置的精度
        normalized = quantity.quantize(self.quantity_precision)
        
        if normalized != quantity and exchange_name:
            logger.debug(
                f"💡 数量精度标准化: {exchange_name} | "
                f"{quantity} → {normalized} (精度: {self.quantity_precision})"
            )
        
        return normalized
    
    async def _balance_positions(
        self,
        target_quantity: Decimal,
        filled_qty_a: Decimal,
        filled_qty_b: Decimal,
        side_a: str,
        side_b: str,
        price_a: Decimal,
        price_b: Decimal,
        operation_type: str,
        order_a_id: Optional[str] = None,  # ✅ 新增：订单 ID
        order_b_id: Optional[str] = None
    ) -> Tuple[Decimal, Decimal]:
        """
        平衡仓位（简化版：容忍小误差，大误差全部平掉）
        
        Args:
            target_quantity: 目标数量
            filled_qty_a: Exchange A 实际成交量
            filled_qty_b: Exchange B 实际成交量
            side_a: Exchange A 方向
            side_b: Exchange B 方向
            price_a: Exchange A 价格
            price_b: Exchange B 价格
            operation_type: 操作类型
            order_a_id: Exchange A 订单 ID
            order_b_id: Exchange B 订单 ID
        
        Returns:
            (最终 Exchange A 数量, 最终 Exchange B 数量)
        """
        # ✅ 1. 检查是否完全匹配
        diff_a = target_quantity - filled_qty_a
        diff_b = target_quantity - filled_qty_b
        
        # ✅ 使用 filled_qty 的精度标准化差异
        if filled_qty_a != 0:
            diff_a = diff_a.quantize(filled_qty_a)
        
        if filled_qty_b != 0:
            diff_b = diff_b.quantize(filled_qty_b)
        
        if diff_a == 0 and diff_b == 0:
            logger.info(f"✅ 仓位平衡，无需调整")
            return filled_qty_a, filled_qty_b
        
        logger.warning(
            f"⚠️ 检测到仓位不平衡:\n"
            f"   目标数量: {target_quantity}\n"
            f"   {self.exchange_a.exchange_name}: {filled_qty_a} (差异: {diff_a})\n"
            f"   {self.exchange_b.exchange_name}: {filled_qty_b} (差异: {diff_b})"
        )
        
        # ✅ 2. 计算最终差异
        final_diff = filled_qty_a - filled_qty_b
        
        # ✅ 3. 设置容忍阈值
        tolerance = Decimal(self.quantity_precision) * 10 # 小于该阈值可能会存在下单不成功

        # ✅ 4. 策略 1️⃣：小误差 → 使用小数量
        if abs(final_diff) <= tolerance:
            final_quantity = min(filled_qty_a, filled_qty_b)
            
            logger.warning(
                f"⚠️ 仓位差异在容忍范围内:\n"
                f"   差异: {abs(final_diff)}\n"
                f"   容忍阈值: {tolerance}\n"
                f"   使用小数量: {final_quantity}\n"
                f"   💡 自动平衡，不做额外处理"
            )
            
            return filled_qty_a, filled_qty_b
        
        # ✅ 5. 策略 2️⃣：大误差 → 全部平掉
        logger.error(
            f"❌ 仓位差异超出容忍范围:\n"
            f"   差异: {abs(final_diff)}\n"
            f"   容忍阈值: {tolerance}\n"
            f"   开始补单"
        )
        
        # ✅ A 需要补单
        if diff_a > tolerance:
            logger.warning(
                f"🔄 补单 {self.exchange_a.exchange_name}:\n"
                f"   已成交: {filled_qty_a}\n"
                f"   目标: {target_quantity}\n"
                f"   需补单: {diff_a} ({side_a})"
            )
            diff_a = self._normalize_quantity(diff_a, self.exchange_a.exchange_name)
            result_retry_a = await self._retry_place_order(
                exchange=self.exchange_a,
                order_type=operation_type,  # ✅ 使用相同操作类型
                side=side_a,  # ✅ 使用相同方向
                quantity=diff_a,  # ✅ 补单剩余数量
                price=price_a,
                retry_mode='aggressive',
                order_limiter=self.order_limiter_a
            )
            if result_retry_a.get('success'):
                supplement_qty = result_retry_a.get('filled_quantity', Decimal('0'))
                  
                filled_qty_a += supplement_qty
                logger.info(
                    f"✅ {self.exchange_a.exchange_name} 补单成功:\n"
                    f"   补单: {supplement_qty}\n"
                    f"   总计: {filled_qty_a} / {target_quantity}"
                )                   
        
        # ✅ B 需要补单
        if diff_b > tolerance:
            logger.warning(
                f"🔄 补单 {self.exchange_b.exchange_name}:\n"
                f"   已成交: {filled_qty_b}\n"
                f"   目标: {target_quantity}\n"
                f"   需补单: {diff_b} ({side_b})"
            )
            diff_b = self._normalize_quantity(diff_b, self.exchange_b.exchange_name)
            result_retry_b = await self._retry_place_order(
                exchange=self.exchange_b,
                order_type=operation_type,
                side=side_b,
                quantity=diff_b,
                price=price_b,
                retry_mode='aggressive',
                order_limiter=self.order_limiter_b
            )

            if result_retry_b.get('success'):
                supplement_qty = result_retry_b.get('filled_quantity', Decimal('0'))
                filled_qty_b += supplement_qty
                logger.info(
                    f"✅ {self.exchange_b.exchange_name} 补单成功:\n"
                    f"   补单: {supplement_qty}\n"
                    f"   总计: {filled_qty_b} / {target_quantity}"
                )    
        
        # ✅ 6. 检查补单后的结果
        final_diff_after = filled_qty_a - filled_qty_b
        
        if abs(final_diff_after) <= tolerance:
            logger.info(
                f"✅ 补单后仓位平衡:\n"
                f"   {self.exchange_a.exchange_name}: {filled_qty_a}\n"
                f"   {self.exchange_b.exchange_name}: {filled_qty_b}\n"
                f"   差异: {abs(final_diff_after)}"
            )
            
            final_quantity = min(filled_qty_a, filled_qty_b)
            return filled_qty_a, filled_qty_b
        
        # ✅ 7. 补单后仍不平衡
        logger.error(
            f"❌ 补单后仍不平衡:\n"
            f"   {self.exchange_a.exchange_name}: {filled_qty_a}\n"
            f"   {self.exchange_b.exchange_name}: {filled_qty_b}\n"
            f"   差异: {abs(final_diff_after)}\n"
        )

        return filled_qty_a, filled_qty_b
        # # ✅ 根据 operation_type 决定平仓方向
        # close_tasks = []
        
        # if filled_qty_a > 0:
        #     logger.warning(f"🔄 平掉 {self.exchange_a.exchange_name}: {filled_qty_a}")
            
        #     if operation_type == 'open':
        #         # ✅ 开仓失败 → 平掉已开仓部分
        #         # side_a = 'sell' (开空) → 需要 'buy' (平空)
        #         close_side_a = 'buy' if side_a == 'sell' else 'sell'
        #         close_qty_a = filled_qty_a.quantize(self.quantity_precision)
        #         close_tasks.append(
        #             self._close_position(
        #                 exchange=self.exchange_a,
        #                 side=close_side_a,
        #                 quantity=close_qty_a,
        #                 price=price_a,
        #                 order_id=order_a_id,
        #                 order_limiter=self.order_limiter_a
        #             )
        #         )
        #     else:
        #         # ✅ 平仓失败 → 继续尝试平掉剩余持仓
        #         remaining_qty = target_quantity - filled_qty_a
                
        #         # ✅ 使用 filled_qty_a 的精度标准化
        #         if filled_qty_a != 0:
        #             remaining_qty = remaining_qty.quantize(self.quantity_precision)
                
        #         logger.critical(
        #             f"🚨 {self.exchange_a.exchange_name} 平仓不完整:\n"
        #             f"   已平仓: {filled_qty_a}\n"
        #             f"   目标数量: {target_quantity}\n"
        #             f"   剩余持仓: {remaining_qty}\n"
        #             f"   🔄 尝试强制平掉剩余部分..."
        #         )
                
        #         # ✅ 继续尝试平掉剩余部分（使用相同方向）
        #         if remaining_qty > 0:
        #             close_tasks.append(
        #                 self._retry_place_order(
        #                     exchange=self.exchange_a,
        #                     order_type='close',
        #                     side=side_a,  # ✅ 使用相同方向
        #                     quantity=remaining_qty,
        #                     price=price_a,
        #                     retry_mode='aggressive',
        #                     order_limiter=self.order_limiter_a
        #                 )
        #             )
        
        # if filled_qty_b > 0:
        #     logger.warning(f"🔄 平掉 {self.exchange_b.exchange_name}: {filled_qty_b}")
            
        #     if operation_type == 'open':
        #         close_side_b = 'buy' if side_b == 'sell' else 'sell'
        #         close_qty_b = filled_qty_b.quantize(self.quantity_precision)    
        #         close_tasks.append(
        #             self._close_position(
        #                 exchange=self.exchange_b,
        #                 side=close_side_b,
        #                 quantity=close_qty_b,
        #                 price=price_b,
        #                 order_id=order_b_id,
        #                 order_limiter=self.order_limiter_b
        #             )
        #         )
        #     else:
        #         # ✅ 平仓失败 → 继续尝试平掉剩余持仓
        #         remaining_qty = target_quantity - filled_qty_b

        #         # ✅ 使用 filled_qty_b 的精度标准化
        #         if filled_qty_b != 0:
        #             remaining_qty = remaining_qty.quantize(self.quantity_precision)
                
                        
        #         logger.critical(
        #             f"🚨 {self.exchange_b.exchange_name} 平仓不完整:\n"
        #             f"   已平仓: {filled_qty_b}\n"
        #             f"   目标数量: {target_quantity}\n"
        #             f"   剩余持仓: {remaining_qty}\n"
        #             f"   🔄 尝试强制平掉剩余部分..."
        #         )
                
        #         # ✅ 继续尝试平掉剩余部分
        #         if remaining_qty > 0:
        #             close_tasks.append(
        #                 self._retry_place_order(
        #                     exchange=self.exchange_b,
        #                     order_type='close',
        #                     side=side_b,  # ✅ 使用相同方向
        #                     quantity=remaining_qty,
        #                     price=price_b,
        #                     retry_mode='aggressive',
        #                     order_limiter=self.order_limiter_b
        #                 )
        #             )
        
        # if close_tasks:
        #     # ✅ 并行执行平仓任务
        #     close_results = await asyncio.gather(*close_tasks, return_exceptions=True)
            
        #     # ✅ 检查平仓结果
        #     for i, result in enumerate(close_results):
        #         if isinstance(result, Exception):
        #             logger.critical(f"🚨 平仓任务异常: {result}")
        #         elif isinstance(result, dict) and result.get('success'):
        #             logger.info(f"✅ 平仓任务 {i+1} 成功")
        #         else:
        #             logger.critical(f"🚨 平仓任务 {i+1} 失败，需要手动处理！")
        # return Decimal('0'), Decimal('0')
    
    async def _retry_place_order(
        self,
        exchange: ExchangeAdapter,
        order_type: str, # 'open' or 'close'
        side: str,
        quantity: Decimal,
        price: Decimal,
        retry_mode: str = 'opportunistic',
        quote_id: Optional[str] = None,
        max_retries: Optional[int] = None,
        order_limiter: Optional[AsyncLimiter] = None
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
        start_time = time.time()
        for attempt in range(1, max_retries + 1):
            retry_start_time = time.time()
            try:
                logger.info(
                    f"🔄 重试下单: {exchange.exchange_name} | "
                    f"类型: {order_type} | 方向: {side} | "
                    f"尝试次数: {attempt}/{max_retries}"
                )
                # ✅ 从第 1 次重试开始，获取最新价格和 quote_id
                try:
                    if order_limiter:
                        time_limiter_start = time.time()
                        await order_limiter.acquire()
                        time_limiter_end = time.time()
                        logger.info(f"✅ 重试订单速率限制器耗时{ (time_limiter_end - time_limiter_start) * 1000:.2f}ms")
                    orderbook = await exchange.get_latest_orderbook(quantity)
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
                    logger.exception(f"⚠️ 获取最新价格失败: {e}，使用初始价格 ${initial_price}")
                current_retry_mode = retry_mode
                orderbook_time_got = time.time()
                logger.info(f"💡 第 {attempt} 次重试，使用 {current_retry_mode} 模式, 获取订单簿耗时为: {(orderbook_time_got - retry_start_time) *1000:.2f}ms")
                
                if order_type == 'open':
                    result = await exchange.place_open_order(
                        side=side,
                        quantity=quantity,
                        price=price,
                        retry_mode=current_retry_mode,
                        quote_id=current_quote_id,
                        slippage=Decimal('0.02')
                    )
                else:  # 'close'
                    result = await exchange.place_close_order(
                        side=side,
                        quantity=quantity,
                        price=price,
                        retry_mode=current_retry_mode,
                        quote_id=current_quote_id,
                        slippage=Decimal('0.02')
                    )
                
                logger.info(f"💡 第 {attempt} 次重试，使用 {current_retry_mode} 模式, 从下单到获取订单状态耗时为: {(time.time() - orderbook_time_got) *1000:.2f}ms")
                logger.info(f" 从第一次重试开始到获取到下单状态的时间为: { time.time() - start_time}")
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
                        'partial_fill': True,  # ✅ 传递部分成交标志
                        'timestamp': result.get('timestamp'),
                        'place_duration_ms': result.get('place_duration_ms'),
                        'execution_duration_ms': result.get('execution_duration_ms'),
                        'attempt': attempt # ✅ 实际尝试次数
                    }
                self.sleep_retries = 0
                if result.get('success'):
                    logger.info(
                        f"✅ 下单成功: {exchange.exchange_name} | "
                        f"类型: {order_type} | 方向: {side} | "
                        f"尝试次数: {attempt}/{max_retries}"
                    )
                    return {
                        **result,
                        'attempt': attempt  # ✅ 实际尝试次数
                    }
                else:
                    logger.error(
                        f"⚠️ 下单失败: {exchange.exchange_name} | "
                        f"类型: {order_type} | 方向: {side} | "
                        f"尝试次数: {attempt}/{max_retries} | "
                        f"错误: {result.get('error')}"
                    )  
            except lighter.exceptions.ApiException as le:
                logger.error(
                    f"❌ 下单异常(429): {exchange.exchange_name} | "
                    f"类型: {order_type} | 方向: {side} | "
                    f"尝试次数: {attempt}/{max_retries} | "
                    f"从本次拉取订单簿到异常耗时为: {(time.time() - retry_start_time) *1000:.2f}ms |"
                    f"从第一次重试开始到本次异常耗时为: {(time.time() - start_time) * 1000:.2f}ms"
                )
                await self.handleLgApiExcep(le)                           
            except Exception as e:
                logger.exception(
                    f"❌ 下单异常: {exchange.exchange_name} | "
                    f"类型: {order_type} | 方向: {side} | "
                    f"尝试次数: {attempt}/{max_retries} | "
                    f"从本次拉取订单簿到异常耗时为: {(time.time() - retry_start_time) *1000:.2f}ms |"
                    f"从第一次重试开始到本次异常耗时为: {(time.time() - start_time) * 1000:.2f}ms"
                )
        return {
            'success': False,
            'order_id': None,
            'filled_price': Decimal('0'),
            'filled_quantity': Decimal('0'),
            'error': 'Max retries exceeded',
            'timestamp': time.time(),
            'attempt': max_retries
        }

    async def execute_open(
        self,
        exchange_a_price: Decimal,
        exchange_b_price: Decimal,
        spread_pct: Decimal,
        exchange_a_quote_id: Optional[str] = None,
        exchange_b_quote_id: Optional[str] = None,
        signal_trigger_time: Optional[float] = None,
        actual_quantity: Optional[Decimal] = None
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
        order_quantity = actual_quantity if actual_quantity is not None else self.quantity

        order_quantity = self._normalize_quantity(order_quantity, "开仓数量")

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
                    quantity=order_quantity,
                    price=exchange_a_price,
                    retry_mode='aggressive',
                    quote_id=exchange_a_quote_id
                )
            )
            
            task_b = asyncio.create_task(
                self.exchange_b.place_open_order(
                    side='buy',
                    quantity=order_quantity,
                    price=exchange_b_price,
                    retry_mode='aggressive',
                    quote_id=exchange_b_quote_id
                )
            )

            
            order_a_result, order_b_result = await asyncio.gather(task_a, task_b, return_exceptions=True)
            
            success_a = False
            success_b = False
            filled_qty_a = Decimal('0')
            filled_qty_b = Decimal('0')

            if isinstance(order_a_result, Exception):
                logger.error(f"❌ 交易所A 下单异常: {order_a_result}")
                if isinstance(order_a_result, lighter.exceptions.ApiException):
                    await self.handleLgApiExcep(order_a_result)  # 如果是 lighter 异常
                order_a_result = {'success': False, 'error': str(order_a_result)}
            else:
                success_a = order_a_result.get('success', False) or order_a_result.get('partial_fill', False)

            if isinstance(order_b_result, Exception):
                logger.error(f"❌ 交易所B 下单异常: {order_b_result}")
                if isinstance(order_b_result, lighter.exceptions.ApiException):
                    await self.handleLgApiExcep(order_b_result)
                order_b_result = {'success': False, 'error': str(order_a_result)}
            else:
                success_b = order_b_result.get('success', False) or order_b_result.get('partial_fill', False)
            
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
            
            # 情况 2️⃣: A失败，B成功 → 重试A
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
                    quantity=order_quantity,
                    price=exchange_a_price,
                    retry_mode='aggressive',
                    quote_id=exchange_a_quote_id,
                    order_limiter=self.order_limiter_a,
                )
                if retry_result_a.get('success'):

                    order_a_result = retry_result_a
                    success_a = True
                    
                    logger.info(
                        f"✅ 开仓成功（A 所重试成功）:\n"
                        f"   {self.exchange_a.exchange_name}: {retry_result_a.get('order_id')}\n"
                        f"   {self.exchange_b.exchange_name}: {order_b_result.get('order_id')}\n"
                        f"   ⏱️ 总耗时: {(time.time() - execution_start_time) * 1000:.2f} ms"
                    )
                else:
                    # ✅ A 所重试失败 → 需要平掉 B 所的仓位
                    logger.error(
                        f"❌ {self.exchange_a.exchange_name} 重试失败，"
                        f"需要平掉 {self.exchange_b.exchange_name} 的单边持仓"
                    )
                    
                    await self._emergency_close_b(
                        order_id=order_b_result.get('order_id'),
                        quantity=order_b_result.get('filled_quantity', actual_quantity)
                    )
                    
                    return False, None
            # 情况 3️⃣: A成功，B失败 → 重试 B
            if success_a and not success_b:
                logger.warning(
                    f"⚠️ {self.exchange_a.exchange_name} 成功，"
                    f"{self.exchange_b.exchange_name} 失败 → 重试 {self.exchange_b.exchange_name}..."
                )

                retry_result_b = await self._retry_place_order(
                    exchange=self.exchange_b,
                    order_type='open',
                    side='buy',
                    quantity=order_quantity,
                    price=exchange_b_price,
                    retry_mode='aggressive',
                    quote_id=exchange_b_quote_id,
                    order_limiter=self.order_limiter_b,)
                if retry_result_b.get('success'):
                    # ✅ 更新 order_b_result 和 success_b
                    order_b_result = retry_result_b
                    success_b = True
                    
                    logger.info(
                        f"✅ 开仓成功（B 所重试成功）:\n"
                        f"   {self.exchange_a.exchange_name}: {order_a_result.get('order_id')}\n"
                        f"   {self.exchange_b.exchange_name}: {retry_result_b.get('order_id')}\n"
                        f"   ⏱️ 总耗时: {(time.time() - execution_start_time) * 1000:.2f} ms\n"
                        f"   🕒 交易所A耗时: {(order_a_result.get('timestamp') - execution_start_time) * 1000:.2f} ms\n"
                        f"   🕒 交易所B耗时: {(order_b_result.get('timestamp') - execution_start_time) * 1000:.2f} ms\n"
                    )
                else:
                    # ✅ B 所重试失败 → 需要平掉 A 所的仓位
                    logger.error(
                        f"❌ {self.exchange_b.exchange_name} 重试失败，"
                        f"需要平掉 {self.exchange_a.exchange_name} 的单边持仓"
                    )
                    
                    await self._emergency_close_a(
                        order_id=order_a_result.get('order_id'),
                        quantity=order_a_result.get('filled_quantity', order_quantity)
                    )
                    
                    return False, None
                
            # 情况 4️⃣: 两所都成功 → 创建持仓
            if success_a and success_b:
                logger.info(
                    f"✅ 两所均下单成功:\n"
                    f"   {self.exchange_a.exchange_name} 订单: {order_a_result.get('order_id')}\n"
                    f"   {self.exchange_b.exchange_name} 订单: {order_b_result.get('order_id')}\n"
                    f"   ⏱️ 耗时: {(time.time() - execution_start_time) * 1000:.2f} ms"
                )

                # ✅ 3. 到这里两所都成功了，检查成交数量
                filled_qty_a = order_a_result.get('filled_quantity', order_quantity)
                filled_qty_b = order_b_result.get('filled_quantity', order_quantity)

                place_duration_a = order_a_result.get('place_duration_ms', 0)
                place_duration_b = order_b_result.get('place_duration_ms', 0)
                execution_duration_a_ms = order_a_result.get('execution_duration_ms', 0)
                execution_duration_b_ms = order_b_result.get('execution_duration_ms', 0)

                attempt_a = order_a_result.get('attempt', 0)
                attempt_b = order_b_result.get('attempt', 0)


                logger.info(
                    f"📊 初始成交结果:\n"
                    f"   {self.exchange_a.exchange_name}: {filled_qty_a} / {order_quantity}\n"
                    f"   {self.exchange_b.exchange_name}: {filled_qty_b} / {order_quantity}\n"
                    f"   下单耗时:\n"
                    f"   {self.exchange_a.exchange_name}: {place_duration_a:.2f} ms\n"
                    f"   {self.exchange_b.exchange_name}: {place_duration_b:.2f} ms\n"
                    f"   执行耗时:\n"
                    f"   {self.exchange_a.exchange_name}: {execution_duration_a_ms:.2f} ms\n"
                    f"   {self.exchange_b.exchange_name}: {execution_duration_b_ms:.2f} ms\n"
                    f"   {self.exchange_a.exchange_name} 重试次数: {attempt_a}\n"
                    f"   {self.exchange_b.exchange_name} 重试次数: {attempt_b}\n"
                )
                
                # ✅ 4. 平衡仓位（关键！）
                balanced_qty_a, balanced_qty_b = await self._balance_positions(
                    target_quantity=order_quantity,
                    filled_qty_a=filled_qty_a,
                    filled_qty_b=filled_qty_b,
                    side_a='sell',
                    side_b='buy',
                    price_a=exchange_a_price,
                    price_b=exchange_b_price,
                    operation_type='open',
                    order_a_id=order_a_result.get('order_id'),  # ✅ 新增
                    order_b_id=order_b_result.get('order_id')
                )
                
                # ✅ 5. 使用平衡后的数量（取较小值）
                final_quantity = min(balanced_qty_a, balanced_qty_b)
                
                # if balanced_qty_a != balanced_qty_b:
                #     logger.error(
                #         "❌ 下单且经仓位平衡后仍不平衡:\n"
                #         f"   建议: 手动检查两所账户余额"
                #     )
                #     return False, None
                
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
                    f"      成交数量: {balanced_qty_a} / {order_quantity}\n"
                    f"   {self.exchange_b.exchange_name}:\n"
                    f"      订单 ID: {order_b_result.get('order_id')}\n"
                    f"      信号价格: ${exchange_b_price}\n"
                    f"      成交价格: ${actual_price_b}\n"
                    f"      滑点: {slippage_b:+.4f}%\n"
                    f"      成交数量: {balanced_qty_b} / {order_quantity}\n"
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
                    place_duration_a_ms=place_duration_a,
                    place_duration_b_ms=place_duration_b,
                    execution_duration_a_ms=execution_duration_a_ms,
                    execution_duration_b_ms=execution_duration_b_ms,
                    attempt_a=attempt_a,
                    attempt_b=attempt_b,
                )
                return True, position
            
        except Exception as e:
            logger.exception(f"🚨 开仓执行异常: {str(e)}")
            return False, None
    
    async def execute_close(
        self,
        position: Position,
        exchange_a_price: Decimal,
        exchange_b_price: Decimal,
        exchange_a_quote_id: Optional[str] = None,
        exchange_b_quote_id: Optional[str] = None,
        signal_trigger_time: Optional[float] = None,
        close_quantity: Optional[Decimal] = None
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
        # ✅ 确定平仓数量
        if close_quantity is None:
            close_quantity = self.quantity  # 默认全部平仓
        else:
            # ✅ 部分平仓：检查数量
            if close_quantity > position.quantity:
                logger.warning(
                    f"⚠️ 平仓数量超过持仓:\n"
                    f"   尝试平仓: {close_quantity}\n"
                    f"   当前持仓: {position.quantity}\n"
                    f"   修正为: {position.quantity}"
                )
                close_quantity = position.quantity
        close_quantity = self._normalize_quantity(close_quantity, "平仓数量")
        
        # ✅ 计算信号触发 → 开始执行的延迟
        if signal_trigger_time:
            signal_to_execution_delay = (execution_start_time - signal_trigger_time) * 1000
            logger.info(f"⏱️ 信号触发 → 开始执行: {signal_to_execution_delay:.2f} ms")
        
        logger.info(
            f"📤 执行平仓:\n"
            f"   平仓数量: {close_quantity} / {position.quantity}\n"
            f"   {self.exchange_a.exchange_name} 平空 @ ${exchange_a_price}\n"
            f"   {self.exchange_b.exchange_name} 平多 @ ${exchange_b_price}"
        )
        
        try:
            # ✅ 1. 并行下单（首次尝试）
            logger.info("🚀 开始并行平仓（首次尝试）...")

            task_a = asyncio.create_task(
                self.exchange_a.place_close_order(
                    side='buy',
                    quantity=close_quantity,
                    price=exchange_a_price,
                    retry_mode='aggressive',
                    quote_id=exchange_a_quote_id
                )
            )

            task_b = asyncio.create_task(
                self.exchange_b.place_close_order(
                    side='sell',
                    quantity=close_quantity,
                    price=exchange_b_price,
                    retry_mode='aggressive',
                    quote_id=exchange_b_quote_id
                )
            )
            
            order_a_result, order_b_result = await asyncio.gather(task_a, task_b, return_exceptions=True)
               
            success_a = False
            success_b = False

            if isinstance(order_a_result, Exception):
                logger.error(f"❌ 交易所A 下单异常: {order_a_result}")
                if isinstance(order_a_result, lighter.exceptions.ApiException):
                    await self.handleLgApiExcep(order_a_result)  # 如果是 lighter 异常
                order_a_result = {'success': False, 'error': str(order_a_result)}
            else:
                success_a = order_a_result.get('success', False) or order_a_result.get('partial_fill', False)

            if isinstance(order_b_result, Exception):
                logger.error(f"❌ 交易所B 下单异常: {order_b_result}")
                if isinstance(order_b_result, lighter.exceptions.ApiException):
                    await self.handleLgApiExcep(order_b_result)
                order_b_result = {'success': False, 'error': str(order_a_result)}
            else:
                success_b = order_b_result.get('success', False) or order_b_result.get('partial_fill', False)
            
            # ✅ 2. 根据结果处理
            # 情况 1️⃣: 两所都失败 → 跳过
            if not success_a and not success_b:
                logger.warning(
                    f"⚠️ 平仓失败（两所都失败）→ 跳过...\n"
                    f"   {self.exchange_a.exchange_name}: {order_a_result.get('error')}\n"
                    f"   {self.exchange_b.exchange_name}: {order_b_result.get('error')}"
                )

                return False, None
            # 情况 2️⃣: A失败，B成功 → 重试A
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
                    quantity=close_quantity,
                    price=exchange_a_price,
                    retry_mode='aggressive',
                    quote_id=exchange_a_quote_id,
                    order_limiter=self.order_limiter_a,
                )

                if retry_result_a.get('success'):
                    order_a_result = retry_result_a
                    success_a = True
                    
                    logger.info(
                        f"✅ 平仓成功（A 所重试成功）:\n"
                        f"   {self.exchange_a.exchange_name}: {retry_result_a.get('order_id')}\n"
                        f"   {self.exchange_b.exchange_name}: {order_b_result.get('order_id')}\n"
                        f"   ⏱️ 总耗时: {(time.time() - execution_start_time) * 1000:.2f} ms \n"
                        f"   🕒 交易所A耗时: {(order_a_result.get('timestamp') - execution_start_time) * 1000:.2f} ms \n"
                        f"   🕒 交易所B耗时: {(order_b_result.get('timestamp') - execution_start_time) * 1000:.2f} ms \n"
                    )
                    
                else:
                    logger.error(
                        f"❌ {self.exchange_a.exchange_name} 重试失败，"
                        f"需要手动处理仓位！"
                    )
                    if self.lark_bot:
                        await self.lark_bot.send_text(
                            f"❌ {self.lark_index_text}{self.exchange_a.exchange_name} 重试失败，需要手动处理仓位！"
                        )
                    return False, None

            # 情况 3️⃣: A成功，B失败 → 重试 B
            if success_a and not success_b:
                logger.warning(
                    f"⚠️ {self.exchange_a.exchange_name} 成功，"
                    f"{self.exchange_b.exchange_name} 失败 → 重试 {self.exchange_b.exchange_name}..."
                )
                
                retry_result_b = await self._retry_place_order(
                    exchange=self.exchange_b,
                    order_type='close',
                    side='sell',
                    quantity=close_quantity,
                    price=exchange_b_price,
                    retry_mode='aggressive',
                    quote_id=exchange_b_quote_id,
                    order_limiter=self.order_limiter_b,
                )
                
                if retry_result_b.get('success'):
                    order_b_result = retry_result_b
                    success_b = True
                    
                    logger.info(
                        f"✅ 平仓成功（B 所重试成功）:\n"
                        f"   {self.exchange_a.exchange_name}: {order_a_result.get('order_id')}\n"
                        f"   {self.exchange_b.exchange_name}: {retry_result_b.get('order_id')}\n"
                        f"   ⏱️ 总耗时: {(time.time() - execution_start_time) * 1000:.2f} ms\n"
                        f"   🕒 交易所A耗时: {(order_a_result.get('timestamp') - execution_start_time) * 1000:.2f} ms\n"
                        f"   🕒 交易所B耗时: {(order_b_result.get('timestamp') - execution_start_time) * 1000:.2f} ms\n"
                    )
                else:
                    logger.critical(
                        f"🚨 {self.exchange_b.exchange_name} 平仓失败（重试后仍失败），"
                        f"需要手动处理！"
                    )
                    if self.lark_bot:
                        await self.lark_bot.send_text(
                            f"🚨 {self.lark_index_text}{self.exchange_b.exchange_name} 平仓失败（重试后仍失败），需要手动处理！"
                        )
                    return False, None
            # 情况 4️⃣: 两所都成功 → 完成
            if success_a and success_b:
                self.sleep_retries = 0
                # ✅ 3. 到这里两所都成功了，检查成交数量
                filled_qty_a = order_a_result.get('filled_quantity', position.quantity)
                filled_qty_b = order_b_result.get('filled_quantity', position.quantity)
                place_duration_a_ms = order_a_result.get('place_duration_ms', 0)
                place_duration_b_ms = order_b_result.get('place_duration_ms', 0)

                execution_duration_a_ms = order_a_result.get('execution_duration_ms', 0)
                execution_duration_b_ms = order_b_result.get('execution_duration_ms', 0)
                attempt_a = order_a_result.get('attempt', 0)
                attempt_b = order_b_result.get('attempt', 0)
                logger.info(
                    f"📊 初始成交结果:\n"
                    f"   {self.exchange_a.exchange_name}: {filled_qty_a} / {position.quantity}\n"
                    f"   {self.exchange_b.exchange_name}: {filled_qty_b} / {position.quantity}\n"
                    f"   下单耗时:\n"
                    f"   {self.exchange_a.exchange_name}: {place_duration_a_ms:.2f} ms\n"
                    f"   {self.exchange_b.exchange_name}: {place_duration_b_ms:.2f} ms\n"
                    f"   执行耗时:\n"
                    f"   {self.exchange_a.exchange_name}: {execution_duration_a_ms:.2f} ms\n"
                    f"   {self.exchange_b.exchange_name}: {execution_duration_b_ms:.2f} ms\n"
                    f"   {self.exchange_a.exchange_name} 重试次数: {attempt_a}\n"
                    f"   {self.exchange_b.exchange_name} 重试次数: {attempt_b}\n"
                )
                
                # ✅ 4. 平衡仓位（关键！）
                balanced_qty_a, balanced_qty_b = await self._balance_positions(
                    target_quantity=close_quantity,
                    filled_qty_a=filled_qty_a,
                    filled_qty_b=filled_qty_b,
                    side_a='buy',
                    side_b='sell',
                    price_a=exchange_a_price,
                    price_b=exchange_b_price,
                    operation_type='close',
                    order_a_id=order_a_result.get('order_id'),  # ✅ 新增
                    order_b_id=order_b_result.get('order_id')  # ✅ 新增
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
                # 新增延迟和尝试次数记录
                position.place_duration_a_ms = place_duration_a_ms
                position.place_duration_b_ms = place_duration_b_ms
                position.execution_duration_a_ms = execution_duration_a_ms
                position.execution_duration_b_ms = execution_duration_b_ms
                position.attempt_a = attempt_a
                position.attempt_b = attempt_b
                # ✅ 只在有效 Position 时计算质量报告
                if (position.exchange_a_signal_entry_price > 0 and 
                    position.exchange_b_signal_entry_price > 0 and
                    position.exchange_a_order_id != 'DUMMY'):
                    
                    quality_report = position.get_execution_quality_report()
                    logger.info(
                        f"✅ 反向开仓成功:\n"
                        f"   {self.exchange_a.exchange_name}:\n"
                        f"      订单 ID: {order_a_result.get('order_id')}\n"
                        f"      信号价格: ${exchange_a_price}\n"
                        f"      成交价格: ${actual_price_a}\n"
                        f"      滑点: {quality_report['exit_slippage']['exchange_a']:+.4f}%\n"
                        f"      成交数量: {balanced_qty_a} / {position.quantity}\n"
                        f"   {self.exchange_b.exchange_name}:\n"
                        f"      订单 ID: {order_b_result.get('order_id')}\n"
                        f"      信号价格: ${exchange_b_price}\n"
                        f"      成交价格: ${actual_price_b}\n"
                        f"      滑点: {quality_report['exit_slippage']['exchange_b']:+.4f}%\n"
                        f"      成交数量: {balanced_qty_b} / {position.quantity}\n"

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
                else:
                    # ✅ 虚拟 Position：简化日志
                    logger.info(
                        f"✅ 平仓成功 (反向开仓):\n"
                        f"   {self.exchange_a.exchange_name}:\n"
                        f"      订单 ID: {order_a_result.get('order_id')}\n"
                        f"      成交价格: ${actual_price_a}\n"
                        f"      成交数量: {balanced_qty_a} / {position.quantity}\n"
                        f"   {self.exchange_b.exchange_name}:\n"
                        f"      订单 ID: {order_b_result.get('order_id')}\n"
                        f"      成交价格: ${actual_price_b}\n"
                        f"      成交数量: {balanced_qty_b} / {position.quantity}\n"
                        f"   ⏱️ 执行耗时: {execution_delay_ms:.2f} ms"
                    )

                return True, position
        
        except Exception as e:
            logger.exception(f"🚨 平仓执行异常: {str(e)}")
            return False, None
        
    # ✅ 紧急平仓方法
    async def _emergency_close_a(self, order_id: str, quantity: Decimal):
        """紧急平仓 A 所（单边持仓风险处理）"""
        try:
            logger.warning(f"🚨 紧急平仓 {self.exchange_a.exchange_name}: {order_id}")
            
            # ✅ 获取最新价格
            orderbook = await self.exchange_a.get_latest_orderbook(quantity)
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
                order_limiter=self.order_limiter_a
            )
            
            if result.get('success'):
                logger.info(f"✅ 紧急平仓成功: {result.get('order_id')}")
            else:
                logger.critical(f"🚨 紧急平仓失败，需要手动处理！")
                if self.lark_bot:
                    await self.lark_bot.send_text(
                        f"🚨 {self.lark_index_text}{self.exchange_a.exchange_name} 紧急平仓失败，需要手动处理！"
                    )

        except Exception as e:
            logger.exception(f"❌ 紧急平仓异常: {e}")
    
    async def _emergency_close_b(self, order_id: str, quantity: Decimal):
        """紧急平仓 B 所（单边持仓风险处理）"""
        try:
            logger.warning(f"🚨 紧急平仓 {self.exchange_b.exchange_name}: {order_id}")
            
            # ✅ 获取最新价格
            orderbook = await self.exchange_b.get_latest_orderbook(quantity)
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
                order_limiter=self.order_limiter_b
            )
            
            if result.get('success'):
                logger.info(f"✅ 紧急平仓成功: {result.get('order_id')}")
            else:
                logger.critical(f"🚨 紧急平仓失败，需要手动处理！")
                if self.lark_bot:
                    await self.lark_bot.send_text(
                        f"🚨 {self.lark_index_text}{self.exchange_b.exchange_name} 紧急平仓失败，需要手动处理！"
                    )
        
        except Exception as e:
            logger.exception(f"❌ 紧急平仓异常: {e}")

    async def _close_position(
        self,
        exchange: ExchangeAdapter,
        side: str,
        quantity: Decimal,
        price: Decimal,
        order_id: Optional[str] = None,
        order_limiter: Optional[AsyncLimiter] = None
    ):
        """平仓辅助方法"""
        try:
            logger.info(
                f"🔄 执行平仓:\n"
                f"   交易所: {exchange.exchange_name}\n"
                f"   方向: {side}\n"
                f"   数量: {quantity}\n"
                f"   原订单 ID: {order_id or 'N/A'}"  # ✅ 添加这一行
            )
            result = await self._retry_place_order(
                exchange=exchange,
                order_type='close',
                side=side,
                quantity=quantity,
                price=price,
                retry_mode='aggressive',
                order_limiter=order_limiter
            )
            
            if result.get('success'):
                logger.info(
                    f"✅ 平仓成功:\n"
                    f"   交易所: {exchange.exchange_name}\n"
                    f"   原订单: {order_id or 'N/A'}\n"  # ✅ 添加这一行
                    f"   平仓订单: {result.get('order_id')}"  # ✅ 添加这一行
                )
            else:
                logger.error(
                    f"❌ 平仓失败:\n"
                    f"   交易所: {exchange.exchange_name}\n"
                    f"   原订单: {order_id or 'N/A'}\n"  # ✅ 添加这一行
                    f"   错误: {result.get('error')}"  # ✅ 添加这一行
                )
        
        except Exception as e:
            logger.exception(
                f"❌ 平仓异常:\n"
                f"   交易所: {exchange.exchange_name}\n"
                f"   原订单: {order_id or 'N/A'}\n"  # ✅ 添加这一行
                f"   异常: {e}"  # ✅ 添加这一行
            )

    async def check_position_balance(self):
        logger.info("🔍 检查两所仓位平衡情况...")
        symbol_a = self.exchange_a.symbol
        symbol_b = self.exchange_b.symbol
        portfolio_a = await self.exchange_a.client.get_portfolio()

        if portfolio_a:
            balance_a = portfolio_a.get('balance')
            upnl_a = portfolio_a.get('upnl')
            logger.info(f"📤 交易所A 权益: 账号余额: {balance_a}, upnl: { upnl_a }")
        else:
            logger.error(f"❌ 交易所A 获取投资组合失败")

        portfolio_b = await self.exchange_b.client.get_portfolio()

        if not (portfolio_b and 'balance' in portfolio_b):
            logger.error(f"❌ 交易所B 获取投资组合失败")
        else: 
            balance_b = float(portfolio_b.get('balance'))
            upnl_b = float(portfolio_b.get('upnl'))
            logger.info(f"📤 交易所B 权益: 账号余额: {balance_b}, upnl: { upnl_b }")

        # 检查仓位是否平衡
        pos_a = await self.exchange_a.get_position(symbol_a)
        pos_b = await self.exchange_b.get_position(symbol_b)
        pos_a_size = pos_a['size'] if pos_a else Decimal('0')
        pos_a_side = pos_a['side'] if pos_a else 'neutral'
        pos_b_size = pos_b['size'] if pos_b else Decimal('0')
        pos_b_side = pos_b['side'] if pos_b else 'neutral'
        if pos_a_size == 0 and pos_b_size == 0:
            logger.info("✅ 两所均无持仓")
            return
        logger.info(f"🔍 校验仓位平衡: {self.exchange_a.exchange_name} {pos_a_side} {pos_a_size}, "
                    f": {self.exchange_b.exchange_name} {pos_b_side} {pos_b_size}")
        if abs(pos_a_size - pos_b_size) < self.quantity_precision * 10 and pos_a_side != pos_b_side:
            logger.info("✅ 仓位平衡，无需调整")
            return
        # 请求订单簿restful接口
        exchange_a_bid_price, exchange_a_ask_price, _ = await self.exchange_a.client.fetch_bbo_prices(symbol_a)

        if pos_a_size > pos_b_size:
            diff_size = pos_a_size - pos_b_size
            if pos_a_side == 'short' and pos_b_side == 'long':
                # Exchange A 空头多于 Exchange B，平A
                logger.info(f"🔄 调整仓位: 在 {self.exchange_a.exchange_name} 买入 {diff_size} {symbol_a} 以平衡仓位")
                await self._retry_place_order(
                    exchange=self.exchange_a,
                    order_type='close',
                    side='buy',
                    quantity=diff_size,
                    price=exchange_a_ask_price,
                    retry_mode='aggressive',
                    order_limiter=self.order_limiter_a,
                )
            elif pos_a_side == 'long' and pos_b_side == 'short':
                # Exchange A 多头多于 Exchange B，多卖出差额
                logger.info(f"🔄 调整仓位: 在 {self.exchange_a.exchange_name} 卖出 {diff_size} {symbol_a} 以平衡仓位")
                await self._retry_place_order(
                    exchange=self.exchange_a,
                    order_type='open',
                    side='sell',
                    quantity=diff_size,
                    price=exchange_a_bid_price,
                    retry_mode='aggressive',
                    order_limiter=self.order_limiter_a,
                )
            elif pos_a_side == 'long' and pos_b_side == 'long':
                logger.info(f'🔄 两边仓位方向相同:long, 调整仓位: 在 {self.exchange_a.exchange_name} 卖出 {pos_a_size + pos_b_size} {symbol_a} 以平衡仓位')
                await self._retry_place_order(
                    exchange=self.exchange_a,
                    order_type='open',
                    side='sell',
                    quantity=pos_a_size + pos_b_size,
                    price=exchange_a_bid_price,
                    retry_mode='aggressive',
                    order_limiter=self.order_limiter_a,
                )
            elif pos_a_side == 'short' and pos_b_side == 'short':
                logger.info(f'🔄 两边仓位方向相同:short,调整仓位: 在 {self.exchange_a.exchange_name} 买入 {pos_a_size + pos_b_size} {symbol_a} 以平衡仓位 ')
                await self._retry_place_order(
                    exchange=self.exchange_a,
                    order_type='close',
                    side='buy',
                    quantity=pos_a_size + pos_b_size,
                    price=exchange_a_ask_price,
                    retry_mode='aggressive',
                    order_limiter=self.order_limiter_a,
                )
            elif pos_b_side == 'neutral':
                if pos_a_side == 'long':
                    logger.info(f"🔄 调整仓位: 在 {self.exchange_a.exchange_name} 卖出 {diff_size} {symbol_a} 以平衡仓位")
                    await self._retry_place_order(
                        exchange=self.exchange_a,
                        order_type='open',
                        side='sell',
                        quantity=diff_size,
                        price=exchange_a_bid_price,
                        retry_mode='aggressive',
                        order_limiter=self.order_limiter_a,
                    )
                elif pos_a_side == 'short':
                    logger.info(f"🔄 调整仓位: 在 {self.exchange_a.exchange_name} 买入 {diff_size} {symbol_a} 以平衡仓位")
                    await self._retry_place_order(
                        exchange=self.exchange_a,
                        order_type='close',
                        side='buy',
                        quantity=diff_size,
                        price=exchange_a_ask_price,
                        retry_mode='aggressive',
                        order_limiter=self.order_limiter_a,
                    )
        if pos_b_size > pos_a_size:
            diff_size = pos_b_size - pos_a_size
            if pos_a_side == 'short' and pos_b_side == 'long':
                # Exchange A 空头少于 Exchange B, A 卖出差额
                logger.info(f"🔄 调整仓位: 在 {self.exchange_a.exchange_name} 买入 {diff_size} {symbol_a} 以平衡仓位")
                await self._retry_place_order(
                    exchange=self.exchange_a,
                    order_type='open',
                    side='sell',
                    quantity=diff_size,
                    price=exchange_a_bid_price,
                    retry_mode='aggressive',
                    order_limiter=self.order_limiter_a,
                )
            elif pos_a_side == 'long' and pos_b_side == 'short':
                # Exchange A 多头少于 Exchange B，A买入差额
                logger.info(f"🔄 调整仓位: 在 {self.exchange_a.exchange_name} 卖出 {diff_size} {symbol_a} 以平衡仓位")
                await self._retry_place_order(
                    exchange=self.exchange_a,
                    order_type='close',
                    side='buy',
                    quantity=diff_size,
                    price=exchange_a_ask_price,
                    retry_mode='aggressive',
                    order_limiter=self.order_limiter_a,
                )
            elif pos_a_side == 'long' and pos_b_side == 'long':
                logger.info(f'🔄 两边仓位方向相同:long, 调整仓位: 在 {self.exchange_a.exchange_name} 卖出 {pos_a_size + pos_b_size} {symbol_a} 以平衡仓位')
                await self._retry_place_order(
                    exchange=self.exchange_a,
                    order_type='open',
                    side='sell',
                    quantity=pos_a_size + pos_b_size,
                    price=exchange_a_bid_price,
                    retry_mode='aggressive',
                    order_limiter=self.order_limiter_a,
                )
            elif pos_a_side == 'short' and pos_b_side == 'short':
                logger.info(f'🔄 两边仓位方向相同:short,调整仓位: 在 {self.exchange_a.exchange_name} 买入 {pos_a_size + pos_b_size} {symbol_a} 以平衡仓位 ')
                await self._retry_place_order(
                    exchange=self.exchange_a,
                    order_type='close',
                    side='buy',
                    quantity=pos_a_size + pos_b_size,
                    price=exchange_a_bid_price,
                    retry_mode='aggressive',
                    order_limiter=self.order_limiter_a,
                )
            elif pos_a_side == 'neutral':
                if pos_b_side == 'long':
                    logger.info(f"🔄 调整仓位: 在 {self.exchange_a.exchange_name} 卖出 {pos_b_size} {symbol_b} 以平衡仓位")
                    await self._retry_place_order(
                        exchange=self.exchange_a,
                        order_type='open',
                        side='sell',
                        quantity=pos_b_size,
                        price=exchange_a_bid_price,
                        retry_mode='aggressive',
                        order_limiter=self.order_limiter_a,
                    )
                elif pos_b_side == 'short':
                    logger.info(f"🔄 调整仓位: 在 {self.exchange_a.exchange_name} 买入 {pos_b_size} {symbol_b} 以平衡仓位")
                    await self._retry_place_order(
                        exchange=self.exchange_a,
                        order_type='close',
                        side='buy',
                        quantity=pos_b_size,
                        price=exchange_a_ask_price,
                        retry_mode='aggressive',
                        order_limiter=self.order_limiter_a,
                    )

        pos_a = await self.exchange_a.get_position(symbol_a)
        pos_b = await self.exchange_b.get_position(symbol_b)
        pos_a_size = pos_a['size'] if pos_a else Decimal('0')
        pos_a_side = pos_a['side'] if pos_a else 'neutral'
        pos_b_size = pos_b['size'] if pos_b else Decimal('0')
        pos_b_side = pos_b['side'] if pos_b else 'neutral'
        logger.info(f"🔍 重新校验仓位平衡: {self.exchange_a.exchange_name} {pos_a_side} {pos_a_size}, "
                    f": {self.exchange_b.exchange_name} {pos_b_side} {pos_b_size}")
        if pos_a_size == pos_b_size and pos_a_size == 0:
            logger.info("✅ 仓位检测后实现仓位平衡，无需调整")
        elif pos_a_size == pos_b_size and ((pos_a_side == 'long' and pos_b_side == 'short') or (pos_a_side == 'short' and pos_b_side == 'long')):
            logger.info("✅ 仓位检测后实现仓位平衡，无需调整")
        else:
            logger.error("❌ 仓位检测后仓位仍不平衡，请手动检查")
            # 飞书通知
            if self.lark_bot:
                await self.lark_bot.send_text(
                    f"❌ {self.lark_index_text}仓位检测后仓位仍不平衡，需要手动处理仓位！"
                    f" {self.exchange_a.exchange_name} {symbol_a} {pos_a_side} {pos_a_size}"
                    f" {self.exchange_b.exchange_name} {symbol_b} {pos_b_side} {pos_b_size}"
                )

    async def handleLgApiExcep(self, e):
        if hasattr(e, 'body') and e.body:
            data = json.loads(e.body)
            error_code = data.get('code', 'Unknown')
            error_msg = data.get('message', str(e))
            logger.error(f"❌ Lighter 下单 API 错误: {error_code} - {error_msg}")
            if error_code =='23000' or error_code == 23000:
                self.sleep_retries = self.sleep_retries + 1
                sleep_interval = self.sleep_interval if self.sleep_retries == 1 else self.sleep_interval_enhance
                logger.info(f"{error_msg}, 等待{sleep_interval}s")
                await asyncio.sleep(sleep_interval)