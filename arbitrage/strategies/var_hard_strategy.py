"""
Variational 硬刷策略
功能：
1. 监控订单簿数据并记录
2. 当点差 < 0.0026% 时，使用同一个 quote_id 同时下买卖单
3. 记录订单簿价格、成交价格、滑点等数据用于分析
"""

import asyncio
import random
import time
import csv
from decimal import Decimal
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Dict
import logging

from ..exchanges.variational_adapter import VariationalAdapter

logger = logging.getLogger(__name__)


class VarHardStrategy:
    """Variational 硬刷策略"""
    
    def __init__(
        self,
        symbol: str,
        exchange: VariationalAdapter,
        quantity: Decimal,
        quantity_range: tuple = (Decimal('0.0011'), Decimal('0.0033')),
        spread_threshold: Decimal = Decimal('0.0026'),  # 点差阈值 0.0026%
        max_slippage: Decimal = Decimal('0.0005'),  # 最大滑点 0.05%
        cooldown_seconds: float = 5.0,  # 冷却时间
        cooldown_range: tuple = (3.0, 6.0),  # ✅ 新增：冷却时间范围（秒）
        poll_interval: float = 0.3,  # 轮询间隔（秒）
        data_dir: Path = None,
        monitor_only: bool = False,
        daily_file: bool = True,
        lark_bot: Any | None = None
    ):
        self.symbol = symbol
        self.exchange = exchange
        self.quantity = quantity
        self.quantity_range = quantity_range  # ✅ 新增：随机数量范围
        self.spread_threshold = spread_threshold
        self.max_slippage = max_slippage
        self.cooldown_seconds = cooldown_seconds
        self.cooldown_range = cooldown_range  # ✅ 新增：冷却时间范围
        self.poll_interval = poll_interval
        self.monitor_only = monitor_only
        self.lark_bot = lark_bot
        self.daily_file = daily_file
        
        # 数据记录
        self.data_dir = data_dir or Path('data/var_hard')
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.current_date = datetime.now().strftime('%Y%m%d')

        # CSV 文件
        if daily_file:
            self._update_csv_files()

            # 按天生成文件：同一天的所有运行记录到同一个文件
            # date_str = datetime.now().strftime('%Y%m%d')
            # self.orderbook_csv = self.data_dir / f'orderbook_{symbol}_{date_str}.csv'
            # self.trades_csv = self.data_dir / f'trades_{symbol}_{date_str}.csv'
            
            # # 如果文件不存在，创建并写入表头
            # if not self.orderbook_csv.exists():
            #     self._init_orderbook_csv()
            # if not self.trades_csv.exists():
            #     self._init_trades_csv()
        else:
            # 每次运行生成新文件
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            self.orderbook_csv = self.data_dir / f'orderbook_{symbol}_{timestamp}.csv'
            self.trades_csv = self.data_dir / f'trades_{symbol}_{timestamp}.csv'
            self._init_csv_files()
        
        # 状态
        self.is_running = False
        self.last_order_time = -float('inf')
        self.trade_count = 0
        self._monitor_task = None
        
        # 统计
        self.stats = {
            'orderbook_samples': 0,
            'spread_opportunities': 0,
            'trades_attempted': 0,
            'trades_success': 0,
            'trades_partial': 0,
            'trades_failed': 0,
            'cooldown_skipped': 0
        }
        
        logger.info(
            f"🎯 Variational 硬刷策略初始化:\n"
            f"   Symbol: {symbol}\n"
            f"   Quantity: {quantity_range[0]} - {quantity_range[1]}\n"
            f"   Spread Threshold: {spread_threshold}%\n"
            f"   Max Slippage: {max_slippage * 100}%\n"
            f"   Cooldown: {cooldown_range[0]} - {cooldown_range[1]}s\n"
            f"   Poll Interval: {poll_interval}s\n"
            f"   Monitor Only: {monitor_only}\n"
            f"   Data Dir: {self.data_dir}"
        )
    
    def _init_csv_files(self):
        """初始化 CSV 文件"""
        # 订单簿数据 CSV
        self._init_orderbook_csv()
        
        # 交易数据 CSV
        self._init_trades_csv()
        logger.info(
            f"✅ CSV 文件已创建:\n"
            f"   订单簿: {self.orderbook_csv}\n"
            f"   交易: {self.trades_csv}"
        )

    def _update_csv_files(self):
        """更新 CSV 文件（按日期切分）"""
        date_str = datetime.now().strftime('%Y%m%d')
        
        # 如果日期变化了，记录日志
        if date_str != self.current_date:
            logger.info(f"📅 日期切换: {self.current_date} → {date_str}，创建新文件")
            self.current_date = date_str
        
        # 设置文件路径
        self.orderbook_csv = self.data_dir / f'orderbook_{self.symbol}_{date_str}.csv'
        self.trades_csv = self.data_dir / f'trades_{self.symbol}_{date_str}.csv'
        
        # 如果文件不存在，创建并写入表头
        if not self.orderbook_csv.exists():
            self._init_orderbook_csv()
        
        if not self.trades_csv.exists():
            self._init_trades_csv()
        
    def _init_orderbook_csv(self):
        """初始化订单簿 CSV"""
        with open(self.orderbook_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'timestamp',
                'datetime',
                'bid_price',
                'bid_size',
                'ask_price',
                'ask_size',
                'spread_abs',
                'spread_pct',
                'mid_price',
                'quote_id',
                'fetch_duration_ms',
                'quantity'
            ])

    def _init_trades_csv(self):
        """初始化交易 CSV"""
        with open(self.trades_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'trade_id',
                'order_timestamp',
                'order_datetime',
                'record_timestamp',
                'record_datetime',
                'quantity',
                'ob_bid_price',
                'ob_ask_price',
                'ob_spread_pct',
                'quote_id',
                'buy_order_id',
                'buy_success',
                'buy_filled_price',
                'buy_filled_qty',
                'buy_duration_ms',
                'buy_slippage_abs',
                'buy_slippage_pct',
                'sell_order_id',
                'sell_success',
                'sell_filled_price',
                'sell_filled_qty',
                'sell_duration_ms',
                'sell_slippage_abs',
                'sell_slippage_pct',
                'actual_spread_pct',
                'spread_loss_pct',
                'total_slippage_pct',
                'status'
            ])
    async def start(self):
        """启动策略"""
        logger.info(f"🚀 启动 Variational 硬刷策略: {self.symbol}")
        
        self.is_running = True
        
        # 启动订单簿监控任务
        self._monitor_task = asyncio.create_task(self._monitor_orderbook())
        
        logger.info(f"✅ 策略已启动")
    
    async def stop(self):
        """停止策略"""
        logger.info(f"⏹️ 停止策略...")
        
        self.is_running = False
        
        # 取消监控任务
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        
        # 打印统计
        self._print_stats()
        
        logger.info(f"✅ 策略已停止")
    
    async def _monitor_orderbook(self):
        """监控订单簿（轮询模式）"""
        logger.info(f"📊 开始监控订单簿（每 {self.poll_interval}s 轮询一次）")
        
        while self.is_running:
            try:
                # ========== 1. 获取报价（订单簿数据） ==========
                fetch_start = time.time()  # ✅ 记录开始时间
                current_quantity = self._get_random_quantity()

                quote_data = await self.exchange.client._fetch_indicative_quote(
                    qty=current_quantity,
                    contract_id=f"{self.symbol}-PERP"
                )
                fetch_duration = (time.time() - fetch_start) * 1000  # ✅ 计算耗时（毫秒）

                if not quote_data or 'quote_id' not in quote_data:
                    logger.warning(f"⚠️ 获取报价失败，跳过本次轮询, quote_data: {quote_data}")
                    await asyncio.sleep(self.poll_interval)
                    continue
                
                # 解析订单簿数据
                bid_price = Decimal(str(quote_data.get('bid', '0')))
                ask_price = Decimal(str(quote_data.get('ask', '0')))
                bid_size = Decimal(str(quote_data.get('bid_size', '0')))
                ask_size = Decimal(str(quote_data.get('ask_size', '0')))
                quote_id = quote_data['quote_id']
                
                # ========== 2. 记录订单簿数据 ==========
                await self._record_orderbook(
                    bid_price=bid_price,
                    ask_price=ask_price,
                    bid_size=bid_size,
                    ask_size=ask_size,
                    quote_id=quote_id,
                    fetch_duration=fetch_duration,
                    quantity=current_quantity
                )
                
                # ========== 3. 检查是否满足交易条件 ==========
                spread_abs = ask_price - bid_price
                spread_pct = (spread_abs / ask_price * 100)
                
                # 检查点差是否小于阈值
                if spread_pct >= self.spread_threshold:
                    await asyncio.sleep(self.poll_interval)
                    continue
                
                self.stats['spread_opportunities'] += 1
                
                # 检查冷却期
                current_time = time.time()
                time_since_last_order = current_time - self.last_order_time
                random_cooldown = random.uniform(self.cooldown_range[0], self.cooldown_range[1])

                if time_since_last_order < random_cooldown:
                    self.stats['cooldown_skipped'] += 1
                    remaining = random_cooldown - time_since_last_order
                    logger.info(
                        f"⏳ 冷却期内，跳过交易 "
                        f"(距上次下单 {time_since_last_order:.1f}s，还需等待 {remaining:.1f}s)"
                    )
                    await asyncio.sleep(remaining)
                    continue
                
                # ========== 4. 执行交易 ==========
                logger.info(
                    f"🎯 检测到交易机会:\n"
                    f"   Bid: ${bid_price} \n"
                    f"   Ask: ${ask_price} \n"
                    f"   点差: {spread_pct:.6f}% < 阈值: {self.spread_threshold}%\n"
                    f"   数量: {current_quantity}\n"
                    f"⏱️  获取订单簿耗时: {fetch_duration:.1f}ms\n"
                    f"⏱️  距上次下单: {time_since_last_order:.1f}s\n"  # ✅ 新增
                    f"   Quote ID: {quote_id}"
                )
                self.last_order_time = current_time

                if self.monitor_only:
                    # 监控模式：只记录，不交易
                    logger.info("📊 监控模式：跳过实际交易")
                    await self._record_virtual_trade(
                        bid_price=bid_price,
                        ask_price=ask_price,
                        spread_pct=spread_pct,
                        quote_id=quote_id
                    )
                else:
                    # 实际交易
                    await self._execute_trade(
                        bid_price=bid_price,
                        ask_price=ask_price,
                        spread_pct=spread_pct,
                        quote_id=quote_id,
                        fetch_duration=fetch_duration,
                        quantity=current_quantity
                    )
                                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ 监控订单簿失败: {e}", exc_info=True)
            
            # 等待下次轮询
            await asyncio.sleep(self.poll_interval)
    
    async def _record_orderbook(
        self,
        bid_price: Decimal,
        ask_price: Decimal,
        bid_size: Decimal,
        ask_size: Decimal,
        quote_id: str,
        fetch_duration: float,
        quantity: Decimal
    ):
        """记录订单簿数据"""
        try:
            if self.daily_file:
                self._update_csv_files()

            timestamp = time.time()
            spread_abs = ask_price - bid_price
            spread_pct = (spread_abs / ask_price * 100)
            mid_price = (bid_price + ask_price) / 2
            
            with open(self.orderbook_csv, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    f'{timestamp:.6f}',
                    datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                    str(bid_price),
                    str(bid_size),
                    str(ask_price),
                    str(ask_size),
                    str(spread_abs),
                    f'{spread_pct:.6f}',
                    str(mid_price),
                    quote_id,
                    f'{fetch_duration:.2f}',
                    str(quantity)
                ])
            
            self.stats['orderbook_samples'] += 1
            
        except Exception as e:
            logger.error(f"❌ 记录订单簿数据失败: {e}")

    def _get_random_quantity(self) -> Decimal:
        """生成随机数量"""
        min_qty, max_qty = self.quantity_range
        # 生成随机浮点数，转换为 Decimal
        random_float = random.uniform(float(min_qty), float(max_qty))
        # 保留 4 位小数
        random_qty = Decimal(str(random_float)).quantize(Decimal('0.0001'))
        return random_qty
    
    async def _execute_trade(
        self,
        bid_price: Decimal,
        ask_price: Decimal,
        spread_pct: Decimal,
        quote_id: str,
        fetch_duration: float,
        quantity: Decimal
    ):
        """执行交易（同时下买卖单）"""
        self.stats['trades_attempted'] += 1
        self.trade_count += 1
        
        trade_id = f"{self.symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{self.trade_count}"
        order_time = time.time()
        order_datetime = datetime.fromtimestamp(order_time).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        logger.info(f"📤 开始执行交易 #{self.trade_count},数量: {quantity}, ⏱️下单时间: {order_datetime} (Quote ID: {quote_id})")

        # 定义买单和卖单任务
        async def place_buy_order(max_retries=3, retry_delay=0.01):
            """下买单"""
            for attempt in range(max_retries):  # ✅ 重试3次
                start = time.time()
                try:
                    result = await self.exchange.client._place_market_order(
                        quote_id=quote_id,
                        side='buy',
                        max_slippage=float(self.max_slippage)
                    )
                    buy_success_time = time.time()
                    duration = (buy_success_time - start) * 1000
                    if result and result.order_id:
                        logger.info(f"✅ 买单下单成功(尝试 {attempt + 1}/{max_retries}): 订单ID {result.order_id},⏱️ 时间: {buy_success_time:.2f}, ⏱️ 耗时: {duration:.2f}ms")

                        return {
                            'success': result.success,
                            'order_id': result.order_id,
                            'duration_ms': duration,
                            'error': None
                        }
                    else:
                        error_msg = result.error_message if result else "返回结果为空"
                        logger.warning(f"⚠️ 买单下单失败 (尝试 {attempt + 1}/{max_retries}): {error_msg}。 {retry_delay}s 后重试...")
                        await asyncio.sleep(retry_delay)

                except Exception as e:
                    duration = (time.time() - start) * 1000
                    logger.error(f"❌ 买单异常 (尝试 {attempt + 1}/{max_retries}): {e}。 {retry_delay}s 后重试...")
                    await asyncio.sleep(retry_delay)
            logger.error(f"❌ 买单在重试 {max_retries} 次后彻底失败。")
            return {'success': False, 'order_id': None, 'duration_ms': 0, 'error': f"重试 {max_retries} 次后失败"}

        async def place_sell_order(max_retries=3, retry_delay=0.01):
            """下卖单"""
            for attempt in range(max_retries):
                start = time.time()
                try:
                    result = await self.exchange.client._place_market_order(
                        quote_id=quote_id,
                        side='sell',
                        max_slippage=float(self.max_slippage)
                    )
                    duration = (time.time() - start) * 1000

                    if result and result.order_id:
                        logger.info(f"✅ 卖单下单成功 (尝试 {attempt + 1}/{max_retries}): 订单ID {result.order_id}, ⏱️ 耗时: {duration:.2f}ms")
                        return {
                            'success': True,
                            'order_id': result.order_id,
                            'duration_ms': duration,
                            'error': None
                        }
                    else:
                        error_msg = result.error_message if result else "返回结果为空"
                        logger.warning(f"⚠️ 卖单下单失败 (尝试 {attempt + 1}/{max_retries}): {error_msg}。 {retry_delay}s 后重试...")
                        await asyncio.sleep(retry_delay)

                except Exception as e:
                    duration = (time.time() - start) * 1000
                    logger.error(f"❌ 卖单异常 (尝试 {attempt + 1}/{max_retries}): {e}。 {retry_delay}s 后重试...")
                    await asyncio.sleep(retry_delay)

            logger.error(f"❌ 卖单在重试 {max_retries} 次后彻底失败。")
            return {'success': False, 'order_id': None, 'duration_ms': 0, 'error': f"重试 {max_retries} 次后失败"}

        async def getOrderInfo(buyOrderId, sellOrderId, max_retries=10):
            """获取订单信息"""
            order_info_start = time.time()
            buyOrderInfo = None
            sellOrderInfo = None
            buy_found_attempt = None
            sell_found_attempt = None
            for attempt in range(max_retries):
                try:
                    n_start_time = time.time()
                    result = await self.exchange.client.get_orders_history()
                    for order in result['result']:
                        if order.get('rfq_id') == buyOrderId and buyOrderInfo is None:
                            buyOrderInfo = order
                            buy_found_attempt = attempt + 1
                            logger.info(f"✅ 第 {buy_found_attempt} 次尝试成功获取买单信息")
                        if order.get('rfq_id') == sellOrderId and sellOrderInfo is None:
                            sellOrderInfo = order
                            sell_found_attempt = attempt + 1
                            logger.info(f"✅ 第 {sell_found_attempt} 次尝试成功获取卖单信息")
                    
                    current_time = time.time()
                    duration_ms = (current_time - n_start_time) * 1000
                    cumulative_ms = (current_time - order_info_start) * 1000
                    logger.info(
                        f"📥 第 {attempt + 1} 次查询订单历史: "
                        f"响应时间={current_time:.2f}, "
                        f"耗时={duration_ms:.2f}ms, "
                        f"累计={cumulative_ms:.2f}ms, "
                        f"买单={'已获取' if buyOrderInfo else '未获取'}, "
                        f"卖单={'已获取' if sellOrderInfo else '未获取'}"
                    )
                    if buyOrderInfo and sellOrderInfo:
                        logger.info(
                            f"🎉 两个订单信息都已获取: "
                            f"买单第 {buy_found_attempt} 次, "
                            f"卖单第 {sell_found_attempt} 次"
                        )
                        return {
                            'buy': buyOrderInfo,
                            'sell': sellOrderInfo,
                            'buy_found_attempt': buy_found_attempt,
                            'sell_found_attempt': sell_found_attempt
                        }

                    # ✅ 如果为空，等待后重试
                    if attempt < max_retries - 1:
                        wait_time = 0.005  # 0.005s 固定等待时间

                        missing = []
                        if not buyOrderInfo:
                            missing.append(f"买单({buyOrderId})")
                        if not sellOrderInfo:
                            missing.append(f"卖单({sellOrderId})")
                        
                        logger.info(
                            f"⏳ 订单信息未完整，缺少: {', '.join(missing)}，"
                            f"{wait_time}s 后重试 (尝试 {attempt + 1}/{max_retries})"
                        )
                        await asyncio.sleep(wait_time)
                    else:
                        # ✅ 达到最大重试次数，返回已获取到的信息
                        logger.warning(
                            f"⚠️ 重试 {max_retries} 次后: "
                            f"买单={'已获取(第'+str(buy_found_attempt)+'次)' if buyOrderInfo else '未获取'}, "
                            f"卖单={'已获取(第'+str(sell_found_attempt)+'次)' if sellOrderInfo else '未获取'}"
                        )
                        return {
                            'buy': buyOrderInfo,
                            'sell': sellOrderInfo,
                            'buy_found_attempt': buy_found_attempt,
                            'sell_found_attempt': sell_found_attempt
                        }
                        
                except Exception as e:
                    if attempt < max_retries - 1:
                        wait_time = 0.3 * (attempt + 1)
                        logger.warning(
                            f"❌ 获取订单信息异常 (尝试 {attempt + 1}/{max_retries}): {e}，"
                            f"{wait_time}s 后重试"
                        )
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error(f"❌ 获取订单信息失败，已重试 {max_retries} 次: {e}")
                        return {
                            'buy': buyOrderInfo,
                            'sell': sellOrderInfo,
                            'buy_found_attempt': buy_found_attempt,
                            'sell_found_attempt': sell_found_attempt
                        }
            
            return {
                'buy': buyOrderInfo,
                'sell': sellOrderInfo,
                'buy_found_attempt': buy_found_attempt,
                'sell_found_attempt': sell_found_attempt
            }
        # 并发执行
        trade_start = time.time()
        buy_result, sell_result = await asyncio.gather(
            place_buy_order(),
            place_sell_order(),
            return_exceptions=True
        )
        total_duration = (time.time() - trade_start) * 1000
        
        # 处理结果
        buy_success = isinstance(buy_result, dict) and buy_result['order_id']
        sell_success = isinstance(sell_result, dict) and sell_result['order_id']
        logger.info("⏱️  并发执行结束，开始等待180ms")
        await asyncio.sleep(0.18)  # 等待订单信息更新
        # 计算滑点和实际点差
        buy_slippage_abs = None
        buy_slippage_pct = None
        sell_slippage_abs = None
        sell_slippage_pct = None
        actual_spread_pct = None
        spread_loss_pct = None
        total_slippage_pct = None
        buy_filled_price = None
        sell_filled_price = None

        if buy_success or sell_success:
            buy_order_id = buy_result.get('order_id') if buy_success else None
            
            sell_order_id = sell_result.get('order_id') if sell_success else None
            logger.info(f"📥 开始获取订单信息: 买单={buy_order_id}, 卖单={sell_order_id}")

            info = await getOrderInfo(buy_order_id, sell_order_id, max_retries=10)
            filled_duration = (time.time() - trade_start) * 1000
            # logger.info(f"获取买单订单信息: {info}")
            if buy_success and info and info.get('buy'):
                buyOrderInfo = info['buy']
                buy_found_attempt = info.get('buy_found_attempt')
                
                if buyOrderInfo:
                    buy_filled_price = Decimal(str(buyOrderInfo['price']))
                    logger.info(f"✅ 买单实际成交价: {buy_filled_price} (第 {buy_found_attempt} 次获取)")
                    
                    buy_slippage_abs = buy_filled_price - ask_price
                    logger.info(f"   买单绝对滑点: {buy_slippage_abs}")
                    buy_slippage_pct = (buy_slippage_abs / ask_price * 100)
                    
                    # 计算买单撮合耗时
                    buy_created_at = buyOrderInfo.get('created_at')
                    buy_executed_at = buyOrderInfo.get('execution_timestamp')
                    
                    if buy_created_at and buy_executed_at:
                        try:
                            logger.info(f"⏱️ 买单创建时间（service）: {buy_created_at}")
                            logger.info(f"⏱️ 买单执行时间（service）: {buy_executed_at}")
                            
                            buy_created_at_dt = datetime.fromisoformat(buy_created_at.replace('Z', '+00:00'))
                            buy_executed_at_dt = datetime.fromisoformat(buy_executed_at.replace('Z', '+00:00'))
                            buy_delay_ms = (buy_executed_at_dt - buy_created_at_dt).total_seconds() * 1000
                            
                            logger.info(f"⏱️ 买单撮合耗时（service）: {buy_delay_ms:.2f} ms")
                            logger.info(f"⏱️ 买单撮合耗时（client）: {filled_duration:.2f} ms")
                        except Exception as e:
                            logger.warning(f"⚠️ 解析买单时间失败: {e}")
                else:
                    logger.warning(f"⚠️ 买单订单信息为空")
            elif buy_success:
                logger.warning(f"⚠️ 未获取到买单订单信息 (订单ID: {buy_order_id})")
            
            # ✅ 处理卖单信息
            if sell_success and info and info.get('sell'):
                sellOrderInfo = info['sell']
                sell_found_attempt = info.get('sell_found_attempt')
                
                if sellOrderInfo:
                    sell_filled_price = Decimal(str(sellOrderInfo['price']))
                    logger.info(f"✅ 卖单实际成交价: {sell_filled_price} (第 {sell_found_attempt} 次获取)")
                    
                    sell_slippage_abs = bid_price - sell_filled_price
                    logger.info(f"   卖单绝对滑点: {sell_slippage_abs}")
                    sell_slippage_pct = (sell_slippage_abs / bid_price * 100)
                    
                    # 计算卖单撮合耗时
                    sell_created_at = sellOrderInfo.get('created_at')
                    sell_executed_at = sellOrderInfo.get('execution_timestamp')
                    
                    if sell_created_at and sell_executed_at:
                        try:
                            logger.info(f"⏱️ 卖单创建时间（service）: {sell_created_at}")
                            logger.info(f"⏱️ 卖单执行时间（service）: {sell_executed_at}")
                            
                            sell_created_at_dt = datetime.fromisoformat(sell_created_at.replace('Z', '+00:00'))
                            sell_executed_at_dt = datetime.fromisoformat(sell_executed_at.replace('Z', '+00:00'))
                            sell_delay_ms = (sell_executed_at_dt - sell_created_at_dt).total_seconds() * 1000
                            
                            logger.info(f"⏱️ 卖单撮合耗时（service）: {sell_delay_ms:.2f} ms")
                            logger.info(f"⏱️ 卖单撮合耗时（client）: {filled_duration:.2f} ms")
                        except Exception as e:
                            logger.warning(f"⚠️ 解析卖单时间失败: {e}")
                else:
                    logger.warning(f"⚠️ 卖单订单信息为空")
            elif sell_success:
                logger.warning(f"⚠️ 未获取到卖单订单信息 (订单ID: {sell_order_id})")
        
        if buy_success and sell_success and sell_filled_price and buy_filled_price:
            # 计算实际成交点差
            actual_spread_pct = (
                -(sell_filled_price - buy_filled_price) / buy_filled_price * 100
            )
            # 点差损失 = 订单簿点差 - 实际点差
            spread_loss_pct = - spread_pct + actual_spread_pct
            # 总滑点
            if buy_slippage_pct is not None and sell_slippage_pct is not None:
                total_slippage_pct = buy_slippage_pct + sell_slippage_pct
        
        
        # 确定状态
        if buy_success and sell_success:
            status = 'SUCCESS'
            self.stats['trades_success'] += 1
        elif buy_success or sell_success:
            status = 'PARTIAL'
            self.stats['trades_partial'] += 1
        else:
            status = 'FAILED'
            self.stats['trades_failed'] += 1
        
        # 记录到 CSV
        await self._record_trade(
            trade_id=trade_id,
            order_time=order_time,
            quantity=quantity,
            bid_price=bid_price,
            ask_price=ask_price,
            spread_pct=spread_pct,
            quote_id=quote_id,
            buy_result=buy_result if isinstance(buy_result, dict) else {},
            sell_result=sell_result if isinstance(sell_result, dict) else {},
            buy_filled_price=buy_filled_price,
            sell_filled_price=sell_filled_price,
            buy_slippage_abs=buy_slippage_abs,
            buy_slippage_pct=buy_slippage_pct,
            sell_slippage_abs=sell_slippage_abs,
            sell_slippage_pct=sell_slippage_pct,
            actual_spread_pct=actual_spread_pct,
            spread_loss_pct=spread_loss_pct,
            total_slippage_pct=total_slippage_pct,
            status=status
        )
        
        # 打印结果
        msg = (f"{'='*30}\n"
            f"📊 交易 #{self.trade_count} 结果: {status}\n"
            f"{'='*30}\n"
            f"下单时间: {order_datetime}\n"
            f"订单簿:\n"
            f"   Bid: ${bid_price}\n"
            f"   Ask: ${ask_price}\n"
            f"   点差: {spread_pct:.6f}%\n"
            f"   数量: {quantity}\n"
            f"⏱️   订单簿耗时: {fetch_duration:.2f} ms\n"
            f"\n"
            f"买单:\n"
            f"   状态: {'✅ 成功' if buy_success else '❌ 失败'}\n"
            f"   订单ID: {buy_result.get('order_id', 'N/A') if isinstance(buy_result, dict) else 'N/A'}\n"
            f"   成交价: ${buy_filled_price if buy_filled_price else 'N/A'}\n"
            f"   滑点: {f'{buy_slippage_pct:+.6f}%' if buy_slippage_pct else 'N/A'}\n"
            f"⏱️  买单耗时: {buy_result.get('duration_ms', 0):.2f} ms\n"
            f"\n"
            f"卖单:\n"
            f"   状态: {'✅ 成功' if sell_success else '❌ 失败'}\n"
            f"   订单ID: {sell_result.get('order_id', 'N/A') if isinstance(sell_result, dict) else 'N/A'}\n"
            f"   成交价: ${sell_filled_price if sell_filled_price else 'N/A'}\n"
            f"   滑点: {f'{sell_slippage_pct:+.6f}%' if sell_slippage_pct else 'N/A'}\n"
            f"⏱️  卖单耗时: {sell_result.get('duration_ms', 0):.2f} ms\n"
            f"\n"
            f"综合:\n"
            f"   实际点差: {f'{actual_spread_pct:.6f}%' if actual_spread_pct else 'N/A'}\n"
            f"   点差损失: {f'{spread_loss_pct:.6f}%' if spread_loss_pct else 'N/A'}\n"
            f"   总滑点: {f'{total_slippage_pct:.6f}%' if total_slippage_pct else 'N/A'}\n"
            f"⏱️  总耗时: {total_duration:.2f} ms\n"
            f"{'='*30}"
        )
        logger.info(msg)
        if self.lark_bot:
            # 发送飞书通知
            await self.lark_bot.send_text(msg)
    
    async def _record_virtual_trade(
        self,
        bid_price: Decimal,
        ask_price: Decimal,
        spread_pct: Decimal,
        quote_id: str,
    ):
        """记录虚拟交易（监控模式）"""
        self.stats['trades_attempted'] += 1
        self.trade_count += 1
        
        trade_id = f"{self.symbol}_VIRTUAL_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{self.trade_count}"
        order_time = time.time()  # ✅ 添加 order_time
        current_quantity = self._get_random_quantity()  # ✅ 生成随机数量

        # 假设成交价 = 订单簿价格（无滑点）
        await self._record_trade(
            trade_id=trade_id,
            order_time=order_time,
            bid_price=bid_price,
            ask_price=ask_price,
            spread_pct=spread_pct,
            quote_id=quote_id,
            quantity=current_quantity,
            buy_result={'success': True, 'order_id': 'VIRTUAL_BUY', 'filled_price': str(ask_price), 'filled_qty': str(current_quantity), 'duration_ms': 0},
            sell_result={'success': True, 'order_id': 'VIRTUAL_SELL', 'filled_price': str(bid_price), 'filled_qty': str(self.quantity), 'duration_ms': 0},
            buy_slippage_abs=Decimal('0'),
            buy_slippage_pct=Decimal('0'),
            sell_slippage_abs=Decimal('0'),
            sell_slippage_pct=Decimal('0'),
            actual_spread_pct=spread_pct,
            spread_loss_pct=Decimal('0'),
            total_slippage_pct=Decimal('0'),
            status='VIRTUAL'
        )
        
        logger.info(f"📊 虚拟交易 #{self.trade_count} 已记录")
    
    async def _record_trade(
        self,
        trade_id: str,
        order_time: float,
        bid_price: Decimal,
        ask_price: Decimal,
        spread_pct: Decimal,
        quote_id: str,
        quantity: Decimal,
        buy_result: Dict,
        sell_result: Dict,
        buy_filled_price: Optional[Decimal],
        sell_filled_price: Optional[Decimal],
        buy_slippage_abs: Optional[Decimal],
        buy_slippage_pct: Optional[Decimal],
        sell_slippage_abs: Optional[Decimal],
        sell_slippage_pct: Optional[Decimal],
        actual_spread_pct: Optional[Decimal],
        spread_loss_pct: Optional[Decimal],
        total_slippage_pct: Optional[Decimal],
        status: str
    ):
        """记录交易数据到 CSV"""
        try:
            if self.daily_file:
                self._update_csv_files()
            
            record_time = time.time()
            
            with open(self.trades_csv, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    trade_id,
                    f'{order_time:.6f}',
                    datetime.fromtimestamp(order_time).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # ✅ 下单日期时间
                    f'{record_time:.6f}',
                    datetime.fromtimestamp(record_time).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # ✅ 记录日期时间
                    str(quantity),
                    # 订单簿
                    str(bid_price),
                    str(ask_price),
                    f'{spread_pct:.6f}',
                    quote_id,
                    # 买单
                    buy_result.get('order_id', ''),
                    buy_result.get('success', False),
                    str(buy_filled_price) if buy_filled_price else '',  # ✅ 使用参数而不是从 buy_result 取
                    buy_result.get('filled_qty', ''),
                    f"{buy_result.get('duration_ms', 0):.2f}",
                    f'{buy_slippage_abs:.8f}' if buy_slippage_abs is not None else '',
                    f'{buy_slippage_pct:.6f}' if buy_slippage_pct is not None else '',
                    # 卖单
                    sell_result.get('order_id', ''),
                    sell_result.get('success', False),
                    str(sell_filled_price) if sell_filled_price else '',  # ✅ 使用参数而不是从 sell_result 取
                    sell_result.get('filled_qty', ''),
                    f"{sell_result.get('duration_ms', 0):.2f}",
                    f'{sell_slippage_abs:.8f}' if sell_slippage_abs is not None else '',
                    f'{sell_slippage_pct:.6f}' if sell_slippage_pct is not None else '',
                    # 综合
                    f'{actual_spread_pct:.6f}' if actual_spread_pct is not None else '',
                    f'{spread_loss_pct:.6f}' if spread_loss_pct is not None else '',
                    f'{total_slippage_pct:.6f}' if total_slippage_pct is not None else '',
                    status
                ])
        except Exception as e:
            logger.error(f"❌ 记录交易数据失败: {e}")
    
    def _print_stats(self):
        """打印统计信息"""
        total = self.stats['trades_attempted']
        if total == 0:
            total = 1  # 避免除零
        
        logger.info(
            f"\n"
            f"{'='*60}\n"
            f"📊 策略统计报告\n"
            f"{'='*60}\n"
            f"订单簿:\n"
            f"   样本数: {self.stats['orderbook_samples']}\n"
            f"   交易机会: {self.stats['spread_opportunities']}\n"
            f"\n"
            f"交易:\n"
            f"   尝试: {self.stats['trades_attempted']}\n"
            f"   成功: {self.stats['trades_success']} ({self.stats['trades_success']/total*100:.1f}%)\n"
            f"   部分成功: {self.stats['trades_partial']} ({self.stats['trades_partial']/total*100:.1f}%)\n"
            f"   失败: {self.stats['trades_failed']} ({self.stats['trades_failed']/total*100:.1f}%)\n"
            f"   冷却跳过: {self.stats['cooldown_skipped']}\n"
            f"\n"
            f"数据文件:\n"
            f"   订单簿: {self.orderbook_csv}\n"
            f"   交易: {self.trades_csv}\n"
            f"{'='*60}"
        )