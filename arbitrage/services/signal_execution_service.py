"""套利信号执行编排服务。"""

import asyncio
import logging
import time
from datetime import datetime
from decimal import Decimal
from typing import Optional

from ..models.position import Position
from ..models.signal import SignalType, TradingSignal
from .signal_mailbox import SignalMailbox

logger = logging.getLogger(__name__)


class SignalExecutionService:
    """围绕单个 symbol 消费最新信号状态并执行交易编排。"""

    def __init__(
        self,
        strategy,
        mailbox: SignalMailbox,
        execution_max_signal_age_ms: float = 50.0,
        processed_ttl_seconds: float = 30.0,
    ):
        self.strategy = strategy
        self.mailbox = mailbox
        self.execution_max_signal_age_ms = float(execution_max_signal_age_ms)
        self.processed_ttl_seconds = processed_ttl_seconds
        self._processed_signals: dict[str, float] = {}
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """启动后台消费协程。"""
        if self._task is not None and not self._task.done():
            return

        self._running = True
        self._task = asyncio.create_task(
            self._consume_loop(),
            name=f"signal-execution-{self.strategy.symbol}",
        )
        logger.info(
            f"✅ SignalExecutionService 已启动: symbol={self.strategy.symbol}, "
            f"execution_max_signal_age_ms={self.execution_max_signal_age_ms:.2f}"
        )

    async def stop(self) -> None:
        """停止后台消费协程。"""
        self._running = False
        if self._task is None:
            return

        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        finally:
            self._task = None

        logger.info("✅ SignalExecutionService 已停止")

    async def submit(self, signal: TradingSignal) -> None:
        """刷新某个 key 的最新信号。"""
        previous = await self.mailbox.set_latest(signal)
        if previous is not None and previous.signal_id != signal.signal_id:
            logger.info(
                f"📬 覆盖同类旧信号: key={signal.mailbox_key}, "
                f"old={previous.signal_id}, new={signal.signal_id}"
            )
        logger.info(
            f"📨 [{signal.symbol}] mailbox 刷新最新信号: "
            f"id={signal.signal_id}, type={signal.signal_type.value}, key={signal.mailbox_key}"
        )

    async def clear(self, mailbox_key: str) -> bool:
        """清空某个 key 的最新信号。"""
        removed = await self.mailbox.clear(mailbox_key)
        if removed is not None:
            logger.info(
                f"🧹 [{removed.symbol}] mailbox 清空信号槽位: "
                f"id={removed.signal_id}, type={removed.signal_type.value}, key={mailbox_key}"
            )
            return True
        return False

    async def _consume_loop(self) -> None:
        """等待 symbol 状态变化，并持续执行最新信号。"""
        symbol = self.strategy.symbol
        last_seen_version = await self.mailbox.get_symbol_version(symbol)

        while self._running:
            last_seen_version = await self.mailbox.wait_for_symbol_update(symbol, last_seen_version)
            await self._drain_symbol(symbol)
            last_seen_version = await self.mailbox.get_symbol_version(symbol)

    async def _drain_symbol(self, symbol: str) -> None:
        """对某个 symbol 持续执行“最新仍有效”的信号。"""
        while self._running:
            if await self.mailbox.is_symbol_running(symbol):
                return

            signal = await self._select_next_signal(symbol)
            if signal is None:
                return

            latest_signal = await self.mailbox.get_latest(signal.mailbox_key)
            if latest_signal is None or latest_signal.signal_id != signal.signal_id:
                continue

            should_execute, skip_reason = self._should_execute_signal(signal)
            if not should_execute:
                await self._discard_signal_if_latest(signal, skip_reason)
                continue

            self._cleanup_processed_signals()
            if signal.signal_id in self._processed_signals:
                await self._discard_signal_if_latest(signal, "重复信号")
                continue

            await self.mailbox.mark_symbol_running(symbol)
            try:
                latest_signal = await self.mailbox.get_latest(signal.mailbox_key)
                if latest_signal is None or latest_signal.signal_id != signal.signal_id:
                    continue

                now = time.time()
                self._processed_signals[signal.signal_id] = now
                queue_wait_ms = (now - signal.created_at) * 1000
                logger.info(
                    f"📥 开始消费最新信号: id={signal.signal_id}, type={signal.signal_type.value}, "
                    f"symbol={signal.symbol}, signal_age={queue_wait_ms:.2f} ms"
                )

                if signal.signal_type == SignalType.OPEN:
                    await self._execute_open_signal(signal)
                elif signal.signal_type == SignalType.CLOSE:
                    await self._execute_close_signal(signal)
                else:
                    logger.warning(f"⚠️ 未知信号类型，跳过执行: {signal.signal_type}")
            except Exception as exc:
                logger.exception(f"❌ 执行信号时出错: {exc}")
            finally:
                await self.mailbox.mark_symbol_idle(symbol)

    async def _select_next_signal(self, symbol: str) -> Optional[TradingSignal]:
        """基于当前仓位语义，选择该 symbol 下最应该执行的最新信号。"""
        priority_keys = self._build_priority_keys()
        return await self.mailbox.peek_preferred_signal(symbol, priority_keys)

    def _build_priority_keys(self) -> list[str]:
        """根据当前仓位状态决定 OPEN/CLOSE 的优先级。"""
        current_qty = self.strategy.position_manager.get_current_position_qty()

        if not self.strategy.position_manager.accumulate_mode:
            if self.strategy.position_manager.has_position():
                return [self._mailbox_key(SignalType.CLOSE), self._mailbox_key(SignalType.OPEN)]
            return [self._mailbox_key(SignalType.OPEN), self._mailbox_key(SignalType.CLOSE)]

        if current_qty < 0:
            return [self._mailbox_key(SignalType.CLOSE), self._mailbox_key(SignalType.OPEN)]
        return [self._mailbox_key(SignalType.OPEN), self._mailbox_key(SignalType.CLOSE)]

    def _should_execute_signal(self, signal: TradingSignal) -> tuple[bool, str]:
        """执行前的轻量二次校验。"""
        signal_age_ms = (time.time() - signal.created_at) * 1000
        if signal_age_ms > self.execution_max_signal_age_ms:
            return False, f"信号超时 {signal_age_ms:.2f} ms"

        if signal.signal_type == SignalType.OPEN:
            if self.strategy.position_manager.accumulate_mode:
                if not self.strategy.position_manager.can_open('short'):
                    return False, "当前仓位状态不允许继续开空"
            elif self.strategy.position_manager.has_position():
                return False, "当前已有持仓，不能再开仓"
            return True, ""

        if signal.signal_type == SignalType.CLOSE:
            if self.strategy.position_manager.accumulate_mode:
                if not self.strategy.position_manager.can_open('long'):
                    return False, "当前仓位状态不允许继续反向开仓"
            else:
                current_position = self.strategy.position_manager.get_position()
                if not self.strategy.position_manager.has_position() or current_position is None:
                    return False, "当前没有可平仓位"
            return True, ""

        return False, "未知信号类型"

    async def _discard_signal_if_latest(self, signal: TradingSignal, reason: str) -> None:
        """如果当前仍是最新信号，则清空，避免执行器反复处理旧状态。"""
        latest_signal = await self.mailbox.get_latest(signal.mailbox_key)
        if latest_signal is None or latest_signal.signal_id != signal.signal_id:
            return

        logger.info(
            f"⏭️ 丢弃最新信号: id={signal.signal_id}, type={signal.signal_type.value}, "
            f"key={signal.mailbox_key}, reason={reason}"
        )
        await self.mailbox.clear(signal.mailbox_key)

    def _mailbox_key(self, signal_type: SignalType) -> str:
        """构造当前策略 symbol 下的 mailbox key。"""
        return f"{self.strategy.symbol}:{signal_type.value}"

    async def _execute_open_signal(self, signal: TradingSignal) -> None:
        """执行开仓信号。"""
        strategy = self.strategy
        prices = signal.prices
        if prices is None:
            logger.warning(f"⚠️ 开仓信号缺少价格快照，跳过执行: {signal.signal_id}")
            return

        current_time = time.time()

        if strategy.monitor_only:
            strategy.signal_stats['open']['executed'] += 1
            strategy._is_executed = True
            virtual_position = Position(
                symbol=strategy.symbol,
                quantity=signal.quantity,
                exchange_a_name=strategy.exchange_a.exchange_name,
                exchange_b_name=strategy.exchange_b.exchange_name,
                exchange_a_signal_entry_price=signal.exchange_a_price,
                exchange_b_signal_entry_price=signal.exchange_b_price,
                exchange_a_entry_price=signal.exchange_a_price,
                exchange_b_entry_price=signal.exchange_b_price,
                exchange_a_order_id='MONITOR_A',
                exchange_b_order_id='MONITOR_B',
                spread_pct=signal.spread_pct,
                signal_entry_time=signal.created_at,
            )

            if strategy.position_manager.accumulate_mode:
                strategy.position_manager.add_position(
                    virtual_position,
                    'short',
                    signal.signal_delay_ms_a,
                    signal.signal_delay_ms_b,
                )
            else:
                strategy.position_manager.set_position(virtual_position)

            strategy._set_stat_arb_position_context_from_signal(signal)
            strategy._last_execution_time = time.time()
            await asyncio.sleep(0.06)

            if strategy.lark_bot:
                if strategy.position_manager.accumulate_mode:
                    await strategy._send_multi_notification('short', virtual_position, signal.spread_pct)
                else:
                    await strategy._send_open_notification(virtual_position, prices)
            return

        async with strategy._executing_lock:
            if strategy.position_manager.accumulate_mode:
                if not strategy.position_manager.can_open('short'):
                    logger.warning("⏳ 开仓操作期间仓位已达阈值，跳过本次开仓")
                    strategy.signal_stats['open']['skipped'] += 1
                    return
            else:
                if strategy.position_manager.has_position():
                    logger.warning("⏳ 开仓操作期间已有持仓，跳过本次开仓")
                    return

            if strategy.order_limiter_a:
                if strategy.order_limiter_a.has_capacity():
                    await strategy.order_limiter_a.acquire()
                else:
                    logger.info("⏳ 开仓操作限流器限流中，直接返回（Exchange A）")
                    strategy.signal_stats['open']['limited_a'] += 1
                    return
            if strategy.order_limiter_b:
                if strategy.order_limiter_b.has_capacity():
                    await strategy.order_limiter_b.acquire()
                else:
                    logger.info("⏳ 开仓操作限流器限流中，直接返回（Exchange B）")
                    strategy.signal_stats['open']['limited_b'] += 1
                    return

            strategy._is_executing = True

            try:
                success, position = await strategy.executor.execute_open(
                    exchange_a_price=signal.exchange_a_price,
                    exchange_b_price=signal.exchange_b_price,
                    spread_pct=signal.spread_pct,
                    exchange_a_quote_id=signal.exchange_a_quote_id,
                    exchange_b_quote_id=signal.exchange_b_quote_id,
                    signal_trigger_time=signal.created_at,
                    actual_quantity=signal.quantity,
                )

                if success and position is not None:
                    strategy.signal_stats['open']['executed'] += 1
                    strategy._is_executed = True
                    strategy._last_execution_time = time.time()

                    if strategy.position_manager.accumulate_mode:
                        strategy.position_manager.add_position(
                            position,
                            'short',
                            signal.signal_delay_ms_a,
                            signal.signal_delay_ms_b,
                        )
                    else:
                        strategy.position_manager.set_position(position)

                    strategy._set_stat_arb_position_context_from_signal(signal)
                    await asyncio.sleep(2)
                    logger.info("🔍 开仓后校验仓位...")
                    expected_qty = strategy.position_manager.get_current_position_qty()

                    is_consistent = await strategy.position_manager.verify_and_sync(
                        exchange_a=strategy.exchange_a,
                        exchange_b=strategy.exchange_b,
                        symbol_a=strategy.symbol_a,
                        symbol_b=strategy.symbol_b,
                        expected_qty=expected_qty,
                        tolerance=strategy.quantity_precision * 10,
                    )

                    if not is_consistent:
                        logger.warning("⚠️ 开仓后仓位校验不一致，已自动修正")
                    logger.info("🔍 开仓后检查仓位平衡...")
                    await strategy.executor.check_position_balance()

                    if strategy.lark_bot:
                        if strategy.position_manager.accumulate_mode:
                            await strategy._send_multi_notification('short', position, signal.spread_pct)
                        else:
                            await strategy._send_open_notification(position, prices)

                else:
                    await asyncio.sleep(2)
                    await strategy.executor.check_position_balance()

                    if current_time - strategy.last_log_time >= strategy.log_interval:
                        logger.debug(
                            f"📊 当前价差: {signal.spread_pct:.4f}% - 开仓执行失败，等待下一次信号"
                        )
                        strategy.last_log_time = current_time

            finally:
                strategy._is_executing = False

        strategy._log_stats_if_needed()

    async def _execute_close_signal(self, signal: TradingSignal) -> None:
        """执行平仓/反向开仓信号。"""
        strategy = self.strategy
        prices = signal.prices
        if prices is None:
            logger.warning(f"⚠️ 平仓信号缺少价格快照，跳过执行: {signal.signal_id}")
            return

        current_time = time.time()
        current_position = strategy.position_manager.get_position()

        if strategy.monitor_only:
            strategy.signal_stats['close']['executed'] += 1
            strategy._is_executed = True

            if strategy.position_manager.accumulate_mode:
                temp_position = Position(
                    symbol=strategy.symbol,
                    quantity=signal.quantity,
                    exchange_a_name=strategy.exchange_a.exchange_name,
                    exchange_b_name=strategy.exchange_b.exchange_name,
                    exchange_a_signal_entry_price=current_position.exchange_a_entry_price if current_position else Decimal('0'),
                    exchange_b_signal_entry_price=current_position.exchange_b_entry_price if current_position else Decimal('0'),
                    exchange_a_entry_price=current_position.exchange_a_entry_price if current_position else Decimal('0'),
                    exchange_b_entry_price=current_position.exchange_b_entry_price if current_position else Decimal('0'),
                    exchange_a_order_id='MONITOR_CLOSE_A',
                    exchange_b_order_id='MONITOR_CLOSE_B',
                    spread_pct=signal.spread_pct,
                    signal_entry_time=signal.created_at,
                )
                temp_position.exchange_a_signal_exit_price = signal.exchange_a_price
                temp_position.exchange_b_signal_exit_price = signal.exchange_b_price
                temp_position.exchange_a_exit_price = signal.exchange_a_price
                temp_position.exchange_b_exit_price = signal.exchange_b_price
                temp_position.exit_time = datetime.now()

                strategy.position_manager.reduce_position(
                    temp_position,
                    'long',
                    signal.signal_delay_ms_a,
                    signal.signal_delay_ms_b,
                )
                strategy._set_stat_arb_position_context_from_signal(signal)

                if strategy.lark_bot:
                    await strategy._send_multi_notification('long', temp_position, signal.spread_pct)
            else:
                if current_position is None:
                    logger.warning("⏳ 监控模式平仓时未发现持仓，跳过")
                    return
                current_position.exchange_a_signal_exit_price = signal.exchange_a_price
                current_position.exchange_b_signal_exit_price = signal.exchange_b_price
                current_position.exchange_a_exit_price = signal.exchange_a_price
                current_position.exchange_b_exit_price = signal.exchange_b_price
                current_position.exit_time = datetime.now()
                pnl_pct = strategy.position_manager.close_position(
                    signal.signal_delay_ms_a,
                    signal.signal_delay_ms_b,
                )
                strategy._clear_stat_arb_position_context("传统模式统计套利平仓完成")
                if strategy.lark_bot:
                    await strategy._send_close_notification(current_position, pnl_pct, prices)

            strategy._last_execution_time = time.time()
            return

        async with strategy._executing_lock:
            current_position = strategy.position_manager.get_position()
            if strategy.position_manager.accumulate_mode:
                if not strategy.position_manager.can_open('long'):
                    logger.warning("⏳ 反向开仓操作期间仓位已达阈值，跳过本次反向开仓")
                    strategy.signal_stats['close']['skipped'] += 1
                    return
            else:
                if not strategy.position_manager.has_position() or current_position is None:
                    logger.warning("⏳ 获取锁后发现持仓已清空，取消平仓")
                    return

            if strategy.order_limiter_a:
                if strategy.order_limiter_a.has_capacity():
                    await strategy.order_limiter_a.acquire()
                else:
                    logger.info("⏳ 反向开仓操作限流器限流中，直接返回（Exchange A）")
                    strategy.signal_stats['close']['limited_a'] += 1
                    return
            if strategy.order_limiter_b:
                if strategy.order_limiter_b.has_capacity():
                    await strategy.order_limiter_b.acquire()
                else:
                    logger.info("⏳ 反向开仓操作限流器限流中，直接返回（Exchange B）")
                    strategy.signal_stats['close']['limited_b'] += 1
                    return

            strategy._is_executing = True

            try:
                if strategy.position_manager.accumulate_mode:
                    close_quantity = signal.quantity
                else:
                    close_quantity = current_position.quantity if current_position else signal.quantity

                success, position = await strategy.executor.execute_close(
                    position=current_position or strategy._create_dummy_position(),
                    exchange_a_price=signal.exchange_a_price,
                    exchange_b_price=signal.exchange_b_price,
                    exchange_a_quote_id=signal.exchange_a_quote_id,
                    exchange_b_quote_id=signal.exchange_b_quote_id,
                    signal_trigger_time=signal.created_at,
                    close_quantity=close_quantity,
                    execution_context='reverse_open' if strategy.position_manager.accumulate_mode else 'strategy',
                )

                if success and position is not None:
                    strategy.signal_stats['close']['executed'] += 1
                    strategy._is_executed = True
                    strategy._last_execution_time = time.time()

                    if strategy.position_manager.accumulate_mode:
                        pnl_pct = strategy.position_manager.reduce_position(
                            position,
                            'long',
                            signal.signal_delay_ms_a,
                            signal.signal_delay_ms_b,
                        )
                        strategy._set_stat_arb_position_context_from_signal(signal)
                    else:
                        strategy.position_manager.position = position
                        pnl_pct = strategy.position_manager.close_position(
                            signal.signal_delay_ms_a,
                            signal.signal_delay_ms_b,
                        )
                        strategy._clear_stat_arb_position_context("传统模式统计套利平仓完成")

                    await asyncio.sleep(2)
                    logger.info("🔍 反向开仓后校验仓位...")
                    expected_qty = strategy.position_manager.get_current_position_qty()
                    is_consistent = await strategy.position_manager.verify_and_sync(
                        exchange_a=strategy.exchange_a,
                        exchange_b=strategy.exchange_b,
                        symbol_a=strategy.symbol_a,
                        symbol_b=strategy.symbol_b,
                        expected_qty=expected_qty,
                        tolerance=strategy.quantity_precision * 10,
                    )

                    if not is_consistent:
                        logger.warning("⚠️ 反向开仓后仓位不一致，已自动修正")
                    logger.info("🔍 反向开仓后检查仓位平衡...")
                    await strategy.executor.check_position_balance()

                    if strategy.lark_bot:
                        if strategy.position_manager.accumulate_mode:
                            await strategy._send_multi_notification('long', position, signal.spread_pct)
                        else:
                            await strategy._send_close_notification(position, pnl_pct, prices)

                else:
                    await asyncio.sleep(2)
                    await strategy.executor.check_position_balance()

                    if current_time - strategy.last_log_time >= strategy.log_interval:
                        logger.info(
                            f"📊 当前价差: {signal.spread_pct:.4f}% - 反向开仓执行失败，等待下一次信号"
                        )
                        strategy.last_log_time = current_time
            finally:
                strategy._is_executing = False

        strategy._log_stats_if_needed()

    def _cleanup_processed_signals(self) -> None:
        """清理已处理信号缓存。"""
        if not self._processed_signals:
            return

        now = time.time()
        expired_ids = [
            signal_id
            for signal_id, processed_at in self._processed_signals.items()
            if now - processed_at > self.processed_ttl_seconds
        ]
        for signal_id in expired_ids:
            self._processed_signals.pop(signal_id, None)
