"""进程级轻量诊断服务"""

import asyncio
import gc
import logging
import math
import os
import threading
import time
from collections import deque
from typing import Any, Dict, Optional

try:
    import psutil
except ImportError:  # pragma: no cover - 仅在极简环境下兜底
    psutil = None


logger = logging.getLogger(__name__)


class ProcessDiagnosticsService:
    """定期输出进程、事件循环和交易所链路的诊断快照。"""

    def __init__(
        self,
        symbol: str,
        exchange_a,
        exchange_b,
        monitor,
        interval_seconds: float = 60.0,
        loop_lag_window_size: int = 60,
    ):
        self.symbol = symbol
        self.exchange_a = exchange_a
        self.exchange_b = exchange_b
        self.monitor = monitor
        self.interval_seconds = max(5.0, float(interval_seconds))
        self.loop_lag_samples = deque(maxlen=max(10, int(loop_lag_window_size)))
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._process = psutil.Process(os.getpid()) if psutil is not None else None
        if self._process is not None:
            # 预热 cpu_percent，避免第一次读数总是 0。
            self._process.cpu_percent(interval=None)

    async def start(self):
        """启动诊断后台任务。"""
        if self._task and not self._task.done():
            return
        self._running = True
        self._task = asyncio.create_task(
            self._run(),
            name=f"process-diagnostics-{self.symbol.lower()}",
        )
        logger.info(
            f"🩺 启动进程级诊断任务: {self.symbol} interval={self.interval_seconds:.0f}s"
        )

    async def stop(self):
        """停止诊断后台任务。"""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info(f"🛑 停止进程级诊断任务: {self.symbol}")

    async def _run(self):
        """按固定节奏打印诊断快照。"""
        next_tick = time.perf_counter() + self.interval_seconds
        while self._running:
            sleep_for = max(0.0, next_tick - time.perf_counter())
            await asyncio.sleep(sleep_for)
            actual = time.perf_counter()
            lag_ms = max(0.0, (actual - next_tick) * 1000)
            self.loop_lag_samples.append(lag_ms)
            next_tick = actual + self.interval_seconds
            self._log_process_snapshot()

    def _log_process_snapshot(self):
        """打印进程级和链路级诊断汇总。"""
        rss_mb = None
        cpu_percent = None
        num_threads = threading.active_count()
        open_fds = None

        if self._process is not None:
            try:
                rss_mb = self._process.memory_info().rss / (1024 * 1024)
                cpu_percent = self._process.cpu_percent(interval=None)
                num_threads = self._process.num_threads()
                if hasattr(self._process, "num_fds"):
                    open_fds = self._process.num_fds()
            except Exception as exc:
                logger.debug(f"读取进程级诊断失败: {exc}")

        loop_lag_p95 = self._calc_percentile(self.loop_lag_samples, 95)
        loop_lag_max = max(self.loop_lag_samples) if self.loop_lag_samples else 0.0
        tasks_count = len(asyncio.all_tasks())
        gc_counts = gc.get_count()

        logger.info(
            f"🩺 [{self.symbol}] 进程诊断 | "
            f"RSS={rss_mb:.1f}MB, "
            f"CPU={cpu_percent:.1f}%, "
            f"线程数={num_threads}, "
            f"Tasks={tasks_count}, "
            f"FD={open_fds if open_fds is not None else '--'}, "
            f"LoopLagP95={loop_lag_p95:.2f}ms, "
            f"LoopLagMax={loop_lag_max:.2f}ms, "
            f"GC={gc_counts}"
        )

        self._log_monitor_snapshot()
        self._log_exchange_snapshot("A", self.exchange_a)
        self._log_exchange_snapshot("B", self.exchange_b)

    def _log_monitor_snapshot(self):
        """输出 PriceMonitor 当前缓存年龄。"""
        now = time.time()
        age_a_ms = (now - self.monitor.last_orderbook_a_time) * 1000 if self.monitor.last_orderbook_a_time > 0 else None
        age_b_ms = (now - self.monitor.last_orderbook_b_time) * 1000 if self.monitor.last_orderbook_b_time > 0 else None
        logger.info(
            f"🩺 [{self.symbol}] 监控缓存 | "
            f"A年龄={self._fmt_ms(age_a_ms)}, "
            f"B年龄={self._fmt_ms(age_b_ms)}, "
            f"A更新次数={self.monitor.orderbook_a_updates}, "
            f"B更新次数={self.monitor.orderbook_b_updates}, "
            f"订阅者数={len(self.monitor._subscribers)}"
        )

    def _log_exchange_snapshot(self, label: str, exchange):
        """输出单侧交易所诊断。"""
        exchange_name = getattr(exchange, "exchange_name", label)

        if hasattr(exchange, "get_runtime_diagnostics"):
            try:
                runtime_diag = exchange.get_runtime_diagnostics()
                logger.info(
                    f"🩺 [{self.symbol}] {label}所 {exchange_name} 运行态 | "
                    f"任意WS间隔={self._fmt_ms(runtime_diag.get('any_ws_gap_ms'))}, "
                    f"order_book间隔={self._fmt_ms(runtime_diag.get('order_book_gap_ms'))}, "
                    f"最近更新间隔={self._fmt_ms(runtime_diag.get('last_update_gap_ms'))}, "
                    f"最近通知间隔={self._fmt_ms(runtime_diag.get('last_notify_gap_ms'))}, "
                    f"本地处理={self._fmt_ms(runtime_diag.get('processing_delay_ms'))}, "
                    f"深度(bids/asks)={runtime_diag.get('bids_levels')}/{runtime_diag.get('asks_levels')}, "
                    f"连续WS超时={runtime_diag.get('consecutive_ws_timeouts')}, "
                    f"watchdog冷却剩余={self._fmt_ms(runtime_diag.get('watchdog_cooldown_remaining_ms'))}"
                )
            except Exception as exc:
                logger.debug(f"读取 {exchange_name} 运行态失败: {exc}")

        client = getattr(exchange, "client", None)
        if client is not None and hasattr(client, "get_request_diagnostics"):
            try:
                request_diag = client.get_request_diagnostics()
                if request_diag:
                    logger.info(
                        f"🩺 [{self.symbol}] {label}所 {exchange_name} 请求态 | "
                        f"{request_diag.get('method', '--')} {request_diag.get('endpoint', '--')} | "
                        f"总耗时={self._fmt_ms(request_diag.get('total_ms'))}, "
                        f"锁等待={self._fmt_ms(request_diag.get('lock_wait_ms'))}, "
                        f"executor总耗时={self._fmt_ms(request_diag.get('executor_wall_ms'))}, "
                        f"scraper执行={self._fmt_ms(request_diag.get('scraper_exec_ms'))}, "
                        f"executor额外开销={self._fmt_ms(request_diag.get('executor_overhead_ms'))}, "
                        f"JSON解析={self._fmt_ms(request_diag.get('json_parse_ms'))}, "
                        f"cookies={request_diag.get('cookies_count', '--')}"
                    )
            except Exception as exc:
                logger.debug(f"读取 {exchange_name} 请求态失败: {exc}")

    @staticmethod
    def _fmt_ms(value: Optional[float]) -> str:
        """格式化毫秒文本。"""
        if value is None:
            return "--"
        return f"{float(value):.2f}ms"

    @staticmethod
    def _calc_percentile(values: deque, percentile: float) -> float:
        """计算简单分位数，样本很少时返回最大值。"""
        if not values:
            return 0.0
        ordered = sorted(values)
        if len(ordered) == 1:
            return float(ordered[0])
        rank = max(0, min(len(ordered) - 1, math.ceil((percentile / 100) * len(ordered)) - 1))
        return float(ordered[rank])
