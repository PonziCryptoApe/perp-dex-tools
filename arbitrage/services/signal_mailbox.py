"""套利信号 mailbox。"""

import asyncio
from typing import Optional

from ..models.signal import TradingSignal


class SignalMailbox:
    """按 `symbol + signal_type` 维护最新信号状态。"""

    def __init__(self):
        self._latest_signals: dict[str, TradingSignal] = {}
        self._running_symbols: set[str] = set()
        self._symbol_versions: dict[str, int] = {}
        self._condition = asyncio.Condition()

    @staticmethod
    def _symbol_from_key(key: str) -> str:
        """从 mailbox key 中提取 symbol。"""
        return key.split(':', 1)[0]

    async def set_latest(self, signal: TradingSignal) -> Optional[TradingSignal]:
        """刷新某个 key 的最新信号。"""
        async with self._condition:
            key = signal.mailbox_key
            previous = self._latest_signals.get(key)
            self._latest_signals[key] = signal
            self._symbol_versions[signal.symbol] = self._symbol_versions.get(signal.symbol, 0) + 1
            self._condition.notify_all()
            return previous

    async def clear(self, key: str) -> Optional[TradingSignal]:
        """清空某个 key 的最新信号。"""
        async with self._condition:
            removed = self._latest_signals.pop(key, None)
            if removed is not None:
                symbol = self._symbol_from_key(key)
                self._symbol_versions[symbol] = self._symbol_versions.get(symbol, 0) + 1
                self._condition.notify_all()
            return removed

    async def get_latest(self, key: str) -> Optional[TradingSignal]:
        """读取某个 key 的最新信号。"""
        async with self._condition:
            return self._latest_signals.get(key)

    async def get_symbol_version(self, symbol: str) -> int:
        """读取某个 symbol 当前的状态版本号。"""
        async with self._condition:
            return self._symbol_versions.get(symbol, 0)

    async def wait_for_symbol_update(self, symbol: str, last_seen_version: int) -> int:
        """等待某个 symbol 的信号状态发生变化。"""
        async with self._condition:
            await self._condition.wait_for(
                lambda: self._symbol_versions.get(symbol, 0) != last_seen_version
            )
            return self._symbol_versions.get(symbol, 0)

    async def peek_preferred_signal(self, symbol: str, preferred_keys: list[str]) -> Optional[TradingSignal]:
        """按优先级读取某个 symbol 当前最想执行的最新信号。"""
        async with self._condition:
            for key in preferred_keys:
                signal = self._latest_signals.get(key)
                if signal is not None and signal.symbol == symbol:
                    return signal
            return None

    async def mark_symbol_running(self, symbol: str) -> None:
        """标记某个 symbol 正在执行。"""
        async with self._condition:
            self._running_symbols.add(symbol)

    async def mark_symbol_idle(self, symbol: str) -> None:
        """标记某个 symbol 执行结束。"""
        async with self._condition:
            self._running_symbols.discard(symbol)

    async def is_symbol_running(self, symbol: str) -> bool:
        """判断某个 symbol 当前是否正在执行。"""
        async with self._condition:
            return symbol in self._running_symbols
