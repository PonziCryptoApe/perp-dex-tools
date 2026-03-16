"""账户级风控服务。"""

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from enum import IntEnum
from typing import Any, Optional

from helpers.lark_bot import LarkBot

logger = logging.getLogger(__name__)


class RiskLevel(IntEnum):
    """风险等级。"""

    NORMAL = 0
    WARN = 1
    REDUCE = 2
    STOP = 3


@dataclass
class ExchangeRiskSnapshot:
    """单个交易所的风险快照。"""

    exchange_name: str
    symbol: str
    position_size: Decimal = Decimal("0")
    mark_price: Decimal = Decimal("0")
    position_notional: Decimal = Decimal("0")
    balance: Decimal = Decimal("0")
    liquidation_price: Optional[Decimal] = None
    liq_distance_pct: Optional[Decimal] = None
    risk_metric_source: str = ""
    error: Optional[str] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class RiskDecision:
    """对策略热路径暴露的风控决策。"""

    level: RiskLevel = RiskLevel.NORMAL
    block_open: bool = False
    need_reduce: bool = False
    target_position_ratio: Decimal = Decimal("1")
    reason: str = ""
    max_position_ratio: Decimal = Decimal("1")
    weak_exchange: str = ""
    exchange_a_cap_ratio: Decimal = Decimal("1")
    exchange_b_cap_ratio: Decimal = Decimal("1")
    current_max_position: Optional[Decimal] = None
    timestamp: float = field(default_factory=time.time)
    exchange_a_snapshot: Optional[ExchangeRiskSnapshot] = None
    exchange_b_snapshot: Optional[ExchangeRiskSnapshot] = None


class RiskControlService:
    """后台轮询账户风控，并向策略提供只读决策。"""

    def __init__(
        self,
        exchange_a,
        exchange_b,
        symbol_a: str,
        symbol_b: str,
        config: Optional[dict] = None,
        lark_bot=None,
        base_max_position: Optional[Decimal] = None,
        position_step: Optional[Decimal] = None,
    ):
        self.exchange_a = exchange_a
        self.exchange_b = exchange_b
        self.symbol_a = symbol_a
        self.symbol_b = symbol_b
        self.config = config or {}
        self._base_max_position = (
            Decimal(str(base_max_position)) if base_max_position is not None else None
        )
        self._position_step = (
            abs(Decimal(str(position_step))) if position_step is not None else None
        )
        self.enabled = bool(self.config.get("enabled", False))
        serious_lark_token = os.getenv("LARK_TOKEN_SERIOUS")
        self._owns_lark_bot = False
        if self.enabled and serious_lark_token:
            self.lark_bot = LarkBot(serious_lark_token)
            self._owns_lark_bot = True
        else:
            self.lark_bot = lark_bot

        self.poll_interval_seconds = float(self.config.get("poll_interval_seconds", 1.0))
        self.stale_after_seconds = float(self.config.get("stale_after_seconds", 3.0))
        self.fail_safe_block_open = bool(self.config.get("fail_safe_block_open", True))
        self.reduce_position_ratio = self._to_decimal(self.config.get("reduce_position_ratio"), Decimal("0.5"))
        self.stop_position_ratio = self._to_decimal(self.config.get("stop_position_ratio"), Decimal("0.5"))
        self.reduce_cooldown_seconds = float(self.config.get("reduce_cooldown_seconds", 5.0))
        raw_liq_distance_warn = self._to_decimal(self.config.get("liq_distance_warn"), Decimal("0.20"))
        raw_liq_distance_reduce = self._to_decimal(self.config.get("liq_distance_reduce"), Decimal("0.10"))
        raw_liq_distance_stop = self._to_decimal(self.config.get("liq_distance_stop"), Decimal("0.05"))
        (
            self.liq_distance_warn,
            self.liq_distance_reduce,
            self.liq_distance_stop,
        ) = self._normalize_liq_distance_thresholds(
            raw_liq_distance_warn,
            raw_liq_distance_reduce,
            raw_liq_distance_stop,
        )
        self.dynamic_position_cap_enabled = bool(
            self.config.get("dynamic_position_cap_enabled", self.enabled)
        )
        self.warn_position_ratio = Decimal("1")
        raw_liq_distance_warn_recover = self._to_decimal(
            self.config.get("liq_distance_warn_recover"),
            None,
        )
        normalized_warn_recover = self._normalize_single_liq_distance_threshold(
            raw_liq_distance_warn_recover
        )
        default_warn_recover = min(Decimal("1"), self.liq_distance_warn + Decimal("0.03"))
        if normalized_warn_recover is None:
            self.liq_distance_warn_recover = default_warn_recover
        else:
            self.liq_distance_warn_recover = max(self.liq_distance_warn, normalized_warn_recover)

        self._task: Optional[asyncio.Task] = None
        self._is_running = False
        self._latest_decision = RiskDecision()
        self._last_snapshot_ts = 0.0
        self._last_status_key: Optional[tuple] = None
        self._last_status_decision: Optional[RiskDecision] = None
        self._last_exchange_levels: dict[str, RiskLevel] = {}

    async def start(self):
        """启动风控后台任务。"""
        if not self.enabled:
            logger.info("🛡️ 风控模块未启用，跳过启动")
            return
        if self._is_running:
            return

        self._is_running = True
        self._task = asyncio.create_task(self._run_loop(), name="risk-control")
        logger.info(
            "🛡️ 风控模块已启动: "
            f"poll={self.poll_interval_seconds:.2f}s, stale={self.stale_after_seconds:.2f}s, "
            f"liq_warn={self.liq_distance_warn:.2%}, "
            f"liq_warn_recover={self.liq_distance_warn_recover:.2%}, "
            f"liq_reduce={self.liq_distance_reduce:.2%}, liq_stop={self.liq_distance_stop:.2%}, "
            f"dynamic_cap={self.dynamic_position_cap_enabled}"
        )

    async def stop(self):
        """停止风控后台任务。"""
        self._is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._owns_lark_bot and self.lark_bot is not None:
            await self.lark_bot.close()
            self.lark_bot = None
        logger.info("🛡️ 风控模块已停止")

    def get_latest_decision(self) -> RiskDecision:
        """供热路径读取最近一次风控决策。"""
        if not self.enabled:
            return RiskDecision()

        if (
            self.fail_safe_block_open
            and self._last_snapshot_ts > 0
            and time.time() - self._last_snapshot_ts > self.stale_after_seconds
        ):
            decision = self._clone_decision(self._latest_decision)
            decision.block_open = True
            if decision.reason:
                decision.reason = f"{decision.reason}; stale"
            else:
                decision.reason = "stale"
            return decision
        return self._clone_decision(self._latest_decision)

    def set_base_max_position(self, value: Decimal):
        """同步策略当前基础最大仓位，供风控日志和通知展示。"""
        self._base_max_position = Decimal(str(value))

    async def _run_loop(self):
        while self._is_running:
            try:
                await self._refresh_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception(f"❌ 风控轮询异常: {e}")
                if self.fail_safe_block_open:
                    self._latest_decision = RiskDecision(
                        level=RiskLevel.WARN,
                        block_open=True,
                        need_reduce=False,
                        target_position_ratio=Decimal("1"),
                        reason=f"fetch_error:{e}",
                    )
            await asyncio.sleep(self.poll_interval_seconds)

    async def _refresh_once(self):
        snapshot_a, snapshot_b = await asyncio.gather(
            self._collect_exchange_snapshot(self.exchange_a, self.symbol_a),
            self._collect_exchange_snapshot(self.exchange_b, self.symbol_b),
        )
        decision = self._build_decision(snapshot_a, snapshot_b)
        self._latest_decision = decision
        self._last_snapshot_ts = time.time()
        self._log_status_change(decision)

    def _build_decision(
        self,
        snapshot_a: ExchangeRiskSnapshot,
        snapshot_b: ExchangeRiskSnapshot,
    ) -> RiskDecision:
        level_a, reason_a = self._evaluate_exchange_level(snapshot_a)
        level_b, reason_b = self._evaluate_exchange_level(snapshot_b)

        worst_level = max(level_a, level_b)
        reasons = []
        if reason_a:
            reasons.append(f"{snapshot_a.exchange_name}:{reason_a}")
        if reason_b:
            reasons.append(f"{snapshot_b.exchange_name}:{reason_b}")

        block_open = False
        need_reduce = False
        target_position_ratio = Decimal("1")
        exchange_a_cap_ratio = self._derive_position_cap_ratio(snapshot_a)
        exchange_b_cap_ratio = self._derive_position_cap_ratio(snapshot_b)
        max_position_ratio = min(exchange_a_cap_ratio, exchange_b_cap_ratio)
        weak_exchange = ""
        if exchange_a_cap_ratio < exchange_b_cap_ratio:
            weak_exchange = snapshot_a.exchange_name
        elif exchange_b_cap_ratio < exchange_a_cap_ratio:
            weak_exchange = snapshot_b.exchange_name

        has_stale_or_error = any(snapshot.error for snapshot in (snapshot_a, snapshot_b))
        if self.fail_safe_block_open and has_stale_or_error:
            block_open = True

        if worst_level == RiskLevel.WARN:
            block_open = block_open
        elif worst_level == RiskLevel.REDUCE:
            block_open = True
            need_reduce = True
            target_position_ratio = (
                min(self.reduce_position_ratio, max_position_ratio)
                if self.dynamic_position_cap_enabled
                else self.reduce_position_ratio
            )
        elif worst_level == RiskLevel.STOP:
            block_open = True
            need_reduce = True
            target_position_ratio = (
                min(self.stop_position_ratio, max_position_ratio)
                if self.dynamic_position_cap_enabled
                else self.stop_position_ratio
            )

        if has_stale_or_error:
            stale_reasons = [snapshot.error for snapshot in (snapshot_a, snapshot_b) if snapshot.error]
            reasons.extend(stale_reasons)

        self._last_exchange_levels[snapshot_a.exchange_name] = level_a
        self._last_exchange_levels[snapshot_b.exchange_name] = level_b
        current_max_position = None
        if self._base_max_position is not None:
            current_max_position = self._align_position_limit_to_step(
                self._base_max_position * max_position_ratio
            )

        return RiskDecision(
            level=worst_level,
            block_open=block_open,
            need_reduce=need_reduce,
            target_position_ratio=target_position_ratio,
            reason=" | ".join(reasons),
            max_position_ratio=max_position_ratio,
            weak_exchange=weak_exchange,
            exchange_a_cap_ratio=exchange_a_cap_ratio,
            exchange_b_cap_ratio=exchange_b_cap_ratio,
            current_max_position=current_max_position,
            timestamp=time.time(),
            exchange_a_snapshot=snapshot_a,
            exchange_b_snapshot=snapshot_b,
        )

    async def _collect_exchange_snapshot(self, exchange, symbol: str) -> ExchangeRiskSnapshot:
        exchange_name = getattr(exchange, "exchange_name", exchange.__class__.__name__)
        snapshot = ExchangeRiskSnapshot(exchange_name=exchange_name, symbol=symbol)

        try:
            position = None
            if hasattr(exchange, "get_position"):
                position = await exchange.get_position(symbol)

            if isinstance(position, dict):
                snapshot.position_size = self._extract_signed_position_size(position)
                position_liq_price = self._to_decimal(position.get("liquidation_price"), None)
                if position_liq_price is not None and position_liq_price > 0:
                    snapshot.liquidation_price = position_liq_price
                    snapshot.risk_metric_source = f"{exchange_name}_position_rest"

            orderbook = None
            if hasattr(exchange, "get_latest_orderbook"):
                orderbook = await exchange.get_latest_orderbook(None)
            if isinstance(orderbook, dict):
                mark_price = self._extract_mark_price(orderbook)
                if mark_price > 0:
                    snapshot.mark_price = mark_price
                    snapshot.risk_metric_source = f"{exchange_name}_orderbook"
                else:
                    bid = self._extract_price(orderbook.get("bids", []))
                    ask = self._extract_price(orderbook.get("asks", []))
                    if bid > 0 and ask > 0:
                        snapshot.mark_price = (bid + ask) / 2
                    elif bid > 0:
                        snapshot.mark_price = bid
                    elif ask > 0:
                        snapshot.mark_price = ask
                    if snapshot.mark_price > 0:
                        snapshot.risk_metric_source = f"{exchange_name}_mid"

            lighter_user_stats = None
            lighter_market_stats = None
            exchange_name_lower = (exchange_name or "").strip().lower()

            if exchange_name_lower == "lighter":
                if hasattr(exchange, "get_lighter_user_stats"):
                    lighter_user_stats = exchange.get_lighter_user_stats()
                if hasattr(exchange, "get_lighter_market_stats"):
                    lighter_market_stats = exchange.get_lighter_market_stats()

                if isinstance(lighter_user_stats, dict):
                    parsed = lighter_user_stats.get("parsed", {})
                    if isinstance(parsed, dict):
                        ws_collateral = self._to_decimal(parsed.get("collateral"), None)
                        if ws_collateral is not None and ws_collateral > 0:
                            snapshot.balance = ws_collateral
                if isinstance(lighter_market_stats, dict):
                    parsed_market = lighter_market_stats.get("parsed", {})
                    if isinstance(parsed_market, dict):
                        ws_mark_price = self._to_decimal(parsed_market.get("mark_price"), None)
                        if ws_mark_price is not None and ws_mark_price > 0:
                            snapshot.mark_price = ws_mark_price
                            snapshot.risk_metric_source = "lighter_market_stats"
            variational_portfolio_ws = None
            if exchange_name_lower == "variational":
                if hasattr(exchange, "client") and hasattr(exchange.client, "get_cached_portfolio_ws"):
                    variational_portfolio_ws = exchange.client.get_cached_portfolio_ws()
                if isinstance(variational_portfolio_ws, dict):
                    portfolio_ws = variational_portfolio_ws.get("portfolio", {})
                    if isinstance(portfolio_ws, dict):
                        ws_balance = self._to_decimal(portfolio_ws.get("balance"), None)
                        if ws_balance is not None and ws_balance > 0:
                            snapshot.balance = ws_balance

                        liq_price = self._extract_variational_liquidation_price(
                            portfolio_ws.get("positions"),
                            symbol=symbol,
                            position_size=snapshot.position_size,
                        )
                        if liq_price is not None and liq_price > 0:
                            snapshot.liquidation_price = liq_price

                elif hasattr(exchange, "client") and hasattr(exchange.client, "get_portfolio"):
                    portfolio = await exchange.client.get_portfolio()
                    if isinstance(portfolio, dict):
                        liq_price = self._extract_variational_liquidation_price(
                            portfolio.get("positions"),
                            symbol=symbol,
                            position_size=snapshot.position_size,
                        )
                        if liq_price is not None and liq_price > 0:
                            snapshot.liquidation_price = liq_price
                            snapshot.risk_metric_source = "variational_portfolio_rest"

            snapshot.position_notional = (
                abs(snapshot.position_size) * snapshot.mark_price if snapshot.mark_price > 0 else Decimal("0")
            )
            snapshot.liq_distance_pct = self._calculate_liq_distance_pct(
                mark_price=snapshot.mark_price,
                liquidation_price=snapshot.liquidation_price,
                position_size=snapshot.position_size,
            )

            if exchange_name_lower == "lighter" and lighter_market_stats:
                logger.info(
                    "🛡️ Lighter 风控快照: "
                    f"mark_price={snapshot.mark_price}, "
                    f"liq_price={snapshot.liquidation_price}, "
                    f"liq_distance={self._format_pct(snapshot.liq_distance_pct)}, "
                    f"balance={snapshot.balance}"
                )

            if exchange_name_lower == "variational" and isinstance(variational_portfolio_ws, dict):
                logger.info(
                    "🛡️ Variational 风控快照使用 portfolio WS: "
                    f"mark_price={snapshot.mark_price}, "
                    f"liq_price={snapshot.liquidation_price}, "
                    f"liq_distance={self._format_pct(snapshot.liq_distance_pct)}, "
                    f"balance={snapshot.balance}"
                )

            if abs(snapshot.position_size) > Decimal("0"):
                if snapshot.mark_price <= Decimal("0"):
                    snapshot.error = "有持仓但缺少有效标记价格"

            return snapshot
        except Exception as e:
            snapshot.error = str(e)
            return snapshot

    def _evaluate_exchange_level(self, snapshot: ExchangeRiskSnapshot) -> tuple[RiskLevel, str]:
        previous_level = self._last_exchange_levels.get(snapshot.exchange_name, RiskLevel.NORMAL)
        if snapshot.error:
            return RiskLevel.WARN, snapshot.error

        if abs(snapshot.position_size) <= Decimal("0"):
            return RiskLevel.NORMAL, ""

        if snapshot.liq_distance_pct is None:
            return RiskLevel.NORMAL, ""

        if snapshot.liq_distance_pct <= Decimal("0"):
            return RiskLevel.STOP, (
                f"liq_distance={snapshot.liq_distance_pct:.2%} "
                f"(mark={snapshot.mark_price}, liq={snapshot.liquidation_price})"
            )
        if snapshot.liq_distance_pct <= self.liq_distance_stop:
            return RiskLevel.STOP, f"liq_distance={snapshot.liq_distance_pct:.2%} <= {self.liq_distance_stop:.2%}"
        if snapshot.liq_distance_pct <= self.liq_distance_reduce:
            return RiskLevel.REDUCE, f"liq_distance={snapshot.liq_distance_pct:.2%} <= {self.liq_distance_reduce:.2%}"
        if snapshot.liq_distance_pct <= self.liq_distance_warn:
            return RiskLevel.WARN, f"liq_distance={snapshot.liq_distance_pct:.2%} <= {self.liq_distance_warn:.2%}"
        if previous_level >= RiskLevel.WARN and snapshot.liq_distance_pct < self.liq_distance_warn_recover:
            return RiskLevel.WARN, (
                f"liq_distance={snapshot.liq_distance_pct:.2%} < "
                f"WARN恢复阈值{self.liq_distance_warn_recover:.2%}"
            )
        return RiskLevel.NORMAL, ""

    def _log_status_change(self, decision: RiskDecision):
        previous_decision = self._last_status_decision
        status_key = (
            decision.level,
            decision.block_open,
            decision.need_reduce,
            decision.weak_exchange,
        )
        if status_key == self._last_status_key:
            return

        # 首次初始化且当前仍为 NORMAL 时，只建立基线状态，不输出噪声日志/通知。
        if previous_decision is None and decision.level == RiskLevel.NORMAL:
            self._last_status_key = status_key
            self._last_status_decision = self._clone_decision(decision)
            return

        previous_level = previous_decision.level.name if previous_decision else "INIT"
        # previous_block_open = previous_decision.block_open if previous_decision else "--"
        # previous_need_reduce = previous_decision.need_reduce if previous_decision else "--"
        current_weak = decision.weak_exchange or "--"
        current_position_summary = self._format_position_limit_summary(decision)

        logger.warning(
            "🛡️ 风控状态变更: "
            f"等级={previous_level}->{decision.level.name}, "
            f"禁止增仓={decision.block_open}, "
            f"需要减仓={decision.need_reduce}, "
            f"{current_position_summary}, "
            f"弱腿交易所={current_weak}, "
            f"原因={decision.reason or '--'}, "
            f"{self._format_snapshot_brief('A', decision.exchange_a_snapshot)}, "
            f"{self._format_snapshot_brief('B', decision.exchange_b_snapshot)}"
        )
        self._last_status_key = status_key
        self._last_status_decision = self._clone_decision(decision)
        if self.lark_bot is not None:
            asyncio.create_task(self._send_lark_notice(previous_decision, decision))

    async def _send_lark_notice(
        self,
        previous_decision: Optional[RiskDecision],
        decision: RiskDecision,
    ):
        try:
            previous_level = previous_decision.level.name if previous_decision else "INIT"
            current_weak = decision.weak_exchange or "--"
            current_position_summary = self._format_position_limit_summary(decision)
            await self.lark_bot.send_text(
                "🛡️ 风控状态变更\n"
                f"等级: {previous_level} -> {decision.level.name}\n"
                f"禁止增加仓位: {decision.block_open}\n"
                f"需要减仓: {decision.need_reduce}\n"
                f"{current_position_summary}\n"
                f"弱腿交易所: {current_weak}\n"
                f"原因: {decision.reason or '--'}\n"
                f"{self._format_snapshot_brief('A', decision.exchange_a_snapshot)}\n"
                f"{self._format_snapshot_brief('B', decision.exchange_b_snapshot)}"
            )
        except Exception as e:
            logger.warning(f"⚠️ 发送风控飞书通知失败: {e}")

    @staticmethod
    def _clone_decision(decision: RiskDecision) -> RiskDecision:
        return RiskDecision(
            level=decision.level,
            block_open=decision.block_open,
            need_reduce=decision.need_reduce,
            target_position_ratio=decision.target_position_ratio,
            reason=decision.reason,
            max_position_ratio=decision.max_position_ratio,
            weak_exchange=decision.weak_exchange,
            exchange_a_cap_ratio=decision.exchange_a_cap_ratio,
            exchange_b_cap_ratio=decision.exchange_b_cap_ratio,
            current_max_position=decision.current_max_position,
            timestamp=decision.timestamp,
            exchange_a_snapshot=decision.exchange_a_snapshot,
            exchange_b_snapshot=decision.exchange_b_snapshot,
        )

    @staticmethod
    def _normalize_liq_distance_thresholds(
        warn: Optional[Decimal],
        reduce: Optional[Decimal],
        stop: Optional[Decimal],
    ) -> tuple[Decimal, Decimal, Decimal]:
        """兼容“距离口径”和历史“接近度口径”两种配置写法。"""
        warn_value = warn if warn is not None else Decimal("0.20")
        reduce_value = reduce if reduce is not None else Decimal("0.10")
        stop_value = stop if stop is not None else Decimal("0.05")

        # 历史配置可能写成 0.80/0.90/0.95，实际语义是“距离 <= 20/10/5%”。
        half = Decimal("0.5")
        if warn_value >= half and reduce_value >= half and stop_value >= half:
            normalized_warn = Decimal("1") - warn_value
            normalized_reduce = Decimal("1") - reduce_value
            normalized_stop = Decimal("1") - stop_value
        else:
            normalized_warn = warn_value
            normalized_reduce = reduce_value
            normalized_stop = stop_value

        # 统一修正为 warn > reduce > stop。
        ordered = sorted(
            [normalized_warn, normalized_reduce, normalized_stop],
            reverse=True,
        )
        return ordered[0], ordered[1], ordered[2]

    @staticmethod
    def _normalize_single_liq_distance_threshold(value: Optional[Decimal]) -> Optional[Decimal]:
        if value is None:
            return None
        half = Decimal("0.5")
        if value >= half:
            return Decimal("1") - value
        return value

    @staticmethod
    def _to_decimal(value: Any, default: Optional[Decimal] = Decimal("0")) -> Optional[Decimal]:
        if value is None:
            return default
        if isinstance(value, Decimal):
            return value
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError):
            return default

    @classmethod
    def _extract_mark_price(cls, orderbook: dict) -> Decimal:
        value = orderbook.get("mark_price")
        decimal_value = cls._to_decimal(value, Decimal("0"))
        return decimal_value if decimal_value is not None else Decimal("0")

    @classmethod
    def _extract_price(cls, levels: Any) -> Decimal:
        if isinstance(levels, list) and levels:
            first = levels[0]
            if isinstance(first, (list, tuple)) and first:
                value = cls._to_decimal(first[0], Decimal("0"))
                return value if value is not None else Decimal("0")
            if isinstance(first, dict):
                value = cls._to_decimal(first.get("price"), Decimal("0"))
                return value if value is not None else Decimal("0")
        return Decimal("0")

    def _derive_position_cap_ratio(self, snapshot: ExchangeRiskSnapshot) -> Decimal:
        """根据单所风险快照计算当前可承受仓位比例。"""
        if not self.dynamic_position_cap_enabled:
            return Decimal("1")
        if abs(snapshot.position_size) <= Decimal("0"):
            return Decimal("1")
        if snapshot.liq_distance_pct is None:
            return Decimal("1")

        liq_distance = snapshot.liq_distance_pct
        reduce_ratio = max(Decimal("0"), min(Decimal("1"), self.reduce_position_ratio))
        stop_ratio = max(Decimal("0"), min(Decimal("1"), self.stop_position_ratio))

        if liq_distance <= self.liq_distance_stop:
            return stop_ratio
        if liq_distance <= self.liq_distance_reduce:
            return self._interpolate_ratio(
                current_value=liq_distance,
                lower_threshold=self.liq_distance_stop,
                upper_threshold=self.liq_distance_reduce,
                lower_ratio=stop_ratio,
                upper_ratio=reduce_ratio,
            )
        if liq_distance < self.liq_distance_warn_recover:
            return self._interpolate_ratio(
                current_value=liq_distance,
                lower_threshold=self.liq_distance_reduce,
                upper_threshold=self.liq_distance_warn_recover,
                lower_ratio=reduce_ratio,
                upper_ratio=Decimal("1"),
            )
        return Decimal("1")

    @staticmethod
    def _interpolate_ratio(
        current_value: Decimal,
        lower_threshold: Decimal,
        upper_threshold: Decimal,
        lower_ratio: Decimal,
        upper_ratio: Decimal,
    ) -> Decimal:
        """在线性区间内按当前距离值插值计算仓位比例。"""
        if upper_threshold <= lower_threshold:
            return upper_ratio
        progress = (current_value - lower_threshold) / (upper_threshold - lower_threshold)
        progress = max(Decimal("0"), min(Decimal("1"), progress))
        return lower_ratio + progress * (upper_ratio - lower_ratio)

    def _align_position_limit_to_step(self, value: Decimal) -> Decimal:
        """将最大仓位按单次成交步长向下对齐，保证目标仓位可执行。"""
        aligned_value = max(Decimal("0"), Decimal(str(value)))
        if self._position_step is None or self._position_step <= Decimal("0"):
            return aligned_value
        step_count = (aligned_value / self._position_step).to_integral_value(rounding=ROUND_DOWN)
        return step_count * self._position_step

    @classmethod
    def _extract_signed_position_size(cls, position: dict) -> Decimal:
        size = cls._to_decimal(
            position.get("size", position.get("position", position.get("quantity", 0))),
            Decimal("0"),
        )
        if size is None:
            return Decimal("0")

        side = str(position.get("side", "")).strip().lower()
        sign = cls._to_decimal(position.get("sign"), None)
        if sign is not None:
            return abs(size) if sign > 0 else -abs(size) if sign < 0 else Decimal("0")
        if side in {"short", "sell"}:
            return -abs(size)
        if side in {"long", "buy"}:
            return abs(size)
        return size

    @staticmethod
    def _calculate_liq_distance_pct(
        mark_price: Decimal,
        liquidation_price: Optional[Decimal],
        position_size: Decimal,
    ) -> Optional[Decimal]:
        if mark_price <= 0 or liquidation_price is None:
            return None
        if position_size > 0:
            return (mark_price - liquidation_price) / mark_price
        if position_size < 0:
            return (liquidation_price - mark_price) / mark_price
        return None

    @staticmethod
    def _format_pct(value: Optional[Decimal]) -> str:
        if value is None:
            return "--"
        return f"{value:.2%}"

    @staticmethod
    def _format_price(value: Optional[Decimal]) -> str:
        if value is None:
            return "--"
        return f"{value:.2f}"

    @staticmethod
    def _format_position_value(value: Optional[Decimal]) -> str:
        if value is None:
            return "--"
        text = format(value.normalize(), "f") if value != 0 else "0"
        return text.rstrip("0").rstrip(".") if "." in text else text

    def _format_position_limit_summary(self, decision: RiskDecision) -> str:
        current_position = self._extract_current_position_abs(decision)
        current_max_position = decision.current_max_position
        return (
            "当前仓位/最大仓位: "
            f"{self._format_position_value(current_position)} / "
            f"{self._format_position_value(current_max_position)}"
        )

    @staticmethod
    def _extract_current_position_abs(decision: RiskDecision) -> Optional[Decimal]:
        snapshots = (
            decision.exchange_a_snapshot,
            decision.exchange_b_snapshot,
        )
        sizes = [
            abs(snapshot.position_size)
            for snapshot in snapshots
            if snapshot is not None
        ]
        if not sizes:
            return None
        return max(sizes)

    def _format_snapshot_brief(self, label: str, snapshot: Optional[ExchangeRiskSnapshot]) -> str:
        if snapshot is None:
            return f"{label}=--"
        return (
            f"{label}({snapshot.exchange_name}):仓位={snapshot.position_size},"
            f"标记价={self._format_price(snapshot.mark_price)},"
            f"清算价={self._format_price(snapshot.liquidation_price)},"
            f"距清算价距离={self._format_pct(snapshot.liq_distance_pct)}"
        )

    def _extract_variational_liquidation_price(
        self,
        positions: Any,
        symbol: str,
        position_size: Decimal,
    ) -> Optional[Decimal]:
        normalized_positions = self._normalize_positions_payload(positions)
        if not isinstance(normalized_positions, list):
            logger.info(
                f"🛡️ Variational 清算价提取跳过: positions 不是 list, symbol={symbol}, "
                f"position_size={position_size}, type={type(positions).__name__}"
            )
            return None

        matched_entry = self._match_position_entry(normalized_positions, symbol=symbol, position_size=position_size)
        if not isinstance(matched_entry, dict):
            logger.info(
                f"🛡️ Variational 清算价提取未匹配到持仓: symbol={symbol}, "
                f"position_size={position_size}, positions_count={len(normalized_positions)}"
            )
            return None

        raw_liq = None
        if "estimated_liquidation_price" in matched_entry:
            raw_liq = matched_entry.get("estimated_liquidation_price")
        elif isinstance(matched_entry.get("position_info"), dict):
            raw_liq = matched_entry.get("position_info", {}).get("estimated_liquidation_price")

        # logger.info(
        #     "🛡️ Variational 清算价匹配结果: "
        #     f"symbol={symbol}, position_size={position_size}, raw_liq={raw_liq}, matched_entry={matched_entry}"
        # )

        return self._extract_decimal_from_paths(
            matched_entry,
            ("estimated_liquidation_price",),
            ("position_info", "estimated_liquidation_price"),
        )

    @staticmethod
    def _normalize_positions_payload(positions: Any) -> Optional[list[dict]]:
        """兼容 list / dict / dict[list] 三种持仓结构。"""
        if isinstance(positions, list):
            return [entry for entry in positions if isinstance(entry, dict)]
        if isinstance(positions, dict):
            normalized: list[dict] = []
            for value in positions.values():
                if isinstance(value, dict):
                    normalized.append(value)
                elif isinstance(value, list):
                    normalized.extend(entry for entry in value if isinstance(entry, dict))
            return normalized
        return None

    def _match_position_entry(
        self,
        positions: list[dict],
        symbol: str,
        position_size: Decimal,
    ) -> Optional[dict]:
        matched = [entry for entry in positions if self._entry_matches_symbol(entry, symbol)]
        candidates = matched or [entry for entry in positions if isinstance(entry, dict)]
        if len(candidates) == 1:
            return candidates[0]

        for entry in candidates:
            size = self._extract_decimal_from_paths(
                entry,
                ("size",),
                ("position",),
                ("position_info", "size"),
                ("position_info", "qty"),
                ("position_info", "quantity"),
                ("qty",),
            )
            if size is None:
                continue
            if position_size > 0 and size > 0:
                return entry
            if position_size < 0 and size < 0:
                return entry
            if position_size == 0 and size != 0:
                return entry
        return candidates[0] if candidates else None

    def _entry_matches_symbol(self, entry: dict, symbol: str) -> bool:
        if not isinstance(entry, dict):
            return False
        target = (symbol or "").strip().upper()
        if not target:
            return False
        tokens = self._collect_string_tokens(entry)
        return any(token == target or target in token for token in tokens)

    @staticmethod
    def _collect_string_tokens(data: Any) -> set[str]:
        tokens: set[str] = set()

        def walk(value: Any):
            if isinstance(value, dict):
                for child in value.values():
                    walk(child)
            elif isinstance(value, list):
                for child in value:
                    walk(child)
            elif value is not None:
                text = str(value).strip().upper()
                if text:
                    tokens.add(text)

        walk(data)
        return tokens

    @classmethod
    def _extract_decimal_from_paths(cls, data: dict, *paths: tuple[str, ...]) -> Optional[Decimal]:
        for path in paths:
            current: Any = data
            ok = True
            for key in path:
                if not isinstance(current, dict) or key not in current:
                    ok = False
                    break
                current = current[key]
            if ok:
                value = cls._to_decimal(current, None)
                if value is not None:
                    return value
        return None
