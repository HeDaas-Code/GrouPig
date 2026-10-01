"""grouppig.gateway.sender.rate-limiter —— 发送节流器。

职责（对应设计 ``grouppig.gateway.sender.rate-limiter``）：

* ``rpc:rate.check``：检查当前是否允许发言（纯查询，不扣令牌）。
* ``rpc:rate.wait``：等待到可发言（真正扣令牌），返回等待时长与决策。

节流模型 = **令牌桶 + 冷却**：

* 令牌桶：``per_minute`` 速率回填、``burst`` 桶容量 —— 控制「短时间说太多」。
* 冷却：``min_interval`` 秒两条之间最小间隔 —— 控制「连着刷」。
* 群规覆盖：``group_rules`` 按群设置更严/更松的规则；``global_rule`` 是跨群总闸。

配置来源（``config/grouppig.toml`` 的 ``[onebot.rate]``，可选，缺省用内置安全值）::

    [onebot.rate]
    per_minute = 20
    burst = 3
    min_interval = 2.0
    [onebot.rate.groups."123456"]
    per_minute = 6
    burst = 2
    min_interval = 8.0

normify id: ``grouppig.gateway.sender.rate-limiter``（叶子模块）。
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

DEFAULT_PER_MINUTE = 20.0
DEFAULT_BURST = 3
DEFAULT_MIN_INTERVAL = 2.0


@dataclass(frozen=True, slots=True)
class RateRule:
    """一条节流规则。"""

    per_minute: float = DEFAULT_PER_MINUTE
    burst: int = DEFAULT_BURST
    min_interval: float = DEFAULT_MIN_INTERVAL
    enabled: bool = True

    @property
    def refill_per_second(self) -> float:
        return max(0.0, self.per_minute) / 60.0

    @classmethod
    def from_mapping(cls, spec: Mapping[str, Any] | None, *, base: RateRule | None = None) -> RateRule:
        base = base or cls()
        if not spec:
            return base
        return cls(
            per_minute=float(spec.get("per_minute", base.per_minute)),
            burst=int(spec.get("burst", base.burst)),
            min_interval=float(spec.get("min_interval", base.min_interval)),
            enabled=bool(spec.get("enabled", base.enabled)),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "per_minute": self.per_minute,
            "burst": self.burst,
            "min_interval": self.min_interval,
            "enabled": self.enabled,
        }


@dataclass(slots=True)
class RateDecision:
    """节流判定结果。"""

    allowed: bool
    wait_seconds: float = 0.0
    reason: str = "ok"
    tokens: float = 0.0
    group_id: int | str | None = None
    rule: RateRule = field(default_factory=RateRule)
    penalized_until: float = 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "allowed": self.allowed,
            "wait_seconds": round(self.wait_seconds, 4),
            "reason": self.reason,
            "tokens": round(self.tokens, 4),
            "group_id": self.group_id,
            "rule": self.rule.as_dict(),
            "penalized_until": self.penalized_until,
        }


@dataclass(slots=True)
class _Bucket:
    tokens: float
    updated_at: float
    last_sent: float = 0.0
    penalized_until: float = 0.0
    sent: int = 0


@dataclass
class RateStats:
    checks: int = 0
    allowed: int = 0
    delayed: int = 0
    denied: int = 0
    consumed: int = 0
    waited_seconds: float = 0.0
    penalties: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "checks": self.checks,
            "allowed": self.allowed,
            "delayed": self.delayed,
            "denied": self.denied,
            "consumed": self.consumed,
            "waited_seconds": round(self.waited_seconds, 4),
            "penalties": self.penalties,
        }


class RateLimiter:
    """按群规与时间窗限制发言频率。"""

    def __init__(
        self,
        *,
        default_rule: RateRule | None = None,
        group_rules: Mapping[Any, Mapping[str, Any] | RateRule] | None = None,
        global_rule: RateRule | None = None,
        logger: Any | None = None,
        clock: Any = time.monotonic,
        sleep: Any = None,
        max_sleep_step: float = 0.25,
    ) -> None:
        self.default_rule = default_rule or RateRule()
        self.group_rules: dict[str, RateRule] = {}
        for key, spec in (group_rules or {}).items():
            rule = spec if isinstance(spec, RateRule) else RateRule.from_mapping(spec)
            self.group_rules[str(key)] = rule
        self.global_rule = global_rule
        self.logger = logger
        self._clock = clock
        self._sleep = sleep or asyncio.sleep
        self.max_sleep_step = max_sleep_step
        self.stats = RateStats()
        self._buckets: dict[str, _Bucket] = {}

    # ---- 配置 ----------------------------------------------------------
    @classmethod
    def from_config(cls, config: Any, **kwargs: Any) -> RateLimiter:
        """从配置构造（``[onebot.rate]`` + ``[onebot.rate.groups.*]``）。"""

        base = RateRule.from_mapping(config.get("onebot.rate") if config is not None else None)
        global_spec = config.get("onebot.rate.global") if config is not None else None
        groups = config.get("onebot.rate.groups", {}) if config is not None else {}
        return cls(
            default_rule=base,
            group_rules=groups if isinstance(groups, Mapping) else {},
            global_rule=RateRule.from_mapping(global_spec, base=base) if global_spec else None,
            **kwargs,
        )

    def rule_for(self, group_id: int | str | None) -> RateRule:
        if group_id is not None and str(group_id) in self.group_rules:
            return self.group_rules[str(group_id)]
        return self.default_rule

    def set_rule(self, group_id: int | str, rule: RateRule) -> None:
        self.group_rules[str(group_id)] = rule

    # ---- 判定 ----------------------------------------------------------
    def check(self, group_id: int | str | None = None, *, cost: float = 1.0, now: float | None = None) -> RateDecision:
        """检查是否允许发送（不扣令牌）。"""

        moment = self._clock() if now is None else now
        self.stats.checks += 1
        rule = self.rule_for(group_id)
        if not rule.enabled:
            self.stats.allowed += 1
            return RateDecision(allowed=True, reason="disabled", group_id=group_id, rule=rule)

        bucket = self._bucket(group_id, moment)
        if bucket.penalized_until > moment:
            wait = bucket.penalized_until - moment
            self.stats.delayed += 1
            return RateDecision(
                allowed=False,
                wait_seconds=wait,
                reason="penalty",
                tokens=bucket.tokens,
                group_id=group_id,
                rule=rule,
                penalized_until=bucket.penalized_until,
            )

        wait_cooldown = max(0.0, rule.min_interval - (moment - bucket.last_sent)) if bucket.last_sent else 0.0
        refill = rule.refill_per_second
        missing = max(0.0, cost - bucket.tokens)
        if missing > 0 and refill <= 0:
            self.stats.denied += 1
            return RateDecision(
                allowed=False,
                wait_seconds=float("inf"),
                reason="rate_zero",
                tokens=bucket.tokens,
                group_id=group_id,
                rule=rule,
            )
        wait_tokens = missing / refill if missing > 0 else 0.0
        wait = max(wait_cooldown, wait_tokens)

        reason = "cooldown" if wait_cooldown >= wait_tokens else "tokens"
        global_decision = self._check_global(cost, moment)
        if global_decision is not None and not global_decision.allowed and global_decision.wait_seconds > wait:
            wait = global_decision.wait_seconds
            reason = "global"

        if wait <= 0:
            self.stats.allowed += 1
            return RateDecision(
                allowed=True, wait_seconds=0.0, reason="ok", tokens=bucket.tokens, group_id=group_id, rule=rule
            )
        self.stats.delayed += 1
        return RateDecision(
            allowed=False,
            wait_seconds=wait,
            reason=reason,
            tokens=bucket.tokens,
            group_id=group_id,
            rule=rule,
        )

    def _check_global(self, cost: float, now: float) -> RateDecision | None:
        if self.global_rule is None:
            return None
        bucket = self._bucket("__global__", now)
        rule = self.global_rule
        if not rule.enabled:
            return None
        wait_cooldown = max(0.0, rule.min_interval - (now - bucket.last_sent)) if bucket.last_sent else 0.0
        missing = max(0.0, cost - bucket.tokens)
        refill = rule.refill_per_second
        wait_tokens = missing / refill if (missing > 0 and refill > 0) else 0.0
        wait = max(wait_cooldown, wait_tokens)
        if wait <= 0:
            return RateDecision(allowed=True, group_id="__global__", rule=rule, tokens=bucket.tokens)
        return RateDecision(
            allowed=False, wait_seconds=wait, reason="global", group_id="__global__", rule=rule, tokens=bucket.tokens
        )

    async def wait(
        self,
        group_id: int | str | None = None,
        *,
        cost: float = 1.0,
        max_wait: float | None = None,
        timeout: float | None = None,
        now: float | None = None,
    ) -> RateDecision:
        """等待到可发送（返回时已扣令牌）；超限返回 ``allowed=False``。"""

        limit = max_wait if max_wait is not None else timeout
        waited = 0.0
        while True:
            moment = self._clock()
            decision = self.check(group_id, cost=cost, now=moment)
            if decision.allowed:
                self.consume(group_id, cost=cost, now=moment)
                decision.wait_seconds = waited
                if waited:
                    self.stats.waited_seconds += waited
                return decision
            if decision.wait_seconds == float("inf"):
                return decision
            step = max(0.0, min(decision.wait_seconds, self.max_sleep_step))
            if limit is not None and waited + step > limit:
                return RateDecision(
                    allowed=False,
                    wait_seconds=decision.wait_seconds,
                    reason="wait_limit",
                    tokens=decision.tokens,
                    group_id=group_id,
                    rule=decision.rule,
                )
            await self._sleep(step)
            waited += step

    def consume(
        self, group_id: int | str | None = None, *, cost: float = 1.0, now: float | None = None
    ) -> RateDecision:
        """记录一次发送（扣令牌 + 记冷却起点）。"""

        moment = self._clock() if now is None else now
        bucket = self._bucket(group_id, moment)
        bucket.tokens = max(0.0, bucket.tokens - cost)
        bucket.last_sent = moment
        bucket.sent += 1
        self.stats.consumed += 1
        if self.global_rule is not None:
            gbucket = self._bucket("__global__", moment)
            gbucket.tokens = max(0.0, gbucket.tokens - cost)
            gbucket.last_sent = moment
            gbucket.sent += 1
        return RateDecision(allowed=True, tokens=bucket.tokens, group_id=group_id, rule=self.rule_for(group_id))

    def penalize(self, group_id: int | str | None, seconds: float, *, reason: str = "") -> float:
        """群规惩罚：一段时间内禁止发言（例如被管理员警告）。"""

        moment = self._clock()
        bucket = self._bucket(group_id, moment)
        bucket.penalized_until = max(bucket.penalized_until, moment + max(0.0, seconds))
        self.stats.penalties += 1
        self._log("warning", "rate.penalized", group_id=group_id, seconds=seconds, reason=reason)
        return bucket.penalized_until

    # ---- 状态 ----------------------------------------------------------
    def snapshot(self, group_id: int | str | None = None) -> dict[str, Any]:
        if group_id is not None:
            key = self._key(group_id)
            bucket = self._buckets.get(key)
            return {
                "group_id": group_id,
                "rule": self.rule_for(group_id).as_dict(),
                "tokens": round(bucket.tokens, 4) if bucket else None,
                "last_sent": bucket.last_sent if bucket else 0.0,
                "sent": bucket.sent if bucket else 0,
                "penalized_until": bucket.penalized_until if bucket else 0.0,
                "stats": self.stats.as_dict(),
            }
        return {
            "default_rule": self.default_rule.as_dict(),
            "group_rules": {k: v.as_dict() for k, v in self.group_rules.items()},
            "global_rule": self.global_rule.as_dict() if self.global_rule else None,
            "buckets": len(self._buckets),
            "stats": self.stats.as_dict(),
        }

    def reset(self, group_id: int | str | None = None) -> None:
        if group_id is None:
            self._buckets.clear()
            return
        self._buckets.pop(self._key(group_id), None)

    # ---- 内部 ----------------------------------------------------------
    @staticmethod
    def _key(group_id: int | str | None) -> str:
        return "__none__" if group_id is None else str(group_id)

    def _bucket(self, group_id: int | str | None, now: float) -> _Bucket:
        key = self._key(group_id)
        rule = self.rule_for(group_id) if key != "__global__" else (self.global_rule or self.default_rule)
        bucket = self._buckets.get(key)
        if bucket is None:
            bucket = _Bucket(tokens=float(rule.burst), updated_at=now)
            self._buckets[key] = bucket
            return bucket
        elapsed = max(0.0, now - bucket.updated_at)
        if elapsed:
            bucket.tokens = min(float(rule.burst), bucket.tokens + elapsed * rule.refill_per_second)
            bucket.updated_at = now
        return bucket

    def _log(self, level: str, event: str, **fields: Any) -> None:
        if self.logger is None:
            return
        with contextlib.suppress(Exception):
            getattr(self.logger, level, self.logger.info)(event, **fields)


# --------------------------------------------------------------------------
# rpc:rate.check / rpc:rate.wait
# --------------------------------------------------------------------------
def make_handlers(limiter: RateLimiter) -> dict[str, Any]:
    async def check(group_id: Any = None, *, cost: float = 1.0, **_: Any) -> dict[str, Any]:
        return limiter.check(group_id, cost=cost).as_dict()

    async def wait(
        group_id: Any = None,
        *,
        cost: float = 1.0,
        max_wait: float | None = None,
        timeout: float | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        return (await limiter.wait(group_id, cost=cost, max_wait=max_wait, timeout=timeout)).as_dict()

    return {"rpc:rate.check": check, "rpc:rate.wait": wait}


def register(registry: Any, limiter: RateLimiter) -> None:
    for name, handler in make_handlers(limiter).items():
        registry.register(name, handler, module="grouppig.gateway.sender.rate-limiter", replace=True)


__all__ = [
    "DEFAULT_BURST",
    "DEFAULT_MIN_INTERVAL",
    "DEFAULT_PER_MINUTE",
    "RateDecision",
    "RateLimiter",
    "RateRule",
    "RateStats",
    "make_handlers",
    "register",
]
