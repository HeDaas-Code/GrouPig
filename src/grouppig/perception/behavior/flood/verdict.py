"""grouppig.perception.behavior.flood.verdict —— 刷屏判定器（``rpc:flood.detect``）。

综合流速（``rpc:flood.velocity``）与重复度（``rpc:flood.repetition``）给出刷屏结论，
规则显式可解释（设计依赖：``rpc:flood.detect`` → ``rpc:flood.velocity`` / ``rpc:flood.repetition``）：

===========  ==================================================  ============
规则          条件                                                结论
===========  ==================================================  ============
``R1``        流速 extreme                                        刷屏（爆发）
``R2``        重复度 extreme                                      刷屏（纯复读轰炸）
``R3``        流速 high 且（复读率 ≥ high 或 图片占比 ≥ 0.5）      刷屏（高速 + 复读/图片）
===========  ==================================================  ============

命中信号进 ``signals``，命中规则进 ``rules``；``confidence`` 随信号数量单调变化，
供聚合器判断「硬结论」还是「模糊窗口（交给轻量模型判别）」。

设计：``grouppig.perception.behavior.flood.verdict``（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

from grouppig.infra.runtime.registry import Registry
from grouppig.perception.behavior.flood.repetition import RepetitionDetector, build_detector
from grouppig.perception.behavior.flood.velocity import VelocityCalculator, build_calculator
from grouppig.perception.runtime import calls as calls_module
from grouppig.perception.runtime import config as config_module

MODULE_ID = "grouppig.perception.behavior.flood.verdict"
RPC_DETECT = "rpc:flood.detect"

#: 设计依赖：两个信号源。
DOWNSTREAM_VELOCITY = "rpc:flood.velocity"
DOWNSTREAM_REPETITION = "rpc:flood.repetition"

#: 刷屏等级。
LEVELS: tuple[str, ...] = ("low", "normal", "high", "extreme")

RULE_BURST = "R1:velocity_extreme"
RULE_REPEAT_BOMB = "R2:repetition_extreme"
RULE_HIGH_MIX = "R3:velocity_high_with_repeat_or_image"
RULE_REPEAT_FLOOD = "R4:repetition_high_in_window"

#: 图片轰炸阈值。
DEFAULT_IMAGE_BOMB = 0.5


class FloodVerdict:
    """流速 + 重复度 → 刷屏结论。"""

    def __init__(
        self,
        *,
        registry: Any = None,
        config: Any = None,
        logger: Any = None,
        velocity: VelocityCalculator | None = None,
        repetition: RepetitionDetector | None = None,
        confidence: float | None = None,
        image_bomb_ratio: float | None = None,
        window: Any = None,
    ) -> None:
        self.registry = calls_module.registry_of(registry)
        self.config = config
        self.logger = logger
        self.velocity = velocity or build_calculator(
            config=config, logger=logger, registry=self.registry, window=window
        )
        self.repetition = repetition or build_detector(
            config=config, logger=logger, registry=self.registry, window=window
        )
        self.confidence_base = float(
            confidence if confidence is not None else config_module.number(config, "perception.flood.confidence", 0.6)
        )
        self.image_bomb_ratio = float(
            image_bomb_ratio
            if image_bomb_ratio is not None
            else config_module.number(config, "perception.flood.image_bomb_ratio", DEFAULT_IMAGE_BOMB)
        )

    # ---- 纯计算 --------------------------------------------------------
    def combine(self, velocity: Mapping[str, Any], repetition: Mapping[str, Any]) -> dict[str, Any]:
        """把两个信号合成刷屏结论（纯函数）。"""

        velocity_level = str(velocity.get("level") or "low")
        repetition_level = str(repetition.get("level") or "low")
        repeat_ratio = float(repetition.get("repeat_ratio") or 0.0)
        image_ratio = float(repetition.get("image_ratio") or 0.0)
        forward_ratio = float(repetition.get("forward_ratio") or 0.0)
        rate = float(velocity.get("rate") or 0.0)

        signals: list[str] = []
        if velocity_level in ("high", "extreme"):
            signals.append("velocity")
        if repetition_level in ("high", "extreme"):
            signals.append("repetition")
        if image_ratio >= self.image_bomb_ratio:
            signals.append("image_bomb")
        if forward_ratio >= self.image_bomb_ratio:
            signals.append("forward_bomb")

        rules: list[str] = []
        if velocity_level == "extreme":
            rules.append(RULE_BURST)
        if repetition_level == "extreme":
            rules.append(RULE_REPEAT_BOMB)
        if velocity_level == "high" and (
            repetition_level in ("high", "extreme") or image_ratio >= self.image_bomb_ratio
        ):
            rules.append(RULE_HIGH_MIX)

        # 高复读率本身就算刷屏（复读轰炸），不必等流速也拉满
        if repetition_level == "high" and repeat_ratio >= 0.6:
            rules.append(RULE_REPEAT_FLOOD)

        flooding = bool(rules)
        if flooding:
            level = "extreme" if {RULE_BURST, RULE_REPEAT_BOMB} & set(rules) else "high"
            confidence = min(0.99, self.confidence_base + 0.1 * max(0, len(signals)))
        else:
            level = "normal" if (rate > 0 or repeat_ratio > 0) else "low"
            confidence = max(0.05, self.confidence_base - 0.15 * len(signals))
        return {
            "flooding": flooding,
            "level": level,
            "confidence": round(confidence, 4),
            "signals": signals,
            "rules": rules,
            "rate": rate,
            "repeat_ratio": repeat_ratio,
            "image_ratio": image_ratio,
            "forward_ratio": forward_ratio,
            "velocity_level": velocity_level,
            "repetition_level": repetition_level,
            "thresholds": {
                "confidence": self.confidence_base,
                "image_bomb_ratio": self.image_bomb_ratio,
                "high_rate": self.velocity.high,
                "extreme_rate": self.velocity.extreme,
                "high_repeat": self.repetition.high,
                "extreme_repeat": self.repetition.extreme,
            },
        }

    # ---- 数据源 --------------------------------------------------------
    async def detect(
        self,
        group_id: int,
        *,
        seconds: float | None = None,
        now: float | None = None,
        messages: Sequence[Mapping[str, Any]] | None = None,
        window: Any = None,
    ) -> dict[str, Any]:
        """检测某群当前是否刷屏（设计依赖两个 ``rpc:`` 信号源）。"""

        stamp = float(now if now is not None else time.time())
        span = float(seconds or self.velocity.window_seconds)
        results: list[calls_module.CallOutcome] = []

        velocity_outcome = await calls_module.call_leaf(
            self.registry,
            DOWNSTREAM_VELOCITY,
            lambda gid, **kw: self.velocity.velocity(gid, **kw),
            int(group_id),
            seconds=span,
            now=stamp,
            messages=messages,
            window=window,
        )
        results.append(velocity_outcome)
        repetition_outcome = await calls_module.call_leaf(
            self.registry,
            DOWNSTREAM_REPETITION,
            lambda gid, **kw: self.repetition.repetition(gid, **kw),
            int(group_id),
            seconds=span,
            now=stamp,
            messages=messages,
            window=window,
        )
        results.append(repetition_outcome)

        velocity = (
            dict(velocity_outcome.result)
            if velocity_outcome.ok and isinstance(velocity_outcome.result, Mapping)
            else {}
        )
        repetition = (
            dict(repetition_outcome.result)
            if repetition_outcome.ok and isinstance(repetition_outcome.result, Mapping)
            else {}
        )
        payload = self.combine(velocity, repetition)
        return {
            "group_id": int(group_id),
            "window_seconds": span,
            "since": stamp - span,
            "until": stamp,
            "velocity": velocity,
            "repetition": repetition,
            "downstream": calls_module.outcomes(results),
            "at": stamp,
            **payload,
        }

    # ---- 注册 ----------------------------------------------------------
    def register(self, registry: Registry, *, replace: bool = True) -> Registry:
        """注册 ``rpc:flood.detect``。"""

        self.registry = registry
        self.velocity.registry = registry
        self.repetition.registry = registry

        async def detect(
            group_id: int,
            seconds: float | None = None,
            now: float | None = None,
            messages: Sequence[Mapping[str, Any]] | None = None,
            window: Any = None,
            **_: Any,
        ) -> dict[str, Any]:
            return await self.detect(group_id, seconds=seconds, now=now, messages=messages, window=window)

        registry.register(RPC_DETECT, detect, module=MODULE_ID, replace=replace)
        return registry

    def snapshot(self) -> dict[str, Any]:
        return {
            "confidence": self.confidence_base,
            "image_bomb_ratio": self.image_bomb_ratio,
            "velocity": self.velocity.snapshot(),
            "repetition": self.repetition.snapshot(),
        }


def build_verdict(*, config: Any = None, logger: Any = None, **kwargs: Any) -> FloodVerdict:
    return FloodVerdict(config=config, logger=logger, **kwargs)


def register(registry: Registry, verdict: FloodVerdict | None = None, **kwargs: Any) -> Registry:
    """把刷屏判定器注册进注册表（未传实例时现建一个）。"""

    return (verdict or build_verdict(**kwargs)).register(registry)


__all__ = [
    "DEFAULT_IMAGE_BOMB",
    "DOWNSTREAM_REPETITION",
    "DOWNSTREAM_VELOCITY",
    "LEVELS",
    "MODULE_ID",
    "RPC_DETECT",
    "RULE_BURST",
    "RULE_HIGH_MIX",
    "RULE_REPEAT_BOMB",
    "RULE_REPEAT_FLOOD",
    "FloodVerdict",
    "build_verdict",
    "register",
]
