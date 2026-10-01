"""grouppig.perception.runtime.config —— 感知层阈值、权重与开关。

所有阈值都在本模块给出**代码默认值**，因此 ``config/grouppig.toml`` 不写
``[perception]`` 段也能跑；一旦配置里出现同名键（``perception.<area>.<knob>``），
以配置为准（见 :func:`get`）。:func:`describe` 返回生效值快照，供健康检查与测试断言。

normify id: ``grouppig.perception.runtime.config``（运行时补充模块，设计树暂无）。
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

#: 配置作用域前缀（``grouppig.perception`` 域）。
SCOPE = "perception"

#: 默认风险词（可被 ``perception.cleaner.risk_keywords`` 整体覆盖）。
DEFAULT_RISK_KEYWORDS: tuple[str, ...] = (
    "刷单",
    "代练",
    "外挂",
    "博彩",
    "赌博",
    "诈骗",
    "加群",
    "私聊我",
    "涉黄",
    "返利",
)

#: 插话价值各分量权重（会在 :func:`weights` 里归一化到 1）。
DEFAULT_INTERRUPT_WEIGHTS: dict[str, float] = {
    "topic_familiarity": 0.25,
    "intimacy": 0.20,
    "silence": 0.25,
    "mentioned": 0.30,
}

#: 全部默认值（``perception.`` 前缀的扁平键）。
DEFAULTS: dict[str, Any] = {
    # observer.window
    "perception.window.seconds": 300,
    "perception.window.max_messages": 1000,
    # observer.buffer
    "perception.buffer.capacity": 512,
    "perception.buffer.auto_drain_at": 0,
    "perception.buffer.drain_batch": 64,
    "perception.buffer.drain_interval": 1.0,
    # normalizer.cleaner
    "perception.cleaner.drop_bot": False,
    "perception.cleaner.risk_keywords": DEFAULT_RISK_KEYWORDS,
    # normalizer.dedup
    "perception.dedup.window_seconds": 120,
    "perception.dedup.similarity": 0.9,
    "perception.dedup.repeat_threshold": 3,
    "perception.dedup.max_recent": 200,
    # normalizer.featurizer
    "perception.features.keywords": 8,
    "perception.features.classify_min_interval": 2.0,
    "perception.features.classify_min_messages": 3,
    # behavior.flood
    "perception.flood.window_seconds": 30,
    "perception.flood.velocity.high": 0.5,
    "perception.flood.velocity.extreme": 1.5,
    "perception.flood.repetition.high": 0.5,
    "perception.flood.repetition.extreme": 0.8,
    "perception.flood.confidence": 0.6,
    # behavior.rhythm
    "perception.rhythm.silent_seconds": 120,
    "perception.rhythm.warming_rate": 0.05,
    "perception.rhythm.dense_rate": 0.4,
    "perception.rhythm.trend_window": 12,
    # behavior.classifier
    "perception.classify.window_seconds": 60,
    "perception.classify.ambiguous_below": 0.55,
    "perception.classify.use_llm": True,
    "perception.classify.cascade_interrupt": True,
    "perception.classify.cascade_presets": True,
    "perception.classify.decision_mode": "active",
    # 滞回保质期（秒）：在任行为保持超过这么久后，滞回不再阻拦切换（见 DEFAULT_STICKY_TTL）。
    "perception.classify.sticky_ttl": 180.0,
    # normalizer.cleaner 的下游级联开关
    "perception.normalizer.cascade": True,
    # interrupt
    "perception.interrupt.cooldown_seconds": 60,
    "perception.interrupt.min_gap_seconds": 20,
    "perception.interrupt.max_per_hour": 12,
    "perception.interrupt.threshold": 0.55,
    "perception.interrupt.weights": DEFAULT_INTERRUPT_WEIGHTS,
    "perception.interrupt.trigger_flow": True,
    # 决定开口即记一次发言（不记的话冷却闸门是死代码，见 decision.DEFAULT_RECORD_ON_SPEAK）。
    "perception.interrupt.record_on_speak": True,
    # 提问密度折让（见 decision.DEFAULT_QUESTION_RATIO_MIN）：不动权重，只降门槛。
    "perception.interrupt.question_ratio_min": 0.34,
    "perception.interrupt.question_discount": 0.08,
    # 多钩子自主激活网络（不替换传统插话分数）。
    "perception.activation.enabled": True,
    "perception.activation.followup_timeout": 900.0,
    "perception.activation.event_gap": 180.0,
    "perception.activation.memory_promote_after": 3,
    "perception.activation.memory_ttl": 86400.0,
    "perception.activation.energy_initial": 1.0,
    "perception.activation.energy_reserve": 0.15,
    "perception.activation.energy_recovery_rate": 0.018,
    "perception.activation.bot_names": (),
    "perception.activation.interest_tags": (),
}


def get(config: Any, path: str, default: Any = None) -> Any:
    """读配置项；``config`` 可以是 :class:`~grouppig.infra.config.loader.Config`、映射或 ``None``。

    未配置时回落到 :data:`DEFAULTS`，再回落到 ``default``。
    """

    fallback = DEFAULTS.get(path, default)
    if config is None:
        return fallback
    getter = getattr(config, "get", None)
    if callable(getter):
        try:
            value = getter(path, None)
        except Exception:  # pragma: no cover - 自定义配置对象
            value = None
        return fallback if value is None else value
    if isinstance(config, Mapping):
        value = config.get(path)
        return fallback if value is None else value
    return fallback


def number(config: Any, path: str, default: float = 0.0) -> float:
    value = get(config, path, default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def integer(config: Any, path: str, default: int = 0) -> int:
    value = get(config, path, default)
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def flag(config: Any, path: str, default: bool = False) -> bool:
    value = get(config, path, default)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "on")
    return bool(value)


def sequence(config: Any, path: str, default: tuple[Any, ...] = ()) -> tuple[Any, ...]:
    value = get(config, path, default)
    if value is None:
        return tuple(default)
    if isinstance(value, str):
        return (value,)
    try:
        return tuple(value)
    except TypeError:  # pragma: no cover - 非法配置
        return tuple(default)


def weights(config: Any, path: str = "perception.interrupt.weights") -> dict[str, float]:
    """插话权重（归一化到总和 1；非法值回落到默认权重）。"""

    raw = get(config, path, DEFAULT_INTERRUPT_WEIGHTS)
    source = dict(DEFAULT_INTERRUPT_WEIGHTS)
    if isinstance(raw, Mapping):
        for key, value in raw.items():
            try:
                source[str(key)] = float(value)
            except (TypeError, ValueError):
                continue
    total = sum(max(0.0, v) for v in source.values())
    if total <= 0:
        source = dict(DEFAULT_INTERRUPT_WEIGHTS)
        total = sum(source.values())
    return {key: max(0.0, value) / total for key, value in source.items()}


def describe(config: Any = None) -> dict[str, Any]:
    """生效值快照（含配置覆盖），供健康检查与验收断言。"""

    snapshot: dict[str, Any] = {}
    for path, default in DEFAULTS.items():
        value = get(config, path, default)
        snapshot[path] = list(value) if isinstance(value, tuple) else value
    snapshot["perception.interrupt.weights"] = weights(config)
    return snapshot


__all__ = [
    "DEFAULTS",
    "DEFAULT_INTERRUPT_WEIGHTS",
    "DEFAULT_RISK_KEYWORDS",
    "SCOPE",
    "describe",
    "flag",
    "get",
    "integer",
    "number",
    "sequence",
    "weights",
]
