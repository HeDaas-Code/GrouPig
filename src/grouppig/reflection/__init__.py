"""grouppig.reflection —— 反思策略层：行为预设库 + 会话复盘 + 策略生成与评估。

设计（`grouppig.reflection`）：「维护行为预设库，在会话结束后反思行为模式，
生成并评估新的行为策略。」

装配（集成层 t10 一行接入）::

    from grouppig.reflection import install_reflection

    await attach_memory(container)              # memory 必须先挂载（读聊天流水与会话档案）
    layer = await install_reflection(container) # 注册本域 12 个 rpc: 名字并订阅会话完成事件
    await layer.close()

模块构成（逐字镜像 normify 设计）:

* :mod:`grouppig.reflection.presets.registry` —— `rpc:presets.load` / `rpc:presets.register`
* :mod:`grouppig.reflection.presets.matcher` —— `rpc:presets.match`
* :mod:`grouppig.reflection.strategy.synthesizer` —— `rpc:strategy.generate`
* :mod:`grouppig.reflection.strategy.validator` —— `rpc:strategy.validate`
* :mod:`grouppig.reflection.strategy.rollback` —— `rpc:strategy.rollback`
* :mod:`grouppig.reflection.evaluator.scoring` —— `rpc:strategy.score`
* :mod:`grouppig.reflection.evaluator.ab-test` —— `rpc:strategy.evaluate`
* :mod:`grouppig.reflection.session-review.timeline` —— `rpc:review.timeline` / `rpc:review.on-session-completed`
* :mod:`grouppig.reflection.session-review.metrics` —— `rpc:review.metrics`
* :mod:`grouppig.reflection.session-review.insights` —— `rpc:review.analyze`

契约：本域 12 个 `rpc:` 名字全部逐字取自 `normify-grouppig/api-index.json`
（见 :data:`REFLECTION_RPC`）。设计里本域**不拥有**任何 `kafka:` 主题：会话完成事件
（`kafka:grouppig.session.completed`）由 session 域发布，本域只**订阅**它并落到
`rpc:review.on-session-completed`（设计事件边）。

**闭环位置**：本域既是闭环的**起点**（`rpc:presets.match` 给表达层选预设想），也是**收尾**：
会话结束 → `rpc:review.timeline` → `rpc:review.metrics` → `rpc:review.analyze` →
`rpc:strategy.generate` → `rpc:strategy.validate` → `rpc:strategy.evaluate` →
`rpc:presets.register`，下一场会话再用 `rpc:presets.match` 读回新版本。

normify id: `grouppig.reflection`（容器模块）。
"""

from __future__ import annotations

import contextlib
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from grouppig.infra.runtime import contract
from grouppig.reflection.evaluator.ab_test import ABTestEvaluator
from grouppig.reflection.evaluator.scoring import StrategyScorer
from grouppig.reflection.presets.matcher import PresetMatcher
from grouppig.reflection.presets.registry import PresetRegistry, builtin_presets
from grouppig.reflection.session_review.insights import InsightGenerator
from grouppig.reflection.session_review.metrics import BehaviorMetrics
from grouppig.reflection.session_review.timeline import TimelineBuilder
from grouppig.reflection.strategy.rollback import RollbackManager
from grouppig.reflection.strategy.synthesizer import StrategySynthesizer
from grouppig.reflection.strategy.validator import StrategyValidator

REFLECTION_SCOPE = "grouppig.reflection"

#: 本域 12 个契约 `rpc:` 名字（逐字抄自 api-index.json）。
REFLECTION_RPC: tuple[str, ...] = (
    "rpc:presets.load",
    "rpc:presets.match",
    "rpc:presets.register",
    "rpc:review.analyze",
    "rpc:review.metrics",
    "rpc:review.on-session-completed",
    "rpc:review.timeline",
    "rpc:strategy.evaluate",
    "rpc:strategy.generate",
    "rpc:strategy.rollback",
    "rpc:strategy.score",
    "rpc:strategy.validate",
)

#: 子域 rpc 名字（装配时按叶子逐个注册并做覆盖自检）。
PRESETS_RPC: tuple[str, ...] = ("rpc:presets.load", "rpc:presets.register", "rpc:presets.match")
STRATEGY_RPC: tuple[str, ...] = (
    "rpc:strategy.generate",
    "rpc:strategy.validate",
    "rpc:strategy.rollback",
)
EVALUATOR_RPC: tuple[str, ...] = ("rpc:strategy.score", "rpc:strategy.evaluate")
SESSION_REVIEW_RPC: tuple[str, ...] = (
    "rpc:review.timeline",
    "rpc:review.metrics",
    "rpc:review.analyze",
    "rpc:review.on-session-completed",
)

#: 会话域发布的完成事件（本域只订阅，不拥有）。
TOPIC_SESSION_COMPLETED = "kafka:grouppig.session.completed"

#: 本域订阅的主题（设计事件边：kafka:grouppig.session.completed → rpc:review.on-session-completed）。
REFLECTION_TOPICS: tuple[str, ...] = (TOPIC_SESSION_COMPLETED,)


@dataclass
class ReflectionContext:
    """本域对外部世界的**唯一入口**。

    本域一切跨域调用都必须写在这里列的契约名字上：

    * `call` —— 由 :func:`install_reflection` 绑定到容器的注册表；
    * `publish` / `bus` —— 事件总线（本域只订阅，`publish` 留给后续扩展）；
    * `now` / `log` —— 可注入的时钟与日志（单测替身用）。

    调用未注册的名字会抛 :class:`grouppig.infra.runtime.errors.HandlerNotRegistered`；
    反思域的叶子对此是**容错**的（拿不到下游就退化为本地计算），因为复盘绝不该
    因为某个下游没挂而整体失败。
    """

    call: Any
    publish: Any
    now: Any
    log: Any = field(default=lambda *args, **kwargs: None)
    registry: Any = None
    bus: Any = None
    allow_missing: bool = True
    calls: dict[str, int] = field(default_factory=dict, repr=False)

    async def invoke(self, name: str, *args: Any, **kwargs: Any) -> Any:
        """带计数的 :meth:`call`（便于诊断哪条契约链路被用到）。"""

        return await self.call(name, *args, **kwargs)

    def stats(self) -> dict[str, Any]:
        return {"calls": dict(sorted(self.calls.items()))}


def _default_now() -> float:
    import time

    return time.time()


def _default_log(level: str, event: str, **fields: Any) -> None:
    return None


def build_context(
    *,
    call: Any,
    publish: Any = None,
    now: Any = None,
    log: Any = None,
    registry: Any = None,
    bus: Any = None,
    allow_missing: bool = True,
) -> ReflectionContext:
    """构造 :class:`ReflectionContext`（`publish` 缺省为 no-op）。"""

    async def _noop_publish(topic: str, payload: Any = None, **kwargs: Any) -> Any:
        return None

    return ReflectionContext(
        call=call,
        publish=publish if publish is not None else _noop_publish,
        now=now if now is not None else _default_now,
        log=log if log is not None else _default_log,
        registry=registry,
        bus=bus,
        allow_missing=bool(allow_missing),
    )


@dataclass
class ReflectionLayer:
    """反思策略层的运行实例（一个进程一份）。"""

    ctx: ReflectionContext
    registry: Any = None
    logger: Any = None
    presets: PresetRegistry | None = None
    matcher: PresetMatcher | None = None
    validator: StrategyValidator | None = None
    synthesizer: StrategySynthesizer | None = None
    rollback: RollbackManager | None = None
    scorer: StrategyScorer | None = None
    evaluator: ABTestEvaluator | None = None
    timeline: TimelineBuilder | None = None
    metrics: BehaviorMetrics | None = None
    insights: InsightGenerator | None = None
    started: bool = False
    extra: dict[str, Any] = field(default_factory=dict)

    # ---- 装配 ----------------------------------------------------------
    def build(self) -> ReflectionLayer:
        """按依赖顺序构造 10 个叶子的实例（不碰外部世界）。"""

        self_id = int(self.extra.get("self_id", 0) or 0)
        self.presets = PresetRegistry(
            self.ctx,
            directory=self.extra.get("presets_dir"),
            presets=self.extra.get("presets"),
            use_builtin=bool(self.extra.get("use_builtin", True)),
            writer=self.extra.get("preset_writer"),
            reader=self.extra.get("preset_reader"),
        )
        self.matcher = PresetMatcher(self.ctx, min_score=float(self.extra.get("min_match_score", 0.0)))
        self.validator = StrategyValidator(self.ctx)
        self.scorer = StrategyScorer(self.ctx, self_id=self_id)
        self.evaluator = ABTestEvaluator(
            self.ctx,
            self_id=self_id,
            group_id=int(self.extra.get("group_id", 0) or 0),
        )
        self.synthesizer = StrategySynthesizer(self.ctx)
        self.rollback = RollbackManager(self.ctx)
        self.timeline = TimelineBuilder(self.ctx, self_id=self_id)
        self.metrics = BehaviorMetrics(self.ctx, self_id=self_id)
        self.insights = InsightGenerator(self.ctx, self_id=self_id, llm=self.extra.get("llm"))
        # 兜底预设：匹配不到任何候选时也要给出可执行的保守动作
        fallback = next((item for item in builtin_presets() if item["preset_id"] == "flood_silence"), None)
        self.matcher.set_fallback(fallback)
        return self

    def register(self) -> Any:
        """把本域处理器注册进注册表（名字与 owner 逐字对齐设计）。"""

        from grouppig.reflection.evaluator import ab_test as ab_module
        from grouppig.reflection.evaluator import scoring as scoring_module
        from grouppig.reflection.presets import matcher as matcher_module
        from grouppig.reflection.presets import registry as registry_module
        from grouppig.reflection.session_review import insights as insights_module
        from grouppig.reflection.session_review import metrics as metrics_module
        from grouppig.reflection.session_review import timeline as timeline_module
        from grouppig.reflection.strategy import rollback as rollback_module
        from grouppig.reflection.strategy import synthesizer as synthesizer_module
        from grouppig.reflection.strategy import validator as validator_module

        assert self.registry is not None
        for module, instance in (
            (registry_module, self.presets),
            (matcher_module, self.matcher),
            (validator_module, self.validator),
            (synthesizer_module, self.synthesizer),
            (rollback_module, self.rollback),
            (scoring_module, self.scorer),
            (ab_module, self.evaluator),
            (timeline_module, self.timeline),
            (metrics_module, self.metrics),
            (insights_module, self.insights),
        ):
            module.register(self.registry, instance)
        return self.registry

    # ---- 事件 ----------------------------------------------------------
    def subscribe(self, bus: Any) -> list[Any]:
        """订阅会话完成事件：`kafka:grouppig.session.completed` → 自动复盘。

        事件载荷处理是**容错**的：字段缺失时按空事件处理，绝不因为一条脏事件
        让订阅回调抛到总线里。
        """

        if bus is None:
            return []
        sub = bus.subscribe(TOPIC_SESSION_COMPLETED, self._on_session_completed, name="reflection:session-review")
        # 记下来供 close() 退订（无论订阅来自 start() 还是外部直接调用 subscribe()）
        self.extra.setdefault("subscriptions", []).append(sub)
        return [sub]

    async def _on_session_completed(self, event: Any) -> dict[str, Any] | None:
        payload = getattr(event, "payload", event) or {}
        if not isinstance(payload, Mapping) or self.timeline is None:
            return None
        try:
            review = await self.timeline.on_session_completed(payload, session_id=str(payload.get("session_id") or ""))
        except Exception as error:  # noqa: BLE001 - 订阅回调不许把总线带崩
            self._log("warning", "reflection.review_failed", error=str(error))
            return None
        self.extra.setdefault("reviews", {})[str(payload.get("session_id") or "")] = review
        self._log(
            "info",
            "reflection.session_reviewed",
            session_id=payload.get("session_id"),
            analyzed=(review or {}).get("analyzed"),
        )
        return review

    # ---- 生命周期 ------------------------------------------------------
    async def start(self, *, bus: Any = None) -> ReflectionLayer:
        """订阅会话完成事件并标记启动。"""

        target = bus if bus is not None else self.ctx.bus
        self.subscribe(target)
        self.started = True
        self._log("info", "reflection.started", rpc=len(REFLECTION_RPC), topics=list(REFLECTION_TOPICS))
        return self

    async def close(self) -> None:
        """退订（总线由容器统一关闭）。"""

        bus = self.ctx.bus
        for sub in self.extra.get("subscriptions") or []:
            if bus is not None:
                with contextlib.suppress(Exception):
                    bus.unsubscribe(sub)
        self.extra["subscriptions"] = []
        self.started = False
        self._log("info", "reflection.closed")

    # ---- 契约 / 健康 ---------------------------------------------------
    def contract_check(self) -> dict[str, list[str]]:
        """已注册名字与契约比对（`scope=grouppig.reflection`）。

        `missing` = 设计里属于本域的 `rpc:` 名字里没注册的；`unknown` = 注册了契约外的名字。
        本域不拥有 `kafka:` 主题，因此不对主题做注册表断言（订阅在 :meth:`subscribe` 里）。
        """

        if self.registry is None:
            return {"missing": list(REFLECTION_RPC), "unknown": []}
        index = contract.api_index()
        expected = {
            name for name, module in index.items() if module.startswith(REFLECTION_SCOPE) and name.startswith("rpc:")
        }
        registered = set(self.registry.names())
        return {
            "missing": sorted(expected - registered),
            "unknown": sorted(name for name in registered if name not in index),
        }

    async def health(self) -> dict[str, Any]:
        """给 `t10` 集成 / `t11` 验证用的健康摘要。"""

        check = self.contract_check()
        return {
            "started": self.started,
            "scope": REFLECTION_SCOPE,
            "contract": {
                "expected_rpc": len(REFLECTION_RPC),
                "subscribed_topics": len(REFLECTION_TOPICS),
                "missing": check["missing"],
                "unknown": check["unknown"],
            },
            "presets": {
                "registry": self.presets.status() if self.presets else {},
                "matcher": self.matcher.status() if self.matcher else {},
            },
            "strategy": {
                "synthesizer": self.synthesizer.status() if self.synthesizer else {},
                "validator": self.validator.status() if self.validator else {},
                "rollback": self.rollback.status() if self.rollback else {},
            },
            "evaluator": {
                "scoring": self.scorer.status() if self.scorer else {},
                "ab_test": self.evaluator.status() if self.evaluator else {},
            },
            "session_review": {
                "timeline": self.timeline.status() if self.timeline else {},
                "metrics": self.metrics.status() if self.metrics else {},
                "insights": self.insights.status() if self.insights else {},
            },
            "reviews": len(self.extra.get("reviews") or {}),
            "context": self.ctx.stats(),
        }

    def _log(self, level: str, event: str, **fields: Any) -> None:
        if self.logger is None:
            return
        with contextlib.suppress(Exception):
            getattr(self.logger, level, self.logger.info)(event, **fields)


def build_reflection(
    *,
    container: Any = None,
    registry: Any = None,
    bus: Any = None,
    logger: Any = None,
    now: Any = None,
    call: Any = None,
    publish: Any = None,
    extra: Mapping[str, Any] | None = None,
) -> ReflectionLayer:
    """装配反思策略层（不订阅事件、不连库；`await layer.start()` 才订阅）。"""

    if container is not None:
        registry = registry if registry is not None else getattr(container, "registry", None)
        bus = bus if bus is not None else getattr(container, "bus", None)
        logger = logger if logger is not None else getattr(container, "logger", None)
    if registry is None and call is None:
        from grouppig.infra.runtime.registry import default_registry

        registry = default_registry
    if call is None:
        call = registry.acall
    if publish is None and bus is not None:
        publish = bus.publish
    ctx = build_context(call=call, publish=publish, now=now, log=None, registry=registry, bus=bus)
    if logger is not None:
        ctx.log = lambda level, event, **fields: getattr(logger, level, logger.info)(event, **fields)
    layer = ReflectionLayer(ctx=ctx, registry=registry, logger=logger, extra=dict(extra or {}))
    return layer.build()


async def install_reflection(
    container: Any,
    *,
    subscribe: bool = True,
    start: bool = False,
    **options: Any,
) -> ReflectionLayer:
    """把反思策略层装进容器：构造叶子 → 注册 12 个 `rpc:` → （可选）订阅会话完成事件。

    设计里 memory 是反思层的下游（`grouppig.reflection` → `grouppig.memory`），
    所以调用前应先 `await attach_memory(container)`；本域对下游缺失是**容错**的
    （拿不到聊天流水就退化为本地计算），因此不会像 social 那样立刻报错。
    """

    layer = build_reflection(container=container, **options)
    layer.register()
    if container is not None:
        container.reflection = layer
    if subscribe and getattr(container, "bus", None) is not None:
        layer.subscribe(container.bus)
    if start:
        await layer.start(bus=getattr(container, "bus", None))
    logger = getattr(container, "logger", None)
    if logger is not None:
        logger.info(
            "reflection.installed",
            rpc=len(REFLECTION_RPC),
            subscribed_topics=len(REFLECTION_TOPICS),
            registered=len(layer.registry.names()) if layer.registry is not None else 0,
        )
    return layer


def register_reflection_handlers(layer: ReflectionLayer) -> Any:
    """幂等注册（重复调用会覆盖同名处理器）。"""

    return layer.register()


__all__ = [
    "EVALUATOR_RPC",
    "PRESETS_RPC",
    "REFLECTION_RPC",
    "REFLECTION_SCOPE",
    "REFLECTION_TOPICS",
    "SESSION_REVIEW_RPC",
    "STRATEGY_RPC",
    "TOPIC_SESSION_COMPLETED",
    "ReflectionContext",
    "ReflectionLayer",
    "build_context",
    "build_reflection",
    "install_reflection",
    "register_reflection_handlers",
]
