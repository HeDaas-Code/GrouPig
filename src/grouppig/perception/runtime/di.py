"""grouppig.perception.runtime.di —— 感知层装配入口（21 个 ``rpc:`` 处理器 + 2 个主题）。

用法（集成入口 t10 只需三行）::

    from grouppig.perception import install

    perception = await install(container)              # 注册 21 个契约名字 + 订阅行为变更事件
    await container.call("rpc:observer.ingest", event) # 网关投递泵就是这么调进来的
    report = await container.call("rpc:behavior.classify", 100200300)   # 行为分类（含插话级联）

装配要点：

* 17 个叶子共用**同一组**运行时对象（滚动窗 / 去重器 / 缓冲器 / 分类器 / 冷却器），
  所以「行为变了 → 插话评分」能读到同一份状态；
* 名字逐字取自 ``normify-grouppig/api-index.json``，owner 与设计树一致
  （``contract.owner(name)``），可用 ``contract.check_registry(scope="grouppig.perception")`` 校验；
* 上游是网关（``rpc:observer.ingest`` / ``kafka:grouppig.behavior.changed``），
  下游是 memory（``rpc:chat.*``）、session（``rpc:threads.segment`` / ``rpc:topic.candidate.generate``）与
  expression（``rpc:flow.start``）—— 未装配时自动跳过（见 ``runtime.calls``），装好后无需改代码。

normify id: ``grouppig.perception.runtime.di``（运行时补充模块，设计树暂无）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from grouppig.infra.runtime import contract
from grouppig.infra.runtime.registry import Registry
from grouppig.perception.behavior.classifier.aggregator import BehaviorAggregator, build_aggregator
from grouppig.perception.behavior.classifier.features import BehaviorFeatureEncoder, build_encoder
from grouppig.perception.behavior.classifier.llm_judge import LLMJudge, build_judge
from grouppig.perception.behavior.classifier.rule_engine import RuleEngine, build_engine
from grouppig.perception.behavior.flood.repetition import RepetitionDetector, build_detector
from grouppig.perception.behavior.flood.velocity import VelocityCalculator, build_calculator
from grouppig.perception.behavior.flood.verdict import FloodVerdict, build_verdict
from grouppig.perception.behavior.rhythm.meter import RhythmMeter, build_meter
from grouppig.perception.behavior.rhythm.trend import RhythmTrend, build_trend
from grouppig.perception.interrupt.cooldown import Cooldown, build_cooldown
from grouppig.perception.interrupt.decision import InterruptDecision, build_decision
from grouppig.perception.interrupt.scorer import InterruptScorer, build_scorer
from grouppig.perception.normalizer.cleaner import Cleaner, build_cleaner
from grouppig.perception.normalizer.dedup import Deduper, build_deduper
from grouppig.perception.normalizer.featurizer import Featurizer, build_featurizer
from grouppig.perception.observer.buffer import MessageBuffer, build_buffer
from grouppig.perception.observer.window import RollingWindow, build_window
from grouppig.perception.runtime import config as config_module
from grouppig.perception.runtime.calls import registry_of
from grouppig.perception.runtime.errors import PerceptionError

#: 感知层作用域（``contract.check_registry(scope=SCOPE)``）。
SCOPE = "grouppig.perception"

#: 感知层负责的 21 个 ``rpc:`` 名字（= api-index.json 中归属 grouppig.perception 的全部 rpc 名字）。
PERCEPTION_RPC: tuple[str, ...] = tuple(
    sorted(n for n, m in contract.api_index().items() if m.startswith(SCOPE) and n.startswith("rpc:"))
)

#: 感知层负责的 2 个 ``kafka:`` 主题（发布行为变更 / 插话触发）。
PERCEPTION_TOPICS: tuple[str, ...] = (
    "kafka:grouppig.behavior.changed",
    "kafka:grouppig.interrupt.triggered",
)

#: 设计依赖里需要外部域提供的名字（用于自检哪些下游还没接）。
EXTERNAL_RPC: tuple[str, ...] = (
    "rpc:chat.append",
    "rpc:chat.query",
    "rpc:chat.window",
    "rpc:threads.segment",
    "rpc:topic.candidate.generate",
    "rpc:presets.match",
    "rpc:flow.start",
)


@dataclass
class Perception:
    """感知层运行实例：17 个叶子对象 + 21 个处理器注册 + 事件订阅。"""

    registry: Registry
    config: Any = None
    logger: Any = None
    bus: Any = None
    window: RollingWindow | None = None
    buffer: MessageBuffer | None = None
    deduper: Deduper | None = None
    featurizer: Featurizer | None = None
    cleaner: Cleaner | None = None
    encoder: BehaviorFeatureEncoder | None = None
    velocity: VelocityCalculator | None = None
    repetition: RepetitionDetector | None = None
    verdict: FloodVerdict | None = None
    trend: RhythmTrend | None = None
    meter: RhythmMeter | None = None
    rules: RuleEngine | None = None
    judge: LLMJudge | None = None
    aggregator: BehaviorAggregator | None = None
    cooldown: Cooldown | None = None
    scorer: InterruptScorer | None = None
    decision: InterruptDecision | None = None
    subscriptions: list[Any] = field(default_factory=list)
    installed: bool = False

    # ---- 注册 ----------------------------------------------------------
    def register(self) -> Registry:
        """把感知层全部处理器注册进注册表（名字逐字对齐 api-index.json）。"""

        assert None not in (
            self.window,
            self.buffer,
            self.deduper,
            self.featurizer,
            self.cleaner,
            self.encoder,
            self.velocity,
            self.repetition,
            self.verdict,
            self.trend,
            self.meter,
            self.rules,
            self.judge,
            self.aggregator,
            self.cooldown,
            self.scorer,
            self.decision,
        ), "感知层组件未装配"
        self.window.register(self.registry)
        self.deduper.register(self.registry)
        self.featurizer.register(self.registry)
        self.cleaner.register(self.registry)
        self.buffer.register(self.registry)
        self.encoder.register(self.registry)
        self.velocity.register(self.registry)
        self.repetition.register(self.registry)
        self.verdict.register(self.registry)
        self.trend.register(self.registry)
        self.meter.register(self.registry)
        self.rules.register(self.registry)
        self.judge.register(self.registry)
        self.aggregator.register(self.registry)
        self.cooldown.register(self.registry)
        self.scorer.register(self.registry)
        self.decision.register(self.registry)
        return self.registry

    def wire(self) -> None:
        """把「设计依赖」里同域的两条事件边接起来（跨域边装配后自然生效）。"""

        if self.registry.has("rpc:interrupt.score"):
            self.verdict.registry = self.registry
        # 行为变更 → 插话评分：总线在场时由订阅驱动，否则分类器内部直接触发
        if self.bus is not None:
            self.aggregator.bus = self.bus
            if not self.aggregator.cascade_interrupt:
                self.subscriptions.append(self.aggregator.subscribe(self.bus))

    def attach_window(self, window: Any) -> None:
        """注入「外部窗口源」（一般是 observer 的内存滚动窗；memory 缺席时的回落数据源）。"""

        self.velocity.window = window
        self.repetition.window = window
        self.verdict.velocity.window = window
        self.verdict.repetition.window = window
        self.meter.window = window
        self.aggregator.window = window
        self.scorer.window = window

    # ---- 生命周期 ------------------------------------------------------
    async def start(self) -> Perception:
        self.register()
        self.wire()
        self.installed = True
        self._log("info", "perception.installed", handlers=len(self.registry), topics=list(PERCEPTION_TOPICS))
        return self

    async def aclose(self) -> None:
        if self.bus is not None:
            for subscription in self.subscriptions:
                try:
                    self.bus.unsubscribe(subscription)
                except Exception:  # pragma: no cover - 退订失败不影响关闭
                    pass
        self.subscriptions.clear()
        self.installed = False

    # ---- 状态 ----------------------------------------------------------
    def contract_check(self) -> dict[str, list[str]]:
        """已注册名字与契约比对（``scope=grouppig.perception``）。

        ``missing`` = 设计里属于本域的 ``rpc:`` 名字里没注册的
        （``kafka:`` 主题是发布/订阅，不进注册表，单独由 :data:`PERCEPTION_TOPICS` 断言）。
        """

        index = contract.api_index()
        registered = set(self.registry.names())
        return {
            "missing": sorted(set(PERCEPTION_RPC) - registered),
            "unknown": sorted(name for name in registered if name not in index),
            "external_missing": sorted(name for name in EXTERNAL_RPC if name not in registered),
        }

    def health(self) -> dict[str, Any]:
        """给 ``t10`` 集成 / ``t11`` 验证用的健康摘要。"""

        check = self.contract_check()
        return {
            "installed": self.installed,
            "contract": {
                "scope": SCOPE,
                "expected_rpc": len(PERCEPTION_RPC),
                "missing": check["missing"],
                "unknown": check["unknown"],
                "external_missing": check["external_missing"],
            },
            "buffer": self.buffer.snapshot() if self.buffer else {},
            "window": self.window.snapshot() if self.window else {},
            "dedup": self.deduper.snapshot() if self.deduper else {},
            "classifier": self.aggregator.snapshot() if self.aggregator else {},
            "interrupt": self.decision.snapshot() if self.decision else {},
            "scorer": {"weights": self.scorer.weights if self.scorer else {}},
            "counters": {
                "ingested": self.buffer.stats.get("ingested", 0) if self.buffer else 0,
                "classified": self.aggregator.stats.get("classified", 0) if self.aggregator else 0,
                "behavior_changes": self.aggregator.stats.get("changed", 0) if self.aggregator else 0,
                "speak": self.decision.stats.get("speak", 0) if self.decision else 0,
            },
        }

    def _log(self, level: str, event: str, **fields: Any) -> None:
        if self.logger is None:
            return
        try:
            getattr(self.logger, level, self.logger.info)(event, **fields)
        except Exception:  # pragma: no cover - 日志失败不影响装配
            pass


def build_perception(
    *,
    config: Any = None,
    logger: Any = None,
    registry: Any = None,
    bus: Any = None,
    container: Any = None,
    window: RollingWindow | None = None,
    clock: Any = None,
    **options: Any,
) -> Perception:
    """装配感知层（不注册；``await perception.start()`` 才注册 + 接线）。"""

    if container is not None:
        config = config if config is not None else container.config
        logger = logger if logger is not None else container.logger
        registry = registry if registry is not None else container.registry
        bus = bus if bus is not None else container.bus
    target = registry_of(registry)
    if target is None:
        from grouppig.infra.runtime.registry import default_registry

        target = default_registry

    rolling = window or build_window(config=config, logger=logger)
    deduper = build_deduper(config=config, logger=logger, **({"clock": clock} if clock else {}))
    featurizer = build_featurizer(config=config, logger=logger, registry=target)
    cleaner = build_cleaner(config=config, logger=logger, registry=target, deduper=deduper, featurizer=featurizer)
    buffer = build_buffer(config=config, logger=logger, registry=target, window=rolling, cleaner=cleaner)
    encoder = build_encoder(config=config, logger=logger)
    velocity = build_calculator(config=config, logger=logger, registry=target, window=rolling)
    repetition = build_detector(config=config, logger=logger, registry=target, window=rolling)
    verdict = build_verdict(config=config, logger=logger, registry=target, velocity=velocity, repetition=repetition)
    trend = build_trend(config=config, logger=logger)
    meter = build_meter(config=config, logger=logger, registry=target, trend=trend, window=rolling)
    rules = build_engine(config=config, logger=logger, registry=target, flood=verdict, rhythm=meter, window=rolling)
    judge = build_judge(config=config, logger=logger, registry=target)
    decision_mode = str(config_module.get(config, "perception.classify.decision_mode", "active")).lower()
    if decision_mode not in {"off", "shadow", "active"}:
        raise PerceptionError(f"perception.classify.decision_mode must be off|shadow|active, got {decision_mode!r}")
    aggregator = build_aggregator(
        config=config,
        decision_mode=decision_mode,
        logger=logger,
        registry=target,
        bus=bus,
        window=rolling,
        encoder=encoder,
        engine=rules,
        judge=judge,
        **options,
    )
    cooldown = build_cooldown(config=config, logger=logger, **({"clock": clock} if clock else {}))
    scorer = build_scorer(
        config=config, logger=logger, registry=target, cooldown=cooldown, rhythm=meter, window=rolling
    )
    decision = build_decision(config=config, logger=logger, registry=target, bus=bus, cooldown=cooldown)

    return Perception(
        registry=target,
        config=config,
        logger=logger,
        bus=bus,
        window=rolling,
        buffer=buffer,
        deduper=deduper,
        featurizer=featurizer,
        cleaner=cleaner,
        encoder=encoder,
        velocity=velocity,
        repetition=repetition,
        verdict=verdict,
        trend=trend,
        meter=meter,
        rules=rules,
        judge=judge,
        aggregator=aggregator,
        cooldown=cooldown,
        scorer=scorer,
        decision=decision,
    )


async def install(container: Any, *, start: bool = True, **options: Any) -> Perception:
    """把感知层装进容器：注册 21 个名字 + 订阅 ``kafka:grouppig.behavior.changed``。"""

    registry = registry_of(container)
    if registry is None:
        raise PerceptionError(f"install 需要 Container 或 Registry，得到 {type(container).__name__}")
    perception = build_perception(container=container, registry=registry, **options)
    if start:
        await perception.start()
    else:
        perception.register()
        perception.wire()
    return perception


def register_perception_handlers(target: Any, **options: Any) -> Perception:
    """同步装配 + 注册（不做事件订阅；测试与无总线场景用）。"""

    perception = build_perception(registry=target, bus=getattr(target, "bus", None), **options)
    perception.register()
    perception.wire()
    perception.installed = True
    return perception


__all__ = [
    "EXTERNAL_RPC",
    "PERCEPTION_RPC",
    "PERCEPTION_TOPICS",
    "SCOPE",
    "Perception",
    "build_perception",
    "install",
    "register_perception_handlers",
]
