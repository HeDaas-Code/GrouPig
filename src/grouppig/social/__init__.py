"""grouppig.social —— 社交层：群友档案、说话画像与 0-99 关系分。

设计（``grouppig.social``）：「建立群友档案与说话画像，维护以自己为中心的社交网和
0-99 关系分，为个性化回复提供依据。」

装配（集成层 t10 一行接入）::

    from grouppig.social import install_social

    await attach_memory(container)          # memory 必须先挂载（social 只通过 rpc: 名字读库）
    layer = await install_social(container) # 注册本域 14 个 rpc: 名字
    await layer.close()

模块构成（逐字镜像 normify 设计）:

* :mod:`grouppig.social.profile.extractor.fact-extractor` —— ``rpc:profile.fact.extract``
* :mod:`grouppig.social.profile.extractor.stance-extractor` —— ``rpc:profile.stance.extract``
* :mod:`grouppig.social.profile.extractor.conflict-resolver` —— ``rpc:profile.conflict``
* :mod:`grouppig.social.profile.manager.lookup` —— ``rpc:profile.get``
* :mod:`grouppig.social.profile.manager.versioning` —— ``rpc:profile.update``
* :mod:`grouppig.social.profile.manager.events` —— 发布 ``kafka:grouppig.profile.updated``
* :mod:`grouppig.social.speech.profiler.lexicon` —— ``rpc:speech.lexicon``
* :mod:`grouppig.social.speech.profiler.temper` —— ``rpc:speech.temper``
* :mod:`grouppig.social.speech.profiler.style-metrics` —— ``rpc:speech.profile`` / ``rpc:speech.style``
* :mod:`grouppig.social.speech.responder.validator` —— ``rpc:speech.validate``
* :mod:`grouppig.social.speech.responder.adapter` —— ``rpc:speech.advise`` / ``rpc:speech.tailor``
* :mod:`grouppig.social.graph.manager.egonet` —— ``rpc:graph.get-egonet``
* :mod:`grouppig.social.graph.manager.tiering` —— ``rpc:graph.tiering``
* :mod:`grouppig.social.graph.relationship.rules` —— ``rpc:relationship.get`` / ``rpc:relationship.adjust``
* :mod:`grouppig.social.graph.relationship.decay` —— ``rpc:relationship.decay``
* :mod:`grouppig.social.graph.manager.events` —— 发布 ``kafka:grouppig.social.changed``

契约：本域 14 个 ``rpc:`` + 2 个 ``kafka:`` 名字，全部逐字取自
``normify-grouppig/api-index.json``（见 :data:`SOCIAL_RPC` / :data:`SOCIAL_TOPICS`）。

normify id: ``grouppig.social``（容器模块）。
"""

from __future__ import annotations

import contextlib
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from grouppig.infra.runtime import contract
from grouppig.social.graph.manager import events as graph_events
from grouppig.social.graph.manager.egonet import EgoNetBuilder
from grouppig.social.graph.manager.events import SocialEventEmitter
from grouppig.social.graph.manager.tiering import Tiering
from grouppig.social.graph.relationship.decay import DecayCalculator
from grouppig.social.graph.relationship.rules import RelationshipRules
from grouppig.social.profile.extractor.conflict_resolver import ConflictResolver
from grouppig.social.profile.extractor.fact_extractor import FactExtractor
from grouppig.social.profile.extractor.stance_extractor import StanceExtractor
from grouppig.social.profile.manager import events as profile_events
from grouppig.social.profile.manager.events import ProfileEventEmitter
from grouppig.social.profile.manager.lookup import ProfileLookup
from grouppig.social.profile.manager.versioning import ProfileVersioning
from grouppig.social.speech.profiler.lexicon import LexiconCounter
from grouppig.social.speech.profiler.style_metrics import StyleMetrics
from grouppig.social.speech.profiler.temper import TemperAnalyzer
from grouppig.social.speech.responder.adapter import StyleAdapter
from grouppig.social.speech.responder.validator import StyleValidator

SOCIAL_SCOPE = "grouppig.social"

#: 本域 14 个契约 ``rpc:`` 名字（逐字抄自 api-index.json）。
SOCIAL_RPC: tuple[str, ...] = (
    "rpc:graph.get-egonet",
    "rpc:graph.tiering",
    "rpc:profile.conflict",
    "rpc:profile.fact.extract",
    "rpc:profile.get",
    "rpc:profile.stance.extract",
    "rpc:profile.update",
    "rpc:relationship.adjust",
    "rpc:relationship.decay",
    "rpc:relationship.get",
    "rpc:speech.advise",
    "rpc:speech.lexicon",
    "rpc:speech.profile",
    "rpc:speech.style",
    "rpc:speech.tailor",
    "rpc:speech.temper",
    "rpc:speech.validate",
)

#: 子域 rpc 名字（装配时按叶子逐个注册并做覆盖自检）。
PROFILE_MANAGER_RPC: tuple[str, ...] = ("rpc:profile.get", "rpc:profile.update")
PROFILE_EXTRACTOR_RPC: tuple[str, ...] = (
    "rpc:profile.fact.extract",
    "rpc:profile.stance.extract",
    "rpc:profile.conflict",
)
SPEECH_PROFILER_RPC: tuple[str, ...] = (
    "rpc:speech.lexicon",
    "rpc:speech.temper",
    "rpc:speech.profile",
    "rpc:speech.style",
)
SPEECH_RESPONDER_RPC: tuple[str, ...] = ("rpc:speech.advise", "rpc:speech.tailor", "rpc:speech.validate")
GRAPH_MANAGER_RPC: tuple[str, ...] = ("rpc:graph.get-egonet", "rpc:graph.tiering")
GRAPH_RELATIONSHIP_RPC: tuple[str, ...] = (
    "rpc:relationship.get",
    "rpc:relationship.adjust",
    "rpc:relationship.decay",
)

#: 本域 2 个契约事件主题。
SOCIAL_TOPICS: tuple[str, ...] = (
    profile_events.TOPIC_PROFILE_UPDATED,
    graph_events.TOPIC_SOCIAL_CHANGED,
)


@dataclass
class SocialContext:
    """本域对外部世界的**唯一入口**。

    本域一切跨域调用都必须写在这里列的契约名字上：

    * ``call`` / ``publish`` —— 由 :func:`install_social` 绑定到容器的注册表与事件总线；
    * ``now`` / ``log`` —— 可注入的时钟与日志（单测替身用）。

    ``allow_missing=True``（默认）时，调用未注册的名字会抛
    :class:`grouppig.infra.runtime.errors.HandlerNotRegistered`；集成层可以把它当健康信号
    而不是崩溃点（social 也支持直接喂 ``messages=`` 的纯离线用法）。
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
) -> SocialContext:
    """构造 :class:`SocialContext`（``publish`` 缺省为 no-op）。"""

    async def _noop_publish(topic: str, payload: Any = None, **kwargs: Any) -> Any:
        return None

    return SocialContext(
        call=call,
        publish=publish if publish is not None else _noop_publish,
        now=now if now is not None else _default_now,
        log=log if log is not None else _default_log,
        registry=registry,
        bus=bus,
        allow_missing=bool(allow_missing),
    )


@dataclass
class SocialLayer:
    """社交层的运行实例（一个进程一份）。"""

    ctx: SocialContext
    registry: Any = None
    logger: Any = None
    profile_lookup: ProfileLookup | None = None
    profile_events: ProfileEventEmitter | None = None
    profile_versioning: ProfileVersioning | None = None
    fact_extractor: FactExtractor | None = None
    stance_extractor: StanceExtractor | None = None
    conflict_resolver: ConflictResolver | None = None
    speech_lexicon: LexiconCounter | None = None
    speech_temper: TemperAnalyzer | None = None
    speech_metrics: StyleMetrics | None = None
    speech_advisor: StyleAdapter | None = None
    speech_validator: StyleValidator | None = None
    egonet: EgoNetBuilder | None = None
    tiering: Tiering | None = None
    relationship: RelationshipRules | None = None
    decay: DecayCalculator | None = None
    social_events: SocialEventEmitter | None = None
    started: bool = False
    extra: dict[str, Any] = field(default_factory=dict)

    # ---- 装配 ----------------------------------------------------------
    def build(self) -> SocialLayer:
        """按依赖顺序构造 15 个叶子的实例（不碰外部世界）。"""

        self.profile_events = ProfileEventEmitter(self.ctx)
        self.social_events = SocialEventEmitter(self.ctx)
        self.profile_lookup = ProfileLookup(self.ctx)
        self.profile_versioning = ProfileVersioning(self.ctx, self.profile_lookup, self.profile_events)
        self.fact_extractor = FactExtractor(self.ctx, llm=self.extra.get("llm"))
        self.stance_extractor = StanceExtractor(self.ctx)
        self.conflict_resolver = ConflictResolver(self.ctx)
        self.speech_lexicon = LexiconCounter(self.ctx)
        self.speech_temper = TemperAnalyzer(self.ctx)
        self.speech_metrics = StyleMetrics(self.ctx)
        self.speech_validator = StyleValidator(self.ctx)
        self.speech_advisor = StyleAdapter(self.ctx)
        self.egonet = EgoNetBuilder(self.ctx)
        self.tiering = Tiering(self.ctx, self.social_events)
        self.relationship = RelationshipRules(self.ctx)
        self.decay = DecayCalculator(self.ctx)
        return self

    def register(self) -> Any:
        """把本域处理器注册进注册表（名字与 owner 逐字对齐设计）。"""

        from grouppig.social.graph.manager import egonet as egonet_module
        from grouppig.social.graph.manager import tiering as tiering_module
        from grouppig.social.graph.relationship import decay as decay_module
        from grouppig.social.graph.relationship import rules as rules_module
        from grouppig.social.profile.extractor import conflict_resolver as conflict_module
        from grouppig.social.profile.extractor import fact_extractor as fact_module
        from grouppig.social.profile.extractor import stance_extractor as stance_module
        from grouppig.social.profile.manager import lookup as lookup_module
        from grouppig.social.profile.manager import versioning as versioning_module
        from grouppig.social.speech.profiler import lexicon as lexicon_module
        from grouppig.social.speech.profiler import style_metrics as metrics_module
        from grouppig.social.speech.profiler import temper as temper_module
        from grouppig.social.speech.responder import adapter as adapter_module
        from grouppig.social.speech.responder import validator as validator_module

        assert self.profile_lookup is not None and self.registry is not None
        for module, instance in (
            (lookup_module, self.profile_lookup),
            (versioning_module, self.profile_versioning),
            (fact_module, self.fact_extractor),
            (stance_module, self.stance_extractor),
            (conflict_module, self.conflict_resolver),
            (lexicon_module, self.speech_lexicon),
            (temper_module, self.speech_temper),
            (metrics_module, self.speech_metrics),
            (validator_module, self.speech_validator),
            (adapter_module, self.speech_advisor),
            (egonet_module, self.egonet),
            (tiering_module, self.tiering),
            (rules_module, self.relationship),
            (decay_module, self.decay),
        ):
            module.register(self.registry, instance)
        return self.registry

    # ---- 事件 ----------------------------------------------------------
    def subscribe(self, bus: Any) -> list[Any]:
        """把档案事件接进本域：``kafka:grouppig.profile.updated`` → 刷新特征索引。"""

        if bus is None:
            return []
        return [
            profile_events.subscribe(bus, self._on_profile_updated, name="social:profile-index"),
            graph_events.subscribe(bus, self._on_social_changed, name="social:graph-log"),
        ]

    async def _on_profile_updated(self, event: Any) -> None:
        payload = getattr(event, "payload", event) or {}
        if not isinstance(payload, Mapping) or self.profile_lookup is None:
            return
        user_id = int(payload.get("user_id") or 0)
        if user_id:
            self.profile_lookup.index_profile(
                {
                    "user_id": user_id,
                    "nickname": payload.get("nickname") or "",
                    "version": payload.get("version") or 1,
                }
            )

    async def _on_social_changed(self, event: Any) -> None:
        payload = getattr(event, "payload", event) or {}
        if not isinstance(payload, Mapping):
            return
        self._log(
            "debug",
            "social.graph.changed",
            user_id=payload.get("user_id"),
            tier=payload.get("tier"),
            previous=payload.get("previous_tier"),
        )

    # ---- 生命周期 ------------------------------------------------------
    async def start(self, *, bus: Any = None) -> SocialLayer:
        """订阅事件主题并标记启动。"""

        subscriptions = self.subscribe(bus if bus is not None else self.ctx.bus)
        self.extra.setdefault("subscriptions", subscriptions)
        self.started = True
        self._log("info", "social.started", rpc=len(SOCIAL_RPC), topics=list(SOCIAL_TOPICS))
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
        self._log("info", "social.closed")

    # ---- 契约 / 健康 ---------------------------------------------------
    def contract_check(self) -> dict[str, list[str]]:
        """已注册名字与契约比对（``scope=grouppig.social``）。

        ``missing`` = 设计里属于本域的 ``rpc:`` 名字里没注册的（``kafka:`` 主题是发布/订阅，
        不进注册表，单独由 :data:`SOCIAL_TOPICS` 断言）；``unknown`` = 注册了契约外的名字。
        """

        if self.registry is None:
            return {"missing": list(SOCIAL_RPC), "unknown": []}
        index = contract.api_index()
        expected = {
            name for name, module in index.items() if module.startswith(SOCIAL_SCOPE) and name.startswith("rpc:")
        }
        registered = set(self.registry.names())
        return {
            "missing": sorted(expected - registered),
            "unknown": sorted(name for name in registered if name not in index),
        }

    async def health(self) -> dict[str, Any]:
        """给 ``t10`` 集成 / ``t11`` 验证用的健康摘要。"""

        check = self.contract_check()
        return {
            "started": self.started,
            "scope": SOCIAL_SCOPE,
            "contract": {
                "expected_rpc": len(SOCIAL_RPC),
                "expected_topics": len(SOCIAL_TOPICS),
                "missing": check["missing"],
                "unknown": check["unknown"],
            },
            "profile": {
                "lookup": self.profile_lookup.status() if self.profile_lookup else {},
                "versioning": self.profile_versioning.status() if self.profile_versioning else {},
                "events": self.profile_events.status() if self.profile_events else {},
                "extractor": {
                    "fact": self.fact_extractor.status() if self.fact_extractor else {},
                    "stance": self.stance_extractor.status() if self.stance_extractor else {},
                    "conflict": self.conflict_resolver.status() if self.conflict_resolver else {},
                },
            },
            "speech": {
                "lexicon": self.speech_lexicon.status() if self.speech_lexicon else {},
                "temper": self.speech_temper.status() if self.speech_temper else {},
                "metrics": self.speech_metrics.status() if self.speech_metrics else {},
                "validator": self.speech_validator.status() if self.speech_validator else {},
                "adapter": self.speech_advisor.status() if self.speech_advisor else {},
            },
            "graph": {
                "egonet": self.egonet.status() if self.egonet else {},
                "tiering": self.tiering.status() if self.tiering else {},
                "relationship": self.relationship.status() if self.relationship else {},
                "decay": self.decay.status() if self.decay else {},
                "events": self.social_events.status() if self.social_events else {},
            },
            "context": self.ctx.stats(),
        }

    def _log(self, level: str, event: str, **fields: Any) -> None:
        if self.logger is None:
            return
        with contextlib.suppress(Exception):
            getattr(self.logger, level, self.logger.info)(event, **fields)


def build_social(
    *,
    container: Any = None,
    registry: Any = None,
    bus: Any = None,
    logger: Any = None,
    now: Any = None,
    call: Any = None,
    publish: Any = None,
    extra: Mapping[str, Any] | None = None,
) -> SocialLayer:
    """装配社交层（不订阅事件、不连库；``await layer.start()`` 才订阅）。"""

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
    layer = SocialLayer(ctx=ctx, registry=registry, logger=logger, extra=dict(extra or {}))
    return layer.build()


async def install_social(
    container: Any,
    *,
    subscribe: bool = True,
    start: bool = False,
    **options: Any,
) -> SocialLayer:
    """把社交层装进容器：构造叶子 → 注册 17 个 ``rpc:`` → （可选）订阅事件主题。

    设计里 memory 是社交层的下游（``grouppig.social`` → ``grouppig.memory``），
    所以调用前必须先 ``await attach_memory(container)``，否则第一次 ``rpc:profile.get``
    会以 ``HandlerNotRegistered`` 暴露出来（而不是静默返回空档）。
    """

    layer = build_social(container=container, **options)
    layer.register()
    if container is not None:
        container.social = layer
    if subscribe and getattr(container, "bus", None) is not None:
        layer.extra["subscriptions"] = layer.subscribe(container.bus)
    if start:
        await layer.start(bus=getattr(container, "bus", None))
    logger = getattr(container, "logger", None)
    if logger is not None:
        logger.info(
            "social.installed",
            rpc=len(SOCIAL_RPC),
            topics=len(SOCIAL_TOPICS),
            registered=len(layer.registry.names()) if layer.registry is not None else 0,
        )
    return layer


def register_social_handlers(layer: SocialLayer) -> Any:
    """幂等注册（重复调用会覆盖同名处理器）。"""

    return layer.register()


__all__ = [
    "GRAPH_MANAGER_RPC",
    "GRAPH_RELATIONSHIP_RPC",
    "PROFILE_EXTRACTOR_RPC",
    "PROFILE_MANAGER_RPC",
    "SOCIAL_RPC",
    "SOCIAL_SCOPE",
    "SOCIAL_TOPICS",
    "SPEECH_PROFILER_RPC",
    "SPEECH_RESPONDER_RPC",
    "SocialContext",
    "SocialLayer",
    "build_context",
    "build_social",
    "install_social",
    "register_social_handlers",
]
