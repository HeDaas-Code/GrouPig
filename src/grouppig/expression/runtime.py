"""grouppig.expression.runtime —— 表达层装配入口（设计：``grouppig.expression.runtime``）。

设计（``runtime.md``）：「表达层装配入口：ExpressionLayer 把 persona/generator（t8）与
orchestrator/identity/slang（t9）接线到容器并注册契约名字。」

本模块就是那个装配入口。t8 的 ``ExpressionLayer``（``grouppig/expression/__init__.py``）
注册 persona + generator 的 6 个名字；t9 的 14 个 ``rpc:`` 名字与 1 个 ``kafka:`` 主题
分属三个子树（``orchestrator`` / ``identity`` / ``slang``），由本模块的 ``FlowLayer`` 注册。
两个层**各自自检、互不越界**（``install_expression`` 的契约自检只比 persona + generator，
在 t9 落地后仍然为绿）；需要「一行装好整域」的集成方用 :func:`install_expression_domain`。

集成层（t10）两种接法，任选其一::

    # ① 一行装整域（设计描述的「一个装配入口」）
    from grouppig.expression.runtime import install_expression_domain

    domain = await install_expression_domain(container)   # 6 + 14 个 rpc: 一次注册
    await domain.aclose()

    # ② 分开装（各自自检、各自关闭）
    from grouppig.expression import install_expression
    from grouppig.expression.runtime import install_expression_flow

    core = await install_expression(container)        # t8：人设 + 生成（6 rpc:）
    flow = await install_expression_flow(container)   # t9：编排 + 身份 + 黑话（14 rpc: + 1 kafka:）
    ...
    await flow.close()

**为什么 t9 不塞进 t8 的入口**：``rpc:identity.deny-ai`` 一旦注册，
``rpc:generator.compose`` 的「身份纪律」块就**必然非空**（话术库是静态表，永远出话术），
这会改变 t8 测试对「下游缺席时各块进 missing」的既有断言。t8 的那组用例
是在 t9 落地前写的、验证的是**降级路径**，不该因为 t9 落地而变红。
两个入口、各自自检，是对上游最不打扰的做法。

``FlowLayer`` 暴露 ``build()`` / ``register()`` / ``start()`` / ``close()`` /
``contract_check()`` / ``health()``，与 t8 的 ``ExpressionLayer`` 同形。

normify id: ``grouppig.expression.runtime``（叶子模块，state=planned；其承载的 12 个叶子
全部逐字镜像设计模块 id）。
"""

from __future__ import annotations

import contextlib
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from grouppig.expression.identity.deflector import ProbeDeflector
from grouppig.expression.identity.denial import SCENARIO_MIN_CONFIDENCE, DenialPhrasebook
from grouppig.expression.orchestrator.flow.emitter import FlowEmitter
from grouppig.expression.orchestrator.flow.state import FlowStateStore
from grouppig.expression.orchestrator.flow.transition import FlowTransition
from grouppig.expression.orchestrator.planner.reviser import PlanReviser
from grouppig.expression.orchestrator.planner.structure import StructurePlanner
from grouppig.expression.orchestrator.selector.cost import CostEstimator
from grouppig.expression.orchestrator.selector.templates import TemplateLibrary
from grouppig.expression.slang.injector import SlangInjector
from grouppig.expression.slang.learner import SlangLearner
from grouppig.expression.slang.recognizer import SlangRecognizer
from grouppig.infra.runtime import contract

#: t9 负责的三个子树（模块 id 前缀）。
FLOW_SCOPE = "grouppig.expression.orchestrator"
IDENTITY_SCOPE = "grouppig.expression.identity"
SLANG_SCOPE = "grouppig.expression.slang"

#: t9 全部子树。
FLOW_SCOPES: tuple[str, ...] = (FLOW_SCOPE, IDENTITY_SCOPE, SLANG_SCOPE)

#: t9 负责的 14 个契约 ``rpc:`` 名字（逐字抄自 api-index.json）。
EXPRESSION_FLOW_RPC: tuple[str, ...] = (
    "rpc:flow.end",
    "rpc:flow.next",
    "rpc:flow.start",
    "rpc:flow.transition",
    "rpc:identity.deflect",
    "rpc:identity.deny-ai",
    "rpc:planner.plan",
    "rpc:planner.revise",
    "rpc:selector.estimate",
    "rpc:selector.pick-preset",
    "rpc:selector.pick-template",
    "rpc:slang.inject",
    "rpc:slang.learn",
    "rpc:slang.recognize",
)

#: t9 负责的契约事件主题（emitter 发布）。
EXPRESSION_FLOW_TOPICS: tuple[str, ...] = ("kafka:grouppig.reply.composed",)

#: 子域分组（装配时逐组注册并做覆盖自检）。
ORCHESTRATOR_RPC: tuple[str, ...] = (
    "rpc:flow.end",
    "rpc:flow.next",
    "rpc:flow.start",
    "rpc:flow.transition",
    "rpc:planner.plan",
    "rpc:planner.revise",
    "rpc:selector.estimate",
    "rpc:selector.pick-preset",
    "rpc:selector.pick-template",
)
IDENTITY_RPC: tuple[str, ...] = ("rpc:identity.deflect", "rpc:identity.deny-ai")
SLANG_RPC: tuple[str, ...] = ("rpc:slang.inject", "rpc:slang.learn", "rpc:slang.recognize")


def _expected_rpc_names() -> set[str]:
    """api-index.json 里归属 t9 三个子树的全部 ``rpc:`` 名字（不写死，随设计树走）。"""

    index = contract.api_index()
    return {
        name
        for name, module in index.items()
        if name.startswith("rpc:") and any(module == scope or module.startswith(scope + ".") for scope in FLOW_SCOPES)
    }


def _expected_topic_names() -> set[str]:
    index = contract.api_index()
    return {
        name
        for name, module in index.items()
        if name.startswith("kafka:") and any(module == scope or module.startswith(scope + ".") for scope in FLOW_SCOPES)
    }


@dataclass
class FlowLayer:
    """t9 的运行实例：编排器 + 身份防御 + 黑话（一个进程一份）。"""

    ctx: Any
    registry: Any = None
    logger: Any = None
    transition: FlowTransition | None = None
    emitter: FlowEmitter | None = None
    store: FlowStateStore | None = None
    planner: StructurePlanner | None = None
    reviser: PlanReviser | None = None
    estimator: CostEstimator | None = None
    templates: TemplateLibrary | None = None
    denial: DenialPhrasebook | None = None
    deflector: ProbeDeflector | None = None
    recognizer: SlangRecognizer | None = None
    learner: SlangLearner | None = None
    injector: SlangInjector | None = None
    started: bool = False
    extra: dict[str, Any] = field(default_factory=dict)

    # ---- 装配 ----------------------------------------------------------
    def build(self) -> FlowLayer:
        """按依赖顺序构造 t9 的 12 个叶子实例（不碰外部世界）。"""

        self.transition = FlowTransition()
        self.emitter = FlowEmitter(
            publish=getattr(self.ctx, "publish", None),
            logger=self.logger,
            clock=getattr(self.ctx, "now", None),
        )
        self.store = FlowStateStore(
            ctx=self.ctx,
            emitter=self.emitter,
            scenario=str(self.extra.get("scenario") or "chat"),
        )
        self.reviser = PlanReviser(ctx=self.ctx, budget_tokens=int(self.extra.get("budget_tokens") or 0))
        self.planner = StructurePlanner(
            ctx=self.ctx,
            scenario=str(self.extra.get("scenario") or "chat"),
            revise=bool(self.extra.get("revise", True)),
        )
        self.estimator = CostEstimator(
            ctx=self.ctx,
            default_output_tokens=int(self.extra.get("default_output_tokens") or 0)
            or CostEstimator().default_output_tokens,
        )
        self.templates = TemplateLibrary(ctx=self.ctx, top=int(self.extra.get("template_top") or 0) or 3)
        self.denial = DenialPhrasebook(
            nickname=str(self.extra.get("nickname") or ""),
            catchphrase=str(self.extra.get("catchphrase") or ""),
            # 可选依赖（设计变更 2026-09-24-laya-system1）：把 ctx.call 注进去，
            # 让 ``rpc:identity.deny-ai`` 能用 ``rpc:model.system1`` 判质疑场景。
            # 没有模型网关（或 handler 缺席）时 caller 会抛错，叶子内部静默回退到关键词判定。
            call=getattr(self.ctx, "call", None),
            min_confidence=float(self.extra.get("scenario_min_confidence") or SCENARIO_MIN_CONFIDENCE),
        )
        self.deflector = ProbeDeflector(ctx=self.ctx)
        self.recognizer = SlangRecognizer(ctx=self.ctx)
        self.learner = SlangLearner(ctx=self.ctx, max_terms=int(self.extra.get("learn_max_terms") or 0) or 5)
        self.injector = SlangInjector(ctx=self.ctx, max_entries=int(self.extra.get("inject_max_entries") or 0) or 3)
        return self

    def register(self) -> Any:
        """把 t9 的 14 个 ``rpc:`` 名字注册进注册表（名字与 owner 逐字对齐设计）。

        ``emitter`` 是**发布方**（生产者），它只拥有一个 kafka 主题、不注册任何名字——
        这也是它 ``make_handlers()`` 返回空字典的原因。
        """

        from grouppig.expression.identity import deflector as deflector_module
        from grouppig.expression.identity import denial as denial_module
        from grouppig.expression.orchestrator.flow import emitter as emitter_module
        from grouppig.expression.orchestrator.flow import state as state_module
        from grouppig.expression.orchestrator.flow import transition as transition_module
        from grouppig.expression.orchestrator.planner import reviser as reviser_module
        from grouppig.expression.orchestrator.planner import structure as structure_module
        from grouppig.expression.orchestrator.selector import cost as cost_module
        from grouppig.expression.orchestrator.selector import templates as templates_module
        from grouppig.expression.slang import injector as injector_module
        from grouppig.expression.slang import learner as learner_module
        from grouppig.expression.slang import recognizer as recognizer_module

        assert self.registry is not None
        for module, instance in (
            (transition_module, self.transition),
            (emitter_module, self.emitter),
            (state_module, self.store),
            (structure_module, self.planner),
            (reviser_module, self.reviser),
            (cost_module, self.estimator),
            (templates_module, self.templates),
            (denial_module, self.denial),
            (deflector_module, self.deflector),
            (recognizer_module, self.recognizer),
            (learner_module, self.learner),
            (injector_module, self.injector),
        ):
            module.register(self.registry, instance)
        return self.registry

    # ---- 生命周期 ------------------------------------------------------
    async def start(self, *, bus: Any = None) -> FlowLayer:
        """标记启动。

        本域**不订阅任何主题**：``kafka:grouppig.reply.composed`` 由
        :class:`~grouppig.expression.orchestrator.flow.emitter.FlowEmitter` 发布，
        订阅方是 gateway 的 ``ReplyComposer``（``subscribe_reply_composed``）。
        """

        self.started = True
        self._log(
            "info",
            "expression.flow_started",
            rpc=len(EXPRESSION_FLOW_RPC),
            topics=list(EXPRESSION_FLOW_TOPICS),
        )
        return self

    async def close(self) -> None:
        """退订 / 释放（本域无订阅，保留接口与其它域一致）。"""

        for sub in self.extra.get("subscriptions") or []:
            bus = getattr(self.ctx, "bus", None)
            if bus is not None:
                with contextlib.suppress(Exception):
                    bus.unsubscribe(sub)
        self.extra["subscriptions"] = []
        self.started = False
        self._log("info", "expression.flow_closed")

    # ---- 契约 / 健康 ---------------------------------------------------
    def contract_check(self) -> dict[str, list[str]]:
        """已注册名字与契约比对（**只比对本域 t9 范围**，不替 t8 背锅）。

        ``missing`` = t9 三个子树设计的 ``rpc:`` 名字里没注册的；
        ``unknown`` = 注册了契约外的名字。
        """

        expected = _expected_rpc_names()
        if self.registry is None:
            return {"missing": sorted(expected), "unknown": []}
        index = contract.api_index()
        registered = set(self.registry.names())
        return {
            "missing": sorted(expected - registered),
            "unknown": sorted(name for name in registered if name not in index),
        }

    async def health(self) -> dict[str, Any]:
        """给集成（t10）/ 验证（t11）用的健康摘要。"""

        check = self.contract_check()
        return {
            "started": self.started,
            "scopes": list(FLOW_SCOPES),
            "contract": {
                "expected_rpc": len(EXPRESSION_FLOW_RPC),
                "domain_rpc": len(_expected_rpc_names()),
                "topics": list(EXPRESSION_FLOW_TOPICS),
                "missing": check["missing"],
                "unknown": check["unknown"],
            },
            "orchestrator": {
                "transition": self.transition.status() if self.transition else {},
                "emitter": self.emitter.status() if self.emitter else {},
                "store": self.store.status() if self.store else {},
                "planner": self.planner.status() if self.planner else {},
                "reviser": self.reviser.status() if self.reviser else {},
                "estimator": self.estimator.status() if self.estimator else {},
                "templates": self.templates.status() if self.templates else {},
            },
            "identity": {
                "denial": self.denial.status() if self.denial else {},
                "deflector": self.deflector.status() if self.deflector else {},
            },
            "slang": {
                "recognizer": self.recognizer.status() if self.recognizer else {},
                "learner": self.learner.status() if self.learner else {},
                "injector": self.injector.status() if self.injector else {},
            },
        }

    def _log(self, level: str, event: str, **fields: Any) -> None:
        if self.logger is None:
            return
        with contextlib.suppress(Exception):
            getattr(self.logger, level, self.logger.info)(event, **fields)


def _default_now() -> float:
    import time

    return time.time()


async def _noop_publish(topic: str, payload: Any = None, **kwargs: Any) -> Any:
    return None


def build_expression_flow(
    *,
    container: Any = None,
    registry: Any = None,
    bus: Any = None,
    logger: Any = None,
    now: Any = None,
    call: Any = None,
    publish: Any = None,
    config: Any = None,
    extra: Mapping[str, Any] | None = None,
) -> FlowLayer:
    """装配 t9 的心流编排层（不连库、不订阅；``await layer.start()`` 才标记启动）。"""

    from grouppig.expression import ExpressionContext

    if container is not None:
        registry = registry if registry is not None else getattr(container, "registry", None)
        bus = bus if bus is not None else getattr(container, "bus", None)
        logger = logger if logger is not None else getattr(container, "logger", None)
        config = config if config is not None else getattr(container, "config", None)
    if registry is None and call is None:
        from grouppig.infra.runtime.registry import default_registry

        registry = default_registry
    if call is None:
        call = registry.acall
    if publish is None:
        publish = bus.publish if bus is not None else _noop_publish
    ctx = ExpressionContext(
        call=call,
        publish=publish,
        now=now if now is not None else _default_now,
        log=lambda *args, **kwargs: None,
        registry=registry,
        bus=bus,
        config=config,
    )
    if logger is not None:
        ctx.log = lambda level, event, **fields: getattr(logger, level, logger.info)(event, **fields)
    layer = FlowLayer(ctx=ctx, registry=registry, logger=logger, extra=dict(extra or {}))
    return layer.build()


async def install_expression_flow(
    container: Any,
    *,
    start: bool = False,
    **options: Any,
) -> FlowLayer:
    """把 t9 的编排层装进容器：构造 12 个叶子 → 注册 14 个 ``rpc:`` 名字。

    与 ``install_expression``（t8）**互不依赖**：t9 的编排器把 t8 的
    ``rpc:generator.compose`` 当契约依赖调用，所以两个入口的安装顺序不敏感
    （只要在真正跑一轮编排前都装好即可）。
    """

    layer = build_expression_flow(container=container, **options)
    layer.register()
    if container is not None:
        container.expression_flow = layer
    if start:
        await layer.start(bus=getattr(container, "bus", None))
    logger = getattr(container, "logger", None)
    if logger is not None:
        logger.info(
            "expression.flow_installed",
            rpc=len(EXPRESSION_FLOW_RPC),
            topics=list(EXPRESSION_FLOW_TOPICS),
            registered=len(layer.registry.names()) if layer.registry is not None else 0,
        )
    return layer


def register_expression_flow_handlers(layer: FlowLayer) -> Any:
    """幂等注册（重复调用会覆盖同名处理器）。"""

    return layer.register()


@dataclass
class ExpressionDomain:
    """整域装配结果（t8 核心 + t9 编排）。"""

    core: Any = None
    flow: FlowLayer | None = None
    registry: Any = None

    def contract_check(self) -> dict[str, list[str]]:
        """整域自检：t8 的 6 个 + t9 的 14 个名字一起比对。"""

        index = contract.api_index()
        expected = {
            name
            for name, module in index.items()
            if name.startswith("rpc:")
            and (module.startswith("grouppig.expression.") or module == "grouppig.expression")
        }
        if self.registry is None:
            return {"missing": sorted(expected), "unknown": []}
        registered = set(self.registry.names())
        return {
            "missing": sorted(expected - registered),
            "unknown": sorted(name for name in registered if name not in index),
        }

    async def health(self) -> dict[str, Any]:
        return {
            "core": await self.core.health() if self.core is not None else {},
            "flow": await self.flow.health() if self.flow is not None else {},
            "contract": self.contract_check(),
        }

    async def aclose(self) -> None:
        if self.flow is not None:
            await self.flow.close()
        if self.core is not None:
            await self.core.close()


async def install_expression_domain(
    container: Any,
    *,
    start: bool = False,
    **options: Any,
) -> ExpressionDomain:
    """一行装好整个表达域：t8 的 ``install_expression`` + 本模块的 ``install_expression_flow``。

    设计里 ``runtime`` 的定位就是「把 persona/generator（t8）与 orchestrator/identity/slang（t9）
    接线到容器并注册契约名字」，本函数是它的完整形态；分开装（见模块 docstring 的接法 ②）
    则用于「只要 t8 的 6 个名字」或「t9 单独回归」的场景。
    """

    from grouppig.expression import install_expression

    core = await install_expression(container, **options)
    flow = await install_expression_flow(container, start=start, **options)
    registry = getattr(flow, "registry", None) or getattr(core, "registry", None)
    domain = ExpressionDomain(core=core, flow=flow, registry=registry)
    if container is not None:
        container.expression_domain = domain
    logger = getattr(container, "logger", None)
    if logger is not None:
        logger.info("expression.domain_installed", rpc=len(EXPRESSION_FLOW_RPC) + 6)
    return domain


#: 别名（集成层按域内习惯写 ``install_flow`` 也能用）。
install_flow = install_expression_flow

__all__ = [
    "EXPRESSION_FLOW_RPC",
    "EXPRESSION_FLOW_TOPICS",
    "FLOW_SCOPES",
    "FLOW_SCOPE",
    "IDENTITY_RPC",
    "IDENTITY_SCOPE",
    "ORCHESTRATOR_RPC",
    "SLANG_RPC",
    "SLANG_SCOPE",
    "ExpressionDomain",
    "FlowLayer",
    "build_expression_flow",
    "install_expression_domain",
    "install_expression_flow",
    "install_flow",
    "register_expression_flow_handlers",
]
