"""grouppig.expression —— 表达层：人设、上下文打包、提示词压缩、文本生成与人性化润色。

设计（``grouppig.expression``）：「以心流多轮结构化编排生成回复：人设、否认 AI、个性化风格、
黑话学习与省 token 生成。」

装配（集成层 t10 一行接入）::

    from grouppig.expression import install_expression

    await attach_memory(container)             # 可选：拿到聊天线/档案/画像的读取能力
    layer = await install_expression(container) # 注册 persona + generator 的 6 个 rpc: 名字
    await layer.close()

**本域分工**（两组叶子分属两个任务，互不越界）：

t8 / expression-core-engineer（本文件负责装配的部分）:

* :mod:`grouppig.expression.persona.profile` —— ``rpc:persona.get``
* :mod:`grouppig.expression.persona.prompt-builder` —— ``rpc:persona.style``
* :mod:`grouppig.expression.generator.context` —— ``rpc:generator.compose``
* :mod:`grouppig.expression.generator.compressor` —— ``rpc:generator.compress``
* :mod:`grouppig.expression.generator.writer` —— ``rpc:generator.write``
* :mod:`grouppig.expression.generator.polisher` —— ``rpc:generator.humanize``

t9 / expression-flow-engineer（本文件只**调用不实现**）:

* :mod:`grouppig.expression.orchestrator.*` —— ``rpc:flow.*`` / ``rpc:planner.*`` / ``rpc:selector.*``
  / ``kafka:grouppig.reply.composed``
* :mod:`grouppig.expression.identity.*` —— ``rpc:identity.deny-ai`` / ``rpc:identity.deflect``
* :mod:`grouppig.expression.slang.*` —— ``rpc:slang.inject`` / ``rpc:slang.recognize`` / ``rpc:slang.learn``

``rpc:generator.compose`` 的设计依赖里含 ``rpc:slang.inject`` 与 ``rpc:identity.deny-ai``：
本域把它们当**契约依赖调用**（缺处理器时优雅降级、记进 ``missing``），t9 落地后链路自动接通。

契约：本域 t8 范围 6 个 ``rpc:`` 名字，全部逐字取自 ``normify-grouppig/api-index.json``
（见 :data:`EXPRESSION_RPC` / :data:`EXPRESSION_CORE_RPC`）。

normify id: ``grouppig.expression``（容器模块）。
"""

from __future__ import annotations

import contextlib
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from grouppig.expression.generator.compressor import PromptCompressor
from grouppig.expression.generator.context import ContextPacker
from grouppig.expression.generator.polisher import HumanizingPolisher
from grouppig.expression.generator.writer import TextWriter
from grouppig.expression.persona.profile import PersonaProfile
from grouppig.expression.persona.prompt_builder import PersonaPromptBuilder
from grouppig.infra.runtime import contract

EXPRESSION_SCOPE = "grouppig.expression"

#: t8 负责的 6 个契约 ``rpc:`` 名字（逐字抄自 api-index.json）。
EXPRESSION_CORE_RPC: tuple[str, ...] = (
    "rpc:generator.compose",
    "rpc:generator.compress",
    "rpc:generator.humanize",
    "rpc:generator.write",
    "rpc:persona.get",
    "rpc:persona.style",
)

#: 设计里归属 ``grouppig.expression.persona`` / ``grouppig.expression.generator`` 的全部 rpc 名字。
EXPRESSION_CORE_SCOPE: tuple[str, ...] = (
    "grouppig.expression.persona",
    "grouppig.expression.generator",
)

#: 本域全部 rpc 名字（t9 的 orchestrator / identity / slang 也在此，供健康检查对照）。
EXPRESSION_RPC: tuple[str, ...] = tuple(
    sorted(
        name
        for name, module in contract.api_index().items()
        if name.startswith("rpc:") and module.startswith(EXPRESSION_SCOPE)
    )
)

#: 本域契约事件主题（``kafka:grouppig.reply.composed`` 由 t9 的 flow.emitter 发布）。
EXPRESSION_TOPICS: tuple[str, ...] = tuple(
    sorted(
        name
        for name, module in contract.api_index().items()
        if name.startswith("kafka:") and module.startswith(EXPRESSION_SCOPE)
    )
)

#: 子域分组（装配时按叶子逐个注册并做覆盖自检）。
PERSONA_RPC: tuple[str, ...] = ("rpc:persona.get", "rpc:persona.style")
GENERATOR_RPC: tuple[str, ...] = (
    "rpc:generator.compose",
    "rpc:generator.compress",
    "rpc:generator.humanize",
    "rpc:generator.write",
)


@dataclass
class ExpressionContext:
    """本域对外部世界的**唯一入口**。

    本域一切跨域调用都必须写在这里列的契约名字上：

    * ``call`` / ``publish`` —— 由 :func:`install_expression` 绑定到容器的注册表与事件总线；
    * ``now`` / ``log`` —— 可注入的时钟与日志（单测替身用）；
    * ``config`` —— 容器配置（人设默认值可从 ``[persona]`` 读）。

    下游缺处理器时抛 :class:`grouppig.infra.runtime.errors.HandlerNotRegistered`；
    表达层对**可选**下游（slang / identity / archive / profile）会自行捕获并降级，
    对**必需**下游（persona / compress / write）则把失败如实记进返回体的 ``missing``。
    """

    call: Any
    publish: Any = None
    now: Any = None
    log: Any = field(default=lambda *args, **kwargs: None)
    registry: Any = None
    bus: Any = None
    config: Any = None
    calls: dict[str, int] = field(default_factory=dict, repr=False)

    async def invoke(self, name: str, *args: Any, **kwargs: Any) -> Any:
        """带计数的 :meth:`call`（便于诊断哪条契约链路被用到）。"""

        self.calls[name] = self.calls.get(name, 0) + 1
        return await self.call(name, *args, **kwargs)

    def stats(self) -> dict[str, Any]:
        return {"calls": dict(sorted(self.calls.items()))}


def _default_now() -> float:
    import time

    return time.time()


def _default_log(level: str, event: str, **fields: Any) -> None:
    return None


async def _noop_publish(topic: str, payload: Any = None, **kwargs: Any) -> Any:
    return None


def build_context(
    *,
    call: Any,
    publish: Any = None,
    now: Any = None,
    log: Any = None,
    registry: Any = None,
    bus: Any = None,
    config: Any = None,
) -> ExpressionContext:
    """构造 :class:`ExpressionContext`（``publish`` 缺省为 no-op）。"""

    return ExpressionContext(
        call=call,
        publish=publish if publish is not None else _noop_publish,
        now=now if now is not None else _default_now,
        log=log if log is not None else _default_log,
        registry=registry,
        bus=bus,
        config=config,
    )


@dataclass
class ExpressionLayer:
    """表达层的运行实例（一个进程一份）。"""

    ctx: ExpressionContext
    registry: Any = None
    logger: Any = None
    persona: PersonaProfile | None = None
    prompt_builder: PersonaPromptBuilder | None = None
    compressor: PromptCompressor | None = None
    writer: TextWriter | None = None
    polisher: HumanizingPolisher | None = None
    packer: ContextPacker | None = None
    started: bool = False
    extra: dict[str, Any] = field(default_factory=dict)

    # ---- 装配 ----------------------------------------------------------
    def build(self) -> ExpressionLayer:
        """按依赖顺序构造 6 个叶子的实例（不碰外部世界）。"""

        config = self.ctx.config
        self.persona = PersonaProfile(ctx=self.ctx, config=config, clock=self.ctx.now)
        self.prompt_builder = PersonaPromptBuilder(ctx=self.ctx)
        self.compressor = PromptCompressor(
            ctx=self.ctx,
            self_id=int(self.extra.get("self_id") or 0),
            budget_tokens=int(self.extra.get("budget_tokens") or 0) or PromptCompressor.budget_tokens,
        )
        self.polisher = HumanizingPolisher(
            ctx=self.ctx,
            max_chars=int(self.extra.get("max_chars") or 0) or HumanizingPolisher.max_chars,
            use_tailor=bool(self.extra.get("use_tailor", True)),
        )
        self.writer = TextWriter(
            ctx=self.ctx,
            candidates=int(self.extra.get("candidates") or 0) or TextWriter.candidates,
            max_chars=int(self.extra.get("max_chars") or 0) or TextWriter.max_chars,
            scenario=str(self.extra.get("scenario") or TextWriter.scenario),
        )
        self.packer = ContextPacker(
            ctx=self.ctx,
            scenario=str(self.extra.get("scenario") or ContextPacker.scenario),
            self_id=int(self.extra.get("self_id") or 0),
            message_limit=int(self.extra.get("message_limit") or 0) or ContextPacker.message_limit,
            thread_limit=int(self.extra.get("thread_limit") or 0) or ContextPacker.thread_limit,
        )
        return self

    def register(self) -> Any:
        """把本域 6 个叶子的处理器注册进注册表（名字与 owner 逐字对齐设计）。"""

        from grouppig.expression.generator import compressor as compressor_module
        from grouppig.expression.generator import context as context_module
        from grouppig.expression.generator import polisher as polisher_module
        from grouppig.expression.generator import writer as writer_module
        from grouppig.expression.persona import profile as profile_module
        from grouppig.expression.persona import prompt_builder as builder_module

        assert self.registry is not None
        for module, instance in (
            (profile_module, self.persona),
            (builder_module, self.prompt_builder),
            (compressor_module, self.compressor),
            (writer_module, self.writer),
            (polisher_module, self.polisher),
            (context_module, self.packer),
        ):
            module.register(self.registry, instance)
        return self.registry

    # ---- 生命周期 ------------------------------------------------------
    async def start(self, *, bus: Any = None) -> ExpressionLayer:
        """标记启动（本域不订阅事件：``kafka:grouppig.reply.composed`` 由 t9 的 flow.emitter 发布）。"""

        self.started = True
        self._log(
            "info",
            "expression.started",
            rpc=len(EXPRESSION_CORE_RPC),
            topics=list(EXPRESSION_TOPICS),
        )
        return self

    async def close(self) -> None:
        """退订 / 释放（本域无订阅，保留接口与其它域一致）。"""

        for sub in self.extra.get("subscriptions") or []:
            bus = self.ctx.bus
            if bus is not None:
                with contextlib.suppress(Exception):
                    bus.unsubscribe(sub)
        self.extra["subscriptions"] = []
        self.started = False
        self._log("info", "expression.closed")

    # ---- 契约 / 健康 ---------------------------------------------------
    def contract_check(self) -> dict[str, list[str]]:
        """已注册名字与契约比对（**只比对本域 t8 范围**，不替 t9 背锅）。

        ``missing`` = t8 设计的 ``rpc:`` 名字里没注册的；
        ``unknown`` = 注册了契约外的名字。
        """

        if self.registry is None:
            return {"missing": list(EXPRESSION_CORE_RPC), "unknown": []}
        index = contract.api_index()
        expected = {
            name
            for name, module in index.items()
            if name.startswith("rpc:")
            and any(module == scope or module.startswith(scope + ".") for scope in EXPRESSION_CORE_SCOPE)
        }
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
            "scope": EXPRESSION_SCOPE,
            "contract": {
                "expected_rpc": len(EXPRESSION_CORE_RPC),
                "domain_rpc": len(EXPRESSION_RPC),
                "topics": list(EXPRESSION_TOPICS),
                "missing": check["missing"],
                "unknown": check["unknown"],
            },
            "persona": {
                "profile": self.persona.status() if self.persona else {},
                "prompt_builder": self.prompt_builder.status() if self.prompt_builder else {},
            },
            "generator": {
                "compressor": self.compressor.status() if self.compressor else {},
                "writer": self.writer.status() if self.writer else {},
                "polisher": self.polisher.status() if self.polisher else {},
                "packer": self.packer.status() if self.packer else {},
            },
            "context": self.ctx.stats(),
        }

    def _log(self, level: str, event: str, **fields: Any) -> None:
        if self.logger is None:
            return
        with contextlib.suppress(Exception):
            getattr(self.logger, level, self.logger.info)(event, **fields)


def build_expression(
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
) -> ExpressionLayer:
    """装配表达层（不连库、不订阅；``await layer.start()`` 才标记启动）。"""

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
    if publish is None and bus is not None:
        publish = bus.publish
    ctx = build_context(call=call, publish=publish, now=now, log=None, registry=registry, bus=bus, config=config)
    if logger is not None:
        ctx.log = lambda level, event, **fields: getattr(logger, level, logger.info)(event, **fields)
    layer = ExpressionLayer(ctx=ctx, registry=registry, logger=logger, extra=dict(extra or {}))
    return layer.build()


async def install_expression(
    container: Any,
    *,
    subscribe: bool = True,
    start: bool = False,
    **options: Any,
) -> ExpressionLayer:
    """把表达层装进容器：构造 6 个叶子 → 注册 6 个 ``rpc:`` 名字。

    设计里 memory 与 social 是表达层的上游（表达层读聊天线 / 档案 / 画像），
    但它们都是**可选**的：没挂时表达层仍能生成（上下文档位缺失，记进 ``missing``）。
    因此本入口不像 social 那样强依赖 ``attach_memory``。
    """

    layer = build_expression(container=container, **options)
    layer.register()
    if container is not None:
        container.expression = layer
    if start:
        await layer.start(bus=getattr(container, "bus", None))
    logger = getattr(container, "logger", None)
    if logger is not None:
        logger.info(
            "expression.installed",
            rpc=len(EXPRESSION_CORE_RPC),
            domain_rpc=len(EXPRESSION_RPC),
            registered=len(layer.registry.names()) if layer.registry is not None else 0,
        )
    return layer


def register_expression_handlers(layer: ExpressionLayer) -> Any:
    """幂等注册（重复调用会覆盖同名处理器）。"""

    return layer.register()


__all__ = [
    "EXPRESSION_CORE_RPC",
    "EXPRESSION_CORE_SCOPE",
    "EXPRESSION_RPC",
    "EXPRESSION_SCOPE",
    "EXPRESSION_TOPICS",
    "GENERATOR_RPC",
    "PERSONA_RPC",
    "ExpressionContext",
    "ExpressionLayer",
    "build_context",
    "build_expression",
    "install_expression",
    "register_expression_handlers",
]
