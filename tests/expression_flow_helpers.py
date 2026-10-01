"""expression 心流编排域（t9）测试辅助：编排环境、假发送器、黑话库夹具。

设计要点：

* :func:`flow_env` —— 一次性搭好「注册表 + 事件总线 + memory 存储 + t8 表达核心 +
  t9 编排层 + 假模型 + 假发送器」，测试只关心编排链路，不关心装配细节；
* :class:`FakeSender` —— 假的 ``rpc:sender.send_reply``，记录发出去的载荷；
  返回体形状**逐字模仿** ``grouppig.gateway.sender.composer.ReplyComposer._result``
  （``ok`` / ``sent`` / ``message_ids`` / ``chunks`` / ``text`` / ``stats``），
  这样编排器的 ``sent`` 判定与真实网关一致；
* :class:`FakeModel` / :class:`Clock` / :class:`EventCollector` 复用 t8 的
  ``tests/expression_helpers.py``（同一套假件，避免两套语义漂移）；
* :func:`seed_slang` —— 往 memory 的 ``slang_entries`` 表里灌几条黑话，
  让 ``rpc:slang.lookup`` 真的有东西可查（走 ``rpc:slang.upsert``，不直接写库）。
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from expression_helpers import (
    BASE_TS,
    EXPRESSION_DSN,
    GROUP_ID,
    PEER_IDS,
    SELF_ID,
    Clock,
    EventCollector,
    FakeModel,
    message,
)
from grouppig.expression import install_expression
from grouppig.expression.runtime import (
    EXPRESSION_FLOW_RPC,
    EXPRESSION_FLOW_TOPICS,
    install_expression_flow,
)
from grouppig.infra.runtime.bus import EventBus
from grouppig.infra.runtime.registry import Registry
from grouppig.memory.runtime.di import register_memory_handlers
from grouppig.memory.runtime.stores import MemoryStores

#: t9 负责的 14 个契约 rpc 名字（与 ``EXPRESSION_FLOW_RPC`` 同源，这里独立抄一份做交叉校验）。
FLOW_RPC: tuple[str, ...] = (
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

#: 设计叶子模块 id → 契约名字（逐字取自 api-index.json）。
EXPECTED_APIS: dict[str, tuple[str, ...]] = {
    "grouppig.expression.orchestrator.flow.state": ("rpc:flow.start", "rpc:flow.next", "rpc:flow.end"),
    "grouppig.expression.orchestrator.flow.transition": ("rpc:flow.transition",),
    "grouppig.expression.orchestrator.flow.emitter": ("kafka:grouppig.reply.composed",),
    "grouppig.expression.orchestrator.planner.structure": ("rpc:planner.plan",),
    "grouppig.expression.orchestrator.planner.reviser": ("rpc:planner.revise",),
    "grouppig.expression.orchestrator.selector.templates": ("rpc:selector.pick-template",),
    "grouppig.expression.orchestrator.selector.cost": ("rpc:selector.estimate", "rpc:selector.pick-preset"),
    "grouppig.expression.identity.denial": ("rpc:identity.deny-ai",),
    "grouppig.expression.identity.deflector": ("rpc:identity.deflect",),
    "grouppig.expression.slang.recognizer": ("rpc:slang.recognize",),
    "grouppig.expression.slang.learner": ("rpc:slang.learn",),
    "grouppig.expression.slang.injector": ("rpc:slang.inject",),
}

#: 设计叶子模块 id → 源码模块名（连字符在 Python 侧是下划线）。
LEAF_MODULES: tuple[tuple[str, str], ...] = (
    ("grouppig.expression.orchestrator.flow.state", "grouppig.expression.orchestrator.flow.state"),
    ("grouppig.expression.orchestrator.flow.transition", "grouppig.expression.orchestrator.flow.transition"),
    ("grouppig.expression.orchestrator.flow.emitter", "grouppig.expression.orchestrator.flow.emitter"),
    ("grouppig.expression.orchestrator.planner.structure", "grouppig.expression.orchestrator.planner.structure"),
    ("grouppig.expression.orchestrator.planner.reviser", "grouppig.expression.orchestrator.planner.reviser"),
    ("grouppig.expression.orchestrator.selector.templates", "grouppig.expression.orchestrator.selector.templates"),
    ("grouppig.expression.orchestrator.selector.cost", "grouppig.expression.orchestrator.selector.cost"),
    ("grouppig.expression.identity.denial", "grouppig.expression.identity.denial"),
    ("grouppig.expression.identity.deflector", "grouppig.expression.identity.deflector"),
    ("grouppig.expression.slang.recognizer", "grouppig.expression.slang.recognizer"),
    ("grouppig.expression.slang.learner", "grouppig.expression.slang.learner"),
    ("grouppig.expression.slang.injector", "grouppig.expression.slang.injector"),
)

#: 设计 frontmatter 里的依赖对（``from_api`` → ``to_api``），逐字抄自 12 个叶子 md。
EXPECTED_DEPENDENCIES: tuple[tuple[str, str, str], ...] = (
    ("grouppig.expression.orchestrator.flow.state", "rpc:flow.start", "rpc:flow.transition"),
    ("grouppig.expression.orchestrator.flow.state", "rpc:flow.start", "rpc:planner.plan"),
    ("grouppig.expression.orchestrator.flow.state", "rpc:flow.next", "rpc:generator.compose"),
    ("grouppig.expression.orchestrator.flow.state", "rpc:flow.end", "rpc:sender.send_reply"),
    ("grouppig.expression.orchestrator.flow.state", "rpc:flow.end", "kafka:grouppig.reply.composed"),
    ("grouppig.expression.orchestrator.planner.structure", "rpc:planner.plan", "rpc:selector.pick-template"),
    ("grouppig.expression.orchestrator.planner.structure", "rpc:planner.plan", "rpc:planner.revise"),
    ("grouppig.expression.orchestrator.planner.reviser", "rpc:planner.revise", "rpc:selector.estimate"),
    ("grouppig.expression.orchestrator.selector.templates", "rpc:selector.pick-template", "rpc:selector.estimate"),
    ("grouppig.expression.orchestrator.selector.cost", "rpc:selector.pick-preset", "rpc:presets.match"),
    ("grouppig.expression.identity.deflector", "rpc:identity.deflect", "rpc:identity.deny-ai"),
    ("grouppig.expression.slang.recognizer", "rpc:slang.recognize", "rpc:slang.lookup"),
    ("grouppig.expression.slang.learner", "rpc:slang.learn", "rpc:slang.upsert"),
    ("grouppig.expression.slang.injector", "rpc:slang.inject", "rpc:slang.lookup"),
)


class FakeSender:
    """假的 ``rpc:sender.send_reply``（形状模仿网关 ``ReplyComposer._result``）。"""

    def __init__(self, *, ok: bool = True, fail: bool = False) -> None:
        self.ok = ok
        self.fail = fail
        self.calls: list[dict[str, Any]] = []

    async def send_reply(self, reply: Any = None, **kwargs: Any) -> dict[str, Any]:
        payload = dict(reply) if isinstance(reply, Mapping) else {"reply": reply}
        payload.update(kwargs)
        self.calls.append(payload)
        if self.fail:
            raise RuntimeError("发送通道不可用（测试注入）")
        text = str(payload.get("text") or "")
        return {
            "ok": bool(self.ok),
            "sent": 1 if self.ok else 0,
            "message_ids": [7001] if self.ok else [],
            "chunks": 1 if self.ok else 0,
            "waited": 0.0,
            "rate": None,
            "recorded": bool(self.ok),
            "skipped": not self.ok,
            "error": "" if self.ok else "rejected",
            "reason": "" if self.ok else "test_reject",
            "target": payload.get("group_id"),
            "text": text,
            "stats": {"sent": 1 if self.ok else 0, "chunks": 1 if self.ok else 0},
        }

    def install(self, registry: Registry, *, replace: bool = True) -> Registry:
        registry.register("rpc:sender.send_reply", self.send_reply, module="test.fake", replace=replace)
        return registry


class FakePresetMatcher:
    """假的 ``rpc:presets.match``（reflection 域的真实实现有独立测试，这里只验调用）。"""

    def __init__(self, *, preset_id: str = "preset-night", reply_probability: float = 0.7, fail: bool = False) -> None:
        self.preset_id = preset_id
        self.reply_probability = reply_probability
        self.fail = fail
        self.calls: list[dict[str, Any]] = []

    async def match(self, features: Any = None, **kwargs: Any) -> dict[str, Any]:
        self.calls.append({"features": dict(features or {}), "kwargs": dict(kwargs)})
        if self.fail:
            raise RuntimeError("预设库不可用（测试注入）")
        preset = {
            "preset_id": self.preset_id,
            "name": "夜间活跃",
            "scenario": "chat",
            "actions": {
                "reply_probability": self.reply_probability,
                "max_replies_per_minute": 3,
                "wait_seconds": [5, 20],
                "tone": "热络",
            },
        }
        return {
            "preset": preset,
            "best": {**preset, "actions": dict(preset["actions"]), "score": 0.82, "matched": True},
            "candidates": [{"preset_id": self.preset_id, "score": 0.82}],
            "rejected": [],
            "count": 1,
            "fallback": False,
            "scenario": "chat",
            "features": dict(features or {}),
            "reason": [],
            "actions": dict(preset["actions"]),
        }

    def install(self, registry: Registry, *, replace: bool = True) -> Registry:
        registry.register("rpc:presets.match", self.match, module="test.fake", replace=replace)
        return registry


@dataclass
class FlowEnv:
    """编排测试环境。"""

    config: Any
    bus: EventBus
    registry: Registry
    stores: MemoryStores
    core: Any
    flow: Any
    clock: Clock
    model: FakeModel
    sender: FakeSender
    collector: EventCollector
    logs: list[tuple[str, str, dict[str, Any]]] = field(default_factory=list)
    presets: FakePresetMatcher | None = None

    async def call(self, name: str, *args: Any, **kwargs: Any) -> Any:
        return await self.registry.acall(name, *args, **kwargs)

    async def run_flow(
        self,
        *,
        group_id: int = GROUP_ID,
        messages: Sequence[Mapping[str, Any]] | None = None,
        user_id: int | None = PEER_IDS[0],
        **kwargs: Any,
    ) -> dict[str, Any]:
        """跑一整轮编排：start → next（若干次）→ end。"""

        rows = [dict(item) for item in (messages if messages is not None else [message(0)])]
        started = await self.call(
            "rpc:flow.start",
            group_id,
            user_id=user_id,
            messages=rows,
            keyword=kwargs.pop("keyword", "打本"),
            **kwargs,
        )
        flow_id = started["flow_id"]
        for _ in range(int(started.get("step_count") or 0)):
            await self.call("rpc:flow.next", flow_id, group_id=group_id)
        ended = await self.call("rpc:flow.end", flow_id, group_id=group_id)
        return {"start": started, "end": ended}

    async def compose(self, **kwargs: Any) -> dict[str, Any]:
        return await self.call("rpc:generator.compose", **kwargs)

    def events(self, topic: str = "kafka:grouppig.reply.composed") -> list[Any]:
        return self.collector.of_topic(topic)

    def log_events(self) -> list[str]:
        return [event for _level, event, _fields in self.logs]

    async def aclose(self) -> None:
        self.collector.close()
        await self.flow.close()
        if self.core is not None:
            await self.core.close()
        await self.stores.aclose()


async def flow_env(
    config: Any,
    *,
    start: float = BASE_TS,
    memory: bool = True,
    core: bool = True,
    model: FakeModel | None = None,
    sender: FakeSender | None = None,
    presets: FakePresetMatcher | None = None,
    extra: Mapping[str, Any] | None = None,
) -> FlowEnv:
    """搭好 t9 编排环境：内存库（可选）+ t8 核心（可选）+ t9 编排层 + 假件。

    ``registry`` 独立，不污染 ``default_registry``（infra 的计数断言依赖它）。
    """

    bus = EventBus(strict_topics=True)
    registry = Registry()
    stores = MemoryStores.from_dsn(EXPRESSION_DSN)
    await stores.migrate()
    if memory:
        register_memory_handlers(registry, stores)
    clock = Clock(start)
    fake = model if model is not None else FakeModel()
    fake.install(registry)
    stub = sender if sender is not None else FakeSender()
    stub.install(registry)
    preset_stub = presets if presets is not None else FakePresetMatcher()
    preset_stub.install(registry)

    logs: list[tuple[str, str, dict[str, Any]]] = []

    def log(level: str, event: str, **fields: Any) -> None:
        logs.append((level, event, dict(fields)))

    class _Holder:
        pass

    holder = _Holder()
    holder.config = config
    holder.registry = registry
    holder.bus = bus
    holder.logger = None

    options = dict(extra or {})
    options.setdefault("self_id", SELF_ID)
    core_layer = None
    if core:
        core_layer = await install_expression(holder, now=clock, extra=options)
        core_layer.ctx.log = log
    flow_layer = await install_expression_flow(holder, now=clock, extra=options)
    flow_layer.ctx.log = log

    collector = EventCollector(bus).listen(*EXPRESSION_FLOW_TOPICS)
    return FlowEnv(
        config=config,
        bus=bus,
        registry=registry,
        stores=stores,
        core=core_layer,
        flow=flow_layer,
        clock=clock,
        model=fake,
        sender=stub,
        collector=collector,
        logs=logs,
        presets=preset_stub,
    )


def flow_contract_coverage(registry: Registry) -> dict[str, list[str]]:
    """t9 契约覆盖度：注册名字 vs 设计里三个子树的 ``rpc:`` 名字。"""

    from grouppig.infra.runtime import contract

    index = contract.api_index()
    scopes = (
        "grouppig.expression.orchestrator",
        "grouppig.expression.identity",
        "grouppig.expression.slang",
    )
    expected = {
        name
        for name, module in index.items()
        if name.startswith("rpc:") and any(module == scope or module.startswith(scope + ".") for scope in scopes)
    }
    registered = set(registry.names())
    return {
        "missing": sorted(expected - registered),
        "unknown": sorted(name for name in registered if name not in index),
        "expected": sorted(expected),
    }


async def seed_slang(
    env: FlowEnv,
    entries: Sequence[Mapping[str, Any]] | None = None,
    *,
    group_id: int = GROUP_ID,
) -> list[dict[str, Any]]:
    """往黑话库里灌词条（走 ``rpc:slang.upsert``，不直接写库）。"""

    rows = list(
        entries
        or (
            {"term": "打本", "meaning": "打副本", "usage_context": "今晚打本吗", "freshness": 0.9, "use_count": 3},
            {"term": "奶妈", "meaning": "治疗职业", "usage_context": "缺个奶妈", "freshness": 0.6, "use_count": 1},
        )
    )
    out: list[dict[str, Any]] = []
    for row in rows:
        payload = {"group_id": group_id, **dict(row)}
        result = await env.call("rpc:slang.upsert", payload)
        out.append(dict(result.get("entry") or {}))
    return out


__all__ = [
    "EXPECTED_APIS",
    "EXPRESSION_FLOW_RPC",
    "EXPECTED_DEPENDENCIES",
    "FLOW_RPC",
    "LEAF_MODULES",
    "FakePresetMatcher",
    "FakeSender",
    "FlowEnv",
    "flow_contract_coverage",
    "flow_env",
    "seed_slang",
]
