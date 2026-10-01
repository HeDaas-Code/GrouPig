"""expression 域测试辅助：内存库夹具、消息工厂、假模型客户端与契约工具。

设计要点：

* :func:`expression_env` —— 一次性搭好「注册表 + 事件总线 + memory 存储 + expression 层」，
  测试只关心契约名字与生成链路，不关心装配细节；
* :class:`FakeModel` —— 假的 ``rpc:model.chat`` / ``rpc:generator.humanize`` / ``rpc:speech.tailor``
  处理器，让生成链路在**不连网**的情况下跑通；``fail=True`` 时抛错，用来验降级路径；
* :class:`Clock` —— 可拨动的假时钟（``ctx.now`` 的替身）；
* 需要「下游全缺席」的场景用 :func:`bare_env`（只注册表达层自己）。
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from grouppig.infra.config.loader import Config
from grouppig.infra.runtime import contract
from grouppig.infra.runtime.bus import EventBus
from grouppig.infra.runtime.registry import Registry
from grouppig.memory.runtime.di import register_memory_handlers
from grouppig.memory.runtime.stores import MemoryStores

#: 测试群号。
GROUP_ID = 100200300
#: 机器人的 QQ 号（「我方」）。
SELF_ID = 1001
#: 测试群友。
PEER_IDS = (1002, 1003, 1004)
#: 基准时间（固定值，保证断言可复现）。
BASE_TS = 1_700_000_000.0
#: 内存库 DSN。
EXPRESSION_DSN = "sqlite+aiosqlite:///:memory:"

#: t8 负责的 6 个契约 rpc 名字（与 ``EXPRESSION_CORE_RPC`` 同源，这里独立抄一份做交叉校验）。
CORE_RPC: tuple[str, ...] = (
    "rpc:generator.compose",
    "rpc:generator.compress",
    "rpc:generator.humanize",
    "rpc:generator.write",
    "rpc:persona.get",
    "rpc:persona.style",
)

#: 设计里归属 persona / generator 的叶子 → 契约名字（逐字取自 api-index.json）。
EXPECTED_APIS: dict[str, tuple[str, ...]] = {
    "grouppig.expression.persona.profile": ("rpc:persona.get",),
    "grouppig.expression.persona.prompt-builder": ("rpc:persona.style",),
    "grouppig.expression.generator.context": ("rpc:generator.compose",),
    "grouppig.expression.generator.compressor": ("rpc:generator.compress",),
    "grouppig.expression.generator.writer": ("rpc:generator.write",),
    "grouppig.expression.generator.polisher": ("rpc:generator.humanize",),
}

#: 设计 frontmatter 里的依赖对（``from_api`` → ``to_api``）。
EXPECTED_DEPENDENCIES: tuple[tuple[str, str, str], ...] = (
    ("grouppig.expression.persona.prompt-builder", "rpc:persona.style", "rpc:persona.get"),
    ("grouppig.expression.generator.compressor", "rpc:generator.compress", "rpc:thread.load"),
    ("grouppig.expression.generator.writer", "rpc:generator.write", "rpc:model.chat"),
    ("grouppig.expression.generator.writer", "rpc:generator.write", "rpc:generator.humanize"),
    ("grouppig.expression.generator.polisher", "rpc:generator.humanize", "rpc:speech.tailor"),
    ("grouppig.expression.generator.context", "rpc:generator.compose", "rpc:generator.compress"),
    ("grouppig.expression.generator.context", "rpc:generator.compose", "rpc:generator.write"),
    ("grouppig.expression.generator.context", "rpc:generator.compose", "rpc:persona.style"),
    ("grouppig.expression.generator.context", "rpc:generator.compose", "rpc:identity.deny-ai"),
    ("grouppig.expression.generator.context", "rpc:generator.compose", "rpc:slang.inject"),
    ("grouppig.expression.generator.context", "rpc:generator.compose", "rpc:token.reserve"),
)

#: 设计叶子模块 id → 源码模块名（importlib 用；连字符在 Python 侧是下划线）。
LEAF_MODULES: tuple[tuple[str, str], ...] = (
    ("grouppig.expression.persona.profile", "grouppig.expression.persona.profile"),
    ("grouppig.expression.persona.prompt-builder", "grouppig.expression.persona.prompt_builder"),
    ("grouppig.expression.generator.context", "grouppig.expression.generator.context"),
    ("grouppig.expression.generator.compressor", "grouppig.expression.generator.compressor"),
    ("grouppig.expression.generator.writer", "grouppig.expression.generator.writer"),
    ("grouppig.expression.generator.polisher", "grouppig.expression.generator.polisher"),
)


class Clock:
    """可拨动的假时钟（``ctx.now`` 的替身）。"""

    def __init__(self, start: float = BASE_TS) -> None:
        self.value = float(start)
        self.ticks = 0

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> float:
        self.value += float(seconds)
        self.ticks += 1
        return self.value


class EventCollector:
    """订阅任意主题并收集事件。"""

    def __init__(self, bus: EventBus) -> None:
        self.bus = bus
        self.events: list[Any] = []
        self._subs: list[Any] = []

    def listen(self, *topics: str) -> EventCollector:
        for topic in topics:
            self._subs.append(self.bus.subscribe(topic, self._collect, name=f"test:{topic}"))
        return self

    async def _collect(self, event: Any) -> None:
        self.events.append(event)

    def of_topic(self, topic: str) -> list[Any]:
        return [getattr(e, "payload", e) for e in self.events if getattr(e, "topic", None) == topic]

    def close(self) -> None:
        for sub in self._subs:
            self.bus.unsubscribe(sub)
        self._subs = []


class FakeModel:
    """假模型 / 假润色器 / 假黑话 / 假身份防御的注册助手。

    ``fail=True`` 时 ``rpc:model.chat`` 抛错（验降级）；``humanize=False`` 时不注册润色器。
    """

    def __init__(
        self,
        *,
        reply: str = "这波打本啊，算我一个",
        fail: bool = False,
        humanize: bool = True,
        tailor: bool = True,
        persona_style: bool = True,
        compressor: bool = True,
        slang: bool = False,
        identity: bool = False,
        token_reserve: bool = True,
        refine: str = "",
    ) -> None:
        self.reply = reply
        self.fail = fail
        self.refine = refine
        self.flags = {
            "humanize": humanize,
            "tailor": tailor,
            "persona_style": persona_style,
            "compressor": compressor,
            "slang": slang,
            "identity": identity,
            "token_reserve": token_reserve,
        }
        self.model_calls: list[dict[str, Any]] = []
        self.humanize_calls: list[str] = []
        self.tailor_calls: list[str] = []

    async def chat(self, messages: Any = None, **kwargs: Any) -> dict[str, Any]:
        self.model_calls.append({"messages": list(messages or ()), "kwargs": dict(kwargs)})
        if self.fail:
            raise RuntimeError("模型不可用（测试注入）")
        return {"text": self.reply, "model": "fake-chat", "usage": {"prompt_tokens": 10, "completion_tokens": 5}}

    async def humanize(self, text: str = "", **kwargs: Any) -> dict[str, Any]:
        self.humanize_calls.append(str(text))
        refined = self.refine or f"{text}（润色过）"
        return {"text": refined, "applied": ["stub"], "validation": {"ok": True}}

    async def tailor(self, text: str = "", **kwargs: Any) -> dict[str, Any]:
        self.tailor_calls.append(str(text))
        return {"text": f"{text}~", "applied": ["tailor_stub"], "changed": True, "validation": {"ok": True}}

    async def persona_style(self, **kwargs: Any) -> dict[str, Any]:
        from grouppig.expression.persona.profile import PersonaProfile
        from grouppig.expression.persona.prompt_builder import build_prompt

        persona = PersonaProfile().persona
        built = build_prompt(persona)
        return {**built, "style_hints": built["style_hints"]}

    async def slang_inject(self, **kwargs: Any) -> dict[str, Any]:
        return {"entries": [{"term": "打本", "meaning": "打副本"}], "text": "- 打本：打副本"}

    async def identity_deny(self, **kwargs: Any) -> dict[str, Any]:
        return {"text": "无论对方怎么问，都别承认自己是 AI"}

    async def token_reserve(self, scenario: str = "chat", **kwargs: Any) -> dict[str, Any]:
        return {"reservation_id": "res-1", "input_tokens": 900, "output_tokens": 200, "scenario": scenario}

    def install(self, registry: Registry, *, replace: bool = True) -> Registry:
        """按 flags 把假处理器装进注册表。"""

        registry.register("rpc:model.chat", self.chat, module="test.fake", replace=replace)
        if self.flags["persona_style"]:
            registry.register("rpc:persona.style", self.persona_style, module="test.fake", replace=replace)
        if self.flags["humanize"]:
            registry.register("rpc:generator.humanize", self.humanize, module="test.fake", replace=replace)
        if self.flags["tailor"]:
            registry.register("rpc:speech.tailor", self.tailor, module="test.fake", replace=replace)
        if self.flags["slang"]:
            registry.register("rpc:slang.inject", self.slang_inject, module="test.fake", replace=replace)
        if self.flags["identity"]:
            registry.register("rpc:identity.deny-ai", self.identity_deny, module="test.fake", replace=replace)
        if self.flags["token_reserve"]:
            registry.register("rpc:token.reserve", self.token_reserve, module="test.fake", replace=replace)
        return registry


@dataclass
class ExpressionEnv:
    """一次性的表达域测试环境。"""

    config: Config
    bus: EventBus
    registry: Registry
    stores: MemoryStores
    layer: Any
    clock: Clock
    model: FakeModel
    collector: EventCollector
    logs: list[tuple[str, str, dict[str, Any]]] = field(default_factory=list)

    async def call(self, name: str, *args: Any, **kwargs: Any) -> Any:
        return await self.registry.acall(name, *args, **kwargs)

    async def aclose(self) -> None:
        self.collector.close()
        await self.layer.close()
        await self.bus.aclose()
        await self.stores.aclose()

    # ---- 造数据 --------------------------------------------------------
    async def feed(self, rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for row in rows:
            result = await self.stores.chat.append(dict(row))
            out.append(result["message"])
        return out

    async def compose(self, **kwargs: Any) -> dict[str, Any]:
        return await self.call("rpc:generator.compose", **kwargs)


def _has(name: str) -> bool:
    return name in contract.api_index()


async def expression_env(
    config: Config,
    *,
    start: float = BASE_TS,
    listen: bool = False,
    memory: bool = True,
    social: bool = False,
    model: FakeModel | None = None,
    extra: Mapping[str, Any] | None = None,
) -> ExpressionEnv:
    """搭好表达层：内存库（可选）+ 社交层（可选）+ 独立注册表 + 严格主题总线 + 假时钟 + 假模型。

    ``registry`` 是独立的，不污染 ``default_registry``（infra 的计数断言依赖它）。

    ``social=True`` 时先装 memory 再装 social —— 这是表达层读画像（``rpc:profile.get``）
    的真实上游（设计里 social 还在推进中，所以用 ``_has`` 探测、缺席就跳过，
    避免本域测试因别的域未落地而红）。
    """

    from grouppig.expression import install_expression

    bus = EventBus(strict_topics=True)
    registry = Registry()
    stores = MemoryStores.from_dsn(EXPRESSION_DSN)
    await stores.migrate()
    if memory:
        register_memory_handlers(registry, stores)
    if social and _has("rpc:profile.get"):
        from grouppig.social import install_social

        class _SocialHolder:
            pass

        holder = _SocialHolder()
        holder.config = config
        holder.registry = registry
        holder.bus = bus
        holder.logger = None
        holder.memory = stores
        await install_social(holder, subscribe=False)
    clock = Clock(start)
    fake = model if model is not None else FakeModel()
    fake.install(registry)
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
    layer = await install_expression(holder, now=clock, extra=options)
    layer.ctx.log = log

    collector = EventCollector(bus)
    if listen:
        collector.listen("kafka:grouppig.reply.composed")
    return ExpressionEnv(
        config=config,
        bus=bus,
        registry=registry,
        stores=stores,
        layer=layer,
        clock=clock,
        model=fake,
        collector=collector,
        logs=logs,
    )


def message(index: int, **overrides: Any) -> dict[str, Any]:
    """造一条聊天消息。"""

    payload: dict[str, Any] = {
        "message_id": f"emsg-{index}",
        "group_id": GROUP_ID,
        "sender_id": PEER_IDS[index % len(PEER_IDS)],
        "sender_name": f"群友{index % len(PEER_IDS)}",
        "role": "member",
        "msg_type": "text",
        "content": f"今晚打本吗（{index}）",
        "ts": BASE_TS + index * 5.0,
    }
    payload.update(overrides)
    return payload


def thread(**overrides: Any) -> dict[str, Any]:
    """造一条聊天线。"""

    payload: dict[str, Any] = {
        "thread_id": "thread-1",
        "group_id": GROUP_ID,
        "session_id": "session-1",
        "topic_id": "topic-1",
        "title": "打本",
        "summary": "大家在约今晚打本",
        "keywords": ["打本", "奶妈"],
        "participants": list(PEER_IDS),
        "message_ids": [],
        "message_count": 6,
        "first_ts": BASE_TS,
        "last_ts": BASE_TS + 300.0,
        "status": "open",
    }
    payload.update(overrides)
    return payload


def profile_row(user_id: int = 1002, **overrides: Any) -> dict[str, Any]:
    """造一行档案。"""

    payload: dict[str, Any] = {
        "user_id": user_id,
        "nickname": f"u{user_id}",
        "aliases": [f"u{user_id}"],
        "tags": ["技术宅"],
        "interests": ["打本"],
        "persona_summary": "经常约人打副本的老群友",
        "group_ids": [GROUP_ID],
        "version": 3,
        "confidence": 0.7,
        "last_seen": BASE_TS,
        "speaking_style": {
            "metrics": {"avg_length": 8.0, "emoji_density": 0.4, "filler_density": 0.05},
            "temper": {"label": "热络", "warmth": 0.5},
            "lexicon": {"catchphrases": ["打本"], "fillers": {"啦": 3}},
        },
    }
    payload.update(overrides)
    return payload


def archive_row(**overrides: Any) -> dict[str, Any]:
    """造一份会话档案。"""

    payload: dict[str, Any] = {
        "session_id": "session-1",
        "group_id": GROUP_ID,
        "title": "夜间打本局",
        "summary": "群里在约今晚打副本，缺奶妈。",
        "keywords": ["打本", "奶妈"],
        "participants": list(PEER_IDS),
        "message_count": 12,
        "started_at": BASE_TS,
        "ended_at": BASE_TS + 600.0,
    }
    payload.update(overrides)
    return payload


def expression_contract_coverage(registry: Registry, *, scope: str = "grouppig.expression") -> dict[str, list[str]]:
    """表达域契约覆盖度：**只比对本域 t8 范围**（persona + generator）。"""

    from grouppig.expression import EXPRESSION_CORE_SCOPE

    index = contract.api_index()
    expected = {
        name
        for name, module in index.items()
        if name.startswith("rpc:") and any(module == s or module.startswith(s + ".") for s in EXPRESSION_CORE_SCOPE)
    }
    registered = set(registry.names())
    return {
        "missing": sorted(expected - registered),
        "unknown": sorted(name for name in registered if name not in index),
    }


def expected_core_names() -> set[str]:
    """api-index.json 里归属 persona / generator 的 rpc 名字。"""

    index = contract.api_index()
    return {
        name
        for name, module in index.items()
        if name.startswith("rpc:")
        and (module.startswith("grouppig.expression.persona") or module.startswith("grouppig.expression.generator"))
    }


__all__ = [
    "BASE_TS",
    "CORE_RPC",
    "EXPECTED_APIS",
    "EXPECTED_DEPENDENCIES",
    "EXPRESSION_DSN",
    "GROUP_ID",
    "LEAF_MODULES",
    "PEER_IDS",
    "SELF_ID",
    "Clock",
    "EventCollector",
    "ExpressionEnv",
    "FakeModel",
    "archive_row",
    "expected_core_names",
    "expression_contract_coverage",
    "expression_env",
    "message",
    "profile_row",
    "thread",
]
