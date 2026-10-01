"""reflection 域测试辅助：内存库夹具、消息/会话工厂、孤立容器与契约工具。

设计要点：

* :func:`reflection_env` —— 一次性搭好「memory 存储 + 注册表 + 事件总线 + reflection 层 +
  session 事件源」，测试只关心契约名字，不关心装配细节；
* 预设落盘目录用 :class:`tempfile.TemporaryDirectory`，每个测试互不污染；
* 时钟用 :class:`Clock` 注入（可拨动），保证时间相关断言可复现；
* 事件用 :class:`EventCollector` 收集，验证 `kafka:grouppig.session.completed` 的订阅链路。
"""

from __future__ import annotations

import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from grouppig.infra.config.loader import Config
from grouppig.infra.runtime import contract
from grouppig.infra.runtime.bus import EventBus
from grouppig.infra.runtime.registry import Registry
from grouppig.memory.runtime.di import register_memory_handlers
from grouppig.memory.runtime.stores import MemoryStores
from grouppig.reflection import (
    REFLECTION_RPC,
    REFLECTION_TOPICS,
    ReflectionLayer,
    install_reflection,
)

#: 测试群号。
GROUP_ID = 100200300
#: 机器人的 QQ 号（「我方」）。
SELF_ID = 1001
#: 测试群友。
PEER_IDS = (1002, 1003, 1004)
#: 基准时间（固定值，保证断言可复现）。
BASE_TS = 1_700_000_000.0
#: 内存库 DSN。
REFLECTION_DSN = "sqlite+aiosqlite:///:memory:"
#: 会话完成事件主题。
TOPIC_SESSION_COMPLETED = "kafka:grouppig.session.completed"


class Clock:
    """可拨动的假时钟（`ctx.now` 的替身）。"""

    def __init__(self, start: float = BASE_TS) -> None:
        self.value = float(start)
        self.ticks = 0

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> float:
        self.value += float(seconds)
        self.ticks += 1
        return self.value

    def advance_days(self, days: float) -> float:
        return self.advance(days * 86400.0)


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
        return [getattr(event, "payload", event) for event in self.events if getattr(event, "topic", None) == topic]

    def close(self) -> None:
        for sub in self._subs:
            self.bus.unsubscribe(sub)
        self._subs = []


@dataclass
class ReflectionEnv:
    """一次性的反思域测试环境。"""

    config: Config
    bus: EventBus
    registry: Registry
    stores: MemoryStores
    layer: ReflectionLayer
    clock: Clock
    collector: EventCollector
    presets_dir: str
    logs: list[tuple[str, str, dict[str, Any]]] = field(default_factory=list)
    _tmp: Any = None

    async def call(self, name: str, *args: Any, **kwargs: Any) -> Any:
        return await self.registry.acall(name, *args, **kwargs)

    async def publish(self, topic: str, payload: Mapping[str, Any]) -> Any:
        return await self.bus.publish(topic, payload, source="test")

    async def feed(self, rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for row in rows:
            result = await self.stores.chat.append(row)
            out.append(result["message"])
        return out

    async def events(self, topic: str) -> list[Any]:
        return self.collector.of_topic(topic)

    async def aclose(self) -> None:
        self.collector.close()
        await self.layer.close()
        await self.bus.aclose()
        await self.stores.aclose()
        if self._tmp is not None:
            self._tmp.cleanup()


async def reflection_env(
    config: Config,
    *,
    start: float = BASE_TS,
    listen: bool = True,
    extra: Mapping[str, Any] | None = None,
    with_memory: bool = True,
) -> ReflectionEnv:
    """搭好反思层：内存库（可选）+ 独立注册表 + 严格主题总线 + 假时钟 + 临时预设目录。

    `registry` 是独立的，不污染 `default_registry`（infra 的计数断言依赖它）。
    """

    bus = EventBus(strict_topics=True)
    registry = Registry()
    stores = MemoryStores.from_dsn(REFLECTION_DSN)
    await stores.migrate()
    if with_memory:
        register_memory_handlers(registry, stores)
    clock = Clock(start)
    logs: list[tuple[str, str, dict[str, Any]]] = []
    tmp = tempfile.TemporaryDirectory(prefix="grp-presets-")

    def log(level: str, event: str, **fields: Any) -> None:
        logs.append((level, event, dict(fields)))

    class _Holder:
        pass

    holder = _Holder()
    holder.config = config
    holder.registry = registry
    holder.bus = bus
    holder.logger = None
    options = {"self_id": SELF_ID, "group_id": GROUP_ID, "presets_dir": tmp.name}
    options.update(extra or {})
    layer = await install_reflection(holder, subscribe=False, now=clock, extra=options)
    layer.ctx.log = log

    collector = EventCollector(bus)
    if listen:
        collector.listen(*REFLECTION_TOPICS)
    return ReflectionEnv(
        config=config,
        bus=bus,
        registry=registry,
        stores=stores,
        layer=layer,
        clock=clock,
        collector=collector,
        presets_dir=tmp.name,
        logs=logs,
        _tmp=tmp,
    )


def message(index: int, **overrides: Any) -> dict[str, Any]:
    """造一条聊天消息（默认是**群友**说的，`self=True` 时是我方）。"""

    is_self = bool(overrides.pop("self", False))
    sender = SELF_ID if is_self else overrides.pop("sender_id", PEER_IDS[index % len(PEER_IDS)])
    payload: dict[str, Any] = {
        "message_id": f"rmsg-{index}",
        "group_id": GROUP_ID,
        "session_id": "session-1",
        "sender_id": sender,
        "sender_name": "我" if is_self else f"群友{sender}",
        "msg_type": "text",
        "content": f"今晚打本吗（{index}）",
        "ts": BASE_TS + index * 10.0,
    }
    payload.update(overrides)
    return payload


def session_event(**overrides: Any) -> dict[str, Any]:
    """造一条会话完成事件（形状与 `session.lifecycle.event-emitter` 一致）。"""

    payload: dict[str, Any] = {
        "event": "session.completed",
        "session_id": "session-1",
        "group_id": GROUP_ID,
        "title": "打本",
        "summary": "群友约打本，聊了半小时。",
        "keywords": ["打本"],
        "participants": [SELF_ID, *PEER_IDS],
        "thread_ids": ["thread-1"],
        "topic_ids": ["topic-1"],
        "message_count": 6,
        "started_at": BASE_TS,
        "ended_at": BASE_TS + 300.0,
        "duration": 300.0,
        "heat": 0.4,
        "reason": "silence",
        "ts": BASE_TS + 300.0,
    }
    payload.update(overrides)
    return payload


def preset(**overrides: Any) -> dict[str, Any]:
    """造一个预设。"""

    payload: dict[str, Any] = {
        "preset_id": "test_preset",
        "name": "测试预设",
        "scenario": "calm",
        "triggers": {"heat_max": 0.5},
        "actions": {
            "reply_probability": 0.3,
            "max_replies_per_minute": 2,
            "duration_seconds": 5,
            "wait_seconds": [3, 10],
            "tone": "平和",
        },
        "notes": "",
        "source": "manual",
    }
    payload.update(overrides)
    return payload


def reflection_contract_coverage(registry: Registry, *, scope: str = "grouppig.reflection") -> dict[str, list[str]]:
    """reflection 域契约覆盖度：`rpc:` 看注册表，订阅主题单独比对常量。"""

    check = contract.check_registry(set(registry.names()), scope=scope)
    expected_topics = {
        name
        for name, module in contract.api_index().items()
        if name.startswith("kafka:") and (module == scope or module.startswith(scope + "."))
    }
    return {
        "missing": [name for name in check["missing"] if not name.startswith("kafka:")],
        "unknown": check["unknown"],
        "owned_topics": sorted(expected_topics),
        "subscribed_topics": sorted(REFLECTION_TOPICS),
    }


def expected_reflection_names() -> set[str]:
    """设计里属于 reflection 域的 `rpc:` 名字（直接读 api-index）。"""

    return {
        name
        for name, module in contract.api_index().items()
        if module.startswith("grouppig.reflection") and name.startswith("rpc:")
    }


__all__ = [
    "BASE_TS",
    "GROUP_ID",
    "PEER_IDS",
    "REFLECTION_DSN",
    "REFLECTION_RPC",
    "SELF_ID",
    "TOPIC_SESSION_COMPLETED",
    "Clock",
    "EventCollector",
    "ReflectionEnv",
    "expected_reflection_names",
    "message",
    "preset",
    "reflection_contract_coverage",
    "reflection_env",
    "session_event",
]
