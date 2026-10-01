"""social 域测试辅助：内存库夹具、消息工厂、孤立容器与契约工具。

设计要点：

* :func:`social_env` —— 一次性搭好「memory 存储 + 注册表 + 事件总线 + social 层」，
  测试只关心契约名字，不关心装配细节；
* 时钟用 :class:`Clock` 注入（可拨动），保证时间相关断言（衰减 / 新鲜度）可复现；
* 事件用 :class:`EventCollector` 收集，验证 ``kafka:`` 主题的发布。
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from grouppig.infra.config.loader import Config
from grouppig.infra.runtime import contract
from grouppig.infra.runtime.bus import EventBus
from grouppig.infra.runtime.registry import Registry
from grouppig.memory.runtime.di import MEMORY_RPC, register_memory_handlers
from grouppig.memory.runtime.stores import MemoryStores
from grouppig.social import SOCIAL_RPC, SOCIAL_TOPICS, SocialLayer, install_social

#: 测试群号。
GROUP_ID = 100200300
#: 测试群友。
SENDER_IDS = (1001, 1002, 1003)
#: 基准时间（固定值，保证断言可复现）。
BASE_TS = 1_700_000_000.0
#: 内存库 DSN。
SOCIAL_DSN = "sqlite+aiosqlite:///:memory:"


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
class SocialEnv:
    """一次性的社交域测试环境。"""

    config: Config
    bus: EventBus
    registry: Registry
    stores: MemoryStores
    layer: SocialLayer
    clock: Clock
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
            result = await self.stores.chat.append(row)
            out.append(result["message"])
        return out

    async def profiles(self) -> list[dict[str, Any]]:
        return await self.stores.profiles.list_profiles(limit=100)

    async def edges(self, **kwargs: Any) -> list[dict[str, Any]]:
        return await self.stores.social.get_edges(**kwargs)

    async def events(self, topic: str) -> list[Any]:
        return self.collector.of_topic(topic)


async def social_env(
    config: Config,
    *,
    start: float = BASE_TS,
    listen: bool = True,
    extra: Mapping[str, Any] | None = None,
) -> SocialEnv:
    """搭好社交层：内存库（memory 全表）+ 独立注册表 + 严格主题总线 + 假时钟。

    ``registry`` 是独立的，不污染 ``default_registry``（infra 的计数断言依赖它）。
    """

    bus = EventBus(strict_topics=True)
    registry = Registry()
    stores = MemoryStores.from_dsn(SOCIAL_DSN)
    await stores.migrate()
    register_memory_handlers(registry, stores)
    clock = Clock(start)
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
    layer = await install_social(holder, subscribe=False, now=clock, extra=dict(extra or {}))
    layer.ctx.log = log

    collector = EventCollector(bus)
    if listen:
        collector.listen(*SOCIAL_TOPICS)
    return SocialEnv(
        config=config,
        bus=bus,
        registry=registry,
        stores=stores,
        layer=layer,
        clock=clock,
        collector=collector,
        logs=logs,
    )


def message(index: int, **overrides: Any) -> dict[str, Any]:
    """造一条聊天消息。"""

    payload: dict[str, Any] = {
        "message_id": f"smsg-{index}",
        "group_id": GROUP_ID,
        "sender_id": SENDER_IDS[index % len(SENDER_IDS)],
        "sender_name": f"群友{index % len(SENDER_IDS)}",
        "msg_type": "text",
        "content": f"今晚打本吗（{index}）",
        "ts": BASE_TS + index * 10.0,
    }
    payload.update(overrides)
    return payload


def thread(**overrides: Any) -> dict[str, Any]:
    """造一条聊天线（供立场抽取用）。"""

    payload: dict[str, Any] = {
        "thread_id": "thread-1",
        "group_id": GROUP_ID,
        "session_id": "session-1",
        "topic_id": "topic-1",
        "title": "打本",
        "keywords": ["打本"],
        "participants": list(SENDER_IDS),
        "message_ids": [],
        "message_count": 0,
        "first_ts": BASE_TS,
        "last_ts": BASE_TS + 300.0,
        "status": "open",
    }
    payload.update(overrides)
    return payload


def social_contract_coverage(registry: Registry, *, scope: str = "grouppig.social") -> dict[str, list[str]]:
    """social 域契约覆盖度：``rpc:`` 看注册表，``kafka:`` 主题单独比对常量。"""

    check = contract.check_registry(set(registry.names()), scope=scope)
    expected_topics = {
        name
        for name, module in contract.api_index().items()
        if name.startswith("kafka:") and (module == scope or module.startswith(scope + "."))
    }
    return {
        "missing": [name for name in check["missing"] if not name.startswith("kafka:")],
        "unknown": check["unknown"],
        "topics_missing": sorted(expected_topics - set(SOCIAL_TOPICS)),
    }


def expected_social_names() -> dict[str, set[str]]:
    """api-index.json 里归属 grouppig.social 的名字。"""

    names = {name for name, module in contract.api_index().items() if module.startswith("grouppig.social")}
    return {
        "rpc": {n for n in names if n.startswith("rpc:")},
        "kafka": {n for n in names if n.startswith("kafka:")},
        "all": names,
    }


def profile_row(user_id: int = 1001, **overrides: Any) -> dict[str, Any]:
    """造一行档案（直写 memory，用来验证读取路径）。"""

    payload: dict[str, Any] = {
        "user_id": user_id,
        "nickname": f"u{user_id}",
        "aliases": [f"u{user_id}"],
        "tags": ["技术宅"],
        "interests": ["打本"],
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


__all__ = [
    "BASE_TS",
    "GROUP_ID",
    "MEMORY_RPC",
    "SENDER_IDS",
    "SOCIAL_DSN",
    "SOCIAL_RPC",
    "SOCIAL_TOPICS",
    "Clock",
    "EventCollector",
    "SocialEnv",
    "expected_social_names",
    "message",
    "profile_row",
    "social_contract_coverage",
    "social_env",
    "thread",
]
