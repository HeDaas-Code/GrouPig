"""session 测试辅助：隔离容器 / 契约常量 / 消息构造器 / 收集器。

其它域（gateway / perception / 集成）要跑「会话层」相关用例时，直接复用本模块的
``session_container`` 夹具与 ``MessageBuilder``，避免各自造一套。

设计对照：``normify-grouppig/api-index.json`` 中归属 ``grouppig.session`` 的
25 个 ``rpc:`` 名字（16 个设计叶子）+ 2 个 ``kafka:`` 主题。
"""

from __future__ import annotations

import contextlib
import time
from typing import Any

#: 设计里归属 ``grouppig.session`` 的 16 个设计叶子名字（逐字抄自 api-index.json）。
SESSION_RPC = (
    "rpc:cross.detect",
    "rpc:cross.match",
    "rpc:cross.parse",
    "rpc:session.archive",
    "rpc:session.archive.check",
    "rpc:session.current",
    "rpc:session.heat",
    "rpc:session.open",
    "rpc:session.sleep",
    "rpc:session.update",
    "rpc:session.wake",
    "rpc:threads.link",
    "rpc:threads.outline",
    "rpc:threads.segment",
    "rpc:threads.weave",
    "rpc:topic.boundary.detect",
    "rpc:topic.candidate.generate",
    "rpc:topic.detect",
    "rpc:topic.embed",
    "rpc:topic.embed.cache.get",
    "rpc:topic.embed.cache.set",
    "rpc:topic.resolve",
    "rpc:topic.similarity",
    "rpc:wake.buffer.pop",
    "rpc:wake.buffer.push",
)

#: 设计里归属 ``grouppig.session`` 的 2 个 ``kafka:`` 主题。
SESSION_TOPICS = (
    "kafka:grouppig.session.completed",
    "kafka:grouppig.topic.changed",
)

#: 无模型依赖的 EmbeddingClient：确定性哈希袋向量（无需模型服务商即可跑相似度链路）。
DEFAULT_DIM = 12


def hash_embedding(text: str, *, dim: int = DEFAULT_DIM) -> list[float]:
    """确定性哈希袋向量：同文本同向量，词面越接近向量越接近。"""

    from grouppig.session.runtime.messages import tokenize

    vector = [0.0] * dim
    for token in tokenize(text):
        vector[sum(ord(char) for char in token) % dim] += 1.0
    norm = sum(value * value for value in vector) ** 0.5
    return [round(value / norm, 6) for value in vector] if norm else vector


class EmbeddingClient:
    """把 ``rpc:model.embed`` 的返回体换成 demo 客户端：返回确定性哈希向量的调用器。"""

    def __init__(self, *, dim: int = DEFAULT_DIM) -> None:
        self.dim = int(dim)
        self.calls: list[list[str]] = []

    async def __call__(self, name: str, *args: Any, **kwargs: Any) -> dict[str, Any]:
        texts = list(args[0]) if args and isinstance(args[0], (list, tuple)) else [str(args[0]) if args else ""]
        self.calls.append(texts)
        return {"model": "hash-embedding", "vectors": [hash_embedding(text, dim=self.dim) for text in texts]}


class CallRecorder:
    """记录跨域 ``rpc:`` 调用（名字 + 参数），用于断言设计依赖确实被走通。"""

    def __init__(self, handlers: dict[str, Any] | None = None) -> None:
        self.handlers = dict(handlers or {})
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    async def __call__(self, name: str, *args: Any, **kwargs: Any) -> Any:
        self.calls.append((name, args, kwargs))
        handler = self.handlers.get(name)
        if handler is None:
            return None
        result = handler(*args, **kwargs) if callable(handler) else handler
        if hasattr(result, "__await__"):
            return await result
        return result

    def names(self) -> list[str]:
        return [name for name, _, _ in self.calls]

    def count(self, name: str) -> int:
        return sum(1 for called, _, _ in self.calls if called == name)

    def args_of(self, name: str) -> list[tuple[tuple[Any, ...], dict[str, Any]]]:
        return [(args, kwargs) for called, args, kwargs in self.calls if called == name]


class MessageBuilder:
    """构造归一化消息（字段与 ``mysql:chat_messages`` 对齐）。"""

    def __init__(self, *, group_id: int = 100, start_ts: float = 1_700_000_000.0, step: float = 3.0) -> None:
        self.group_id = int(group_id)
        self.ts = float(start_ts)
        self.step = float(step)
        self.index = 0

    def next(
        self,
        content: str,
        *,
        sender_id: int = 201,
        sender_name: str = "阿May",
        role: str = "member",
        reply_to: str = "",
        mentions: list[int] | None = None,
        ts: float | None = None,
        step: float | None = None,
    ) -> dict[str, Any]:
        self.index += 1
        if ts is None:
            self.ts += self.step if step is None else float(step)
            ts = self.ts
        else:
            self.ts = float(ts)
        return {
            "message_id": f"m{self.index}",
            "group_id": self.group_id,
            "sender_id": int(sender_id),
            "sender_name": sender_name,
            "role": role,
            "msg_type": "text",
            "content": content,
            "mentions": list(mentions or []),
            "reply_to": reply_to,
            "ts": float(ts),
            "tokens": len(content),
        }

    def thread(
        self, texts: list[str], *, sender_id: int = 201, step: float = 3.0, **kwargs: Any
    ) -> list[dict[str, Any]]:
        return [self.next(text, sender_id=sender_id, step=step, **kwargs) for text in texts]


def make_messages(
    texts: list[str],
    *,
    group_id: int = 100,
    start_ts: float = 1_700_000_000.0,
    step: float = 3.0,
    sender_id: int = 201,
) -> list[dict[str, Any]]:
    """便捷函数：把一串文本变成一串消息（时间递增）。"""

    builder = MessageBuilder(group_id=group_id, start_ts=start_ts, step=step)
    return [builder.next(text, sender_id=sender_id) for text in texts]


@contextlib.asynccontextmanager
async def session_container(
    config: Any,
    *,
    dsn: str = "sqlite+aiosqlite:///:memory:",
    with_memory: bool = True,
    with_session: bool = True,
    **kwargs: Any,
):
    """独立注册表的容器 + memory 挂载 + 会话层挂载（不污染全局 default_registry）。

    产出 ``(container, layer)``：``container`` 上有 ``memory`` 与 ``session``，
    可直接 ``await container.call("rpc:topic.detect", ...)``。
    """

    from grouppig.infra.logger import RuntimeLogger
    from grouppig.infra.model_gateway.router import ModelRouter
    from grouppig.infra.runtime.bus import EventBus
    from grouppig.infra.runtime.di import Container
    from grouppig.infra.runtime.registry import Registry
    from grouppig.infra.token_budget.meter import TokenMeter
    from grouppig.memory.runtime.stores import MemoryStores

    registry = Registry()
    logger = RuntimeLogger()
    meter = TokenMeter(logger=logger, config=config)
    container = Container(
        config=config,
        registry=registry,
        logger=logger,
        bus=EventBus(logger=logger, strict_topics=True),
        meter=meter,
        router=ModelRouter(config, logger=logger, meter=meter),
    )
    await container.start()
    stores = MemoryStores.from_dsn(dsn)
    await stores.migrate()
    container.memory = stores
    if with_memory:
        from grouppig.memory.runtime.di import register_memory_handlers

        register_memory_handlers(container, stores)
    layer: Any = None
    if with_session:
        from grouppig.session.runtime.di import attach_session

        layer = await attach_session(container, **kwargs)
    try:
        yield container, layer
    finally:
        if layer is not None:
            await layer.aclose()
        await stores.aclose()
        await container.aclose()


async def attach_session_layer(container: Any, **kwargs: Any) -> Any:
    """便捷入口：给已有容器挂会话层。"""

    from grouppig.session.runtime.di import attach_session

    return await attach_session(container, **kwargs)


def now() -> float:
    return time.time()
