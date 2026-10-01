"""grouppig.session.runtime.di —— session 域装配入口（16 个 ``rpc:`` 处理器注册 + 域内协作接线）。

用法（集成入口 t10 只需两行）::

    from grouppig.session.runtime.di import attach_session

    layer = await attach_session(container)          # 注册 16 个 rpc: + 接管跨域回调
    await container.call("rpc:topic.detect", group_id=100, messages=window)

端到端事件拓扑（单进程内）::

    gateway 发布 kafka:grouppig.qq.message.received
      → SessionLayer.on_message
        → rpc:topic.detect        （话题识别 → 会话开启 → kafka:grouppig.topic.changed）
        → rpc:session.update      （热度 → 归档检查 → 必要时自动归档）
        → rpc:threads.weave       （聊天线编织 → rpc:thread.save）
        → rpc:session.archive     （归档 → rpc:archive.save → kafka:grouppig.session.completed
                                   → rpc:review.on-session-completed）

设计树里没有「session 域装配」这一叶子，本模块是运行时补充（与 ``grouppig.memory.runtime.di`` 同例）。
每个处理器的 ``module`` 都登记为设计树里该名字的归属模块（``contract.owner(name)``），
便于 ``contract.check_registry`` 按域校验。

normify id: ``grouppig.session.runtime.di``（运行时补充模块，设计树暂无）。
"""

from __future__ import annotations

import contextlib
from collections import deque
from collections.abc import Mapping
from typing import Any

from grouppig.infra.runtime import contract
from grouppig.infra.runtime.registry import Registry
from grouppig.session.lifecycle import archive_trigger as archive_trigger_module
from grouppig.session.lifecycle import event_emitter as event_emitter_module
from grouppig.session.lifecycle import heat as heat_module
from grouppig.session.lifecycle import state_machine as state_machine_module
from grouppig.session.lifecycle.archive_trigger import ArchiveTrigger
from grouppig.session.lifecycle.event_emitter import SessionEventEmitter
from grouppig.session.lifecycle.heat import HeatManager
from grouppig.session.lifecycle.state_machine import SessionStateMachine
from grouppig.session.runtime import messages as messages_module
from grouppig.session.runtime.errors import SessionError
from grouppig.session.threads.cross import matcher as matcher_module
from grouppig.session.threads.cross import reference_parser as reference_parser_module
from grouppig.session.threads.cross.matcher import CrossSessionMatcher
from grouppig.session.threads.cross.reference_parser import ReferenceParser
from grouppig.session.threads.weaver import linker as linker_module
from grouppig.session.threads.weaver import outliner as outliner_module
from grouppig.session.threads.weaver import segmenter as segmenter_module
from grouppig.session.threads.weaver.linker import ThreadLinker
from grouppig.session.threads.weaver.outliner import ThreadOutliner
from grouppig.session.threads.weaver.segmenter import MessageSegmenter
from grouppig.session.topic.detector import boundary as boundary_module
from grouppig.session.topic.detector import candidate as candidate_module
from grouppig.session.topic.detector import ranker as ranker_module
from grouppig.session.topic.detector.boundary import BoundaryDetector
from grouppig.session.topic.detector.candidate import TopicCandidateGenerator
from grouppig.session.topic.detector.ranker import TopicRanker
from grouppig.session.topic.embedder import cache as cache_module
from grouppig.session.topic.embedder import similarity as similarity_module
from grouppig.session.topic.embedder.cache import EmbeddingCache
from grouppig.session.topic.embedder.similarity import TopicEmbedder
from grouppig.session.wake import buffer as buffer_module
from grouppig.session.wake import restorer as restorer_module
from grouppig.session.wake.buffer import WakeBuffer
from grouppig.session.wake.restorer import SessionRestorer

#: 本域前缀。
SESSION_SCOPE = "grouppig.session"

#: 本域注册的 16 个 ``rpc:`` 名字（= api-index.json 中归属 grouppig.session 的全部 rpc 名字）。
SESSION_RPC: tuple[str, ...] = tuple(
    sorted(n for n, m in contract.api_index().items() if m.startswith(SESSION_SCOPE) and n.startswith("rpc:"))
)

#: 本域负责的 2 个 ``kafka:`` 主题（发布语义，不进注册表）。
SESSION_TOPICS: tuple[str, ...] = (
    event_emitter_module.TOPIC,
    ranker_module.TOPIC_CHANGED,
)

#: 入站主题：群消息到达后驱动整个会话层（gateway 发布）。
TOPIC_MESSAGE_RECEIVED = "kafka:grouppig.qq.message.received"

#: 会话完成后发给反思域的名字（设计事件边的 to_api）。
RPC_REVIEW = event_emitter_module.RPC_REVIEW

#: 入站窗口长度：交给话题识别的「最近同群消息」条数上限。
#:
#: 一条消息单独交给 ``ranker.detect`` 时，候选话题只能从这一条消息里切短语：措辞稍变就是
#: 另一个短语、另一个 ``topic_id``，于是每条消息都判 changed 并开新会话（会话被切成 1~2 条
#: 消息的碎片）。带上最近窗口后，候选来自整段对话，配合 ``TopicRanker.merge_with_current``
#: 的归并，同一话题的连续消息才会落进同一个会话。
DEFAULT_WINDOW_MESSAGES = 12

#: 窗口的时间跨度（秒）：超过它就认为上一段对话已经结束，窗口重新开始。
#: 取值与 ``boundary.DEFAULT_SILENCE_GAP`` 一致 —— 静默这么久本来就算话题边界。
DEFAULT_WINDOW_SECONDS = 300.0


def _registry_of(target: Any) -> Registry:
    """接受 ``Container`` 或 ``Registry``。"""

    if isinstance(target, Registry):
        return target
    registry = getattr(target, "registry", None)
    if not isinstance(registry, Registry):
        raise SessionError(f"attach/register 需要 Container 或 Registry，得到 {type(target).__name__}")
    return registry


class SessionLayer:
    """会话层的运行实例：16 个叶子实例 + 跨域回调 + 入站订阅。"""

    def __init__(
        self,
        *,
        clock: Any = None,
        heat: HeatManager | None = None,
        archive_trigger: ArchiveTrigger | None = None,
        emitter: SessionEventEmitter | None = None,
        states: SessionStateMachine | None = None,
        cache: EmbeddingCache | None = None,
        embedder: TopicEmbedder | None = None,
        boundary: BoundaryDetector | None = None,
        candidate: TopicCandidateGenerator | None = None,
        ranker: TopicRanker | None = None,
        segmenter: MessageSegmenter | None = None,
        outliner: ThreadOutliner | None = None,
        linker: ThreadLinker | None = None,
        matcher: CrossSessionMatcher | None = None,
        reference_parser: ReferenceParser | None = None,
        buffer: WakeBuffer | None = None,
        restorer: SessionRestorer | None = None,
        container: Any = None,
        window_messages: int = DEFAULT_WINDOW_MESSAGES,
        window_seconds: float = DEFAULT_WINDOW_SECONDS,
    ) -> None:
        self.container = container
        self._provided = {
            "heat": heat,
            "archive_trigger": archive_trigger,
            "emitter": emitter,
            "states": states,
            "cache": cache,
            "embedder": embedder,
            "boundary": boundary,
            "candidate": candidate,
            "ranker": ranker,
            "segmenter": segmenter,
            "outliner": outliner,
            "linker": linker,
            "matcher": matcher,
            "reference_parser": reference_parser,
            "buffer": buffer,
            "restorer": restorer,
        }
        self.clock = clock
        self.window_messages = max(0, int(window_messages))
        self.window_seconds = max(0.0, float(window_seconds))
        self.windows: dict[int, deque[dict[str, Any]]] = {}
        self._wire()
        self.subscription: Any = None
        self.ingested: list[dict[str, Any]] = []

    # ---- 接线 ----------------------------------------------------------
    def _wire(self) -> None:
        p = self._provided
        self.heat = p["heat"] or HeatManager(caller=self.call)
        self.emitter = p["emitter"] or SessionEventEmitter(bus=getattr(self.container, "bus", None), caller=self.call)
        self.archive_trigger = p["archive_trigger"] or ArchiveTrigger(heat=self.heat, caller=self.call)
        self.states = p["states"] or SessionStateMachine(
            heat=self.heat, archive_trigger=self.archive_trigger, emitter=self.emitter, caller=self.call
        )
        self.cache = p["cache"] or EmbeddingCache()
        self.embedder = p["embedder"] or TopicEmbedder(caller=self.call, cache=self.cache, router=self._router())
        self.boundary = p["boundary"] or BoundaryDetector(caller=self.call)
        self.candidate = p["candidate"] or TopicCandidateGenerator(boundary=self.boundary, caller=self.call)
        self.ranker = p["ranker"] or TopicRanker(
            candidate=self.candidate,
            embedder=self.embedder,
            sessions=self.states,
            caller=self.call,
            publisher=self._publish,
        )
        self.candidate.ranker = self.ranker
        self.segmenter = p["segmenter"] or MessageSegmenter(caller=self.call)
        self.outliner = p["outliner"] or ThreadOutliner()
        self.linker = p["linker"] or ThreadLinker(
            sessions=self.states,
            outliner=self.outliner,
            reference_parser=None,
            segmenter=self.segmenter,
            caller=self.call,
        )
        self.matcher = p["matcher"] or CrossSessionMatcher(caller=self.call, restorer=None, embedder=self.embedder)
        self.reference_parser = p["reference_parser"] or ReferenceParser(matcher=self.matcher, caller=self.call)
        self.linker.reference_parser = self.reference_parser
        self.buffer = p["buffer"] or WakeBuffer()
        self.restorer = p["restorer"] or SessionRestorer(buffer=self.buffer, caller=self.call)
        self.matcher.restorer = self.restorer

    def _router(self) -> Any:
        router = getattr(self.container, "router", None)
        return router

    # ---- 跨域回调 ------------------------------------------------------
    async def call(self, name: str, *args: Any, **kwargs: Any) -> Any:
        """按契约名字调用（容器注册表优先；缺名字时抛 ``HandlerNotRegistered``）。"""

        if self.container is None:
            raise SessionError(f"session 层未绑定容器，无法调用 {name}")
        return await self.container.call(name, *args, **kwargs)

    async def _publish(self, topic: str, payload: Any = None) -> Any:
        bus = getattr(self.container, "bus", None)
        if bus is None:
            return None
        return await bus.publish(topic, payload, source=SESSION_SCOPE)

    # ---- 注册 ----------------------------------------------------------
    def register(self, registry: Registry | None = None) -> Registry:
        """把 16 个 ``rpc:`` 处理器注册进注册表。"""

        target = registry if registry is not None else _registry_of(self.container)
        cache_module.register(target, self.cache)
        similarity_module.register(target, self.embedder)
        boundary_module.register(target, self.boundary)
        candidate_module.register(target, self.candidate)
        ranker_module.register(target, self.ranker)
        heat_module.register(target, self.heat)
        archive_trigger_module.register(target, self.archive_trigger)
        event_emitter_module.register(target, self.emitter)
        state_machine_module.register(target, self.states)
        segmenter_module.register(target, self.segmenter)
        outliner_module.register(target, self.outliner)
        linker_module.register(target, self.linker)
        matcher_module.register(target, self.matcher)
        reference_parser_module.register(target, self.reference_parser)
        buffer_module.register(target, self.buffer)
        restorer_module.register(target, self.restorer)
        return target

    def contract_check(self, scope: str = SESSION_SCOPE) -> dict[str, list[str]]:
        """已注册名字与契约比对（``missing`` 只含本域 ``rpc:`` 名字）。"""

        registry = _registry_of(self.container)
        registered = set(registry.names())
        check = contract.check_registry(registered, scope=scope)
        missing = [name for name in check["missing"] if name.startswith("rpc:")]
        unknown = [
            name
            for name in check["unknown"]
            if name.startswith("rpc:") and (contract.owner(name) or "").startswith(scope)
        ]
        return {"missing": missing, "unknown": unknown}

    # ---- 入站窗口 ------------------------------------------------------
    def window_of(self, group_id: int, message: Mapping[str, Any]) -> list[dict[str, Any]]:
        """把消息并入该群的滚动窗口，返回「最近一段对话」（含本条）。

        * 窗口条数上限 ``window_messages``（0 = 关掉窗口，退回「只看本条消息」）；
        * 相邻两条间隔超过 ``window_seconds`` 时窗口重开：静默这么久本来就是话题边界，
          新话题不该被上一段的词面拖住。
        """

        if self.window_messages <= 0:
            return [dict(message)]
        window = self.windows.setdefault(int(group_id or 0), deque(maxlen=self.window_messages))
        stamp = float(message.get("ts") or 0.0)
        if window and stamp and self.window_seconds:
            last = float(window[-1].get("ts") or 0.0)
            if last and abs(stamp - last) > self.window_seconds:
                window.clear()
        window.append(dict(message))
        return list(window)

    async def on_message(self, event: Any) -> dict[str, Any]:
        """群消息到达后的会话层主管线（订阅 ``kafka:grouppig.qq.message.received``）。

        话题识别拿到的是「本条消息 + 最近窗口」，而不是光秃秃的一条消息 —— 这是同话题连续
        消息能落进同一个会话的前提（见 ``DEFAULT_WINDOW_MESSAGES``）。
        """

        payload = _payload_of(event)
        message = _session_message(payload)
        group_id = int(message.get("group_id", 0) or 0)
        now = float(message.get("ts") or 0.0) or None
        window = self.window_of(group_id, message)
        detected = await self.ranker.detect(group_id, window, now=now, open_session=True)
        woven: dict[str, Any] | None = None
        updated: dict[str, Any] | None = None
        with contextlib.suppress(Exception):
            woven = await self.linker.weave(group_id, [message], now=now)
        with contextlib.suppress(Exception):
            updated = await self.states.update(
                (detected.get("session") or {}).get("session_id"),
                group_id=group_id,
                messages=[message],
                now=now,
            )
        record = {
            "group_id": group_id,
            "topic_id": detected.get("topic_id", ""),
            "changed": bool(detected.get("changed")),
            "merged": bool(detected.get("merged")),
            "window": len(window),
            "session_id": str((detected.get("session") or {}).get("session_id", "") or ""),
            "thread_id": str((woven or {}).get("thread_id", "") or ""),
            "archived": bool((updated or {}).get("archived")),
        }
        self.ingested.append(record)
        return {
            "message": message,
            "window": window,
            "topic": detected,
            "thread": woven,
            "session": updated,
            "record": record,
            "count": len(self.ingested),
        }

    def subscribe(self, bus: Any = None, *, topic: str = TOPIC_MESSAGE_RECEIVED) -> Any:
        """把 ``on_message`` 挂到事件总线（gateway 发消息即触发）。"""

        target = bus if bus is not None else getattr(self.container, "bus", None)
        if target is None:
            return None
        self.subscription = target.subscribe(topic, self.on_message, name=f"{SESSION_SCOPE}.ingest")
        return self.subscription

    # ---- 生命周期 ------------------------------------------------------
    async def start(self) -> SessionLayer:
        self.register()
        self.subscribe()
        logger = getattr(self.container, "logger", None)
        if logger is not None:
            logger.info("session.attached", handlers=len(SESSION_RPC), topics=list(SESSION_TOPICS))
        return self

    async def aclose(self) -> None:
        bus = getattr(self.container, "bus", None)
        if self.subscription is not None and bus is not None:
            with contextlib.suppress(Exception):
                bus.unsubscribe(self.subscription)
        self.subscription = None

    # ---- 健康 ----------------------------------------------------------
    def health(self) -> dict[str, Any]:
        return {
            "handlers": len(SESSION_RPC),
            "topics": list(SESSION_TOPICS),
            "contract": self.contract_check() if self.container is not None else {},
            "states": self.states.stats(),
            "cache": self.cache.stats(),
            "buffer": self.buffer.stats(),
            "wake": self.restorer.stats(),
            "emitter": self.emitter.stats(),
            "ingested": len(self.ingested),
            "window": {"messages": self.window_messages, "seconds": self.window_seconds, "groups": len(self.windows)},
            "subscribed": self.subscription is not None,
        }


def _session_message(payload: Any, *, index: int = 0) -> dict[str, Any]:
    """把网关的 OneBot 载荷整理成会话层认的「归一化消息」形状。

    会话层的输入约定是 chat_messages 的列名（content / sender_id / ts ...），而网关照原样
    给的是 OneBot 事件字段（text / user_id / time / sender ...）。不转换的话
    normalize_message 只能读到空的 content —— 话题候选生成器拿不到任何文本，
    rpc:topic.candidate.generate 恒返回 0 个候选，会话永远开不起来（话题/聊天线整条腿失效）。
    """

    if not isinstance(payload, Mapping):
        return messages_module.normalize_message({"content": str(payload)}, index=index)

    sender = payload.get("sender") if isinstance(payload.get("sender"), Mapping) else {}
    mentions: list[int] = []
    reply_to = ""
    for segment in payload.get("segments") or ():
        if not isinstance(segment, Mapping):
            continue
        data = segment.get("data") if isinstance(segment.get("data"), Mapping) else {}
        if segment.get("type") == "at" and str(data.get("qq", "")).lstrip("-").isdigit():
            mentions.append(int(data["qq"]))
        elif segment.get("type") == "reply" and not reply_to:
            reply_to = str(data.get("id") or "")
    user_id = int(payload.get("user_id") or 0)
    self_id = int(payload.get("self_id") or 0)
    stamp = payload.get("time") or payload.get("ts")
    data: dict[str, Any] = {
        "message_id": payload.get("message_id"),
        "group_id": payload.get("group_id") or 0,
        "sender_id": user_id,
        "sender_name": str(sender.get("card") or sender.get("nickname") or ""),
        "role": "self" if self_id and user_id == self_id else "member",
        "msg_type": "text",
        "content": payload.get("text") or payload.get("content") or "",
        "mentions": mentions,
        "reply_to": reply_to,
    }
    if stamp:
        data["ts"] = float(stamp)
    return messages_module.normalize_message(data, index=index)


def _payload_of(event: Any) -> Any:
    """从事件总线信封（Event）或裸载荷里取出真正的消息字典。

    EventBus.publish 统一把载荷包进 Event（topic / payload / ts ...）再交给订阅者；
    只有 Mapping 才是「裸载荷」。早先这里只判断 Mapping，于是总线上的 Event 被当成
    消息本体，group_id 恒为 0、ts 恒为 None —— 所有群的消息都被塞进同一个
    group_id=0 的会话里。这里与 perception.runtime / gateway.router.demux 的取值方式
    保持一致：不是 Mapping 但有 payload 属性，就取 payload。
    """

    if isinstance(event, Mapping):
        return event.get("payload") if "payload" in event else event
    payload = getattr(event, "payload", None)
    return event if payload is None else payload


def register_session_handlers(target: Any, layer: SessionLayer, *, replace: bool = True) -> Registry:
    """把 16 个 ``rpc:`` 处理器注册进容器 / 注册表（逐个校验归属模块）。"""

    registry = _registry_of(target)
    layer.register(registry)
    registered = set(registry.names())
    missing = [name for name in SESSION_RPC if name not in registered]
    if missing:  # pragma: no cover - 静态自查
        raise SessionError(f"session 处理器未覆盖契约名字：{missing}")
    for name in SESSION_RPC:
        registration = registry.get(name)
        if registration.module != contract.owner(name):  # pragma: no cover - 归属校验
            raise SessionError(f"{name} 的归属模块应为 {contract.owner(name)}，实为 {registration.module}")
    return registry


async def attach_session(
    container: Any,
    *,
    subscribe: bool = True,
    layer: SessionLayer | None = None,
    **kwargs: Any,
) -> SessionLayer:
    """把会话层挂到容器：注册 16 个处理器 → 订阅入站主题 → 挂 ``container.session``。"""

    instance = layer if layer is not None else SessionLayer(container=container, **kwargs)
    if instance.container is None:
        instance.container = container
        instance._wire()
    elif container is not instance.container and container is not None:
        # 允许先用 isolated Registry 装配、再挂到真正的容器上（单测常用）
        instance.container = container
    register_session_handlers(container, instance)
    if subscribe:
        instance.subscribe()
    if container is not None:
        container.session = instance
    return instance


def get_session(container: Any) -> SessionLayer:
    """取容器上已挂载的会话层；未挂载抛错。"""

    layer = getattr(container, "session", None)
    if layer is None:
        raise SessionError("容器未挂载会话层：请先 await attach_session(container)")
    return layer


__all__ = [
    "RPC_REVIEW",
    "SESSION_RPC",
    "SESSION_SCOPE",
    "SESSION_TOPICS",
    "TOPIC_MESSAGE_RECEIVED",
    "SessionLayer",
    "attach_session",
    "get_session",
    "register_session_handlers",
]
