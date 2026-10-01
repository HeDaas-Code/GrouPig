"""grouppig.session.threads.weaver.linker —— 引用链接器（``rpc:threads.weave`` / ``rpc:threads.link``）。

职责（对应设计 ``grouppig.session.threads.weaver.linker``「把片段链接进当前会话聊天线，
续接已有线程，并保存到聊天线存储」）：

* ``rpc:threads.weave`` —— 把一批消息编进当前会话的聊天线：
  取当前会话（设计依赖 ``rpc:threads.weave`` → ``rpc:session.current``）→
  按 ``群 + 会话 + 话题`` 算出稳定 thread_id → 续接已有聊天线（``rpc:thread.load``，
  契约内名字、非设计边）→ 合并消息 id / 关键词 / 参与者 / 时间区间 →
  ``rpc:thread.save`` 落库（设计依赖）；
* ``rpc:threads.link`` —— 链接两个片段：生成 / 更新大纲（设计依赖
  ``rpc:threads.link`` → ``rpc:threads.outline``）并检测跨会话引用（设计依赖
  ``rpc:threads.link`` → ``rpc:cross.detect``），返回线边与引用信息。

线边（``chat_thread_edges``）：同一聊天线内 ``reply_to`` 形成 ``reply`` 边；
跨会话引用命中时形成 ``reference`` 边（parent_id = 被引用的旧线）。

设计：``grouppig.session.threads.weaver.linker``（叶子模块）。
"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Mapping, Sequence
from typing import Any

from grouppig.session.runtime.errors import DependencyMissing
from grouppig.session.runtime.messages import (
    extract_keywords,
    keywords_of,
    message_ids_of,
    normalize_messages,
    participants_of,
    texts_of,
    time_span,
)
from grouppig.session.threads.weaver.segmenter import MessageSegmenter, segment_messages

#: normify 模块 id。
MODULE = "grouppig.session.threads.weaver.linker"

#: 本叶子负责的契约名字（逐字取自 api-index.json）。
RPC = ("rpc:threads.weave", "rpc:threads.link")

RPC_WEAVE, RPC_LINK = RPC

#: 依赖名字（设计边）。
RPC_SESSION_CURRENT = "rpc:session.current"
RPC_THREAD_SAVE = "rpc:thread.save"
RPC_THREADS_OUTLINE = "rpc:threads.outline"
RPC_CROSS_DETECT = "rpc:cross.detect"

#: 契约内补充取数名字（续接已有聊天线）。
RPC_THREAD_LOAD = "rpc:thread.load"

#: 单条聊天线保留的消息 id 上限。
MAX_MESSAGE_IDS = 200

#: 聊天线状态。
THREAD_STATUS_OPEN = "open"

#: 线边类型。
EDGE_REPLY = "reply"
EDGE_REFERENCE = "reference"


def thread_id_for(group_id: int | None = None, session_id: str | None = None, topic_id: str | None = None) -> str:
    """按 ``群 + 会话 + 话题`` 算稳定聊天线 id（同一话题续接同一条线）。"""

    seed = f"{int(group_id or 0)}|{session_id or ''}|{topic_id or ''}"
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:12]  # noqa: S324 - 稳定 id，非安全用途
    return f"thread-{digest}"


class ThreadLinker:
    """消息编织进聊天线 + 片段链接。"""

    def __init__(
        self,
        *,
        sessions: Any = None,
        outliner: Any = None,
        reference_parser: Any = None,
        segmenter: MessageSegmenter | None = None,
        caller: Any = None,
        update_session: bool = True,
        clock: Any = time.time,
    ) -> None:
        self.sessions = sessions
        self.outliner = outliner
        self.reference_parser = reference_parser
        self.segmenter = segmenter if segmenter is not None else MessageSegmenter(caller=caller)
        self.caller = caller
        self.update_session = bool(update_session)
        self.clock = clock

    async def weave(
        self,
        group_id: int | None = None,
        messages: Sequence[Mapping[str, Any]] | None = None,
        *,
        session_id: str | None = None,
        topic_id: str | None = None,
        title: str = "",
        thread_id: str | None = None,
        now: float | None = None,
        outline: bool = True,
        detect_cross: bool = True,
        save: bool = True,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """把消息编入当前会话的聊天线。"""

        stamp = float(now if now is not None else self.clock())
        items = normalize_messages(messages)
        session = await self._current_session(group_id) if session_id is None or topic_id is None else None
        resolved_session = str(session_id or (session or {}).get("session_id", "") or "")
        resolved_topic = str(topic_id or (session or {}).get("topic_id", "") or "")
        resolved_group = int(group_id or (session or {}).get("group_id", 0) or 0)
        if not items and not resolved_session:
            raise DependencyMissing("rpc:threads.weave 需要 messages，或能取到当前会话")

        resolved_thread_id = str(thread_id or thread_id_for(resolved_group, resolved_session, resolved_topic))
        existing = await self._load_thread(resolved_thread_id, session_id=resolved_session)
        merged = self._merge(existing, items, resolved_group, resolved_session, resolved_topic, title, stamp)

        segments = segment_messages(items) if items else []
        link_result = await self.link(
            merged,
            segments=segments,
            messages=items,
            outline=outline,
            detect_cross=detect_cross,
            session_id=resolved_session,
            group_id=resolved_group,
            now=stamp,
            **kwargs,
        )
        thread = link_result["thread"]
        edges = link_result["edges"]
        saved: dict[str, Any] | None = None
        if save:
            saved = await self._save_thread(thread, edges=edges)
        if self.update_session and self.sessions is not None and resolved_session:
            try:
                await self.sessions.update(resolved_session, thread_ids=[resolved_thread_id], now=stamp, advance=False)
            except Exception:  # noqa: BLE001 - 会话已归档等情况不应阻断编织
                pass
        return {
            "thread": thread,
            "thread_id": resolved_thread_id,
            "created": existing is None,
            "edges": len(edges),
            "edge_list": edges,
            "saved": saved,
            "outline": link_result.get("outline"),
            "cross": link_result.get("cross"),
            "segments": len(segments),
            "session_id": resolved_session,
            "topic_id": resolved_topic,
            "group_id": resolved_group,
            "message_count": int(thread.get("message_count", 0) or 0),
            "now": stamp,
        }

    async def link(
        self,
        thread: Mapping[str, Any] | None = None,
        *,
        child: Mapping[str, Any] | None = None,
        messages: Sequence[Mapping[str, Any]] | None = None,
        segments: Sequence[Mapping[str, Any]] | None = None,
        edge_type: str = EDGE_REPLY,
        weight: float = 1.0,
        outline: bool = True,
        detect_cross: bool = True,
        session_id: str | None = None,
        group_id: int | None = None,
        now: float | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        """生成大纲、检测跨会话引用，并给出线边。"""

        stamp = float(now if now is not None else self.clock())
        base = dict(thread or {})
        items = normalize_messages(messages)
        edges = self._edges(base, items, segments=segments)

        outline_result: dict[str, Any] | None = None
        if outline and self.outliner is not None:
            outline_result = await self.outliner.outline(
                items or None, segments=segments or None, thread=base, now=stamp
            )
            base["title"] = base.get("title") or str(outline_result.get("title", "") or "")
            base["summary"] = str(outline_result.get("summary", "") or "") or str(base.get("summary", "") or "")
            if outline_result.get("keywords"):
                base["keywords"] = list(outline_result["keywords"])

        cross: dict[str, Any] | None = None
        if detect_cross and self.reference_parser is not None and items:
            cross = await self.reference_parser.detect(items[-1], group_id=group_id, session_id=session_id, now=stamp)
            target = (
                str((cross.get("match") or {}).get("threads", [{}])[0].get("thread_id", "") or "")
                if cross.get("matched")
                else ""
            )
            if target and target != str(base.get("thread_id", "")):
                edges.append(
                    {
                        "thread_id": str(base.get("thread_id", "")),
                        "edge_type": EDGE_REFERENCE,
                        "parent_id": target,
                        "child_id": str(base.get("thread_id", "")),
                        "from_message_id": str(items[-1]["message_id"]),
                        "to_message_id": "",
                        "weight": float((cross.get("match") or {}).get("threads", [{}])[0].get("score", 0.0) or 0.0),
                        "attrs": {"confidence": float(cross.get("confidence", 0.0) or 0.0)},
                    }
                )
                base["cross_thread_id"] = target
        if child is not None:
            edges.append(self._edge(base, child, edge_type=edge_type, weight=weight))
        return {
            "thread": base,
            "edges": edges,
            "outline": outline_result,
            "cross": cross,
            "linked": bool(edges),
            "now": stamp,
        }

    # ---- 内部 ----------------------------------------------------------
    async def _current_session(self, group_id: int | None) -> dict[str, Any] | None:
        if self.sessions is not None:
            result = await self.sessions.current(group_id=group_id)
            session = (result or {}).get("session") if isinstance(result, Mapping) else None
            return dict(session) if isinstance(session, Mapping) else None
        if self.caller is None:
            return None
        try:
            result = await self.caller(RPC_SESSION_CURRENT, group_id)
        except Exception:  # noqa: BLE001 - 取不到会话时仍允许按显式参数编织
            return None
        session = (result or {}).get("session") if isinstance(result, Mapping) else None
        return dict(session) if isinstance(session, Mapping) else None

    async def _load_thread(self, thread_id: str, *, session_id: str | None) -> dict[str, Any] | None:
        if self.caller is None:
            return None
        try:
            result = await self.caller(RPC_THREAD_LOAD, session_id or None, thread_id=thread_id, limit=1)
        except Exception:  # noqa: BLE001 - 首次编织时线还不存在
            return None
        threads = result.get("threads") if isinstance(result, Mapping) else result
        for thread in threads or ():
            if str(thread.get("thread_id", "")) == thread_id:
                return dict(thread)
        return None

    async def _save_thread(
        self, thread: Mapping[str, Any], *, edges: Sequence[Mapping[str, Any]]
    ) -> dict[str, Any] | None:
        if self.caller is None:
            return None
        result = await self.caller(RPC_THREAD_SAVE, dict(thread), edges=[dict(edge) for edge in edges])
        if isinstance(result, Mapping):
            saved = result.get("thread")
            return dict(saved) if isinstance(saved, Mapping) else dict(result)
        return None

    def _merge(
        self,
        existing: Mapping[str, Any] | None,
        items: Sequence[Mapping[str, Any]],
        group_id: int,
        session_id: str,
        topic_id: str,
        title: str,
        stamp: float,
    ) -> dict[str, Any]:
        base = dict(existing or {})
        new_ids = message_ids_of(items)
        old_ids = [str(mid) for mid in (base.get("message_ids") or ())]
        message_ids = [*old_ids, *[mid for mid in new_ids if mid not in old_ids]][-MAX_MESSAGE_IDS:]
        span = time_span(items)
        keywords = extract_keywords([*[str(k) for k in (base.get("keywords") or ())], *texts_of(items)], top=10)
        participants = [int(p) for p in (base.get("participants") or ())]
        for sender in participants_of(items):
            if sender not in participants:
                participants.append(sender)
        first_ts = float(base.get("first_ts") or 0.0)
        if span["first_ts"]:
            first_ts = min(first_ts, span["first_ts"]) if first_ts else span["first_ts"]
        last_ts = max(float(base.get("last_ts") or 0.0), span["last_ts"] or stamp)
        return {
            **base,
            "thread_id": str(base.get("thread_id") or thread_id_for(group_id, session_id, topic_id)),
            "group_id": int(base.get("group_id") or group_id),
            "session_id": str(base.get("session_id") or session_id),
            "topic_id": str(base.get("topic_id") or topic_id),
            "title": str(title or base.get("title") or ""),
            "summary": str(base.get("summary") or ""),
            "keywords": keywords or keywords_of(items),
            "participants": participants,
            "message_ids": message_ids,
            "message_count": int(base.get("message_count") or 0) + len(new_ids),
            "first_ts": first_ts,
            "last_ts": last_ts,
            "status": str(base.get("status") or THREAD_STATUS_OPEN),
        }

    def _edges(
        self,
        thread: Mapping[str, Any],
        items: Sequence[Mapping[str, Any]],
        *,
        segments: Sequence[Mapping[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        by_id = {str(item["message_id"]): item for item in items}
        edges: list[dict[str, Any]] = []
        for item in items:
            reply_to = str(item.get("reply_to", "") or "")
            if not reply_to:
                continue
            edges.append(
                {
                    "thread_id": str(thread.get("thread_id", "")),
                    "edge_type": EDGE_REPLY,
                    "parent_id": "",
                    "child_id": "",
                    "from_message_id": str(item["message_id"]),
                    "to_message_id": reply_to,
                    "weight": 1.0,
                    "attrs": {"target_sender_id": int((by_id.get(reply_to) or {}).get("sender_id", 0) or 0)},
                }
            )
        for segment in segments or ():
            if segment.get("target_id"):
                edges.append(
                    {
                        "thread_id": str(thread.get("thread_id", "")),
                        "edge_type": EDGE_REPLY,
                        "parent_id": "",
                        "child_id": "",
                        "from_message_id": str((segment.get("message_ids") or [""])[0]),
                        "to_message_id": "",
                        "weight": 1.0,
                        "attrs": {
                            "target_sender_id": int(segment["target_id"]),
                            "segment_id": str(segment.get("segment_id", "")),
                        },
                    }
                )
        return edges

    def _edge(
        self, parent: Mapping[str, Any], child: Mapping[str, Any], *, edge_type: str, weight: float
    ) -> dict[str, Any]:
        return {
            "thread_id": str(parent.get("thread_id", "")),
            "edge_type": str(edge_type),
            "parent_id": str(parent.get("thread_id", "")),
            "child_id": str(child.get("thread_id", "")),
            "from_message_id": "",
            "to_message_id": "",
            "weight": float(weight),
            "attrs": {},
        }


# --------------------------------------------------------------------------
# 处理器注册
# --------------------------------------------------------------------------
def register(registry: Any, linker: ThreadLinker | None = None, *, replace: bool = True) -> Any:
    """把本叶子的 2 个 ``rpc:`` 处理器注册进注册表。"""

    instance = linker if linker is not None else ThreadLinker()

    async def threads_weave(group_id: int | None = None, messages: Any = None, **kwargs: Any) -> dict[str, Any]:
        return await instance.weave(group_id, messages, **kwargs)

    async def threads_link(thread: Any = None, **kwargs: Any) -> dict[str, Any]:
        return await instance.link(thread, **kwargs)

    registry.register(RPC_WEAVE, threads_weave, module=MODULE, replace=replace)
    registry.register(RPC_LINK, threads_link, module=MODULE, replace=replace)
    return instance


__all__ = [
    "EDGE_REFERENCE",
    "EDGE_REPLY",
    "MAX_MESSAGE_IDS",
    "MODULE",
    "RPC",
    "RPC_CROSS_DETECT",
    "RPC_LINK",
    "RPC_SESSION_CURRENT",
    "RPC_THREAD_LOAD",
    "RPC_THREAD_SAVE",
    "RPC_THREADS_OUTLINE",
    "RPC_WEAVE",
    "THREAD_STATUS_OPEN",
    "ThreadLinker",
    "register",
    "thread_id_for",
]
