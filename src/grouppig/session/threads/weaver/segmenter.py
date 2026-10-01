"""grouppig.session.threads.weaver.segmenter —— 消息分段器（``rpc:threads.segment``）。

职责（对应设计 ``grouppig.session.threads.weaver.segmenter``「把消息流切分为可编织的片段：
说者、对象、内容、引用」）：

* ``rpc:threads.segment`` —— 把消息流切成「片段」：每个片段是一个说者在一次连续发言里
  说出的一组消息（含被 @ 的对象、回复目标、引用标记、立场与时间区间）。

切分规则（确定性）：

1. 说者变化 → 新片段；
2. 相邻消息间隔 ≥ ``gap_seconds``（默认 180 秒）→ 新片段；
3. 回复对象变化（回复了另一个人）→ 新片段；
4. 片段消息数达到 ``max_segment``（默认 20）→ 强制切分。

引用标记来自：``reply_to``（回复某条消息）、``mentions``（@ 某人）与文本里的
指代/回溯表达（``上次``、``刚才`` 等，与 ``reference-parser`` 的标记保持一致）。

设计：``grouppig.session.threads.weaver.segmenter``（叶子模块）。
"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Mapping, Sequence
from typing import Any

from grouppig.session.runtime.errors import DependencyMissing
from grouppig.session.runtime.messages import (
    is_question,
    normalize_messages,
    snippet,
    stance_of,
    time_span,
)

#: normify 模块 id。
MODULE = "grouppig.session.threads.weaver.segmenter"

#: 本叶子负责的契约名字（逐字取自 api-index.json）。
RPC = ("rpc:threads.segment",)

RPC_SEGMENT = RPC[0]

#: 契约内补充取数名字（本叶子在设计上没有出边，取数仅在 caller 可用时使用）。
RPC_CHAT_QUERY = "rpc:chat.query"

#: 默认切分阈值。
DEFAULT_GAP_SECONDS = 180.0
DEFAULT_MAX_SEGMENT = 20

#: 回溯 / 指代表达（与 reference-parser 共用一套标记）。
ANAPHORA_MARKERS = (
    "上次",
    "之前",
    "刚才",
    "前面",
    "那个",
    "还记得",
    "提到过",
    "刚说的",
    "继续说",
    "接着",
    "又来了",
)


def text_references(text: str) -> list[dict[str, str]]:
    """文本里的回溯 / 指代标记。"""

    value = str(text or "")
    return [{"type": "anaphora", "value": marker} for marker in ANAPHORA_MARKERS if marker in value]


def segment_message(
    message: Mapping[str, Any] | str, *, index: int = 0, by_id: Mapping[str, Mapping[str, Any]] | None = None
) -> dict[str, Any]:
    """把一条消息变成一个「片段原子」（说者 / 对象 / 内容 / 引用）。"""

    item = normalize_messages([message], sort=False)[0]
    content = str(item.get("content", "") or "")
    reply_to = str(item.get("reply_to", "") or "")
    target_message = (by_id or {}).get(reply_to)
    references: list[dict[str, Any]] = []
    if reply_to:
        references.append(
            {
                "type": "reply",
                "value": reply_to,
                "target_sender_id": int((target_message or {}).get("sender_id", 0) or 0),
            }
        )
    for mention in item.get("mentions") or ():
        references.append({"type": "mention", "value": str(mention)})
    references.extend(text_references(content))
    target_id = int((target_message or {}).get("sender_id", 0) or 0)
    if not target_id and item.get("mentions"):
        target_id = int(item["mentions"][0])
    return {
        "message_id": str(item.get("message_id", "") or f"msg-{index}"),
        "index": index,
        "speaker_id": int(item.get("sender_id", 0) or 0),
        "speaker_name": str(item.get("sender_name", "") or ""),
        "target_id": target_id,
        "target_message_id": reply_to,
        "content": content,
        "snippet": snippet(content),
        "references": references,
        "kind": "reply" if reply_to else ("question" if is_question(content) else "statement"),
        "stance": stance_of(content),
        "ts": float(item.get("ts") or 0.0),
        "tokens": int(item.get("tokens") or len(content)),
        "is_self": str(item.get("role", "")) == "self",
    }


def segment_messages(
    messages: Sequence[Mapping[str, Any]] | None,
    *,
    gap_seconds: float = DEFAULT_GAP_SECONDS,
    max_segment: int = DEFAULT_MAX_SEGMENT,
) -> list[dict[str, Any]]:
    """把消息流切成片段（纯函数）。"""

    items = normalize_messages(messages)
    by_id = {str(item["message_id"]): item for item in items}
    atoms = [segment_message(item, index=index, by_id=by_id) for index, item in enumerate(items)]
    segments: list[dict[str, Any]] = []
    current: list[dict[str, Any]] = []
    for atom in atoms:
        if current:
            previous = current[-1]
            gap = float(atom["ts"]) - float(previous["ts"])
            new_speaker = atom["speaker_id"] != previous["speaker_id"]
            long_gap = gap >= float(gap_seconds)
            new_target = (
                bool(atom["target_id"]) and bool(previous["target_id"]) and atom["target_id"] != previous["target_id"]
            )
            if new_speaker or long_gap or new_target or len(current) >= int(max_segment):
                segments.append(_build_segment(current, len(segments)))
                current = []
        current.append(atom)
    if current:
        segments.append(_build_segment(current, len(segments)))
    return segments


def _build_segment(atoms: Sequence[Mapping[str, Any]], index: int) -> dict[str, Any]:
    message_ids = [str(atom["message_id"]) for atom in atoms]
    digest = hashlib.sha1(",".join(message_ids).encode("utf-8")).hexdigest()[:12]  # noqa: S324
    contents = [str(atom["content"]) for atom in atoms if atom["content"]]
    references = [dict(ref) for atom in atoms for ref in atom["references"]]
    span = time_span([{"ts": atom["ts"]} for atom in atoms])
    kinds = {str(atom["kind"]) for atom in atoms}
    return {
        "segment_id": f"seg-{digest}",
        "index": index,
        "speaker_id": int(atoms[0]["speaker_id"]),
        "speaker_name": str(atoms[0]["speaker_name"]),
        "target_id": next((int(atom["target_id"]) for atom in atoms if atom["target_id"]), 0),
        "message_ids": message_ids,
        "message_count": len(atoms),
        "content": " ".join(contents),
        "snippet": snippet(" ".join(contents)),
        "kind": "reply" if "reply" in kinds else ("question" if "question" in kinds else "statement"),
        "stance": str(atoms[-1]["stance"]),
        "references": references,
        "first_ts": span["first_ts"],
        "last_ts": span["last_ts"],
        "tokens": sum(int(atom["tokens"]) for atom in atoms),
    }


class MessageSegmenter:
    """消息流分段。"""

    def __init__(
        self,
        *,
        caller: Any = None,
        gap_seconds: float = DEFAULT_GAP_SECONDS,
        max_segment: int = DEFAULT_MAX_SEGMENT,
        clock: Any = time.time,
    ) -> None:
        self.caller = caller
        self.gap_seconds = float(gap_seconds)
        self.max_segment = int(max_segment)
        self.clock = clock

    async def segment(
        self,
        messages: Sequence[Mapping[str, Any]] | None = None,
        *,
        group_id: int | None = None,
        session_id: str | None = None,
        since: float | None = None,
        until: float | None = None,
        limit: int = 200,
        now: float | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        """切分消息流；未给 ``messages`` 时（且注入了 caller）按条件取数。"""

        stamp = float(now if now is not None else self.clock())
        source = "messages"
        items = list(messages or ())
        if messages is None:
            items = await self.fetch(group_id=group_id, session_id=session_id, since=since, until=until, limit=limit)
            source = "query"
        segments = segment_messages(items, gap_seconds=self.gap_seconds, max_segment=self.max_segment)
        return {
            "segments": segments,
            "count": len(segments),
            "message_count": len(items),
            "speakers": sorted({int(segment["speaker_id"]) for segment in segments}),
            "group_id": int(group_id or 0),
            "session_id": str(session_id or ""),
            "source": source,
            "now": stamp,
            "gap_seconds": self.gap_seconds,
            "max_segment": self.max_segment,
        }

    async def fetch(
        self,
        *,
        group_id: int | None = None,
        session_id: str | None = None,
        since: float | None = None,
        until: float | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        if self.caller is None:
            raise DependencyMissing(f"rpc:threads.segment 未传 messages 时需要 caller 才能调用 {RPC_CHAT_QUERY}")
        params = {
            "group_id": group_id,
            "session_id": session_id,
            "since": since,
            "until": until,
            "limit": int(limit),
            "order": "asc",
        }
        result = await self.caller(RPC_CHAT_QUERY, {key: value for key, value in params.items() if value is not None})
        if isinstance(result, Mapping):
            return [dict(item) for item in (result.get("messages") or ())]
        return [dict(item) for item in (result or ())]


# --------------------------------------------------------------------------
# 处理器注册
# --------------------------------------------------------------------------
def register(registry: Any, segmenter: MessageSegmenter | None = None, *, replace: bool = True) -> Any:
    """把本叶子的 ``rpc:`` 处理器注册进注册表。"""

    instance = segmenter if segmenter is not None else MessageSegmenter()

    async def threads_segment(
        messages: Sequence[Mapping[str, Any]] | None = None,
        *,
        group_id: int | None = None,
        session_id: str | None = None,
        since: float | None = None,
        until: float | None = None,
        limit: int = 200,
        now: float | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        return await instance.segment(
            messages,
            group_id=group_id,
            session_id=session_id,
            since=since,
            until=until,
            limit=limit,
            now=now,
            **kwargs,
        )

    registry.register(RPC_SEGMENT, threads_segment, module=MODULE, replace=replace)
    return instance


__all__ = [
    "ANAPHORA_MARKERS",
    "DEFAULT_GAP_SECONDS",
    "DEFAULT_MAX_SEGMENT",
    "MODULE",
    "RPC",
    "RPC_CHAT_QUERY",
    "RPC_SEGMENT",
    "MessageSegmenter",
    "register",
    "segment_message",
    "segment_messages",
    "text_references",
]
