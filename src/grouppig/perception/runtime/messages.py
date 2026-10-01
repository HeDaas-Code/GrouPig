"""grouppig.perception.runtime.messages —— QQEvent → 感知内部消息。

网关与感知层之间的入站契约是
:class:`grouppig.gateway.adapter.event_codec.QQEvent` 的 ``as_dict()`` 载荷
（``kafka:grouppig.qq.message.received`` 的 payload，也是 ``rpc:observer.ingest`` 的入参）。
本模块把它归一化为感知层内部消息（字段与 ``chat_messages`` 表对齐，可直接喂给
``rpc:chat.append``），并提供窗口统计（发言分布 / 复读 / 静默时长）。

内部消息字段：``message_id / group_id / sender_id / sender_name / role / msg_type /
content / raw_content / segments / mentions / at_self / reply_to / ts / event_id / kind /
self_id / known / raw``。

normify id: ``grouppig.perception.runtime.messages``（运行时补充模块，设计树暂无）。
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from grouppig.perception.runtime import text as text_utils
from grouppig.perception.runtime.errors import PayloadError

#: 内部消息字段（顺序固定，便于断言与落库）。
MESSAGE_FIELDS: tuple[str, ...] = (
    "message_id",
    "group_id",
    "sender_id",
    "sender_name",
    "role",
    "msg_type",
    "content",
    "raw_content",
    "segments",
    "mentions",
    "at_self",
    "reply_to",
    "ts",
    "event_id",
    "kind",
    "self_id",
    "known",
)

#: 消息类型（与 memory.chat-store.schema 的 MESSAGE_TYPES 对齐）。
MSG_TYPES: tuple[str, ...] = ("text", "image", "face", "record", "video", "forward", "mixed", "other")

#: 群消息事件类型。
KIND_GROUP_MESSAGE = "message.group"

_TEXT_SEGMENTS = ("text",)


def new_message_id() -> str:
    """上游没给 ``message_id`` 时本地生成（与 memory 侧前缀一致）。"""

    return f"msg-{uuid.uuid4().hex[:16]}"


def _int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def segments_of(payload: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    """取出消息段（缺省从 ``raw_message`` 的 CQ 码粗解）。"""

    raw = payload.get("segments") or payload.get("message") or ()
    if isinstance(raw, str):
        from grouppig.gateway.adapter.event_codec import parse_cq_string

        return tuple(dict(seg) for seg in parse_cq_string(raw))
    out: list[dict[str, Any]] = []
    for seg in raw or ():
        if isinstance(seg, Mapping):
            out.append({"type": str(seg.get("type", "text")), "data": dict(seg.get("data") or {})})
        elif isinstance(seg, str):
            out.append({"type": "text", "data": {"text": seg}})
    return tuple(out)


def msg_type_of(segments: Sequence[Mapping[str, Any]], *, fallback: str = "text") -> str:
    """消息类型：纯文本 / 单类富媒体 / 混合。"""

    kinds = [str(seg.get("type", "text")) for seg in segments]
    rich = [kind for kind in kinds if kind not in _TEXT_SEGMENTS]
    if not rich:
        return fallback if fallback in MSG_TYPES else "text"
    unique = {kind for kind in rich}
    if len(unique) == 1:
        kind = unique.pop()
        return kind if kind in MSG_TYPES else "other"
    return "mixed"


def mentions_of(payload: Mapping[str, Any]) -> tuple[int, ...]:
    """被 @ 的 QQ 号（含消息段与 ``mentions`` 字段两种来源）。"""

    found: list[int] = []
    for value in payload.get("mentions") or ():
        qq = _int(value, -1)
        if qq > 0:
            found.append(qq)
    for seg in segments_of(payload):
        if seg.get("type") == "at":
            qq = _int(seg.get("data", {}).get("qq"), -1)
            if qq > 0:
                found.append(qq)
    return tuple(dict.fromkeys(found))


def is_group_message(payload: Mapping[str, Any]) -> bool:
    """是否是群消息（感知层的入站范围；私聊/通知/元事件不入观察窗）。"""

    if not isinstance(payload, Mapping):
        return False
    post_type = str(payload.get("post_type") or "")
    if post_type:
        return post_type == "message" and str(payload.get("message_type") or "") == "group"
    # 已是内部消息：有 group_id 即视为群消息
    return _int(payload.get("group_id"), 0) > 0


def from_event(payload: Mapping[str, Any], *, now: float | None = None) -> dict[str, Any]:
    """把入站载荷归一化为感知内部消息。

    兼容三种输入：QQEvent 的 ``as_dict()``、``{"event": {...}}`` 包装、以及已归一化的内部消息。
    """

    if not isinstance(payload, Mapping):
        raise PayloadError(f"入站载荷应为映射，得到 {type(payload).__name__}")
    event: Mapping[str, Any] = payload
    if "post_type" not in event and isinstance(event.get("event"), Mapping):
        event = event["event"]  # type: ignore[assignment]
    if not isinstance(event, Mapping):  # pragma: no cover - 上面的分支已保证
        raise PayloadError("入站载荷缺少事件体")

    segments = segments_of(event)
    content = str(event.get("text") or event.get("content") or "")
    if not content:
        content = _text_of_segments(segments, include_at=False)
    stamp = _float(event.get("ts") or event.get("time"), 0.0) or float(now if now is not None else time.time())
    sender = dict(event.get("sender") or {})
    msg_type = str(event.get("msg_type") or "") or msg_type_of(segments)
    return {
        "message_id": str(event.get("message_id") or new_message_id()),
        "group_id": _int(event.get("group_id"), 0),
        "sender_id": _int(event.get("sender_id") or event.get("user_id"), 0),
        "sender_name": str(sender.get("card") or sender.get("nickname") or event.get("sender_name") or ""),
        "role": str(sender.get("role") or event.get("role") or "member"),
        "msg_type": msg_type if msg_type in MSG_TYPES else "other",
        "content": content,
        "raw_content": str(event.get("raw_message") or content),
        "segments": [dict(seg) for seg in segments],
        "mentions": list(mentions_of(event)),
        "at_self": bool(event.get("at_self", False)),
        "reply_to": str(event.get("reply_to") or ""),
        "ts": stamp,
        "event_id": str(event.get("event_id") or ""),
        "kind": str(event.get("kind") or KIND_GROUP_MESSAGE),
        "self_id": _int(event.get("self_id"), 0),
        "known": bool(event.get("known", True)),
    }


def _text_of_segments(segments: Sequence[Mapping[str, Any]], *, include_at: bool = False) -> str:
    parts: list[str] = []
    for seg in segments:
        kind = str(seg.get("type", "text"))
        data = seg.get("data") or {}
        if kind == "text":
            parts.append(str(data.get("text") or ""))
        elif kind == "at" and include_at:
            parts.append(f"@{data.get('qq', '')}")
        elif kind in ("face", "image", "record", "video", "forward", "file"):
            parts.append(
                f"[{ {'face': '表情', 'image': '图片', 'record': '语音', 'video': '视频', 'forward': '合并转发', 'file': '文件'}.get(kind, kind) }]"
            )
    return "".join(parts)


def is_bot_message(message: Mapping[str, Any]) -> bool:
    """是否机器人自己 / 其他机器人发的消息（清洗时可选丢弃）。"""

    role = str(message.get("role") or "").lower()
    self_id = _int(message.get("self_id"), 0)
    sender_id = _int(message.get("sender_id"), 0)
    if role in ("bot", "robot"):
        return True
    return bool(self_id) and self_id == sender_id


def ts_of(message: Mapping[str, Any]) -> float:
    return _float(message.get("ts"), 0.0)


def text_of(message: Mapping[str, Any]) -> str:
    """消息的当前文本（清洗后用 ``content``，否则回落到 ``raw_content``）。"""

    return str(message.get("content") or message.get("raw_content") or "")


def slice_by_time(
    messages: Iterable[Mapping[str, Any]],
    *,
    since: float | None = None,
    until: float | None = None,
) -> list[dict[str, Any]]:
    """按时间区间切片（``since`` 含、``until`` 含），保持原顺序。"""

    out: list[dict[str, Any]] = []
    for message in messages:
        stamp = ts_of(message)
        if since is not None and stamp < since:
            continue
        if until is not None and stamp > until:
            continue
        out.append(dict(message))
    return out


def summarize(
    messages: Sequence[Mapping[str, Any]],
    *,
    now: float | None = None,
    top: int = 3,
) -> dict[str, Any]:
    """窗口统计：条数、发言人数、分布、复读、静默时长、集中度。"""

    items = [dict(message) for message in messages]
    stamps = [ts_of(message) for message in items]
    contents = [text_of(message) for message in items]
    senders = [str(message.get("sender_id", "")) for message in items]
    stamp = float(now if now is not None else (max(stamps) if stamps else time.time()))
    last_ts = max(stamps) if stamps else 0.0
    sender_counts = text_utils.counter_of(senders)
    content_counts = text_utils.counter_of([text_utils.content_fingerprint(value) for value in contents])
    samples = {text_utils.content_fingerprint(value): value for value in contents}
    top_fingerprint = next(iter(content_counts), "")
    return {
        "message_count": len(items),
        "sender_count": len(sender_counts),
        "senders": sorted(sender_counts),
        "sender_counts": sender_counts,
        "avg_length": text_utils.mean([len(value) for value in contents]),
        "top_sender": next(iter(sender_counts), ""),
        "top_sender_share": (next(iter(sender_counts.values())) / len(items)) if items and sender_counts else 0.0,
        "first_ts": min(stamps) if stamps else 0.0,
        "last_ts": last_ts,
        "span_seconds": (max(stamps) - min(stamps)) if stamps else 0.0,
        "silence_seconds": max(0.0, stamp - last_ts) if stamps else 0.0,
        "repeat_max": next(iter(content_counts.values()), 0),
        "unique_contents": len(content_counts),
        "top_content": samples.get(top_fingerprint, ""),
        "concentration": text_utils.concentration(contents, top=top),
        "keywords": [list(item) for item in text_utils.keywords(contents, top=8)],
        "at_self_count": sum(1 for message in items if message.get("at_self")),
        "mention_count": sum(len(message.get("mentions") or ()) for message in items),
    }


def half_split(
    messages: Sequence[Mapping[str, Any]],
    *,
    since: float,
    until: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """按时间中点把窗口切成前/后半段（流速加速度用）。"""

    middle = (float(since) + float(until)) / 2.0
    first = slice_by_time(messages, since=since, until=middle)
    second = slice_by_time(messages, since=middle, until=until)
    return first, second


__all__ = [
    "KIND_GROUP_MESSAGE",
    "MESSAGE_FIELDS",
    "MSG_TYPES",
    "from_event",
    "half_split",
    "is_bot_message",
    "is_group_message",
    "mentions_of",
    "msg_type_of",
    "new_message_id",
    "segments_of",
    "slice_by_time",
    "summarize",
    "text_of",
    "ts_of",
]
