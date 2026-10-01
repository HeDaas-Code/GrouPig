"""grouppig.gateway.adapter.event-codec —— OneBot v11 事件编解码器。

职责（对应设计 ``grouppig.gateway.adapter.event-codec``）：

* ``rpc:codec.decode``：把 OneBot 事件 JSON（``post_type=message|notice|request|meta_event``）
  解码为内部事件对象 :class:`QQEvent`；``message`` 字段支持**数组段**与 **CQ 字符串**两种格式。
* ``rpc:codec.encode``：把回复内容编码为 OneBot **消息段数组**（引用 / @ / 文本 / 表情），
  供 ``rpc:onebot.send`` 使用。

内部事件对象是网关与感知层之间的**唯一入站契约**，字段固定（见 :meth:`QQEvent.as_dict`）：

``event_id / kind / post_type / message_type / sub_type / notice_type / request_type /
meta_event_type / time / self_id / group_id / user_id / operator_id / message_id /
raw_message / text / segments / sender / at_self / known``

normify id: ``grouppig.gateway.adapter.event-codec``（叶子模块）。
"""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from grouppig.infra.runtime.errors import GrouPigError

POST_TYPES = ("message", "notice", "request", "meta_event")
MESSAGE_TYPES = ("group", "private")
SEGMENT_TYPES = (
    "text",
    "at",
    "face",
    "image",
    "record",
    "video",
    "reply",
    "forward",
    "json",
    "markdown",
)

_CQ_RE = re.compile(r"\[CQ:([A-Za-z0-9_.\-]+)((?:,[^\[\]]*)?)\]")
_CQ_UNESCAPE = (
    ("&amp;", "&"),
    ("&#91;", "["),
    (
        "&#93;",
        "]",
    ),
    ("&comma;", ","),
    ("&lt;", "<"),
    ("&gt;", ">"),
)
_CQ_ESCAPE = (
    ("&", "&amp;"),
    (",", "&comma;"),
    ("[", "&#91;"),
    ("]", "&#93;"),
)


class CodecError(GrouPigError):
    """事件 JSON / 消息段无法解码或编码。"""


# --------------------------------------------------------------------------
# CQ 码 <-> 消息段
# --------------------------------------------------------------------------
def _unescape(value: str) -> str:
    for src, dst in _CQ_UNESCAPE:
        value = value.replace(src, dst)
    return value


def _escape(value: str) -> str:
    for src, dst in _CQ_ESCAPE:
        value = value.replace(src, dst)
    return value


def text_segment(text: str) -> dict[str, Any]:
    return {"type": "text", "data": {"text": text}}


def at_segment(user_id: int | str, *, name: str | None = None) -> dict[str, Any]:
    data: dict[str, Any] = {"qq": str(user_id)}
    if name is not None:
        data["name"] = name
    return {"type": "at", "data": data}


def reply_segment(message_id: int | str) -> dict[str, Any]:
    return {"type": "reply", "data": {"id": str(message_id)}}


def face_segment(face_id: int | str) -> dict[str, Any]:
    return {"type": "face", "data": {"id": str(face_id)}}


def image_segment(file: str, *, url: str | None = None) -> dict[str, Any]:
    data: dict[str, Any] = {"file": file}
    if url is not None:
        data["url"] = url
    return {"type": "image", "data": data}


def normalize_segment(segment: Mapping[str, Any] | str) -> dict[str, Any]:
    """把一段宽松输入规范成 ``{"type": ..., "data": {...}}``。"""

    if isinstance(segment, str):
        return text_segment(segment)
    if not isinstance(segment, Mapping):
        raise CodecError(f"消息段应为映射或字符串，得到 {type(segment).__name__}")
    seg_type = str(segment.get("type", "")).strip()
    if not seg_type:
        raise CodecError(f"消息段缺少 type：{segment!r}")
    data = segment.get("data", {})
    if isinstance(data, str):  # OneBot 里也允许 data 直接是字符串（如 image file）
        data = {"file": data}
    if not isinstance(data, Mapping):
        raise CodecError(f"消息段 {seg_type} 的 data 应为映射，得到 {type(data).__name__}")
    return {"type": seg_type, "data": {str(k): v for k, v in data.items()}}


def parse_cq_string(raw: str) -> tuple[dict[str, Any], ...]:
    """CQ 字符串 → 消息段数组（``[CQ:at,qq=1]你好`` → ``[at, text]``）。"""

    segments: list[dict[str, Any]] = []
    cursor = 0
    for match in _CQ_RE.finditer(raw):
        if match.start() > cursor:
            segments.append(text_segment(_unescape(raw[cursor : match.start()])))
        seg_type = match.group(1)
        data: dict[str, Any] = {}
        for pair in match.group(2).lstrip(",").split(","):
            if not pair:
                continue
            key, _, value = pair.partition("=")
            if key:
                data[key.strip()] = _unescape(value)
        segments.append({"type": seg_type, "data": data})
        cursor = match.end()
    if cursor < len(raw):
        segments.append(text_segment(_unescape(raw[cursor:])))
    return tuple(segments)


def to_cq_string(segments: Iterable[Mapping[str, Any] | str]) -> str:
    """消息段数组 → CQ 字符串（文本段直接内联，其余段转 ``[CQ:...]``）。"""

    parts: list[str] = []
    for segment in segments:
        seg = normalize_segment(segment)
        if seg["type"] == "text":
            parts.append(_escape(str(seg["data"].get("text", ""))))
            continue
        params = ",".join(f"{k}={_escape(str(v))}" for k, v in seg["data"].items())
        parts.append(f"[CQ:{seg['type']}{',' + params if params else ''}]")
    return "".join(parts)


def decode_message(message: Any) -> tuple[dict[str, Any], ...]:
    """``message`` 字段（数组 / CQ 字符串 / 纯字符串）→ 规范化消息段数组。"""

    if message is None:
        return ()
    if isinstance(message, str):
        if not message:
            return ()
        return parse_cq_string(message) if "[CQ:" in message else (text_segment(message),)
    if isinstance(message, Sequence) and not isinstance(message, (bytes, bytearray)):
        return tuple(normalize_segment(seg) for seg in message)
    if isinstance(message, Mapping):
        return (normalize_segment(message),)
    raise CodecError(f"无法解码 message 字段：{type(message).__name__}")


def text_of(segments: Iterable[Mapping[str, Any]], *, include_at: bool = False) -> str:
    """取消息段里的纯文本（可选把 ``at`` 渲染成 ``@qq``）。"""

    parts: list[str] = []
    for segment in segments:
        seg = normalize_segment(segment)
        if seg["type"] == "text":
            parts.append(str(seg["data"].get("text", "")))
        elif seg["type"] == "at" and include_at:
            qq = str(seg["data"].get("qq", ""))
            parts.append("@全体成员" if qq == "all" else f"@{qq}")
        elif seg["type"] == "face":
            parts.append(f"[表情{seg['data'].get('id', '')}]")
    return "".join(parts)


# --------------------------------------------------------------------------
# 内部事件对象
# --------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class QQEvent:
    """一条 OneBot 事件的内部表示。"""

    post_type: str
    time: int = 0
    self_id: int = 0
    message_type: str = ""
    sub_type: str = ""
    notice_type: str = ""
    request_type: str = ""
    meta_event_type: str = ""
    group_id: int | None = None
    user_id: int | None = None
    operator_id: int | None = None
    target_id: int | None = None
    message_id: int | None = None
    raw_message: str = ""
    text: str = ""
    command_text: str = ""
    segments: tuple[dict[str, Any], ...] = ()
    sender: dict[str, Any] = field(default_factory=dict)
    at_self: bool = False
    known: bool = True
    event_id: str = ""
    raw: dict[str, Any] = field(default_factory=dict)

    # ---- 派生 ----------------------------------------------------------
    @property
    def kind(self) -> str:
        """路由键：``message.group`` / ``notice.group_recall`` / ``meta_event.heartbeat`` …"""

        if self.post_type == "message":
            return f"message.{self.message_type or 'unknown'}"
        if self.post_type == "notice":
            return f"notice.{self.notice_type or 'unknown'}"
        if self.post_type == "request":
            return f"request.{self.request_type or 'unknown'}"
        if self.post_type == "meta_event":
            return f"meta_event.{self.meta_event_type or 'unknown'}"
        return f"unknown.{self.post_type or 'empty'}"

    @property
    def is_group_message(self) -> bool:
        return self.post_type == "message" and self.message_type == "group"

    @property
    def is_private_message(self) -> bool:
        return self.post_type == "message" and self.message_type == "private"

    @property
    def is_recall(self) -> bool:
        return self.post_type == "notice" and self.notice_type in ("group_recall", "friend_recall")

    def mentions(self, user_id: int | str) -> bool:
        """是否 @ 了指定用户。

        ``@全体成员`` **不算**。它在 OneBot 里同样是 ``at`` 段，只是 ``qq == "all"``；
        此前被一并当成「叫了机器人」，于是每条管理员公告都会：
        * 被提升到 ``PRIORITY_MENTION`` 最高优先级进队（挤掉真实群聊消息）；
        * 拿满插话分的 mention 分量，把机器人变成公告的应声虫。

        需要区分全体成员时用 :attr:`at_all`。
        """

        target = str(user_id)
        return any(seg["type"] == "at" and str(seg["data"].get("qq", "")) == target for seg in self.segments)

    @property
    def at_all(self) -> bool:
        """是否 ``@全体成员``（与被点名区分开，供需要时使用）。"""

        return any(seg["type"] == "at" and str(seg["data"].get("qq", "")) == "all" for seg in self.segments)

    # ---- 序列化 --------------------------------------------------------
    def as_dict(self) -> dict[str, Any]:
        """总线载荷（``kafka:grouppig.qq.message.received`` 的 payload）。"""

        return {
            "event_id": self.event_id,
            "kind": self.kind,
            "post_type": self.post_type,
            "message_type": self.message_type,
            "sub_type": self.sub_type,
            "notice_type": self.notice_type,
            "request_type": self.request_type,
            "meta_event_type": self.meta_event_type,
            "time": self.time,
            "self_id": self.self_id,
            "group_id": self.group_id,
            "user_id": self.user_id,
            "operator_id": self.operator_id,
            "target_id": self.target_id,
            "message_id": self.message_id,
            "raw_message": self.raw_message,
            "text": self.text,
            "command_text": self.command_text or text_of(self.segments),
            "segments": [dict(seg) for seg in self.segments],
            "sender": dict(self.sender),
            "at_self": self.at_self,
            "known": self.known,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> QQEvent:
        """从总线载荷还原（保留 :meth:`as_dict` 的字段名）。"""

        if not isinstance(payload, Mapping):
            raise CodecError(f"事件载荷应为映射，得到 {type(payload).__name__}")
        if "post_type" in payload and "kind" in payload:
            return cls(
                post_type=str(payload.get("post_type", "")),
                time=int(payload.get("time", 0) or 0),
                self_id=int(payload.get("self_id", 0) or 0),
                message_type=str(payload.get("message_type", "") or ""),
                sub_type=str(payload.get("sub_type", "") or ""),
                notice_type=str(payload.get("notice_type", "") or ""),
                request_type=str(payload.get("request_type", "") or ""),
                meta_event_type=str(payload.get("meta_event_type", "") or ""),
                group_id=_opt_int(payload.get("group_id")),
                user_id=_opt_int(payload.get("user_id")),
                operator_id=_opt_int(payload.get("operator_id")),
                target_id=_opt_int(payload.get("target_id")),
                message_id=_opt_int(payload.get("message_id")),
                raw_message=str(payload.get("raw_message", "") or ""),
                text=str(payload.get("text", "") or ""),
                command_text=str(payload.get("command_text", "") or ""),
                segments=decode_message(payload.get("segments") or payload.get("message")),
                sender=dict(payload.get("sender") or {}),
                at_self=bool(payload.get("at_self", False)),
                known=bool(payload.get("known", True)),
                event_id=str(payload.get("event_id", "") or ""),
            )
        # 兜底：直接给 OneBot 原始事件
        return decode_event(payload)


def _opt_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def decode_event(raw: str | bytes | bytearray | Mapping[str, Any], *, strict: bool = False) -> QQEvent:
    """解码 OneBot v11 事件。

    ``raw`` 可以是 JSON 文本/字节，也可以已经是解析好的映射。
    ``strict=True`` 时 ``post_type`` 缺失或非法直接抛 :class:`CodecError`；
    默认宽松模式把未知 ``post_type`` 标成 ``known=False`` 交给路由器丢弃。
    """

    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError as exc:  # pragma: no cover - 非法 UTF-8
            raise CodecError(f"事件不是合法 UTF-8：{exc}") from exc
    if isinstance(raw, str):
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise CodecError(f"事件不是合法 JSON：{exc}") from exc
    else:
        payload = raw
    if not isinstance(payload, Mapping):
        raise CodecError(f"事件应为 JSON 对象，得到 {type(payload).__name__}")

    post_type = str(payload.get("post_type", "") or "")
    known = post_type in POST_TYPES
    if not known and strict:
        raise CodecError(f"未知 post_type：{post_type!r}（允许 {POST_TYPES}）")

    message = payload.get("message", payload.get("raw_message", ""))
    segments = decode_message(message) if post_type in ("message", "") else ()
    raw_message = str(payload.get("raw_message") or (to_cq_string(segments) if segments else ""))
    self_id = _opt_int(payload.get("self_id")) or 0
    text = str(payload.get("text") or text_of(segments, include_at=True))
    command_text = str(payload.get("command_text") or text_of(segments))
    event = QQEvent(
        post_type=post_type,
        time=int(payload.get("time", 0) or 0),
        self_id=self_id,
        message_type=str(payload.get("message_type", "") or ""),
        sub_type=str(payload.get("sub_type", "") or ""),
        notice_type=str(payload.get("notice_type", "") or ""),
        request_type=str(payload.get("request_type", "") or ""),
        meta_event_type=str(payload.get("meta_event_type", "") or ""),
        group_id=_opt_int(payload.get("group_id")),
        user_id=_opt_int(payload.get("user_id")),
        operator_id=_opt_int(payload.get("operator_id")),
        target_id=_opt_int(payload.get("target_id")),
        message_id=_opt_int(payload.get("message_id")),
        raw_message=raw_message,
        text=text,
        command_text=command_text,
        segments=segments,
        sender=dict(payload.get("sender") or {}),
        known=known,
        event_id=str(payload.get("event_id") or f"qq-{uuid.uuid4().hex[:12]}"),
        raw=dict(payload),
    )
    at_self = bool(self_id and event.mentions(self_id))
    return event if at_self == event.at_self else _with_at_self(event, at_self)


def _with_at_self(event: QQEvent, at_self: bool) -> QQEvent:
    return QQEvent(
        post_type=event.post_type,
        time=event.time,
        self_id=event.self_id,
        message_type=event.message_type,
        sub_type=event.sub_type,
        notice_type=event.notice_type,
        request_type=event.request_type,
        meta_event_type=event.meta_event_type,
        group_id=event.group_id,
        user_id=event.user_id,
        operator_id=event.operator_id,
        target_id=event.target_id,
        message_id=event.message_id,
        raw_message=event.raw_message,
        text=event.text,
        command_text=event.command_text,
        segments=event.segments,
        sender=event.sender,
        at_self=at_self,
        known=event.known,
        event_id=event.event_id,
        raw=event.raw,
    )


# --------------------------------------------------------------------------
# 编码回复
# --------------------------------------------------------------------------
def encode_reply(
    reply: Any, *, reply_to: int | str | None = None, at: Iterable[int | str] | None = None
) -> list[dict[str, Any]]:
    """把回复内容编码成 OneBot 消息段数组。

    ``reply`` 接受：纯文本、CQ 字符串、消息段数组、``{"text"|"message"|"segments": ...}`` 映射。
    """

    if isinstance(reply, Mapping):
        payload: Any = reply.get("segments", reply.get("message", reply.get("text", "")))
        if "reply_to" in reply:
            reply_to = reply["reply_to"]
        elif "quote" in reply:
            reply_to = reply["quote"]
        if "at" in reply:
            at = reply["at"]
        elif "mentions" in reply:
            at = reply["mentions"]
    else:
        payload = reply

    segments = [
        dict(seg)
        for seg in decode_message(payload)
        if not (seg["type"] == "text" and not str(seg["data"].get("text", "")))
    ]
    prefix: list[dict[str, Any]] = []
    if reply_to is not None:
        prefix.append(reply_segment(reply_to))
    for user_id in at or ():
        prefix.append(at_segment(user_id))
    if not segments:
        raise CodecError("回复内容为空，无法编码为 OneBot 消息段")
    return [*prefix, *segments]


def encode_text(text: str) -> list[dict[str, Any]]:
    return [text_segment(text)]


# --------------------------------------------------------------------------
# rpc:codec.decode / rpc:codec.encode
# --------------------------------------------------------------------------
async def decode(payload: Any, *, strict: bool = False, **_: Any) -> dict[str, Any]:
    """``rpc:codec.decode``：事件 JSON → 内部事件字典。"""

    return decode_event(payload, strict=strict).as_dict()


async def encode(reply: Any, *, reply_to: int | str | None = None, at: Any = None, **_: Any) -> dict[str, Any]:
    """``rpc:codec.encode``：回复内容 → OneBot 消息段数组（附纯文本预览）。"""

    segments = encode_reply(reply, reply_to=reply_to, at=at)
    return {
        "message": segments,
        "message_format": "array",
        "segment_count": len(segments),
        "text": text_of(segments),
    }


def register(registry: Any) -> None:
    """把本模块的 ``rpc:`` 名字注册进注册表（名字逐字对齐 api-index.json）。"""

    registry.register("rpc:codec.decode", decode, module="grouppig.gateway.adapter.event-codec", replace=True)
    registry.register("rpc:codec.encode", encode, module="grouppig.gateway.adapter.event-codec", replace=True)


__all__ = [
    "MESSAGE_TYPES",
    "POST_TYPES",
    "SEGMENT_TYPES",
    "CodecError",
    "QQEvent",
    "at_segment",
    "decode",
    "decode_event",
    "decode_message",
    "encode",
    "encode_reply",
    "encode_text",
    "face_segment",
    "image_segment",
    "normalize_segment",
    "parse_cq_string",
    "register",
    "reply_segment",
    "text_of",
    "text_segment",
    "to_cq_string",
]
