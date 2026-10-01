"""grouppig.gateway.sender.composer —— 回复包装器 / 发送入口。

职责（对应设计 ``grouppig.gateway.sender.composer``）：

* ``rpc:composer.wrap``：把生成文本包装成回复 —— 引用原文（``reply`` 段）、
  追加 ``@`` 提及与表情（``face`` 段）、超长文本切分成多条。
* ``rpc:sender.send_reply``：发送入口 —— 节流检查（``rpc:rate.check``）→
  必要时等待（``rpc:rate.wait``）→ 逐条发送（``rpc:onebot.send``）→
  把自发言记入聊天流水（``rpc:chat.append``，best-effort）。

入参（``rpc:sender.send_reply`` 的 ``reply`` 映射，字段都有等价关键字参数）::

    {
      "text": "生成出来的回复",          # 或 "segments"/"message"
      "group_id": 123, "user_id": 456,   # 二选一
      "reply_to": 789,                   # 引用的 message_id
      "at": [456], "emoji": [178],       # 提及与表情
      "source": "flow", "command": "/help"
    }

返回::

    {"ok": true, "sent": 1, "message_ids": [9001], "chunks": 1, "waited": 0.0,
     "rate": {...}, "recorded": true, "skipped": false, "error": ""}

normify id: ``grouppig.gateway.sender.composer``（叶子模块）。
"""

from __future__ import annotations

import contextlib
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from grouppig.gateway.adapter import event_codec
from grouppig.gateway.adapter.onebot import OneBotAdapter
from grouppig.gateway.sender.rate_limiter import RateLimiter
from grouppig.infra.runtime.errors import HandlerNotRegistered

TOPIC_REPLY_COMPOSED = "kafka:grouppig.reply.composed"
MODULE_ID = "grouppig.gateway.sender.composer"
CHAT_APPEND = "rpc:chat.append"

DEFAULT_MAX_LENGTH = 400
DEFAULT_EMOJI_POOL: tuple[int, ...] = (178, 179, 182, 187)
_SPLIT_PRIORITY = "。！？!?…；;\n，,、 "  # 优先在这些字符后断句


@dataclass(slots=True)
class ComposedReply:
    """包装后的回复（含切分后的多条消息）。"""

    text: str
    chunks: list[list[dict[str, Any]]] = field(default_factory=list)
    reply_to: int | str | None = None
    at: tuple[int | str, ...] = ()
    emoji: tuple[int | str, ...] = ()
    length: int = 0
    truncated: bool = False

    @property
    def segments(self) -> list[dict[str, Any]]:
        """未切分的完整消息段（供预览 / 测试）。"""

        flat: list[dict[str, Any]] = []
        for chunk in self.chunks:
            flat.extend(chunk)
        return flat

    @property
    def chunk_count(self) -> int:
        return len(self.chunks)

    def as_dict(self) -> dict[str, Any]:
        return {
            "text": self.text,
            "chunks": [[dict(seg) for seg in chunk] for chunk in self.chunks],
            "segments": [dict(seg) for seg in self.segments],
            "chunk_count": self.chunk_count,
            "reply_to": self.reply_to,
            "at": list(self.at),
            "emoji": list(self.emoji),
            "length": self.length,
        }


@dataclass
class ComposerStats:
    wrapped: int = 0
    sent: int = 0
    pending: int = 0
    chunks: int = 0
    skipped: int = 0
    rate_waits: int = 0
    failures: int = 0
    recorded: int = 0
    record_failures: int = 0
    last_error: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "wrapped": self.wrapped,
            "sent": self.sent,
            "chunks": self.chunks,
            "skipped": self.skipped,
            "rate_waits": self.rate_waits,
            "failures": self.failures,
            "recorded": self.recorded,
            "record_failures": self.record_failures,
            "last_error": self.last_error,
        }


class ReplyComposer:
    """回复包装 + 节流发送 + 记录自发言。"""

    def __init__(
        self,
        adapter: OneBotAdapter,
        *,
        limiter: RateLimiter | None = None,
        registry: Any | None = None,
        recorder: Any | None = None,
        retractor: Any | None = None,
        logger: Any | None = None,
        max_length: int = DEFAULT_MAX_LENGTH,
        emoji_pool: Sequence[int] = DEFAULT_EMOJI_POOL,
        auto_emoji: bool = True,
        quote: bool = True,
        record_own: bool = True,
        clock: Any = time.time,
    ) -> None:
        self.adapter = adapter
        self.limiter = limiter
        self.registry = registry
        self.recorder = recorder
        self.retractor = retractor
        self.logger = logger
        self.max_length = max(20, int(max_length))
        self.emoji_pool = tuple(emoji_pool or ())
        self.auto_emoji = auto_emoji
        self.quote = quote
        self.record_own = record_own
        self._clock = clock
        self.stats = ComposerStats()
        self._emoji_cursor = 0

    # ---- 包装 ----------------------------------------------------------
    def wrap(
        self,
        reply: Any,
        *,
        reply_to: int | str | None = None,
        at: Sequence[int | str] | None = None,
        emoji: Sequence[int | str] | None = None,
        quote: bool | None = None,
        max_length: int | None = None,
    ) -> ComposedReply:
        """包装回复内容：引用 + @ + 文本（切分）+ 表情。"""

        payload: Any = reply
        if isinstance(reply, Mapping):
            payload = reply.get("segments", reply.get("message", reply.get("text", "")))
            if "reply_to" in reply:
                reply_to = reply["reply_to"]
            elif "quote" in reply:
                reply_to = reply["quote"]
            if "at" in reply:
                at = reply["at"]
            elif "mentions" in reply:
                at = reply["mentions"]
            if "emoji" in reply:
                emoji = reply["emoji"]

        body = [dict(seg) for seg in event_codec.decode_message(payload)]
        if not body:
            raise event_codec.CodecError("回复内容为空，无法包装")
        text = event_codec.text_of(body)
        limit = self.max_length if max_length is None else max(20, int(max_length))

        use_quote = self.quote if quote is None else bool(quote)
        prefix: list[dict[str, Any]] = []
        if use_quote and reply_to is not None:
            prefix.append(event_codec.reply_segment(reply_to))
        at_list = tuple(at or ())
        for user_id in at_list:
            prefix.append(event_codec.at_segment(user_id))

        emoji_list = tuple(emoji) if emoji else self._pick_emoji(text)
        suffix = [event_codec.face_segment(face) for face in emoji_list]

        chunks = self._chunk(body, limit)
        if prefix:
            chunks[0] = [*prefix, *chunks[0]]
        if suffix:
            chunks[-1] = [*chunks[-1], *suffix]

        self.stats.wrapped += 1
        return ComposedReply(
            text=text,
            chunks=chunks,
            reply_to=reply_to,
            at=at_list,
            emoji=emoji_list,
            length=len(text),
        )

    def _pick_emoji(self, text: str) -> tuple[int, ...]:
        if not self.auto_emoji or not self.emoji_pool or not text:
            return ()
        if text.rstrip().endswith("]"):  # 已经是表情收尾
            return ()
        face = self.emoji_pool[self._emoji_cursor % len(self.emoji_pool)]
        self._emoji_cursor += 1
        return (face,)

    def _chunk(self, body: list[dict[str, Any]], limit: int) -> list[list[dict[str, Any]]]:
        chunks: list[list[dict[str, Any]]] = []
        current: list[dict[str, Any]] = []
        used = 0
        for segment in body:
            if segment.get("type") != "text":
                current.append(segment)
                continue
            text = str(segment.get("data", {}).get("text", ""))
            while text:
                room = limit - used
                if room <= 0:
                    chunks.append(current)
                    current, used = [], 0
                    room = limit
                if len(text) <= room:
                    current.append(event_codec.text_segment(text))
                    used += len(text)
                    text = ""
                else:
                    head, text = self._split(text, room)
                    current.append(event_codec.text_segment(head))
                    used += len(head)
                    chunks.append(current)
                    current, used = [], 0
        if current:
            chunks.append(current)
        return chunks or [[]]

    @staticmethod
    def _split(text: str, room: int) -> tuple[str, str]:
        window = text[:room]
        cut = -1
        for index in range(len(window) - 1, max(-1, len(window) // 2 - 1), -1):
            if window[index] in _SPLIT_PRIORITY:
                cut = index + 1
                break
        if cut <= 0:
            cut = room
        return text[:cut], text[cut:]

    # ---- 发送 ----------------------------------------------------------
    async def send_reply(
        self,
        reply: Any = None,
        *,
        group_id: int | str | None = None,
        user_id: int | str | None = None,
        source: str = "",
        drop_if_limited: bool = False,
        max_wait: float | None = None,
        record: bool | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """发送入口：节流 → 包装 → 逐条发送 → 记流水。"""

        if isinstance(reply, Mapping):
            group_id = reply.get("group_id", group_id)
            user_id = reply.get("user_id", user_id)
            source = str(reply.get("source", source) or source)
        if group_id is None and user_id is None:
            return self._result(ok=False, error="missing_target", reason="group_id/user_id 至少给一个")

        target: int | str = group_id if group_id is not None else user_id  # type: ignore[assignment]
        rate: dict[str, Any] | None = None
        waited = 0.0
        limiter = self.limiter
        if limiter is not None:
            # 速率（rejected）或延迟（delayed）都走 wait；wait 会再 check 一次并真正扣令牌
            decision = limiter.check(target)
            if not decision.allowed:
                if drop_if_limited:
                    self.stats.skipped += 1
                    return self._result(
                        ok=False, skipped=True, reason=decision.reason, rate=decision.as_dict(), target=target
                    )
                self.stats.rate_waits += 1
                decision = await limiter.wait(target, max_wait=max_wait)  # wait 成功时已扣令牌
                waited = float(decision.wait_seconds or 0.0)
                if not decision.allowed:
                    self.stats.skipped += 1
                    return self._result(
                        ok=False, skipped=True, reason=decision.reason, rate=decision.as_dict(), target=target
                    )
            else:
                limiter.consume(target)  # 立刻放行：自己扣令牌并记录 last_sent
            rate = decision.as_dict()

        try:
            composed = self.wrap(reply, **kwargs)
        except event_codec.CodecError as exc:
            self.stats.failures += 1
            self.stats.last_error = str(exc)
            return self._result(ok=False, error=str(exc), reason="wrap_failed", rate=rate, target=target)

        message_ids: list[Any] = []
        sent_chunks = 0
        for chunk in composed.chunks:
            try:
                if group_id is not None:
                    result = await self.adapter.send_group_message(group_id, chunk, raise_on_error=False)
                else:
                    result = await self.adapter.send_private_message(user_id, chunk, raise_on_error=False)
            except Exception as exc:
                self.stats.failures += 1
                self.stats.last_error = f"{type(exc).__name__}: {exc}"
                self._log("error", "composer.send_failed", error=self.stats.last_error, target=target)
                return self._result(
                    ok=False,
                    sent=sent_chunks,
                    message_ids=message_ids,
                    error=self.stats.last_error,
                    reason="send_failed",
                    rate=rate,
                    waited=waited,
                    target=target,
                    chunks=composed.chunk_count,
                )
            if not result.get("ok"):
                self.stats.failures += 1
                self.stats.last_error = f"{result.get('action')}: {result.get('wording') or result.get('status')}"
                return self._result(
                    ok=False,
                    sent=sent_chunks,
                    message_ids=message_ids,
                    error=self.stats.last_error,
                    reason="retcode",
                    rate=rate,
                    waited=waited,
                    target=target,
                    chunks=composed.chunk_count,
                )
            sent_chunks += 1
            message_ids.append(result.get("message_id"))
            if limiter is not None and sent_chunks > 1:
                # 多条消息之间也走冷却，避免一次吐一屏
                await limiter.wait(target, max_wait=max_wait)

        self.stats.sent += 1
        self.stats.chunks += sent_chunks
        recorded = False
        if self.record_own if record is None else record:
            recorded = await self._record(
                composed, group_id=group_id, user_id=user_id, source=source, message_ids=message_ids
            )
        if self.retractor is not None and message_ids:
            with contextlib.suppress(Exception):
                self.retractor.remember(
                    message_ids[0], group_id=group_id, user_id=user_id, text=composed.text, source=source
                )
        return self._result(
            ok=True,
            sent=sent_chunks,
            message_ids=message_ids,
            rate=rate,
            waited=waited,
            recorded=recorded,
            target=target,
            chunks=composed.chunk_count,
            text=composed.text,
        )

    async def _record(
        self,
        composed: ComposedReply,
        *,
        group_id: int | str | None,
        user_id: int | str | None,
        source: str,
        message_ids: Sequence[Any],
    ) -> bool:
        """把自发言写入聊天流水（``rpc:chat.append``）；失败不阻断发送。"""

        payload = {
            "group_id": group_id,
            "sender_id": user_id,
            "user_id": user_id,
            "message_id": message_ids[0] if message_ids else None,
            "message_ids": list(message_ids),
            # memory 的 chat_messages 列名是 content / sender_id（不是 text / user_id）：
            # 少了这两个键，自发言会以空 content 落库，画像与复盘读到的是空串。
            "content": composed.text,
            "text": composed.text,
            "segments": [dict(seg) for seg in composed.segments],
            "raw_message": event_codec.to_cq_string(composed.segments),
            "self": True,
            # memory 的 chat_messages.role 只认 member / self / system；
            # 写 assistant 会被 normalize_message 静默降级成 member，
            # 机器人自己的发言就和群友的混在一起了（下游画像/复盘会把它当群友说）。
            "role": "self",
            "source": source or "sender.composer",
            "ts": self._clock(),
        }
        append = self.recorder
        try:
            if append is not None:
                result = append(payload)
                if hasattr(result, "__await__"):
                    result = await result
            elif self.registry is not None:
                result = await self.registry.acall(CHAT_APPEND, payload)
            else:
                return False
        except HandlerNotRegistered:
            self.stats.record_failures += 1
            self._log("warning", "composer.record_skipped", name=CHAT_APPEND)
            return False
        except Exception as exc:
            self.stats.record_failures += 1
            self.stats.last_error = f"{type(exc).__name__}: {exc}"
            self._log("warning", "composer.record_failed", error=self.stats.last_error)
            return False
        self.stats.recorded += 1
        return bool(result) if isinstance(result, bool) else True

    def _result(self, *, ok: bool, **fields: Any) -> dict[str, Any]:
        result = {
            "ok": ok,
            "sent": 0,
            "message_ids": [],
            "chunks": 0,
            "waited": 0.0,
            "rate": None,
            "recorded": False,
            "skipped": False,
            "error": "",
            "reason": "",
            "target": None,
            "text": "",
        }
        result.update(fields)
        result["stats"] = self.stats.as_dict()
        return result

    def status(self) -> dict[str, Any]:
        return {
            "max_length": self.max_length,
            "emoji_pool": list(self.emoji_pool),
            "quote": self.quote,
            "auto_emoji": self.auto_emoji,
            "record_own": self.record_own,
            "stats": self.stats.as_dict(),
        }

    def _log(self, level: str, event: str, **fields: Any) -> None:
        if self.logger is None:
            return
        with contextlib.suppress(Exception):
            getattr(self.logger, level, self.logger.info)(event, **fields)


def subscribe_reply_composed(bus: Any, composer: ReplyComposer) -> Any:
    """把 composer 接到 ``kafka:grouppig.reply.composed``（表达层产出 → 发送）。

    处理器本身很快返回，真正的发送在总线派生的任务里跑：``rpc:rate.wait`` 可能要等几秒，
    不能让表达层的发布被出站节流拖住。需要等结果的调用方轮询 ``composer.stats``（``pending``/``sent``）。
    """

    async def on_reply_composed(event: Any) -> None:
        payload = getattr(event, "payload", event)
        if not isinstance(payload, Mapping):
            return
        composer.stats.pending += 1
        try:
            await composer.send_reply(dict(payload))
        finally:
            composer.stats.pending -= 1

    return bus.subscribe(TOPIC_REPLY_COMPOSED, on_reply_composed, name="grouppig.gateway.sender.composer")


# --------------------------------------------------------------------------
# rpc:composer.wrap / rpc:sender.send_reply
# --------------------------------------------------------------------------
def make_handlers(composer: ReplyComposer) -> dict[str, Any]:
    async def wrap(
        reply: Any = None, *, reply_to: Any = None, at: Any = None, emoji: Any = None, **_: Any
    ) -> dict[str, Any]:
        return composer.wrap(reply, reply_to=reply_to, at=at, emoji=emoji).as_dict()

    async def send_reply(reply: Any = None, **kwargs: Any) -> dict[str, Any]:
        return await composer.send_reply(reply, **kwargs)

    return {"rpc:composer.wrap": wrap, "rpc:sender.send_reply": send_reply}


def register(registry: Any, composer: ReplyComposer) -> None:
    for name, handler in make_handlers(composer).items():
        registry.register(name, handler, module=MODULE_ID, replace=True)


__all__ = [
    "CHAT_APPEND",
    "DEFAULT_EMOJI_POOL",
    "DEFAULT_MAX_LENGTH",
    "MODULE_ID",
    "TOPIC_REPLY_COMPOSED",
    "ComposedReply",
    "ComposerStats",
    "ReplyComposer",
    "make_handlers",
    "register",
    "subscribe_reply_composed",
]
