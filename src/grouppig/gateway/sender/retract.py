"""grouppig.gateway.sender.retract —— 撤回补救器。

职责（对应设计 ``grouppig.gateway.sender.retract``）：

* ``rpc:retract.recall``：撤回已发消息（``rpc:onebot.send`` 发 ``delete_msg``）。
* ``rpc:retract.notify``：记录撤回原因，供反思层（session-review）复盘。

典型用法：编排层发现误发 / 触发群规 → ``recall`` 撤回并留下原因；
群友撤回自己的消息时，路由器（:mod:`grouppig.gateway.router.demux`）也会 ``notify``
记一笔，供反思时判断「刚说的被撤了」。

撤回原因分类（``REASONS``）：``misfire``（误发）、``rule_violation``（触发群规）、
``duplicate``（重复）、``hallucination``（胡言）、``group_recall`` / ``friend_recall``（群友撤回）。

normify id: ``grouppig.gateway.sender.retract``（叶子模块）。
"""

from __future__ import annotations

import contextlib
import time
from collections import deque
from dataclasses import dataclass
from typing import Any

from grouppig.gateway.adapter.onebot import OneBotAdapter

MODULE_ID = "grouppig.gateway.sender.retract"
REASONS = ("misfire", "rule_violation", "duplicate", "hallucination", "group_recall", "friend_recall", "manual")


@dataclass(slots=True)
class SentMessage:
    """最近发出去的消息（供 ``recall_last`` 用）。"""

    message_id: int | str
    group_id: int | str | None = None
    user_id: int | str | None = None
    text: str = ""
    source: str = ""
    at: float = 0.0
    #: 同一次 ``rpc:sender.send_reply`` 发出的多条消息共用的批次号。
    #: 超长回复会被切分成多条，撤回时必须整批撤（见 :meth:`Retractor.recall_batch`）。
    batch: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "message_id": self.message_id,
            "group_id": self.group_id,
            "user_id": self.user_id,
            "text": self.text,
            "source": self.source,
            "at": self.at,
            "batch": self.batch,
        }


@dataclass(slots=True)
class Retraction:
    """一次撤回的结果。"""

    message_id: int | str | None
    group_id: int | str | None
    reason: str
    ok: bool
    retcode: Any = None
    status: str = ""
    error: str = ""
    at: float = 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "message_id": self.message_id,
            "group_id": self.group_id,
            "reason": self.reason,
            "ok": self.ok,
            "retcode": self.retcode,
            "status": self.status,
            "error": self.error,
            "at": self.at,
        }


class Retractor:
    """撤回 + 原因记录。"""

    def __init__(
        self,
        adapter: OneBotAdapter,
        *,
        logger: Any | None = None,
        max_reasons: int = 200,
        max_sent: int = 50,
        on_notify: Any | None = None,
        clock: Any = time.time,
    ) -> None:
        self.adapter = adapter
        self.logger = logger
        self.max_reasons = max(1, int(max_reasons))
        self.max_sent = max(1, int(max_sent))
        self.on_notify = on_notify
        self._clock = clock
        self._reasons: deque[dict[str, Any]] = deque(maxlen=self.max_reasons)
        self._sent: deque[SentMessage] = deque(maxlen=self.max_sent)
        self.stats: dict[str, int] = {"recalls": 0, "recall_failures": 0, "notifications": 0, "remembered": 0}

    # ---- 记录 ----------------------------------------------------------
    def remember(
        self,
        message_id: int | str,
        *,
        group_id: int | str | None = None,
        user_id: int | str | None = None,
        text: str = "",
        source: str = "",
        batch: str = "",
    ) -> SentMessage:
        """记住一条自己发出的消息（由 composer 调用）。"""

        record = SentMessage(
            message_id=message_id,
            group_id=group_id,
            user_id=user_id,
            text=text,
            source=source,
            at=self._clock(),
            batch=batch,
        )
        self._sent.append(record)
        self.stats["remembered"] += 1
        return record

    def batch_of(self, batch: str) -> list[SentMessage]:
        """取某个批次里还记着的全部已发消息（按发送顺序）。"""

        wanted = str(batch or "")
        if not wanted:
            return []
        return [record for record in self._sent if record.batch == wanted]

    def notify(
        self,
        *,
        reason: str,
        message_id: int | str | None = None,
        group_id: int | str | None = None,
        user_id: int | str | None = None,
        operator_id: int | str | None = None,
        detail: str = "",
        **extra: Any,
    ) -> dict[str, Any]:
        """``rpc:retract.notify``：记录撤回原因。"""

        entry = {
            "reason": reason,
            "message_id": message_id,
            "group_id": group_id,
            "user_id": user_id,
            "operator_id": operator_id,
            "detail": detail,
            "at": self._clock(),
            "extra": dict(extra) if extra else {},
        }
        self._reasons.append(entry)
        self.stats["notifications"] += 1
        if self.on_notify is not None:
            with contextlib.suppress(Exception):
                self.on_notify(entry)
        self._log("info", "retract.notified", reason=reason, message_id=message_id, group_id=group_id)
        return entry

    def reasons(self, limit: int = 50, *, reason: str | None = None) -> list[dict[str, Any]]:
        items = [entry for entry in self._reasons if reason is None or entry["reason"] == reason]
        return items[-max(1, int(limit)) :]

    def last_sent(self, group_id: int | str | None = None) -> SentMessage | None:
        for record in reversed(self._sent):
            if group_id is None or str(record.group_id) == str(group_id):
                return record
        return None

    # ---- 撤回 ----------------------------------------------------------
    async def recall(
        self,
        message_id: int | str,
        *,
        group_id: int | str | None = None,
        reason: str = "misfire",
        detail: str = "",
        notify: bool = True,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """``rpc:retract.recall``：撤回一条消息（先记原因，再发 ``delete_msg``）。"""

        if notify:
            self.notify(reason=reason, message_id=message_id, group_id=group_id, detail=detail)
        try:
            result = await self.adapter.send("delete_msg", {"message_id": _int(message_id)}, timeout=timeout)
        except Exception as exc:
            self.stats["recall_failures"] += 1
            self._log("error", "retract.recall_failed", message_id=message_id, error=str(exc))
            return Retraction(
                message_id=message_id,
                group_id=group_id,
                reason=reason,
                ok=False,
                error=f"{type(exc).__name__}: {exc}",
                at=self._clock(),
            ).as_dict()

        ok = bool(result.get("ok"))
        self.stats["recalls" if ok else "recall_failures"] += 1
        retraction = Retraction(
            message_id=message_id,
            group_id=group_id,
            reason=reason,
            ok=ok,
            retcode=result.get("retcode"),
            status=str(result.get("status") or ""),
            error="" if ok else str(result.get("wording") or result.get("status") or "delete_msg failed"),
            at=self._clock(),
        )
        if ok:
            self._sent = deque((r for r in self._sent if str(r.message_id) != str(message_id)), maxlen=self.max_sent)
        self._log("info" if ok else "warning", "retract.recalled", **retraction.as_dict())
        return retraction.as_dict()

    async def recall_last(
        self,
        *,
        group_id: int | str | None = None,
        reason: str = "misfire",
        detail: str = "",
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """撤回最近一条自己发出的消息（群内）。"""

        record = self.last_sent(group_id)
        if record is None:
            return Retraction(
                message_id=None, group_id=group_id, reason=reason, ok=False, error="no_sent_message", at=self._clock()
            ).as_dict()
        return await self.recall(
            record.message_id, group_id=record.group_id, reason=reason, detail=detail, timeout=timeout
        )

    async def recall_batch(
        self,
        batch: str,
        *,
        reason: str = "misfire",
        detail: str = "",
        notify: bool = True,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """撤回同一次发送产生的**全部**消息。

        超长回复会被切分成多条（``composer.wrap`` 的 chunks）。此前只有第一条被
        ``remember``，于是「撤回这条」只删掉第一段，剩下的错话留在群里。
        现在 composer 给整批打同一个 ``batch``，这里逐条撤回并汇总结果。
        """

        records = self.batch_of(batch)
        if not records:
            return {
                "batch": batch,
                "ok": False,
                "recalled": 0,
                "failed": 0,
                "message_ids": [],
                "results": [],
                "error": "no_sent_message",
            }
        results: list[dict[str, Any]] = []
        recalled = 0
        failed = 0
        for index, record in enumerate(records):
            # 只有第一条记原因，避免同一批在原因环里刷屏
            outcome = await self.recall(
                record.message_id,
                group_id=record.group_id,
                reason=reason,
                detail=detail,
                notify=notify and index == 0,
                timeout=timeout,
            )
            results.append(outcome)
            if outcome.get("ok"):
                recalled += 1
            else:
                failed += 1
        return {
            "batch": batch,
            "ok": failed == 0,
            "recalled": recalled,
            "failed": failed,
            "message_ids": [record.message_id for record in records],
            "results": results,
            "error": "" if failed == 0 else f"{failed} 条撤回失败",
        }

    def status(self) -> dict[str, Any]:
        return {
            "reasons": len(self._reasons),
            "sent_tracked": len(self._sent),
            "last_sent": self._sent[-1].as_dict() if self._sent else None,
            "stats": dict(self.stats),
        }

    def _log(self, level: str, event: str, **fields: Any) -> None:
        if self.logger is None:
            return
        with contextlib.suppress(Exception):
            getattr(self.logger, level, self.logger.info)(event, **fields)


def _int(value: Any) -> Any:
    try:
        return int(value)
    except (TypeError, ValueError):
        return value


# --------------------------------------------------------------------------
# rpc:retract.recall / rpc:retract.notify
# --------------------------------------------------------------------------
def make_handlers(retractor: Retractor) -> dict[str, Any]:
    async def recall(
        message_id: int | str,
        *,
        group_id: int | str | None = None,
        reason: str = "misfire",
        detail: str = "",
        timeout: float | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        return await retractor.recall(message_id, group_id=group_id, reason=reason, detail=detail, timeout=timeout)

    async def notify(
        reason: str = "manual",
        *,
        message_id: int | str | None = None,
        group_id: int | str | None = None,
        user_id: int | str | None = None,
        operator_id: int | str | None = None,
        detail: str = "",
        **extra: Any,
    ) -> dict[str, Any]:
        return retractor.notify(
            reason=reason,
            message_id=message_id,
            group_id=group_id,
            user_id=user_id,
            operator_id=operator_id,
            detail=detail,
            **extra,
        )

    return {"rpc:retract.recall": recall, "rpc:retract.notify": notify}


def register(registry: Any, retractor: Retractor) -> None:
    for name, handler in make_handlers(retractor).items():
        registry.register(name, handler, module=MODULE_ID, replace=True)


__all__ = [
    "MODULE_ID",
    "REASONS",
    "Retraction",
    "Retractor",
    "SentMessage",
    "make_handlers",
    "register",
]
