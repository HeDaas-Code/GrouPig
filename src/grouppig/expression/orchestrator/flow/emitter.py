"""grouppig.expression.orchestrator.flow.emitter —— 编排事件发布器（``kafka:grouppig.reply.composed``）。

职责（设计：``grouppig.expression.orchestrator.flow.emitter``「发布回复编排完成事件」）：

* 本叶子**只拥有一个 kafka 主题**：``kafka:grouppig.reply.composed``（逐字对齐 api-index.json），
  没有 ``rpc:`` 名字；
* 心流编排跑完（``rpc:flow.end``）后由 :mod:`grouppig.expression.orchestrator.flow.state`
  调用 :meth:`FlowEmitter.reply_composed` 发布**回复编排完成事件**；
* 下游订阅方是 ``grouppig.gateway.sender.composer``（``subscribe_reply_composed``）——
  它拿到载荷后走节流发送。因此**载荷字段必须与 composer 对得上**：
  ``group_id`` / ``user_id`` / ``text`` / ``source`` 是 composer ``send_reply`` 直接读的键
  （见 ``composer.send_reply`` 开头对 ``Mapping`` 的解包）。

**为什么发布不能抛**：发送是闭环的最后一环，编排成功但发布失败时，回复仍应尽量送出去
（调用方拿到 ``ok=False`` 后可以退回直调 ``rpc:sender.send_reply``）。因此 :meth:`publish`
把总线异常吞掉并记进 ``errors``，返回 ``{"ok": bool, ...}`` 而不是向上抛。

设计：``grouppig.expression.orchestrator.flow.emitter``（叶子模块）。
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from grouppig.infra.runtime import contract

MODULE_ID = "grouppig.expression.orchestrator.flow.emitter"

#: 契约 kafka 主题（逐字对齐 api-index.json；本叶子是本域唯一拥有主题的叶子）。
NAMES: tuple[str, ...] = ("kafka:grouppig.reply.composed",)
TOPIC_REPLY_COMPOSED = "kafka:grouppig.reply.composed"
contract.assert_known_name(TOPIC_REPLY_COMPOSED)

#: 事件名（载荷里的 ``event`` 字段；下游可据此过滤）。
EVENT_REPLY_COMPOSED = "reply.composed"

#: 载荷里 composer ``send_reply`` 直接读的键（改这里必须同步 composer）。
COMPOSER_KEYS: tuple[str, ...] = ("group_id", "user_id", "text", "source")

#: 载荷的固定字段（顺序稳定，便于断言与落库）。
PAYLOAD_FIELDS: tuple[str, ...] = (
    "event",
    "flow_id",
    "session_id",
    "group_id",
    "user_id",
    "reply_to",
    "text",
    "candidates",
    "candidate_count",
    "stage",
    "plan_id",
    "template_id",
    "preset_id",
    "tokens",
    "degraded",
    "degraded_paths",
    "composed_at",
    "source",
)


def build_payload(
    *,
    text: str = "",
    group_id: int = 0,
    user_id: int | None = None,
    reply_to: int | None = None,
    flow_id: str = "",
    session_id: str = "",
    stage: str = "",
    plan: Mapping[str, Any] | None = None,
    selection: Mapping[str, Any] | None = None,
    candidates: Any = None,
    tokens: Mapping[str, Any] | None = None,
    degraded: bool = False,
    degraded_paths: Any = None,
    composed_at: float = 0.0,
    source: str = MODULE_ID,
) -> dict[str, Any]:
    """组 ``kafka:grouppig.reply.composed`` 的载荷（纯函数，可断言）。"""

    plan = dict(plan or {})
    selection = dict(selection or {})
    candidate_list = [str(item) for item in (candidates or ()) if str(item).strip()]
    return {
        "event": EVENT_REPLY_COMPOSED,
        "flow_id": str(flow_id or ""),
        "session_id": str(session_id or ""),
        "group_id": int(group_id or 0),
        "user_id": int(user_id) if user_id is not None else None,
        "reply_to": int(reply_to) if reply_to is not None else None,
        "text": str(text or ""),
        "candidates": candidate_list,
        "candidate_count": len(candidate_list),
        "stage": str(stage or ""),
        "plan_id": str(plan.get("plan_id") or ""),
        "template_id": str(selection.get("template_id") or ""),
        "preset_id": str(selection.get("preset_id") or ""),
        "tokens": dict(tokens or {}),
        "degraded": bool(degraded),
        "degraded_paths": [str(item) for item in (degraded_paths or ())],
        "composed_at": float(composed_at or 0.0),
        "source": str(source),
    }


@dataclass
class FlowEmitter:
    """编排事件发布器（设计：``grouppig.expression.orchestrator.flow.emitter``）。

    ``publish`` 是容器的发布函数（``bus.publish``）；缺省时退化为 no-op，
    让「没接总线」的装配也能跑通（只记 ``emitted=0``）。
    """

    publish: Any = None
    logger: Any = None
    clock: Any = None
    emitted: int = field(default=0, init=False)
    failed: int = field(default=0, init=False)
    errors: list[str] = field(default_factory=list, init=False)
    last: dict[str, Any] = field(default_factory=dict, init=False)

    def _now(self) -> float:
        clock = self.clock
        return float(clock()) if callable(clock) else 0.0

    def _log(self, level: str, event: str, **fields: Any) -> None:
        logger = self.logger
        if logger is None:
            return
        try:
            getattr(logger, level, logger.info)(event, **fields)
        except Exception:  # pragma: no cover - 日志失败不影响主链路
            return

    async def reply_composed(
        self,
        payload: Mapping[str, Any] | None = None,
        *,
        topic: str = TOPIC_REPLY_COMPOSED,
        **fields: Any,
    ) -> dict[str, Any]:
        """发布回复编排完成事件；返回 ``{"ok": bool, "topic": ..., "payload": ...}``。"""

        if payload is None:
            data = build_payload(composed_at=fields.pop("composed_at", 0.0) or self._now(), **fields)
        else:
            data = dict(payload)
            data.setdefault("event", EVENT_REPLY_COMPOSED)
            data.setdefault("composed_at", self._now())
        return await self.emit(data, topic=topic)

    async def emit(self, payload: Mapping[str, Any], *, topic: str = TOPIC_REPLY_COMPOSED) -> dict[str, Any]:
        """把已组好的载荷发到主题（发布失败不抛，记进 ``errors``）。"""

        data = dict(payload)
        publisher = self.publish
        if not callable(publisher):
            self._log("debug", "flow.reply_composed_skipped", reason="no_publisher", topic=topic)
            self.last = {"ok": False, "topic": topic, "payload": data, "reason": "no_publisher"}
            return self.last
        try:
            await publisher(topic, data)
        except Exception as error:  # 发布失败不阻断发送：调用方可退回直调 sender
            self.failed += 1
            message = f"{type(error).__name__}: {error}"
            self.errors.append(message)
            self._log("warning", "flow.reply_composed_failed", topic=topic, error=message)
            self.last = {"ok": False, "topic": topic, "payload": data, "error": message, "reason": "publish_failed"}
            return self.last
        self.emitted += 1
        self._log(
            "info",
            "flow.reply_composed",
            topic=topic,
            group_id=data.get("group_id"),
            text_chars=len(str(data.get("text") or "")),
            degraded=bool(data.get("degraded")),
        )
        self.last = {"ok": True, "topic": topic, "payload": data}
        return self.last

    def status(self) -> dict[str, Any]:
        return {
            "module": MODULE_ID,
            "topic": TOPIC_REPLY_COMPOSED,
            "emitted": self.emitted,
            "failed": self.failed,
            "errors": list(self.errors[-5:]),
        }


def make_handlers(emitter: FlowEmitter) -> dict[str, Any]:
    """本叶子**没有 rpc: 处理器**，只导出主题常量。

    返回空字典是**有意为之**：emitter 是发布方（生产者）而不是订阅方，
    契约里它只拥有一个 kafka 主题。`register` 因此不注册任何名字，
    测试据此断言「emitter 不占用任何 rpc 名字」。
    """

    return {}


def register(registry: Any, emitter: FlowEmitter | None = None, *, replace: bool = True) -> Any:
    """不注册任何名字（emitter 是纯发布方）；保留接口与其它叶子一致。"""

    del registry, emitter, replace
    return None


__all__ = [
    "COMPOSER_KEYS",
    "EVENT_REPLY_COMPOSED",
    "MODULE_ID",
    "NAMES",
    "PAYLOAD_FIELDS",
    "TOPIC_REPLY_COMPOSED",
    "FlowEmitter",
    "build_payload",
    "make_handlers",
    "register",
]
