"""grouppig.social.graph.manager.events —— 社交事件发布器（``kafka:grouppig.social.changed``）。

职责（设计：``grouppig.social.graph.manager.events``「发布社交网变化事件，供反思与表达层订阅」）：

* 发布社交网变化：设计依赖 ``rpc:graph.tiering`` → ``kafka:grouppig.social.changed``（逐字对齐）；
* 载荷结构由 :meth:`SocialEventEmitter.payload` 定义::

    {
      "user_id": 1001, "group_id": 100200300,
      "tier": "friend", "previous_tier": "acquaintance",
      "tier_label": "熟识", "score": 52.0, "delta": 4.0,
      "reason": "replied", "source": "grouppig.social.graph.manager.tiering",
      "ts": 1700000000.0,
    }

本叶子只**发布**事件，不注册 ``rpc:`` 处理器。

设计：``grouppig.social.graph.manager.events``（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from grouppig.infra.runtime import contract

if TYPE_CHECKING:  # pragma: no cover - 仅供类型标注
    from grouppig.social import SocialContext

MODULE_ID = "grouppig.social.graph.manager.events"

#: 契约主题名（逐字对齐 api-index.json）。
TOPIC_SOCIAL_CHANGED = "kafka:grouppig.social.changed"
contract.assert_known_name(TOPIC_SOCIAL_CHANGED)

#: 本叶子涉及的契约名字。
NAMES: tuple[str, ...] = (TOPIC_SOCIAL_CHANGED,)


def build_payload(
    *,
    user_id: int,
    tier: str,
    previous_tier: str | None = None,
    group_id: int = 0,
    score: float | None = None,
    delta: float | None = None,
    tier_label: str = "",
    reason: str = "",
    source: str = MODULE_ID,
    ts: float | None = None,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """组装社交网变化事件载荷（纯函数）。"""

    payload: dict[str, Any] = {
        "user_id": int(user_id),
        "group_id": int(group_id),
        "tier": str(tier),
        "previous_tier": None if previous_tier is None else str(previous_tier),
        "tier_label": str(tier_label or ""),
        "score": None if score is None else round(float(score), 4),
        "delta": None if delta is None else round(float(delta), 4),
        "reason": str(reason or ""),
        "source": str(source or MODULE_ID),
        "ts": float(ts if ts is not None else time.time()),
    }
    if extra:
        payload["extra"] = dict(extra)
    return payload


@dataclass
class SocialEventEmitter:
    """社交网变化事件发布器（设计：``grouppig.social.graph.manager.events``）。"""

    ctx: SocialContext
    source: str = MODULE_ID
    published: int = field(default=0, init=False)
    last_payload: dict[str, Any] | None = field(default=None, init=False)

    def payload(self, **kwargs: Any) -> dict[str, Any]:
        kwargs.setdefault("source", self.source)
        kwargs.setdefault("ts", self.ctx.now())
        return build_payload(**kwargs)

    async def emit(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        """发布一条社交网变化事件。"""

        data = dict(payload)
        event = await self.ctx.publish(TOPIC_SOCIAL_CHANGED, data)
        self.published += 1
        self.last_payload = data
        self.ctx.log("debug", "social.event.published", topic=TOPIC_SOCIAL_CHANGED, user_id=data.get("user_id"))
        return {"topic": TOPIC_SOCIAL_CHANGED, "payload": data, "event_id": getattr(event, "id", None)}

    async def changed(self, **kwargs: Any) -> dict[str, Any]:
        """组装 + 发布（``kwargs`` 见 :func:`build_payload`）。"""

        return await self.emit(self.payload(**kwargs))

    async def emit_many(self, payloads: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        return [await self.emit(item) for item in payloads]

    def subscribe(self, bus: Any, handler: Callable[..., Any], *, name: str | None = None) -> Any:
        return bus.subscribe(TOPIC_SOCIAL_CHANGED, handler, name=name or "social:graph.changed")

    def status(self) -> dict[str, Any]:
        return {"topic": TOPIC_SOCIAL_CHANGED, "published": self.published, "last": self.last_payload}


def subscribe(bus: Any, handler: Callable[..., Any], *, name: str | None = None) -> Any:
    """模块级便捷订阅。"""

    return bus.subscribe(TOPIC_SOCIAL_CHANGED, handler, name=name or "social:graph.changed")


def make_handlers(ctx: SocialContext, emitter: SocialEventEmitter) -> dict[str, Any]:
    """事件叶子不提供 ``rpc:`` 处理器。"""

    return {}


def register(registry: Any, emitter: SocialEventEmitter, *, replace: bool = True) -> None:
    """事件叶子无需注册处理器；保留同名函数以便容器统一调用。"""

    return None


__all__ = [
    "MODULE_ID",
    "NAMES",
    "TOPIC_SOCIAL_CHANGED",
    "SocialEventEmitter",
    "build_payload",
    "make_handlers",
    "register",
    "subscribe",
]
