"""grouppig.social.profile.manager.events —— 档案事件发布器（``kafka:grouppig.profile.updated``）。

职责：把「档案被更新」这件事变成一条可订阅的群内事件，供社交网与画像模块订阅
（设计依赖：``rpc:profile.update`` → ``kafka:grouppig.profile.updated``）。

本叶子只**发布**事件，不注册 ``rpc:`` 处理器；载荷结构由 :meth:`ProfileEventEmitter.payload` 定义::

    {
      "user_id": 1001, "group_id": 100200300,
      "version": 3, "previous_version": 2,
      "changed": ["nickname", "interests"],
      "nickname": "阿猪", "facts": 5, "superseded": 1,
      "reason": "fact-extract", "source": "grouppig.social.profile.manager.versioning",
      "ts": 1700000000.0,
    }

订阅方（反思层 / 表达层）::

    from grouppig.social.profile.manager.events import TOPIC_PROFILE_UPDATED, subscribe
    subscribe(container.bus, my_handler)

设计：``grouppig.social.profile.manager.events``（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from grouppig.infra.runtime import contract

if TYPE_CHECKING:  # pragma: no cover - 仅供类型标注，避免与容器包循环导入
    from grouppig.social import SocialContext

MODULE_ID = "grouppig.social.profile.manager.events"

#: 契约主题名（逐字对齐 api-index.json）。
TOPIC_PROFILE_UPDATED = "kafka:grouppig.profile.updated"
contract.assert_known_name(TOPIC_PROFILE_UPDATED)

#: 本叶子涉及的契约名字（``kafka:`` 主题不进注册表，由容器常量统一断言）。
NAMES: tuple[str, ...] = (TOPIC_PROFILE_UPDATED,)


def build_payload(
    *,
    user_id: int,
    version: int,
    previous_version: int | None = None,
    group_id: int = 0,
    changed: Sequence[str] = (),
    nickname: str = "",
    facts: int = 0,
    superseded: int = 0,
    reason: str = "",
    source: str = MODULE_ID,
    ts: float | None = None,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """组装档案更新事件载荷（纯函数，便于测试与复用）。"""

    payload: dict[str, Any] = {
        "user_id": int(user_id),
        "group_id": int(group_id),
        "version": int(version),
        "previous_version": None if previous_version is None else int(previous_version),
        "changed": [str(item) for item in changed],
        "nickname": str(nickname or ""),
        "facts": int(facts),
        "superseded": int(superseded),
        "reason": str(reason or ""),
        "source": str(source or MODULE_ID),
        "ts": float(ts if ts is not None else time.time()),
    }
    if extra:
        payload["extra"] = dict(extra)
    return payload


@dataclass
class ProfileEventEmitter:
    """档案事件的发布器（设计：``grouppig.social.profile.manager.events``）。"""

    ctx: SocialContext
    source: str = MODULE_ID
    published: int = field(default=0, init=False)
    last_payload: dict[str, Any] | None = field(default=None, init=False)

    # ---- 载荷 ----------------------------------------------------------
    def payload(self, **kwargs: Any) -> dict[str, Any]:
        kwargs.setdefault("source", self.source)
        kwargs.setdefault("ts", self.ctx.now())
        return build_payload(**kwargs)

    # ---- 发布 ----------------------------------------------------------
    async def emit(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        """发布一条档案更新事件；返回 ``{"topic", "payload", "event_id"}``。"""

        data = dict(payload)
        event = await self.ctx.publish(TOPIC_PROFILE_UPDATED, data)
        self.published += 1
        self.last_payload = data
        self.ctx.log("debug", "profile.event.published", topic=TOPIC_PROFILE_UPDATED, user_id=data.get("user_id"))
        return {"topic": TOPIC_PROFILE_UPDATED, "payload": data, "event_id": getattr(event, "id", None)}

    async def updated(self, **kwargs: Any) -> dict[str, Any]:
        """组装 + 发布（``kwargs`` 见 :func:`build_payload`）。"""

        return await self.emit(self.payload(**kwargs))

    # ---- 订阅（供反思/表达层复用）--------------------------------------
    def subscribe(self, bus: Any, handler: Callable[..., Any], *, name: str | None = None) -> Any:
        """把处理器订阅到本主题（``bus`` 为 :class:`grouppig.infra.runtime.bus.EventBus`）。"""

        return bus.subscribe(TOPIC_PROFILE_UPDATED, handler, name=name or "social:profile.updated")

    def status(self) -> dict[str, Any]:
        return {"topic": TOPIC_PROFILE_UPDATED, "published": self.published, "last": self.last_payload}


def subscribe(bus: Any, handler: Callable[..., Any], *, name: str | None = None) -> Any:
    """模块级便捷订阅（见 :meth:`ProfileEventEmitter.subscribe`）。"""

    return bus.subscribe(TOPIC_PROFILE_UPDATED, handler, name=name or "social:profile.updated")


def make_handlers(ctx: SocialContext, emitter: ProfileEventEmitter) -> dict[str, Any]:
    """事件叶子不提供 ``rpc:`` 处理器（发布走 :meth:`ProfileEventEmitter.emit`）。"""

    return {}


def register(registry: Any, emitter: ProfileEventEmitter, *, replace: bool = True) -> None:
    """事件叶子无需注册处理器；保留同名函数以便容器统一调用。"""

    return None


__all__ = [
    "MODULE_ID",
    "NAMES",
    "TOPIC_PROFILE_UPDATED",
    "ProfileEventEmitter",
    "build_payload",
    "make_handlers",
    "register",
    "subscribe",
]
