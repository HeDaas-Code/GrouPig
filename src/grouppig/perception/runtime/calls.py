"""grouppig.perception.runtime.calls —— 下游 ``rpc:`` 的最佳努力调用与节流。

感知层的设计依赖里有 6 个跨域调用（``rpc:chat.append`` / ``rpc:chat.query`` /
``rpc:chat.window`` / ``rpc:threads.segment`` / ``rpc:topic.candidate.generate`` /
``rpc:behavior.classify`` 的下游 ``rpc:presets.match`` / ``rpc:flow.start``、
以及 ``rpc:model.classify``）。这些域可能尚未装配（单域测试、分阶段上线），
因此一律走 :func:`maybe_call`：**未注册就跳过并记账**，绝不因为下游缺席而炸掉感知回路。

:class:`CallOutcome` 把「跳过 / 成功 / 失败」显式记进返回值，方便端到端排查与断言。
:class:`Throttle` 用于把「每条消息都触发一次分类」削峰成「每 N 秒至多一次」。

normify id: ``grouppig.perception.runtime.calls``（运行时补充模块，设计树暂无）。
"""

from __future__ import annotations

import inspect
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from grouppig.infra.runtime.errors import HandlerNotRegistered
from grouppig.infra.runtime.registry import Registry

#: 调用结果状态。
STATUS_OK = "ok"
STATUS_SKIPPED = "skipped"
STATUS_FAILED = "failed"

#: 设计依赖里的跨域名字（用于自检「哪些下游还没接」）。
DOWNSTREAM_NAMES: tuple[str, ...] = (
    "rpc:chat.append",
    "rpc:chat.query",
    "rpc:chat.window",
    "rpc:threads.segment",
    "rpc:topic.candidate.generate",
    "rpc:presets.match",
    "rpc:model.classify",
    "rpc:flow.start",
)


@dataclass(frozen=True)
class CallOutcome:
    """一次下游调用的结果（可 JSON 序列化）。"""

    name: str
    status: str
    result: Any = None
    error: str = ""
    fallback: bool = False

    @property
    def ok(self) -> bool:
        return self.status == STATUS_OK

    @property
    def called(self) -> bool:
        return self.status != STATUS_SKIPPED

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status,
            "fallback": self.fallback,
            "error": self.error,
        }


def registry_of(target: Any) -> Registry | None:
    """从 ``Container`` / ``Registry`` / ``None`` 里取出注册表。"""

    if target is None:
        return None
    if isinstance(target, Registry):
        return target
    registry = getattr(target, "registry", None)
    return registry if isinstance(registry, Registry) else None


async def maybe_call(registry: Any, name: str, *args: Any, **kwargs: Any) -> CallOutcome:
    """按名字调用下游处理器；未注册 → ``skipped``，抛错 → ``failed``（不向上冒泡）。"""

    target = registry_of(registry)
    if target is None or not target.has(name):
        return CallOutcome(name=name, status=STATUS_SKIPPED)
    try:
        result = await target.acall(name, *args, **kwargs)
    except HandlerNotRegistered:  # pragma: no cover - has() 已挡掉
        return CallOutcome(name=name, status=STATUS_SKIPPED)
    except Exception as exc:  # noqa: BLE001 - 下游失败不能拖垮感知回路
        return CallOutcome(name=name, status=STATUS_FAILED, error=f"{type(exc).__name__}: {exc}")
    return CallOutcome(name=name, status=STATUS_OK, result=result)


async def call_leaf(
    registry: Any,
    name: str,
    fallback: Callable[..., Any] | None,
    *args: Any,
    **kwargs: Any,
) -> CallOutcome:
    """同域叶子调用：注册表里有就走 ``rpc:`` 边，没有就调用同域对象（保证叶子可单测）。

    ``fallback`` 可以是同步或协程函数；返回的 :class:`CallOutcome` 里
    ``fallback=True`` 表示走了进程内直连而不是 ``rpc:`` 边。
    """

    target = registry_of(registry)
    if target is not None and target.has(name):
        return await maybe_call(target, name, *args, **kwargs)
    if fallback is None:
        return CallOutcome(name=name, status=STATUS_SKIPPED)
    try:
        result = fallback(*args, **kwargs)
        if inspect.isawaitable(result):
            result = await result
    except Exception as exc:  # noqa: BLE001 - 同上
        return CallOutcome(name=name, status=STATUS_FAILED, fallback=True, error=f"{type(exc).__name__}: {exc}")
    return CallOutcome(name=name, status=STATUS_OK, fallback=True, result=result)


@dataclass
class _ThrottleState:
    last: float = 0.0
    count: int = 0
    passed: int = 0
    blocked: int = 0


@dataclass
class Throttle:
    """按 key 削峰：``min_interval`` 秒内至多放行一次，且累计 ``min_messages`` 条才放行。

    ``min_messages`` 的语义是「窗口里至少要攒够这么多条消息才值得触发一次分类」。
    """

    min_interval: float = 0.0
    min_messages: int = 1
    clock: Callable[[], float] = time.monotonic
    states: dict[str, _ThrottleState] = field(default_factory=dict)

    def allow(self, key: Any, *, now: float | None = None, count: int = 1) -> bool:
        state = self.states.setdefault(str(key), _ThrottleState())
        state.count += max(0, int(count))
        stamp = float(now if now is not None else self.clock())
        if state.last and (stamp - state.last) < self.min_interval:
            state.blocked += 1
            return False
        if state.count < max(1, int(self.min_messages)):
            state.blocked += 1
            return False
        state.last = stamp
        state.count = 0
        state.passed += 1
        return True

    def reset(self, key: Any = None) -> None:
        if key is None:
            self.states.clear()
        else:
            self.states.pop(str(key), None)

    def snapshot(self) -> dict[str, Any]:
        return {
            key: {"count": state.count, "passed": state.passed, "blocked": state.blocked, "last": state.last}
            for key, state in sorted(self.states.items())
        }


async def recent_messages(
    registry: Any,
    group_id: int,
    *,
    seconds: float,
    now: float | None = None,
    limit: int = 200,
    fallback: Any = None,
) -> dict[str, Any]:
    """取最近 ``seconds`` 秒的消息（设计依赖 ``rpc:chat.window``）。

    优先级：``rpc:chat.window``（memory 侧落库窗口）→ ``fallback`` 内存滚动窗。
    返回 ``{messages, count, source, since, until, window_seconds, window, outcome}``。
    """

    stamp = float(now if now is not None else time.time())
    window_seconds = float(seconds)
    outcome = await maybe_call(
        registry,
        "rpc:chat.window",
        int(group_id),
        seconds=window_seconds,
        limit=limit,
        now=stamp,
    )
    if outcome.ok and isinstance(outcome.result, Mapping):
        payload = dict(outcome.result)
        messages = [dict(item) for item in payload.get("messages") or () if isinstance(item, Mapping)]
        return {
            "messages": messages,
            "count": len(messages),
            "source": "chat.window",
            "since": float(payload.get("since", stamp - window_seconds)),
            "until": float(payload.get("until", stamp)),
            "window_seconds": window_seconds,
            "window": dict(payload.get("window") or {}),
            "outcome": outcome.as_dict(),
        }
    fallback_messages: list[dict[str, Any]] = []
    if fallback is not None:
        slicer = getattr(fallback, "slice", None)
        if callable(slicer):
            sliced = slicer(int(group_id), seconds=window_seconds, now=stamp, limit=limit)
            fallback_messages = [dict(item) for item in (sliced.get("messages") or ())]
    return {
        "messages": fallback_messages,
        "count": len(fallback_messages),
        "source": "observer.window" if fallback is not None else "none",
        "since": stamp - window_seconds,
        "until": stamp,
        "window_seconds": window_seconds,
        "window": {},
        "outcome": outcome.as_dict(),
    }


def outcomes(items: Sequence[CallOutcome]) -> dict[str, Any]:
    """把一组调用结果压成摘要（``{name: status}`` + 计数），供返回值里的 ``downstream``。"""

    return {
        "calls": {item.name: item.status for item in items},
        "skipped": sorted(item.name for item in items if item.status == STATUS_SKIPPED),
        "failed": sorted(item.name for item in items if item.status == STATUS_FAILED),
    }


__all__ = [
    "DOWNSTREAM_NAMES",
    "STATUS_FAILED",
    "STATUS_OK",
    "STATUS_SKIPPED",
    "CallOutcome",
    "Throttle",
    "call_leaf",
    "maybe_call",
    "outcomes",
    "recent_messages",
    "registry_of",
]
