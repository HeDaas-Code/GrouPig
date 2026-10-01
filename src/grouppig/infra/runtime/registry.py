"""grouppig.infra.runtime.registry —— ``rpc:`` / ``kafka:`` 名字到处理器的注册表。

契约纪律：注册的名字必须逐字出现在 ``normify-grouppig/api-index.json`` 中，
否则 :class:`~grouppig.infra.runtime.errors.UnknownNameError`。

用法（各域模块在自己的文件里自注册）::

    from grouppig.infra.runtime.registry import rpc, topic

    @rpc("model.chat")
    async def chat(messages, **kwargs): ...

    @topic("kafka:grouppig.session.completed")
    async def on_session_completed(event): ...

normify id: ``grouppig.infra.runtime.registry``（运行时补充模块，设计树暂无）。
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from grouppig.infra.runtime import contract
from grouppig.infra.runtime.errors import HandlerNotRegistered, UnknownNameError

Handler = Callable[..., Any]
_KINDS = ("rpc", "kafka", "mysql")


def kind_of(name: str) -> str:
    prefix = name.split(":", 1)[0]
    if prefix not in _KINDS:
        raise UnknownNameError(f"名字 {name!r} 缺少合法前缀（应为 rpc: / kafka: / mysql:）")
    return prefix


@dataclass(frozen=True)
class Registration:
    name: str
    handler: Handler
    kind: str
    module: str | None = None
    is_async: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "kind": self.kind,
            "module": self.module,
            "handler": f"{getattr(self.handler, '__module__', '?')}.{getattr(self.handler, '__qualname__', '?')}",
            "async": self.is_async,
        }


class Registry:
    """名字 → 处理器 的注册表（进程内 RPC 总线）。"""

    def __init__(self, *, validate_names: bool = True) -> None:
        self._handlers: dict[str, Registration] = {}
        self.validate_names = validate_names

    # ---- 注册 ----------------------------------------------------------
    def register(
        self,
        name: str,
        handler: Handler,
        *,
        module: str | None = None,
        replace: bool = False,
    ) -> Registration:
        if not callable(handler):
            raise TypeError(f"{name} 的处理器必须可调用，得到 {type(handler)!r}")
        if self.validate_names:
            contract.assert_known_name(name)
        if name in self._handlers and not replace:
            raise ValueError(
                f"名字 {name!r} 已由 {self._handlers[name].module or '?'} 注册（如需覆盖请传 replace=True）"
            )
        reg = Registration(
            name=name,
            handler=handler,
            kind=kind_of(name),
            module=module or getattr(handler, "__module__", None),
            is_async=inspect.iscoroutinefunction(handler),
        )
        self._handlers[name] = reg
        return reg

    def rpc(self, name: str, *, module: str | None = None, replace: bool = False) -> Callable[[Handler], Handler]:
        """装饰器：注册 ``rpc:<name>``。"""

        def decorate(handler: Handler) -> Handler:
            self.register(name, handler, module=module, replace=replace)
            return handler

        return decorate

    def topic(self, name: str, *, module: str | None = None, replace: bool = False) -> Callable[[Handler], Handler]:
        """装饰器：注册 ``kafka:<topic>`` 订阅处理器。"""

        def decorate(handler: Handler) -> Handler:
            self.register(name, handler, module=module, replace=replace)
            return handler

        return decorate

    # ---- 查询 / 调用 ---------------------------------------------------
    def get(self, name: str) -> Registration:
        try:
            return self._handlers[name]
        except KeyError as exc:
            raise HandlerNotRegistered(f"名字 {name!r} 尚未注册处理器") from exc

    def handler(self, name: str) -> Handler:
        return self.get(name).handler

    def has(self, name: str) -> bool:
        return name in self._handlers

    def names(self, *, kind: str | None = None) -> tuple[str, ...]:
        items = self._handlers.values() if kind is None else (r for r in self._handlers.values() if r.kind == kind)
        return tuple(sorted(r.name for r in items))

    def call(self, name: str, *args: Any, **kwargs: Any) -> Any:
        """同步调用（处理器必须是同步函数）。"""

        reg = self.get(name)
        if reg.is_async:
            raise TypeError(f"{name} 是协程处理器，请用 acall()")
        return reg.handler(*args, **kwargs)

    async def acall(self, name: str, *args: Any, **kwargs: Any) -> Any:
        """异步调用（同步处理器直接返回）。"""

        reg = self.get(name)
        result = reg.handler(*args, **kwargs)
        if inspect.isawaitable(result):
            return await result
        return result

    def describe(self) -> list[dict[str, Any]]:
        return [self._handlers[n].as_dict() for n in self.names()]

    def check_contract(self, *, scope: str | None = None) -> dict[str, list[str]]:
        """与 api-index.json 比对：``missing`` 是契约里有但没注册的名字。"""

        return contract.check_registry(set(self._handlers), scope=scope)

    def __len__(self) -> int:
        return len(self._handlers)

    def __contains__(self, name: object) -> bool:
        return name in self._handlers


default_registry = Registry()


def rpc(name: str, *, module: str | None = None, replace: bool = False, registry: Registry | None = None):
    target = registry if registry is not None else default_registry
    return target.rpc(name, module=module, replace=replace)


def topic(name: str, *, module: str | None = None, replace: bool = False, registry: Registry | None = None):
    target = registry if registry is not None else default_registry
    return target.topic(name, module=module, replace=replace)


__all__ = [
    "Handler",
    "Registration",
    "Registry",
    "default_registry",
    "kind_of",
    "rpc",
    "topic",
]
