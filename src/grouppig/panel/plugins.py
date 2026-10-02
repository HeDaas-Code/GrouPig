"""可插拔 Web 面板模块协议。

业务原子模块只需导出 ``register_panel_plugin(registry)`` 或在运行时通过
``app.panel_plugins`` 提供插件，主面板就能自动增加一个标签与数据端点。核心
不扫描任意 Python 文件，避免 import 副作用。
"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

SnapshotProvider = Callable[[Any], Mapping[str, Any]]
HistoryProvider = Callable[[Any, float | None, float | None], Mapping[str, Any]]


@dataclass(frozen=True)
class PanelPlugin:
    key: str
    title: str
    description: str = ""
    order: int = 100
    icon: str = "▣"
    snapshot: SnapshotProvider | None = None
    history: HistoryProvider | None = None
    schema: Mapping[str, Any] = field(default_factory=dict)
    source: str = "builtin"

    def manifest(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "title": self.title,
            "description": self.description,
            "order": self.order,
            "icon": self.icon,
            "source": self.source,
            "schema": dict(self.schema),
        }


class PanelPluginRegistry:
    def __init__(self) -> None:
        self._plugins: dict[str, PanelPlugin] = {}

    def register(self, plugin: PanelPlugin | None = None, **kwargs: Any) -> PanelPlugin:
        item = plugin if plugin is not None else PanelPlugin(**kwargs)
        key = str(item.key).strip().lower()
        if not key or "/" in key or " " in key:
            raise ValueError(f"非法面板插件 key: {item.key!r}")
        if key in self._plugins:
            raise ValueError(f"面板插件 key 重复: {key}")
        if key != item.key:
            item = PanelPlugin(**{**item.__dict__, "key": key})
        self._plugins[key] = item
        return item

    def get(self, key: str) -> PanelPlugin | None:
        return self._plugins.get(str(key).lower())

    def all(self) -> list[PanelPlugin]:
        return sorted(self._plugins.values(), key=lambda item: (item.order, item.key))

    def manifests(self) -> list[dict[str, Any]]:
        return [item.manifest() for item in self.all()]


def _domain_snapshot(domain: Any, app: Any) -> dict[str, Any]:
    if domain is None:
        return {"available": False}
    for name in ("panel_snapshot", "health", "status"):
        value = getattr(domain, name, None)
        if callable(value):
            try:
                result = value()
                if inspect.isawaitable(result):
                    close = getattr(result, "close", None)
                    if callable(close):
                        close()
                    return {"available": False, "reason": "异步数据请通过运行时事件提供"}
                return dict(result) if isinstance(result, Mapping) else {"value": result}
            except Exception as exc:  # noqa: BLE001
                return {"available": False, "error": f"{type(exc).__name__}: {exc}"}
    return {"available": True}


def builtin_plugins() -> PanelPluginRegistry:
    registry = PanelPluginRegistry()
    registry.register(PanelPlugin("overview", "总览", "运行状态、实时统计与告警", 0, "◈"))
    registry.register(PanelPlugin("activation", "激活与精力", "激活因子、事件线、精力与记忆碎片", 10, "◎"))
    registry.register(PanelPlugin("memory", "记忆系统", "原子记忆、表存储与长期晋升", 20, "◇"))
    registry.register(PanelPlugin("events", "事件时间线", "实时事件和可检索历史事件", 30, "⋮"))
    registry.register(PanelPlugin("traces", "决策链", "结构化步骤摘要，不展示隐性思维链", 40, "↯"))
    registry.register(PanelPlugin("tools", "工具链", "工具调用、成功率、延迟与失败", 50, "⌁"))
    registry.register(PanelPlugin("analysis", "历史分析", "趋势图、聚类分析与关联性分析", 60, "∷"))
    registry.register(PanelPlugin("config", "配置与审计", "固定键中文化的配置和操作审计", 90, "⚙"))
    return registry


def discover_plugins(app: Any = None) -> PanelPluginRegistry:
    registry = builtin_plugins()
    candidates: list[Any] = []
    if app is not None:
        candidates.extend(getattr(app, "panel_plugins", ()) or ())
        candidates.extend(getattr(getattr(app, "container", None), "panel_plugins", ()) or ())
        for domain in (getattr(app, "domains", {}) or {}).values():
            provider = getattr(domain, "panel_plugin", None)
            if provider is not None:
                candidates.append(provider)
    for candidate in candidates:
        try:
            if callable(candidate) and not isinstance(candidate, PanelPlugin):
                result = candidate(registry)
                if isinstance(result, PanelPlugin):
                    registry.register(result)
            elif isinstance(candidate, PanelPlugin):
                registry.register(candidate)
        except ValueError:
            raise
        except Exception:
            continue
    return registry


def plugin_payload(
    plugin: PanelPlugin,
    app: Any,
    dashboard: Mapping[str, Any],
    store: Any,
    *,
    since: float | None = None,
    until: float | None = None,
) -> dict[str, Any]:
    key = plugin.key
    builtin = {
        "overview": dashboard.get("overview", {}),
        "activation": dashboard.get("activation", {}),
        "memory": dashboard.get("memory", {}),
        "events": {"events": dashboard.get("events", [])},
        "traces": {"spans": dashboard.get("traces", {}).get("spans", [])},
        "tools": dashboard.get("tools", {}),
        "analysis": dashboard.get("analysis", {}),
        "config": {"config": dashboard.get("config", {}), "audit": dashboard.get("audit", [])},
    }
    if key in builtin:
        data = builtin[key]
    elif plugin.snapshot is not None:
        result = plugin.snapshot(app)
        data = dict(result) if isinstance(result, Mapping) else {"value": result}
    else:
        data = {"available": False, "reason": "插件未提供 snapshot"}
    return {"plugin": plugin.manifest(), "generated_at": dashboard.get("generated_at"), "data": data}


__all__ = ["PanelPlugin", "PanelPluginRegistry", "builtin_plugins", "discover_plugins", "plugin_payload"]
