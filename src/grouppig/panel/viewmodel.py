"""Web/TUI 共用的中文仪表盘视图模型。"""

from __future__ import annotations

import inspect
import time
from typing import Any

from grouppig.panel.management import audit_tail, config_view, redact
from grouppig.panel.plugins import discover_plugins
from grouppig.panel.snapshot import SnapshotOptions, build_snapshot
from grouppig.panel.telemetry import get_store

DASHBOARD_VERSION = 2


def _safe_call(obj: Any, method: str) -> dict[str, Any]:
    try:
        value = getattr(obj, method, None)
        result = value() if callable(value) else {}
        if inspect.isawaitable(result):
            close = getattr(result, "close", None)
            if callable(close):
                close()
            return {"unavailable": "异步健康检查需由运行时事件提供"}
        return dict(result) if isinstance(result, dict) else {"value": result}
    except Exception as exc:  # noqa: BLE001
        return {"error": f"{type(exc).__name__}: {exc}"}


def _domain_views(app: Any) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name, domain in (getattr(app, "domains", {}) or {}).items():
        if domain is None:
            result[name] = {"installed": False, "status": "missing"}
            continue
        health = _safe_call(domain, "health")
        if not health or "error" in health:
            health = _safe_call(domain, "status")
        result[name] = {"installed": True, "status": "ok" if "error" not in health else "degraded", "health": health}
    return result


def _activation(app: Any) -> dict[str, Any]:
    perception = getattr(app, "perception", None)
    health = _safe_call(perception, "health") if perception is not None else {}
    activation = dict(health.get("activation") or {})
    memory = dict(activation.get("memory") or {})
    energy = dict(activation.get("energy") or {})
    safe_events = []
    for event in (activation.get("events") or [])[-30:]:
        if isinstance(event, dict):
            safe_events.append(
                {k: redact(v, k) for k, v in event.items() if k not in {"messages", "rows", "content", "text"}}
            )
    return {
        "available": bool(activation),
        "enabled": activation.get("enabled", False),
        "bot_names": activation.get("bot_names", []),
        "interest_tags": activation.get("interest_tags", []),
        "weights": activation.get("weights", {}),
        "energy": energy,
        "memory": {
            "count": memory.get("count", memory.get("items", 0)),
            "stable": memory.get("stable", memory.get("promoted", 0)),
            "groups": memory.get("groups", []),
            "fragments": [
                {k: redact(v, k) for k, v in item.items() if k not in {"content", "normalized"}}
                for item in (memory.get("fragments") or [])[-30:]
                if isinstance(item, dict)
            ],
        },
        "events": safe_events,
        "groups": activation.get("groups", []),
    }


def _runtime_observability(app: Any, store: Any, window_seconds: int = 3600) -> dict[str, Any]:
    router = getattr(getattr(app, "container", None), "router", None)
    meter = getattr(getattr(app, "container", None), "meter", None)
    reporter = getattr(getattr(app, "container", None), "reporter", None)
    model = _safe_call(router, "health") if router is not None else {}
    budget = {}
    if meter is not None:
        try:
            budget = meter.snapshot().as_dict()
        except Exception as exc:
            budget = {"error": f"{type(exc).__name__}: {exc}"}
    usage = {}
    if reporter is not None:
        try:
            usage = reporter.report(since=time.time() - 86400, limit=30)
        except Exception as exc:
            usage = {"error": f"{type(exc).__name__}: {exc}"}
    return {"model": model, "budget": budget, "usage": usage, "history": store.summary(since=time.time() - window_seconds)}


def build_dashboard_snapshot(
    app: Any = None, options: SnapshotOptions | None = None, *, window_seconds: int = 3600
) -> dict[str, Any]:
    base = build_snapshot(app, options)
    now = time.time()
    window_seconds = max(300, min(7 * 86400, int(window_seconds)))
    app_status = dict(base.get("app") or {})
    domains = _domain_views(app) if app is not None else {}
    pumps = []
    for pump in base.get("pumps") or []:
        item = dict(pump)
        item["status"] = "running" if item.get("running") else ("failed" if item.get("errors") else "stopped")
        pumps.append(item)
    missing = list((base.get("contract") or {}).get("missing") or [])
    alerts: list[dict[str, Any]] = []
    if missing:
        alerts.append({"level": "error", "code": "contract.missing", "message": f"缺少 {len(missing)} 个契约条目"})
    for pump in pumps:
        if pump.get("errors"):
            alerts.append(
                {
                    "level": "warn",
                    "code": f"pump.{pump.get('name')}",
                    "message": pump.get("last_error") or "后台泵有错误",
                }
            )
    activation = _activation(app) if app is not None else {"available": False, "energy": {}}
    energy = activation.get("energy") or {}
    try:
        if energy and float(energy.get("global_energy", 1.0) or 0.0) < float(energy.get("reserve", 0.15) or 0.15):
            alerts.append({"level": "warn", "code": "activation.energy_low", "message": "全局精力低于保留阈值"})
    except (TypeError, ValueError):
        pass
    table_data = (base.get("tables") or {}).get("tables") or {}
    store = get_store()
    telemetry = (
        _runtime_observability(app, store, window_seconds)
        if app is not None
        else {"history": store.summary(since=now - window_seconds)}
    )
    spans = store.spans(since=now - window_seconds, limit=300)
    tool_spans = [
        s
        for s in spans
        if s.get("tool_name")
        or "tool" in str(s.get("stage", "")).lower()
        or "tool" in str(s.get("component", "")).lower()
    ]
    bucket = max(60, min(3600, window_seconds // 24 or 60))
    history_metrics = store.metrics(since=now - window_seconds, bucket=bucket)
    analysis = store.analytics(since=now - 86400)
    overview = {
        "status": "ok" if not alerts else ("critical" if any(a["level"] == "error" for a in alerts) else "warn"),
        "started": bool(app_status.get("started")),
        "uptime": app_status.get("uptime", 0),
        "domain_count": len(domains),
        "healthy_domains": sum(1 for d in domains.values() if d.get("status") == "ok"),
        "running_pumps": sum(1 for p in pumps if p.get("running")),
        "event_count": len(base.get("events") or []),
        "table_rows": sum(v for v in table_data.values() if isinstance(v, int)),
        "energy": energy,
        "history_events": telemetry.get("history", {}).get("events", 0),
        "history_spans": telemetry.get("history", {}).get("spans", 0),
        "model_calls": (telemetry.get("usage") or {}).get("calls", 0),
        "tool_calls": len(tool_spans),
    }
    plugins = discover_plugins(app)
    dashboard = {
        "dashboard_version": DASHBOARD_VERSION,
        "generated_at": now,
        "overview": overview,
        "runtime": {
            "app": app_status,
            "registry": base.get("registry", {}),
            "contract": base.get("contract", {}),
            "options": base.get("options", {}),
            "observability": telemetry,
        },
        "domains": domains,
        "pumps": pumps,
        "activation": activation,
        "energy": energy,
        "events": base.get("events", []),
        "memory": {"tables": base.get("tables", {}), "activation": activation.get("memory", {})},
        "tables": base.get("tables", {}),
        "traces": {"spans": spans},
        "tools": {
            "spans": tool_spans,
            "count": len(tool_spans),
            "errors": sum(1 for s in tool_spans if s.get("status") not in ("ok", "success")),
        },
        "history": {"window_seconds": window_seconds, "metrics": history_metrics, "summary": telemetry.get("history", {})},
        "analysis": analysis,
        "plugins": plugins.manifests(),
        "config": config_view(app),
        "alerts": alerts,
        "audit": audit_tail(100),
        "app": base.get("app", {}),
        "contract": base.get("contract", {}),
        "registry": base.get("registry", {}),
    }
    return dashboard


__all__ = ["DASHBOARD_VERSION", "build_dashboard_snapshot"]
