"""Safe management primitives shared by the Web and TUI consoles.

The panel is an operations surface, not an arbitrary database shell.  This module
therefore only exposes configuration and lifecycle operations with explicit
allow-lists, redaction and an in-memory audit trail.
"""

from __future__ import annotations

import asyncio
import contextlib
import copy
import os
import tempfile
import time
from collections import deque
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from sqlalchemy import text

from grouppig.infra.config.loader import LOCAL_CONFIG_FILE, Config, load_config
from grouppig.infra.config.validator import ValidationReport, validate_config

SECRET_MARKERS = ("secret", "password", "credential", "authorization")
SECRET_EXACT = {"key", "token", "api_key", "access_token", "refresh_token", "client_secret"}
AUDIT = deque(maxlen=300)


def is_secret_key(path: str) -> bool:
    # Inspect the leaf only: ``token.policies.*.max_output_tokens`` is a
    # budget setting, not a credential, while ``panel.token`` is protected.
    leaf = str(path).replace("[]", "").split(".")[-1].lower()
    return (
        leaf in SECRET_EXACT
        or leaf.endswith(("_secret", "_password", "_credential", "_authorization"))
        or any(marker in leaf for marker in SECRET_MARKERS)
    )


def redact(value: Any, path: str = "") -> Any:
    if is_secret_key(path):
        return "<redacted>" if value not in (None, "") else value
    if isinstance(value, Mapping):
        return {str(k): redact(v, f"{path}.{k}" if path else str(k)) for k, v in value.items()}
    if isinstance(value, list):
        return [redact(v, f"{path}[]") for v in value]
    return value


def _flatten(value: Mapping[str, Any], prefix: str = "") -> dict[str, Any]:
    """Flatten a JSON-like patch while retaining list item paths.

    Secret fields can appear inside provider/policy arrays, so treating a list
    as one scalar would let a sensitive leaf slip past the write guard.
    """
    out: dict[str, Any] = {}
    for key, item in value.items():
        path = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(item, Mapping):
            out.update(_flatten(item, path))
        elif isinstance(item, list):
            for index, child in enumerate(item):
                child_path = f"{path}[{index}]"
                if isinstance(child, Mapping):
                    out.update(_flatten(child, child_path))
                else:
                    out[child_path] = child
            if not item:
                out[path] = item
        else:
            out[path] = item
    return out


def _merge(base: dict[str, Any], patch: Mapping[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(base)
    for key, value in patch.items():
        if isinstance(value, Mapping) and isinstance(result.get(key), Mapping):
            result[key] = _merge(dict(result[key]), value)
        else:
            result[str(key)] = copy.deepcopy(value)
    return result


def _toml_value(value: Any) -> str:
    if value is None:
        return '""'
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return '"' + value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n") + '"'
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(_toml_value(item) for item in value) + "]"
    return _toml_value(str(value))


def to_toml(data: Mapping[str, Any]) -> str:
    """Small TOML writer for the JSON-like config tree used by GrouPig."""
    lines: list[str] = []

    def emit_table(table: Mapping[str, Any], prefix: str = "") -> None:
        scalars = [(str(k), v) for k, v in table.items() if not isinstance(v, Mapping)]
        nested = [(str(k), v) for k, v in table.items() if isinstance(v, Mapping)]
        if prefix and (scalars or not nested):
            lines.append(f"[{prefix}]")
        for key, value in scalars:
            lines.append(f"{key} = {_toml_value(value)}")
        if scalars:
            lines.append("")
        for key, value in nested:
            emit_table(value, f"{prefix}.{key}" if prefix else key)

    emit_table(data)
    return "\n".join(lines).rstrip() + "\n"


def record_audit(action: str, *, ok: bool = True, actor: str = "panel", **details: Any) -> dict[str, Any]:
    item = {"ts": time.time(), "time": time.strftime("%Y-%m-%d %H:%M:%S"), "action": action, "ok": ok, "actor": actor}
    item.update({str(k): redact(v, str(k)) for k, v in details.items()})
    AUDIT.append(item)
    return item


def audit_tail(limit: int = 100) -> list[dict[str, Any]]:
    return list(AUDIT)[-max(0, int(limit)) :]


def config_of(app: Any = None) -> Config | None:
    candidates = [getattr(getattr(app, "container", None), "config", None), getattr(app, "config", None)] if app else []
    for config in candidates:
        if isinstance(config, Config) or hasattr(config, "to_dict"):
            return config
    return None


def config_view(app: Any = None) -> dict[str, Any]:
    config = config_of(app)
    if config is None:
        return {"available": False, "data": {}, "fingerprint": None, "sources": []}
    data = config.to_dict() if hasattr(config, "to_dict") else dict(getattr(config, "raw", {}) or {})
    return {
        "available": True,
        "data": redact(data),
        "fingerprint": getattr(config, "fingerprint", None),
        "source": getattr(config, "source", None),
        "sources": list(getattr(config, "sources", ()) or ()),
        "editable": True,
        "secrets_policy": "secret/key/token/password fields are redacted and cannot be written by the panel",
    }


def validate_patch(app: Any, patch: Mapping[str, Any]) -> dict[str, Any]:
    blocked = sorted(path for path in _flatten(patch) if is_secret_key(path))
    config = config_of(app)
    if blocked:
        return {
            "ok": False,
            "error": "secret fields cannot be edited in the panel",
            "blocked": blocked,
            "validation": None,
        }
    if config is None:
        return {"ok": False, "error": "no active configuration", "blocked": [], "validation": None}
    candidate = (
        config.with_overrides(patch) if hasattr(config, "with_overrides") else Config(_merge(dict(config.raw), patch))
    )
    report: ValidationReport = validate_config(candidate)
    return {
        "ok": report.ok,
        "error": None if report.ok else "; ".join(str(i) for i in report.errors),
        "blocked": [],
        "validation": report.as_dict(),
        "fingerprint": candidate.fingerprint,
    }


def _install_config(app: Any, candidate: Config) -> None:
    """Install a validated config snapshot into the live runtime.

    The panel runs in an HTTP worker thread while the runtime owns the
    container. Config objects are immutable, so replacing the references is
    safe; components that cache the config are updated explicitly as well.
    """
    container = getattr(app, "container", None)
    if container is None:
        if app is not None and hasattr(app, "config"):
            app.config = candidate
        return

    container.config = candidate
    if app is not None and hasattr(app, "config"):
        app.config = candidate

    with contextlib.suppress(Exception):
        from grouppig.infra.config import loader

        loader.set_config(candidate)
    with contextlib.suppress(Exception):
        from grouppig.infra.token_budget import policy as budget_policy

        budget_policy.configure_policies(candidate)

    for component in (
        getattr(container, "router", None),
        getattr(container, "meter", None),
        getattr(app, "router", None),
        getattr(app, "meter", None),
    ):
        if component is not None and hasattr(component, "config"):
            component.config = candidate


def apply_patch(app: Any, patch: Mapping[str, Any], *, persist: bool = False) -> dict[str, Any]:
    check = validate_patch(app, patch)
    if not check["ok"]:
        record_audit("config.apply", ok=False, patch=patch, error=check.get("error"))
        return check
    config = config_of(app)
    assert config is not None
    candidate = config.with_overrides(patch)
    _install_config(app, candidate)
    saved = None
    if persist:
        saved = save_local_config(candidate, root=Path.cwd())
    result = {
        "ok": True,
        "changed": candidate.fingerprint != getattr(config, "fingerprint", None),
        "fingerprint": candidate.fingerprint,
        "persisted": saved,
    }
    record_audit("config.apply", patch=patch, persist=persist, result=result)
    return result


def save_local_config(config: Config, *, root: Path | None = None) -> str:
    root = root or Path.cwd()
    path = root / LOCAL_CONFIG_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    data = config.to_dict()

    # Never copy secret material into a local override file.
    def without_secrets(value: Any, path: str = "") -> Any:
        if isinstance(value, Mapping):
            return {
                str(k): without_secrets(v, f"{path}.{k}" if path else str(k))
                for k, v in value.items()
                if not is_secret_key(f"{path}.{k}" if path else str(k))
            }
        if isinstance(value, list):
            return [without_secrets(v, f"{path}[]") for v in value]
        return value

    filtered = without_secrets(data)
    fd, tmp_name = tempfile.mkstemp(prefix=".grouppig.local.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(to_toml(filtered))
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_name, path)
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(tmp_name)
    record_audit("config.save", path=str(path))
    return str(path)


def reload_config(app: Any = None) -> dict[str, Any]:
    config = config_of(app)
    path = getattr(config, "source", None) if config else None
    try:
        candidate = load_config(path)
        report = validate_config(candidate)
        if not report.ok:
            result = {"ok": False, "error": "; ".join(str(i) for i in report.errors), "validation": report.as_dict()}
        else:
            if app is not None:
                _install_config(app, candidate)
            result = {"ok": True, "fingerprint": candidate.fingerprint, "validation": report.as_dict()}
    except Exception as exc:  # noqa: BLE001
        result = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    record_audit("config.reload", ok=bool(result.get("ok")), result=result)
    return result


def table_rows(
    app: Any, name: str, *, limit: int = 50, offset: int = 0, loop: asyncio.AbstractEventLoop | None = None
) -> dict[str, Any]:
    """Read a whitelisted contract table through the app event loop."""
    from grouppig.memory.runtime.schema import CONTRACT_TABLES

    table = str(name)
    if table not in CONTRACT_TABLES:
        return {"ok": False, "error": f"unknown contract table: {table}"}
    database = getattr(getattr(app, "memory", None), "db", None)
    if database is None:
        return {"ok": False, "error": "database unavailable"}
    active_loop = loop
    if active_loop is None:
        task = getattr(database, "_task", None)
        active_loop = task.get_loop() if task is not None else None
    if active_loop is None or not active_loop.is_running():
        return {"ok": False, "error": "a running app event loop is required"}
    bounded_limit = max(1, min(200, int(limit)))
    bounded_offset = max(0, int(offset))

    async def fetch() -> list[dict[str, Any]]:
        rows = await database.fetch_all(
            text(f"SELECT * FROM {table} LIMIT :limit OFFSET :offset"),
            {"limit": bounded_limit, "offset": bounded_offset},
        )
        output: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            for key, value in list(item.items()):
                if (
                    key.lower() in {"text", "content", "message", "raw", "body", "reply", "prompt", "answer"}
                    and isinstance(value, str)
                    and len(value) > 12
                ):
                    item[key] = f"<已脱敏 {len(value)} 字>"
            output.append(item)
        return output

    try:
        rows = asyncio.run_coroutine_threadsafe(fetch(), active_loop).result(timeout=10)
        result = {"ok": True, "table": table, "limit": bounded_limit, "offset": bounded_offset, "rows": rows}
    except Exception as exc:  # noqa: BLE001
        result = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    record_audit("data.table.read", ok=bool(result.get("ok")), table=table, limit=bounded_limit, offset=bounded_offset)
    return result


def clear_events() -> dict[str, Any]:
    from grouppig.panel.snapshot import EVENTS

    before = len(EVENTS)
    EVENTS.clear()
    result = {"ok": True, "cleared": before}
    record_audit("events.clear", cleared=before)
    return result


def control_pump(app: Any, name: str, action: str, *, loop: asyncio.AbstractEventLoop | None = None) -> dict[str, Any]:
    pump = next((item for item in getattr(app, "pumps", ()) if str(getattr(item, "name", "")) == str(name)), None)
    if pump is None:
        return {"ok": False, "error": f"unknown pump: {name}"}
    if action not in {"start", "stop", "restart"}:
        return {"ok": False, "error": f"unsupported action: {action}"}
    try:
        if action in {"stop", "restart"}:
            task_loop = loop or (pump._task.get_loop() if getattr(pump, "_task", None) else None)
            if task_loop and task_loop.is_running():
                asyncio.run_coroutine_threadsafe(pump.stop(), task_loop).result(timeout=5)
            elif getattr(pump, "_task", None):
                return {"ok": False, "error": "pump belongs to another event loop"}
        if action in {"start", "restart"}:
            if loop and loop.is_running():
                loop.call_soon_threadsafe(pump.start)
            else:
                return {"ok": False, "error": "a running event loop is required to start a pump"}
        result = {"ok": True, "name": name, "action": action}
    except Exception as exc:  # noqa: BLE001
        result = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    record_audit(f"pump.{action}", ok=bool(result.get("ok")), pump=name)
    return result


__all__ = [
    "clear_events",
    "AUDIT",
    "apply_patch",
    "audit_tail",
    "config_view",
    "control_pump",
    "table_rows",
    "is_secret_key",
    "record_audit",
    "reload_config",
    "save_local_config",
    "to_toml",
    "validate_patch",
]
