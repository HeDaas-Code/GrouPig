"""grouppig.infra.logger —— 运行日志与结构化追踪（``rpc:logger.log`` / ``rpc:logger.trace``）。

normify id: ``grouppig.infra.logger``。

* 结构化 JSON Lines 输出（默认 stderr，可落文件）
* ``trace`` 上下文管理器记录耗时，供 ``rpc:logger.trace`` 与业务代码使用
* 敏感字段脱敏（``api_key`` / ``token`` / ``authorization`` 等）
"""

from __future__ import annotations

import json
import logging
import sys
import time
import uuid
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TextIO

LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "WARN": 30, "ERROR": 40, "CRITICAL": 50}
SENSITIVE_KEYS = ("api_key", "apikey", "token", "access_token", "authorization", "password", "secret", "cookie")
REDACTED = "***"


def level_value(level: str | int) -> int:
    if isinstance(level, int):
        return level
    return LEVELS.get(str(level).upper(), 20)


def _redact(value: Any) -> Any:
    if isinstance(value, Mapping):
        out = {}
        for key, item in value.items():
            if isinstance(key, str) and any(s in key.lower() for s in SENSITIVE_KEYS):
                out[key] = REDACTED
            else:
                out[key] = _redact(item)
        return out
    if isinstance(value, (list, tuple)):
        return [_redact(v) for v in value]
    return value


@dataclass(frozen=True, slots=True)
class LogRecord:
    ts: float
    level: str
    event: str
    message: str = ""
    logger: str = "grouppig"
    trace_id: str | None = None
    span_id: str | None = None
    duration_ms: float | None = None
    fields: Mapping[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "ts": round(self.ts, 6),
            "time": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(self.ts)),
            "level": self.level,
            "logger": self.logger,
            "event": self.event,
        }
        if self.message:
            data["message"] = self.message
        if self.trace_id:
            data["trace_id"] = self.trace_id
        if self.span_id:
            data["span_id"] = self.span_id
        if self.duration_ms is not None:
            data["duration_ms"] = round(self.duration_ms, 3)
        if self.fields:
            data["fields"] = dict(self.fields)
        return data

    def to_json(self) -> str:
        return json.dumps(self.as_dict(), ensure_ascii=False, default=str)


class RuntimeLogger:
    """结构化运行日志器。"""

    def __init__(
        self,
        name: str = "grouppig",
        *,
        level: str | int = "INFO",
        json_output: bool = True,
        sink: TextIO | None = None,
        file: str | Path | None = None,
        trace_id: str | None = None,
        span_id: str | None = None,
        context: Mapping[str, Any] | None = None,
        on_record: Callable[[LogRecord], None] | None = None,
    ) -> None:
        self.name = name
        self.level = level_value(level)
        self.json_output = json_output
        self.sink = sink if sink is not None else sys.stderr
        self.file = Path(file) if file else None
        self.trace_id = trace_id
        self.span_id = span_id
        self.context: dict[str, Any] = dict(context or {})
        self.on_record = on_record
        self.records: list[LogRecord] = []
        self._file_handle: TextIO | None = None

    # ---- 配置 ----------------------------------------------------------
    def configure(
        self,
        *,
        level: str | int | None = None,
        json_output: bool | None = None,
        sink: TextIO | None = None,
        file: str | Path | None = None,
    ) -> RuntimeLogger:
        if level is not None:
            self.level = level_value(level)
        if json_output is not None:
            self.json_output = json_output
        if sink is not None:
            self.sink = sink
        if file is not None:
            self.file = Path(file) if file else None
            self._close_handle()
        return self

    def child(self, name: str | None = None, **context: Any) -> RuntimeLogger:
        merged = {**self.context, **context}
        return RuntimeLogger(
            name or self.name,
            level=self.level,
            json_output=self.json_output,
            sink=self.sink,
            file=self.file,
            trace_id=context.get("trace_id", self.trace_id),
            span_id=context.get("span_id", self.span_id),
            context=merged,
            on_record=self.on_record,
        )

    def bind(self, **context: Any) -> RuntimeLogger:
        return self.child(**context)

    # ---- 写入 ----------------------------------------------------------
    def log(self, level: str | int, event: str, message: str = "", **fields: Any) -> LogRecord:
        record = LogRecord(
            ts=time.time(),
            level=str(level).upper() if isinstance(level, str) else logging.getLevelName(level),
            event=event,
            message=message,
            logger=self.name,
            trace_id=fields.pop("trace_id", self.trace_id),
            span_id=fields.pop("span_id", self.span_id),
            duration_ms=fields.pop("duration_ms", None),
            fields=_redact({**self.context, **fields}),
        )
        self.records.append(record)
        if self.on_record is not None:
            self.on_record(record)
        if level_value(record.level) >= self.level:
            self._emit(record)
        return record

    def debug(self, event: str, message: str = "", **fields: Any) -> LogRecord:
        return self.log("DEBUG", event, message, **fields)

    def info(self, event: str, message: str = "", **fields: Any) -> LogRecord:
        return self.log("INFO", event, message, **fields)

    def warning(self, event: str, message: str = "", **fields: Any) -> LogRecord:
        return self.log("WARNING", event, message, **fields)

    warn = warning

    def error(self, event: str, message: str = "", **fields: Any) -> LogRecord:
        return self.log("ERROR", event, message, **fields)

    def exception(self, event: str, message: str = "", **fields: Any) -> LogRecord:
        import traceback

        return self.log("ERROR", event, message, traceback=traceback.format_exc(limit=5), **fields)

    def trace_event(self, name: str, duration_ms: float | None = None, **fields: Any) -> LogRecord:
        """记录一条结构化追踪（对应 ``rpc:logger.trace``）。"""

        return self.log("DEBUG", f"trace.{name}", "", duration_ms=duration_ms, **fields)

    @contextmanager
    def trace(self, name: str, **fields: Any) -> Iterator[LogRecord]:
        """``with logger.trace("model.chat", model=...): ...`` 记录耗时。"""

        started = time.perf_counter()
        record = LogRecord(
            ts=time.time(),
            level="DEBUG",
            event=f"trace.{name}",
            logger=self.name,
            trace_id=fields.pop("trace_id", self.trace_id) or uuid.uuid4().hex[:16],
            span_id=uuid.uuid4().hex[:16],
            fields=_redact({**self.context, **fields}),
        )
        try:
            yield record
        finally:
            duration = (time.perf_counter() - started) * 1000
            final = LogRecord(
                ts=record.ts,
                level=record.level,
                event=record.event,
                message=record.message,
                logger=record.logger,
                trace_id=record.trace_id,
                span_id=record.span_id,
                duration_ms=duration,
                fields=record.fields,
            )
            self.records.append(final)
            if self.on_record is not None:
                self.on_record(final)
            if level_value(final.level) >= self.level:
                self._emit(final)

    # ---- 内部 ----------------------------------------------------------
    def _emit(self, record: LogRecord) -> None:
        line = record.to_json() if self.json_output else self._format_text(record)
        target = self._handle() or self.sink
        if target is None:
            return
        try:
            target.write(line + "\n")
            target.flush()
        except Exception:  # pragma: no cover - 输出失败不得影响业务
            pass

    @staticmethod
    def _format_text(record: LogRecord) -> str:
        stamp = time.strftime("%H:%M:%S", time.localtime(record.ts))
        parts = [stamp, record.level, record.event]
        if record.message:
            parts.append(record.message)
        if record.duration_ms is not None:
            parts.append(f"{record.duration_ms:.1f}ms")
        if record.fields:
            parts.append(json.dumps(record.fields, ensure_ascii=False, default=str))
        return " ".join(parts)

    def _handle(self) -> TextIO | None:
        if self.file is None:
            return None
        if self._file_handle is None:
            self.file.parent.mkdir(parents=True, exist_ok=True)
            self._file_handle = self.file.open("a", encoding="utf-8")
        return self._file_handle

    def _close_handle(self) -> None:
        if self._file_handle is not None:
            self._file_handle.close()
            self._file_handle = None

    def close(self) -> None:
        self._close_handle()


# ---- 进程内默认日志器 ----------------------------------------------------
_default: RuntimeLogger = RuntimeLogger()


def configure_logging(
    level: str | int | None = None,
    *,
    json_output: bool | None = None,
    sink: TextIO | None = None,
    file: str | Path | None = None,
    config: Any | None = None,
    bridge_stdlib: bool = True,
) -> RuntimeLogger:
    """配置全局日志器；``config`` 为 :class:`~grouppig.infra.config.loader.Config` 时读 ``[logging]``。"""

    global _default
    if config is not None:
        level = level if level is not None else config.get("logging.level", "INFO")
        json_output = json_output if json_output is not None else bool(config.get("logging.json", True))
        file = file if file is not None else (config.get("logging.file") or None)
    _default.configure(level=level, json_output=json_output, sink=sink, file=file)
    if bridge_stdlib:
        _install_stdlib_bridge(_default)
    return _default


def get_logger(name: str | None = None, **context: Any) -> RuntimeLogger:
    """取全局日志器（可命名与绑定上下文字段）。"""

    return _default.child(name, **context) if (name or context) else _default


def set_logger(logger: RuntimeLogger) -> RuntimeLogger:
    global _default
    previous, _default = _default, logger
    return previous


class _StdlibBridge(logging.Handler):
    """把标准库 logging（第三方库日志）桥接到结构化日志器。"""

    def __init__(self, logger: RuntimeLogger) -> None:
        super().__init__()
        self.logger = logger

    def emit(self, record: logging.LogRecord) -> None:  # pragma: no cover - 由第三方库触发
        try:
            self.logger.log(
                record.levelname,
                f"stdlib.{record.name}",
                record.getMessage(),
                pathname=record.pathname,
                lineno=record.lineno,
            )
        except Exception:
            pass


def _install_stdlib_bridge(logger: RuntimeLogger) -> None:
    root = logging.getLogger()
    for handler in list(root.handlers):
        if isinstance(handler, _StdlibBridge):
            root.removeHandler(handler)
    root.addHandler(_StdlibBridge(logger))
    if root.level > logging.INFO:
        root.setLevel(logging.INFO)


# ---- rpc 处理器（由 runtime.di 注册） ------------------------------------
def log(level: str = "INFO", event: str = "", message: str = "", **fields: Any) -> dict[str, Any]:
    """``rpc:logger.log`` —— 记录运行日志。"""

    record = _default.log(level, event, message, **fields)
    return record.as_dict()


def trace(name: str, *, duration_ms: float | None = None, **fields: Any) -> dict[str, Any]:
    """``rpc:logger.trace`` —— 记录结构化追踪。"""

    record = _default.trace_event(name, duration_ms, **fields)
    return record.as_dict()


__all__ = [
    "LEVELS",
    "LogRecord",
    "REDACTED",
    "RuntimeLogger",
    "configure_logging",
    "get_logger",
    "log",
    "set_logger",
    "trace",
]
