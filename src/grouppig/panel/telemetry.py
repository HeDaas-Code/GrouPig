"""持久化的面板观测层。

这里保存的是可观测元数据，不是模型隐性思维链原文：事件、步骤、工具调用、
耗时、token、状态和脱敏后的摘要。实现刻意只依赖标准库，既能服务 Web/TUI，
也能在没有完整运行时的测试环境中独立工作。
"""

from __future__ import annotations

import contextlib
import json
import math
import os
import sqlite3
import threading
import time
import uuid
from collections import Counter, defaultdict
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from grouppig.panel.management import redact

DEFAULT_HISTORY_PATH = os.environ.get("GROUPPIG_PANEL_HISTORY", "var/grouppig-panel.sqlite3")


def _json(value: Any) -> str:
    return json.dumps(redact(value), ensure_ascii=False, default=str, separators=(",", ":"))


def _safe_payload(value: Any) -> Any:
    if isinstance(value, Mapping):
        return redact(dict(value))
    if isinstance(value, (list, tuple)):
        return [redact(v) for v in value]
    return redact(value)


class TelemetryStore:
    """线程安全 SQLite 观测存储，支持实时尾部、历史查询与轻量分析。"""

    def __init__(self, path: str | os.PathLike[str] | None = None, *, max_rows: int = 50_000) -> None:
        self.path = ":memory:" if path is None else str(path)
        if self.path != ":memory:":
            Path(self.path).expanduser().parent.mkdir(parents=True, exist_ok=True)
        self.max_rows = max(100, int(max_rows))
        self._lock = threading.RLock()
        self._db = sqlite3.connect(self.path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA busy_timeout=3000")
        self._init()

    def _init(self) -> None:
        with self._lock, self._db:
            self._db.executescript(
                """
                CREATE TABLE IF NOT EXISTS panel_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL NOT NULL,
                    topic TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT '',
                    event_id TEXT NOT NULL DEFAULT '',
                    correlation_id TEXT NOT NULL DEFAULT '',
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    summary TEXT NOT NULL DEFAULT ''
                );
                CREATE INDEX IF NOT EXISTS idx_panel_events_ts ON panel_events(ts);
                CREATE INDEX IF NOT EXISTS idx_panel_events_topic_ts ON panel_events(topic, ts);
                CREATE TABLE IF NOT EXISTS panel_spans (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trace_id TEXT NOT NULL,
                    span_id TEXT NOT NULL,
                    parent_span_id TEXT NOT NULL DEFAULT '',
                    ts REAL NOT NULL,
                    duration_ms REAL NOT NULL DEFAULT 0,
                    stage TEXT NOT NULL DEFAULT '',
                    component TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'ok',
                    model TEXT NOT NULL DEFAULT '',
                    tool_name TEXT NOT NULL DEFAULT '',
                    input_tokens INTEGER NOT NULL DEFAULT 0,
                    output_tokens INTEGER NOT NULL DEFAULT 0,
                    confidence REAL,
                    decision TEXT NOT NULL DEFAULT '',
                    summary TEXT NOT NULL DEFAULT '',
                    metadata_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_panel_spans_ts ON panel_spans(ts);
                CREATE INDEX IF NOT EXISTS idx_panel_spans_trace ON panel_spans(trace_id, ts);
                CREATE TABLE IF NOT EXISTS panel_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL NOT NULL,
                    name TEXT NOT NULL,
                    value REAL NOT NULL,
                    labels_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_panel_metrics_ts ON panel_metrics(ts);
                """
            )

    @staticmethod
    def _summary(payload: Any) -> str:
        if payload is None:
            return ""
        if isinstance(payload, Mapping):
            parts: list[str] = []
            for key, value in list(payload.items())[:12]:
                key_text = str(key)
                safe = redact(value, key_text)
                if isinstance(safe, (dict, list)):
                    shown = f"[{len(safe)}]" if isinstance(safe, list) else f"{{{len(safe)}}}"
                else:
                    shown = str(safe)
                parts.append(f"{key_text}={shown[:120]}")
            return " ".join(parts)
        return str(redact(payload))[:240]

    def record_event(
        self, event: Any = None, *, topic: str = "", payload: Any = None, source: str = "", ts: float | None = None
    ) -> dict[str, Any]:
        if event is not None:
            topic = str(getattr(event, "topic", topic) or topic)
            payload = getattr(event, "payload", payload)
            source = str(getattr(event, "source", source) or source)
            event_id = str(getattr(event, "event_id", "") or "")
            correlation_id = str(getattr(event, "correlation_id", "") or "")
            ts = float(getattr(event, "ts", ts or time.time()) or time.time())
        else:
            event_id = ""
            correlation_id = ""
            ts = float(ts or time.time())
        safe = _safe_payload(payload)
        summary = self._summary(safe)
        with self._lock, self._db:
            self._db.execute(
                "INSERT INTO panel_events(ts,topic,source,event_id,correlation_id,payload_json,summary) VALUES(?,?,?,?,?,?,?)",
                (ts, topic, source, event_id, correlation_id, _json(safe), summary),
            )
            self._trim("panel_events")
        return {"ts": ts, "topic": topic, "source": source, "event_id": event_id, "summary": summary, "payload": safe}

    def record_span(
        self,
        *,
        trace_id: str | None = None,
        span_id: str | None = None,
        parent_span_id: str = "",
        ts: float | None = None,
        duration_ms: float = 0,
        stage: str = "",
        component: str = "",
        status: str = "ok",
        model: str = "",
        tool_name: str = "",
        input_tokens: int = 0,
        output_tokens: int = 0,
        confidence: float | None = None,
        decision: str = "",
        summary: str = "",
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        trace_id = trace_id or f"trace-{uuid.uuid4().hex[:16]}"
        span_id = span_id or f"span-{uuid.uuid4().hex[:16]}"
        safe_meta = _safe_payload(metadata or {})
        row = {
            "trace_id": trace_id,
            "span_id": span_id,
            "parent_span_id": parent_span_id,
            "ts": float(ts or time.time()),
            "duration_ms": round(float(duration_ms or 0), 3),
            "stage": stage,
            "component": component,
            "status": status,
            "model": model,
            "tool_name": tool_name,
            "input_tokens": int(input_tokens or 0),
            "output_tokens": int(output_tokens or 0),
            "confidence": confidence,
            "decision": decision,
            "summary": str(redact(summary))[:500],
            "metadata": safe_meta,
        }
        with self._lock, self._db:
            self._db.execute(
                """INSERT INTO panel_spans(trace_id,span_id,parent_span_id,ts,duration_ms,stage,component,status,model,tool_name,input_tokens,output_tokens,confidence,decision,summary,metadata_json)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    row["trace_id"],
                    row["span_id"],
                    row["parent_span_id"],
                    row["ts"],
                    row["duration_ms"],
                    row["stage"],
                    row["component"],
                    row["status"],
                    row["model"],
                    row["tool_name"],
                    row["input_tokens"],
                    row["output_tokens"],
                    row["confidence"],
                    row["decision"],
                    row["summary"],
                    _json(safe_meta),
                ),
            )
            self._trim("panel_spans")
        return row

    def record_metric(
        self, name: str, value: float, *, labels: Mapping[str, Any] | None = None, ts: float | None = None
    ) -> None:
        with self._lock, self._db:
            self._db.execute(
                "INSERT INTO panel_metrics(ts,name,value,labels_json) VALUES(?,?,?,?)",
                (float(ts or time.time()), name, float(value), _json(labels or {})),
            )
            self._trim("panel_metrics")

    def _trim(self, table: str) -> None:
        self._db.execute(
            f"DELETE FROM {table} WHERE id NOT IN (SELECT id FROM {table} ORDER BY id DESC LIMIT ?)", (self.max_rows,)
        )

    @staticmethod
    def _window(since: float | None, until: float | None) -> tuple[float | None, float | None]:
        return (float(since) if since is not None else None, float(until) if until is not None else None)

    def events(
        self, *, since: float | None = None, until: float | None = None, topic: str | None = None, limit: int = 200
    ) -> list[dict[str, Any]]:
        since, until = self._window(since, until)
        clauses, args = [], []
        if since is not None:
            clauses.append("ts >= ?")
            args.append(since)
        if until is not None:
            clauses.append("ts <= ?")
            args.append(until)
        if topic:
            clauses.append("topic = ?")
            args.append(topic)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._lock:
            rows = self._db.execute(
                f"SELECT * FROM panel_events {where} ORDER BY ts DESC, id DESC LIMIT ?",
                (*args, max(1, min(2000, int(limit)))),
            ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            payload_json = item.pop("payload_json", None)
            if payload_json is not None:
                with contextlib.suppress(Exception):
                    item["payload"] = json.loads(payload_json)
            result.append(item)
        return result

    def spans(
        self,
        *,
        since: float | None = None,
        until: float | None = None,
        trace_id: str | None = None,
        stage: str | None = None,
        limit: int = 300,
    ) -> list[dict[str, Any]]:
        since, until = self._window(since, until)
        clauses, args = [], []
        if since is not None:
            clauses.append("ts >= ?")
            args.append(since)
        if until is not None:
            clauses.append("ts <= ?")
            args.append(until)
        if trace_id:
            clauses.append("trace_id = ?")
            args.append(trace_id)
        if stage:
            clauses.append("stage = ?")
            args.append(stage)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._lock:
            rows = self._db.execute(
                f"SELECT * FROM panel_spans {where} ORDER BY ts DESC, id DESC LIMIT ?",
                (*args, max(1, min(2000, int(limit)))),
            ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            with contextlib.suppress(Exception):
                item["metadata"] = json.loads(item.pop("metadata_json"))
            result.append(item)
        return result

    def metrics(
        self, *, since: float | None = None, until: float | None = None, bucket: int = 300
    ) -> list[dict[str, Any]]:
        now = time.time()
        since = float(since if since is not None else now - 3600)
        until = float(until if until is not None else now)
        bucket = max(10, int(bucket))
        with self._lock:
            events = self._db.execute(
                "SELECT ts,topic FROM panel_events WHERE ts BETWEEN ? AND ? ORDER BY ts", (since, until)
            ).fetchall()
            spans = self._db.execute(
                "SELECT ts,duration_ms,status,input_tokens,output_tokens,stage,tool_name FROM panel_spans WHERE ts BETWEEN ? AND ? ORDER BY ts",
                (since, until),
            ).fetchall()
        bins: dict[int, dict[str, Any]] = {}
        for row in events:
            b = int(float(row["ts"]) // bucket * bucket)
            item = bins.setdefault(b, self._blank_metric(b))
            item["events"] += 1
            item["topics"][row["topic"]] += 1
        for row in spans:
            b = int(float(row["ts"]) // bucket * bucket)
            item = bins.setdefault(b, self._blank_metric(b))
            item["spans"] += 1
            item["latency_ms"] += float(row["duration_ms"] or 0)
            item["input_tokens"] += int(row["input_tokens"] or 0)
            item["output_tokens"] += int(row["output_tokens"] or 0)
            item["errors"] += int(row["status"] not in ("ok", "success"))
            item["tools"] += int(bool(row["tool_name"]))
        out = []
        for b in sorted(bins):
            item = bins[b]
            item["topics"] = dict(item["topics"])
            out.append(item)
        return out

    @staticmethod
    def _blank_metric(bucket: int) -> dict[str, Any]:
        return {
            "ts": bucket,
            "events": 0,
            "spans": 0,
            "latency_ms": 0.0,
            "input_tokens": 0,
            "output_tokens": 0,
            "errors": 0,
            "tools": 0,
            "topics": Counter(),
        }

    def summary(self, *, since: float | None = None) -> dict[str, Any]:
        now = time.time()
        since = float(since if since is not None else now - 3600)
        with self._lock:
            e = self._db.execute("SELECT COUNT(*) n FROM panel_events WHERE ts >= ?", (since,)).fetchone()[0]
            s = self._db.execute(
                "SELECT COUNT(*) n, COALESCE(SUM(input_tokens),0) i, COALESCE(SUM(output_tokens),0) o, COALESCE(AVG(duration_ms),0) d, COALESCE(SUM(status != 'ok'),0) x FROM panel_spans WHERE ts >= ?",
                (since,),
            ).fetchone()
            topics = self._db.execute(
                "SELECT topic,COUNT(*) n FROM panel_events WHERE ts >= ? GROUP BY topic ORDER BY n DESC LIMIT 12",
                (since,),
            ).fetchall()
        return {
            "since": since,
            "until": now,
            "events": int(e),
            "spans": int(s[0]),
            "input_tokens": int(s[1]),
            "output_tokens": int(s[2]),
            "avg_latency_ms": round(float(s[3]), 2),
            "errors": int(s[4]),
            "top_topics": [{"topic": r[0], "count": int(r[1])} for r in topics],
        }

    def analytics(self, *, since: float | None = None) -> dict[str, Any]:
        now = time.time()
        since = float(since if since is not None else now - 86400)
        events = self.events(since=since, limit=2000)
        spans = self.spans(since=since, limit=2000)
        groups: dict[str, dict[str, float]] = defaultdict(
            lambda: {"events": 0, "triggers": 0, "errors": 0, "last_ts": 0}
        )
        for item in events:
            payload = item.get("payload") or {}
            gid = (
                str(payload.get("group_id") or payload.get("group") or "unknown")
                if isinstance(payload, Mapping)
                else "unknown"
            )
            row = groups[gid]
            row["events"] += 1
            row["last_ts"] = max(row["last_ts"], float(item.get("ts", 0)))
            row["triggers"] += int(
                any(word in str(item.get("topic", "")).lower() for word in ("trigger", "activation", "interrupt"))
            )
        for item in spans:
            key = str((item.get("metadata") or {}).get("group_id") or "unknown")
            groups[key]["errors"] += int(item.get("status") not in ("ok", "success"))
        if not groups:
            clusters = {"sample_count": 0, "status": "insufficient_data", "items": []}
        else:
            values = sorted(v["events"] for v in groups.values())
            med = values[len(values) // 2]
            items = []
            for gid, row in groups.items():
                label = (
                    "高活跃"
                    if row["events"] > med * 1.5
                    else ("低活跃" if row["events"] < max(1, med * 0.5) else "中活跃")
                )
                items.append({"group": gid, **row, "cluster": label})
            clusters = {
                "sample_count": len(items),
                "status": "ok" if len(items) >= 2 else "insufficient_data",
                "method": "按事件频率的确定性分层",
                "items": sorted(items, key=lambda x: (-x["events"], x["group"])),
            }
        metrics = self.metrics(since=since)
        xs = [float(m["events"]) for m in metrics]
        ys = [float(m["spans"]) for m in metrics]
        corr = _pearson(xs, ys) if len(xs) >= 3 else None
        correlations = {
            "sample_count": len(xs),
            "status": "ok" if corr is not None else "insufficient_data",
            "items": [{"left": "事件量", "right": "决策步骤量", "coefficient": round(corr, 4)}]
            if corr is not None
            else [],
        }
        return {
            "since": since,
            "until": now,
            "clusters": clusters,
            "correlations": correlations,
            "notes": ["分析只使用聚合元数据，不展示模型隐性思维链原文。", "样本不足时不会伪造结论。"],
        }

    def close(self) -> None:
        with self._lock:
            self._db.close()


def _pearson(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) != len(ys) or len(xs) < 3:
        return None
    mx = sum(xs) / len(xs)
    my = sum(ys) / len(ys)
    dx = [x - mx for x in xs]
    dy = [y - my for y in ys]
    den = math.sqrt(sum(v * v for v in dx) * sum(v * v for v in dy))
    return None if den == 0 else sum(a * b for a, b in zip(dx, dy, strict=True)) / den


_STORE: TelemetryStore | None = None
_STORE_LOCK = threading.Lock()


def get_store(path: str | os.PathLike[str] | None = None) -> TelemetryStore:
    global _STORE
    wanted = str(path or DEFAULT_HISTORY_PATH)
    with _STORE_LOCK:
        if _STORE is None or _STORE.path != wanted:
            if _STORE is not None and _STORE.path != ":memory:":
                _STORE.close()
            _STORE = TelemetryStore(wanted)
        return _STORE


def set_store(store: TelemetryStore | None) -> TelemetryStore | None:
    global _STORE
    with _STORE_LOCK:
        old, _STORE = _STORE, store
        return old


def record_event(event: Any = None, **kwargs: Any) -> dict[str, Any]:
    return get_store().record_event(event, **kwargs)


def record_span(**kwargs: Any) -> dict[str, Any]:
    return get_store().record_span(**kwargs)


__all__ = ["DEFAULT_HISTORY_PATH", "TelemetryStore", "get_store", "set_store", "record_event", "record_span"]
