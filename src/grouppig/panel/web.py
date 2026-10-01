"""grouppig.panel.web —— 只读 Web 面板（标准库 ``http.server``）。

设计取舍：

* **零第三方依赖**：``http.server`` + 单页 HTML/原生 JS，不引 FastAPI/uvicorn；
* **默认只监听 127.0.0.1**：对外暴露必须显式 ``--host``，并自行加反向代理与鉴权；
* **只读**：所有接口只 GET，写操作（手动回复 / 归档会话）不在本变更范围；
* **渲染与数据分离**：页面外壳是静态 HTML，数据一律走 ``/api/snapshot``，
  这样 TUI 与 Web 用的是同一份快照结构。

端点：``GET /`` 页面、``GET /api/snapshot`` 快照 JSON、``GET /api/health`` 健康摘要、
``GET /api/events`` 事件流尾部。
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from grouppig.panel.snapshot import EVENTS, SnapshotOptions, build_snapshot

#: 单页外壳：只做布局与轮询，不做业务；样式内联以免额外静态文件。
PAGE = """<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8">
<title>GrouPig 面板</title>
<style>
body{font:13px/1.6 ui-monospace,Menlo,Consolas,monospace;margin:0;background:#0f1116;color:#d7dae0}
header{padding:10px 16px;background:#161a22;border-bottom:1px solid #262c38;display:flex;gap:16px;align-items:center}
h1{font-size:14px;margin:0;font-weight:600}
main{padding:16px;display:grid;gap:16px;grid-template-columns:repeat(auto-fit,minmax(320px,1fr))}
section{background:#161a22;border:1px solid #262c38;border-radius:6px;padding:12px}
h2{font-size:12px;margin:0 0 8px;color:#8b93a7;text-transform:uppercase;letter-spacing:.08em}
pre{margin:0;white-space:pre-wrap;word-break:break-all}
.ok{color:#7ee787}.bad{color:#ff7b72}.muted{color:#8b93a7}
table{width:100%;border-collapse:collapse}td{padding:2px 0;border-bottom:1px solid #1e2430}
</style></head><body>
<header><h1>GrouPig 面板</h1><span id="status" class="muted">加载中…</span>
<span id="clock" class="muted"></span></header>
<main>
<section><h2>运行状态</h2><pre id="app">-</pre></section>
<section><h2>契约自检</h2><pre id="contract">-</pre></section>
<section><h2>域 / 泵</h2><pre id="domains">-</pre></section>
<section><h2>数据表行数</h2><table id="tables"></table></section>
<section><h2>事件流</h2><pre id="events">-</pre></section>
</main>
<script>
const $ = (id) => document.getElementById(id);
const fmt = (v) => JSON.stringify(v, null, 1);
async function refresh() {
  try {
    const s = await (await fetch("/api/snapshot")).json();
    $("app").textContent = fmt(s.app);
    $("contract").textContent = fmt(s.contract);
    $("domains").textContent = fmt({ domains: s.domains, pumps: s.pumps });
    const t = s.tables && s.tables.tables ? s.tables.tables : {};
    $("tables").innerHTML = Object.entries(t).map(
      ([k, v]) => `<tr><td>${k}</td><td>${typeof v === "number" ? v : fmt(v)}</td></tr>`).join("");
    $("events").textContent = (s.events || []).slice().reverse()
      .map((e) => `${e.time} ${e.topic} ${e.summary}`).join("\n") || "(暂无事件)";
    $("status").textContent = s.app && s.app.started ? "运行中" : "未启动";
    $("status").className = s.app && s.app.started ? "ok" : "bad";
    $("clock").textContent = new Date(s.generated_at * 1000).toLocaleTimeString();
  } catch (err) { $("status").textContent = "取数失败：" + err; $("status").className = "bad"; }
}
refresh(); setInterval(refresh, 2000);
</script></body></html>"""


class PanelApp:
    """把 ``app`` 与渲染逻辑包在一起，便于测试与嵌入式启动。"""

    def __init__(self, app: Any = None, options: SnapshotOptions | None = None) -> None:
        self.app = app
        self.options = options or SnapshotOptions()

    def snapshot(self) -> dict[str, Any]:
        return build_snapshot(self.app, self.options)

    def health(self) -> dict[str, Any]:
        snapshot = self.snapshot()
        return {
            "started": bool(snapshot.get("app", {}).get("started")),
            "contract_missing": len(snapshot.get("contract", {}).get("missing", []) or []),
            "registry_total": snapshot.get("registry", {}).get("total", 0),
        }


def make_handler(panel: PanelApp) -> type[BaseHTTPRequestHandler]:
    """构造绑定到 ``panel`` 的请求处理器（标准库工厂模式）。"""

    class Handler(BaseHTTPRequestHandler):
        server_version = "GrouPigPanel/1"

        def do_GET(self) -> None:  # noqa: N802 - 标准库回调名
            route = urlparse(self.path)
            query = parse_qs(route.query)
            if route.path in ("/", "/index.html"):
                return self._send(200, PAGE, "text/html; charset=utf-8")
            if route.path == "/api/snapshot":
                return self._send_json(panel.snapshot())
            if route.path == "/api/health":
                return self._send_json(panel.health())
            if route.path == "/api/events":
                limit = int((query.get("limit") or ["50"])[0])
                return self._send_json({"events": EVENTS.tail(limit)})
            return self._send_json({"error": "not found", "path": route.path}, status=404)

        def _send_json(self, payload: Any, *, status: int = 200) -> None:
            body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
            self._send(status, body, "application/json; charset=utf-8")

        def _send(self, status: int, body: Any, content_type: str) -> None:
            data = body.encode("utf-8") if isinstance(body, str) else body
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003 - 标准库签名
            return  # 面板自己不打访问日志，避免污染运行日志

    return Handler


def serve(
    app: Any = None,
    *,
    host: str = "127.0.0.1",
    port: int = 8848,
    options: SnapshotOptions | None = None,
    ready: threading.Event | None = None,
) -> ThreadingHTTPServer:
    """启动面板 HTTP 服务并返回服务器对象（调用方负责 ``shutdown``）。"""

    panel = PanelApp(app, options)
    httpd = ThreadingHTTPServer((host, port), make_handler(panel))
    if ready is not None:
        ready.set()
    return httpd


def serve_forever(
    app: Any = None,
    *,
    host: str = "127.0.0.1",
    port: int = 8848,
    options: SnapshotOptions | None = None,
) -> None:
    """阻塞式启动（CLI 用）；``KeyboardInterrupt`` 时优雅关闭。"""

    httpd = serve(app, host=host, port=port, options=options)
    address, actual_port = httpd.server_address[0], httpd.server_address[1]
    print(f"GrouPig 面板已启动：http://{address}:{actual_port}/（只读，Ctrl-C 退出）")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.shutdown()
        httpd.server_close()


__all__ = ["PAGE", "PanelApp", "make_handler", "serve", "serve_forever"]
