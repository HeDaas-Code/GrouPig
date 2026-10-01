"""grouppig.panel.web —— 只读 Web 面板（标准库 ``http.server``）。

设计取舍：

* **零第三方依赖**：``http.server`` + 单页 HTML/原生 JS，不引 FastAPI/uvicorn；
* **默认只监听 127.0.0.1**：绑非本机地址必须显式 ``--panel-allow-remote``，并且会打一条
  醒目警告 —— 「不小心把运维面板开到公网」比多敲一个参数代价大得多；
* **可选共享密钥**：``--panel-token`` / ``[panel].token``；设了就要求
  ``X-Panel-Token`` 头或 ``?token=``（浏览器地址栏能用），否则 401；
* **Host 头白名单**：默认只认本机名，钝化 DNS rebinding（浏览器里的恶意页面
  用攻击者的域名解析到 127.0.0.1 来读面板）；
* **只读**：所有接口只 GET，写操作一律 501；
* **渲染与数据分离**：页面外壳是静态 HTML，数据一律走 ``/api/snapshot``，
  这样 TUI 与 Web 用的是同一份快照结构。

端点：``GET /`` 页面、``GET /api/snapshot`` 快照 JSON、``GET /api/health`` 健康摘要、
``GET /api/events`` 事件流尾部。
"""

from __future__ import annotations

import hmac
import ipaddress
import json
import sys
import threading
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from grouppig.panel.snapshot import EVENTS, SnapshotOptions, build_snapshot

#: 只认本机的主机名集合（``Host`` 头白名单的基底）。
LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1", "[::1]"})

#: 承载共享密钥的请求头（浏览器地址栏用 ``?token=``，脚本用这个头）。
TOKEN_HEADER = "X-Panel-Token"


class PanelBindError(RuntimeError):
    """面板拒绝绑定：绑了非本机地址却没有显式 opt-in。"""


def is_loopback_host(host: str) -> bool:
    """判断监听地址是不是本机（``localhost`` / 回环 IP；空串是「任意地址」，不算本机）。"""

    name = str(host or "").strip().lower()
    if name in ("localhost", "::1", "[::1]"):
        return True
    try:
        return ipaddress.ip_address(name.strip("[]")).is_loopback
    except ValueError:
        return False


def host_name_of(value: str) -> str:
    """从 ``Host`` 头里取出主机名（去掉端口，保留 IPv6 方括号）。"""

    text = str(value or "").strip()
    if text.startswith("["):
        end = text.find("]")
        return text[: end + 1] if end != -1 else text
    return text.rsplit(":", 1)[0] if ":" in text else text


@dataclass(frozen=True)
class PanelSettings:
    """面板的访问控制设置（零依赖的安全底线）。

    默认只监听本机、可选共享密钥、``Host`` 头白名单。
    """

    token: str | None = None
    allow_remote: bool = False
    #: ``Host`` 头白名单；``None`` 表示不校验（绑通配地址 + 显式 opt-in 时）。
    allowed_hosts: frozenset[str] | None = None

    @classmethod
    def for_bind(
        cls,
        host: str,
        *,
        token: str | None = None,
        allow_remote: bool = False,
    ) -> PanelSettings:
        """按监听地址推导设置；非本机地址需要 ``allow_remote``。"""

        if not is_loopback_host(host) and not allow_remote:
            raise PanelBindError(
                f"拒绝绑定非本机地址 {host!r}：面板无鉴权且会回显运行时状态。"
                "确实要对外暴露请显式加 --panel-allow-remote（并自行加反向代理与鉴权）"
            )
        wildcard = str(host or "").strip() in ("", "0.0.0.0", "::", "[::]")
        allowed: frozenset[str] | None
        if wildcard:
            # 绑通配地址时无法预知外部会用什么名字访问，白名单只会误伤。
            allowed = None
        else:
            allowed = LOOPBACK_HOSTS | {host_name_of(host).lower()}
        return cls(token=str(token) if token else None, allow_remote=bool(allow_remote), allowed_hosts=allowed)

    @classmethod
    def from_config(
        cls,
        config: Any = None,
        *,
        host: str = "127.0.0.1",
        token: str | None = None,
        allow_remote: bool = False,
    ) -> PanelSettings:
        """读 ``[panel]``（命令行显式参数优先），再按监听地址推导。"""

        if config is not None:
            if not token:
                configured = config.get("panel.token", None)
                token = str(configured) if configured else None
            if not allow_remote:
                allow_remote = bool(config.get("panel.allow_remote", False))
        return cls.for_bind(host, token=token, allow_remote=allow_remote)

    def check_host(self, value: str) -> bool:
        """``Host`` 头是否可信（``None`` 白名单 = 不校验）。"""

        if self.allowed_hosts is None:
            return True
        name = host_name_of(value).lower()
        return not name or name in self.allowed_hosts

    def check_token(self, supplied: str | None) -> bool:
        """共享密钥是否匹配（没设密钥就是公开只读面板）。"""

        if self.token is None:
            return True
        return hmac.compare_digest(str(supplied or ""), self.token)


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
// 设了共享密钥时地址栏带 ?token=…，页面内所有请求都把它带上。
const TOKEN = new URLSearchParams(location.search).get("token") || "";
const api = (path) => TOKEN ? path + (path.includes("?") ? "&" : "?") + "token=" + encodeURIComponent(TOKEN) : path;
async function refresh() {
  try {
    const s = await (await fetch(api("/api/snapshot"))).json();
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
    """把 ``app``、渲染逻辑与访问控制包在一起，便于测试与嵌入式启动。"""

    def __init__(
        self,
        app: Any = None,
        options: SnapshotOptions | None = None,
        settings: PanelSettings | None = None,
    ) -> None:
        self.app = app
        self.options = options or SnapshotOptions()
        self.settings = settings or PanelSettings()

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
            # 顺序：先 Host（DNS rebinding），再密钥；两者都不泄露任何运行时数据。
            if not panel.settings.check_host(self.headers.get("Host", "")):
                return self._deny(403, "Host 头不在白名单内")
            if not panel.settings.check_token(self._supplied_token(query)):
                return self._deny(401, "缺少或错误的共享密钥")
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

        def _supplied_token(self, query: dict[str, list[str]]) -> str | None:
            """密钥来源：请求头优先，其次 ``?token=``（浏览器地址栏能直接带）。"""

            header = self.headers.get(TOKEN_HEADER)
            if header:
                return header
            values = query.get("token") or []
            return values[0] if values else None

        def _deny(self, status: int, reason: str) -> None:
            self._send_json({"error": reason, "status": status}, status=status)

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
    token: str | None = None,
    allow_remote: bool = False,
    settings: PanelSettings | None = None,
    panel: PanelApp | None = None,
) -> ThreadingHTTPServer:
    """启动面板 HTTP 服务并返回服务器对象（调用方负责 ``shutdown``）。

    绑非本机地址且没显式 ``allow_remote`` 时抛 :class:`PanelBindError`；
    opt-in 之后打一条醒目警告（stderr，终端与日志都看得见）。

    ``panel`` 用于传入自定义 :class:`PanelApp`（例如 CLI 里那个能异步查表的子类）；
    它自带 ``settings``，但**绑定策略仍然在这里统一强制**，免得绕开这条底线。
    """

    if panel is None:
        resolved = settings or PanelSettings.for_bind(host, token=token, allow_remote=allow_remote)
        panel = PanelApp(app, options, resolved)
    else:
        resolved = panel.settings
    if not is_loopback_host(host) and not resolved.allow_remote:
        raise PanelBindError(
            f"拒绝绑定非本机地址 {host!r}：面板无鉴权且会回显运行时状态。"
            "确实要对外暴露请显式加 --panel-allow-remote（并自行加反向代理与鉴权）"
        )
    if not is_loopback_host(host):
        print(
            f"⚠️  GrouPig 面板正在监听非本机地址 {host}:{port}："
            "快照会暴露运行状态与事件摘要，请自行加反向代理 / 防火墙"
            + ("（已启用共享密钥）" if resolved.token else "（未设共享密钥，任何人都能读）"),
            file=sys.stderr,
        )
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
    token: str | None = None,
    allow_remote: bool = False,
) -> None:
    """阻塞式启动（CLI 用）；``KeyboardInterrupt`` 时优雅关闭。"""

    httpd = serve(app, host=host, port=port, options=options, token=token, allow_remote=allow_remote)
    address, actual_port = httpd.server_address[0], httpd.server_address[1]
    print(f"GrouPig 面板已启动：http://{address}:{actual_port}/（只读，Ctrl-C 退出）")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.shutdown()
        httpd.server_close()


__all__ = [
    "LOOPBACK_HOSTS",
    "PAGE",
    "TOKEN_HEADER",
    "PanelApp",
    "PanelBindError",
    "PanelSettings",
    "host_name_of",
    "is_loopback_host",
    "make_handler",
    "serve",
    "serve_forever",
]
