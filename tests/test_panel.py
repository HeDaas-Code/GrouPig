"""grouppig.panel 域测试：快照层 + Web 端点 + TUI 降级。

只读面板不参与业务链路，因此这里全部是自包含测试：

* 快照：无 app 也要出合法结构；有 app 时要带 contract/registry/domains/pumps；
* 事件：环形缓冲有上限、尾部顺序正确、负载压成一行摘要；
* Web：真实起服务打端点（``/``、``/api/snapshot``、``/api/health``、``/api/events``、404）；
* TUI：无 TTY 时降级成文本而不是抛异常。
"""

from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request

import pytest

from grouppig.panel import tui, web
from grouppig.panel.snapshot import (
    EVENT_BUFFER_SIZE,
    SNAPSHOT_VERSION,
    EventRing,
    SnapshotOptions,
    build_snapshot,
    registry_summary,
)


@pytest.fixture
async def panel_app():
    """全装配的 app（不连 OneBot），面板的主要输入。

    刻意用**隔离注册表**：``build_app`` 默认往全局 registry 写名字，
    会污染 ``default_registry``，让「隔离注册表不外泄」这类测试在全量运行下失败。
    """

    from grouppig.infra.runtime.registry import Registry
    from grouppig.runtime.app import build_app

    app = build_app(registry=Registry(), dsn="sqlite+aiosqlite:///:memory:", connect=False)
    await app.start()
    try:
        yield app
    finally:
        await app.aclose()


# ---- 快照 --------------------------------------------------------------
def test_snapshot_without_app_is_well_formed():
    snapshot = build_snapshot(None)
    assert snapshot["version"] == SNAPSHOT_VERSION
    assert snapshot["app"]["started"] is False
    assert set(["contract", "registry", "domains", "pumps", "events"]) <= set(snapshot)
    json.dumps(snapshot, ensure_ascii=False)  # 必须可序列化


async def test_snapshot_reports_contract_and_domains(panel_app):
    """全装配的 app：契约零缺口、注册表非空、六域都在。"""

    snapshot = build_snapshot(panel_app, SnapshotOptions(include_tables=False))
    assert snapshot["contract"]["missing"] == []
    assert snapshot["registry"]["total"] > 0
    assert snapshot["domains"]["gateway"] is True
    assert snapshot["app"]["started"] is True
    assert snapshot["tables"] == {}  # include_tables=False 时不查表（保持空壳，前端结构稳定）


async def test_snapshot_accepts_bare_container(container):
    """容器没有 contract_check / domains：退化成按注册表比对，且不许抛错。"""

    snapshot = build_snapshot(container, SnapshotOptions(include_tables=False))
    assert isinstance(snapshot["contract"]["missing"], list)
    assert snapshot["registry"]["total"] > 0
    assert snapshot["domains"] == {}


def test_registry_summary_groups_by_prefix():
    class FakeRegistry:
        def __iter__(self):
            return iter(["rpc:a", "rpc:b", "kafka:c", "plain"])

    class FakeContainer:
        registry = FakeRegistry()

    summary = registry_summary(FakeContainer())
    assert summary["total"] == 4
    assert summary["by_prefix"] == {"rpc": 2, "kafka": 1, "other": 1}


# ---- 事件环 ------------------------------------------------------------
def test_event_ring_keeps_tail_and_bounds_size():
    ring = EventRing(size=3)
    for index in range(5):
        ring.record("kafka:grouppig.topic.changed", {"index": index, "text": "x" * 10}, source="t")
    assert len(ring) == 3
    tail = ring.tail(2)
    assert [item["summary"] for item in tail] == ["index=3 text=xxxxxxxxxx", "index=4 text=xxxxxxxxxx"]
    assert tail[0]["topic"] == "kafka:grouppig.topic.changed"
    assert ring.tail(0) == []


def test_event_ring_summarizes_payloads():
    ring = EventRing()
    item = ring.record("t", {"a": 1, "b": [1, 2, 3], "c": {"d": 1}, "e": None})
    assert item["summary"] == "a=1 b=[3] c={1} e=None"
    assert ring.record("t2", "raw text")["summary"] == "raw text"
    assert ring.record("t3", None)["summary"] == ""
    assert EVENT_BUFFER_SIZE >= 50


# ---- Web ---------------------------------------------------------------
@pytest.fixture
def panel_server():
    httpd = web.serve(None, port=0)
    port = httpd.server_address[1]
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)


def _get(url: str) -> tuple[int, str, str]:
    with urllib.request.urlopen(url, timeout=10) as response:
        return response.status, response.headers.get("Content-Type", ""), response.read().decode("utf-8")


def test_web_serves_page_and_snapshot(panel_server):
    status, content_type, body = _get(panel_server + "/")
    assert status == 200
    assert content_type.startswith("text/html")
    assert "GrouPig" in body and "/api/snapshot" in body

    status, content_type, body = _get(panel_server + "/api/snapshot")
    assert status == 200 and content_type.startswith("application/json")
    snapshot = json.loads(body)
    assert snapshot["version"] == SNAPSHOT_VERSION


def test_web_health_and_events_endpoints(panel_server):
    status, _, body = _get(panel_server + "/api/health")
    assert status == 200
    health = json.loads(body)
    assert set(health) == {"started", "contract_missing", "registry_total"}

    status, _, body = _get(panel_server + "/api/events?limit=1")
    assert status == 200
    assert set(json.loads(body)) == {"events"}


def test_web_unknown_path_returns_404(panel_server):
    with pytest.raises(urllib.error.HTTPError) as excinfo:
        _get(panel_server + "/nope")
    assert excinfo.value.code == 404


def test_web_handler_rejects_writes(panel_server):
    request = urllib.request.Request(panel_server + "/api/snapshot", method="POST", data=b"{}")
    with pytest.raises(urllib.error.HTTPError) as excinfo:
        urllib.request.urlopen(request, timeout=10)
    assert excinfo.value.code in (501, 405)  # 标准库对未实现的 do_POST 返回 501


def test_web_serves_live_snapshot_endpoint(panel_app):
    """面板在真实 app 上要能反映出域、注册表与表行数。"""

    panel = web.PanelApp(panel_app, SnapshotOptions())
    snapshot = panel.snapshot()
    assert snapshot["app"]["started"] is True
    assert snapshot["registry"]["total"] > 0
    assert snapshot["domains"]["gateway"] is True
    assert panel.health()["started"] is True


# ---- CLI ---------------------------------------------------------------
def test_cli_snapshot_json(capsys, monkeypatch):
    """``python -m grouppig.panel --snapshot --json`` 必须输出合法 JSON。

    CLI 会自己装配 app：这里把全局 registry 换成本次调用专用的实例，
    避免污染 ``default_registry``（会破坏「隔离注册表不外泄」那类测试）。
    """

    from grouppig.infra.runtime import di as di_module
    from grouppig.infra.runtime import registry as registry_module
    from grouppig.panel.__main__ import main

    # di 在导入时就把 default_registry 绑进了 field 默认值，要同时换掉那个引用
    isolated = registry_module.Registry()
    monkeypatch.setattr(registry_module, "default_registry", isolated)
    monkeypatch.setattr(di_module, "default_registry", isolated)
    assert main(["--snapshot", "--json", "--dsn", "sqlite+aiosqlite:///:memory:"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["version"] == SNAPSHOT_VERSION
    assert payload["app"]["started"] is True
    assert payload["contract"]["missing"] == []


def test_cli_default_mode_prints_text(capsys, monkeypatch):
    from grouppig.infra.runtime import di as di_module
    from grouppig.infra.runtime import registry as registry_module
    from grouppig.panel.__main__ import main

    isolated = registry_module.Registry()
    monkeypatch.setattr(registry_module, "default_registry", isolated)
    monkeypatch.setattr(di_module, "default_registry", isolated)
    assert main([]) == 0
    assert "GrouPig 面板（文本模式）" in capsys.readouterr().out


# ---- TUI ---------------------------------------------------------------
def test_tui_render_text_is_plain_text():
    snapshot = build_snapshot(None)
    snapshot["events"] = [{"time": "10:00:00", "topic": "kafka:t", "summary": "group_id=1"}]
    text = tui.render_text(snapshot)
    assert "GrouPig 面板（文本模式）" in text
    assert "契约缺口: 0" in text
    assert "kafka:t" in text
    assert text.isprintable() or chr(10) in text


def test_tui_once_prints_and_returns_zero(capsys):
    assert tui.run(None, once=True) == 0
    assert "GrouPig 面板（文本模式）" in capsys.readouterr().out


def test_tui_without_tty_degrades(capsys, monkeypatch):
    """没有 TTY 时不能崩溃：要么降级文本，要么（如果 curses 可用）正常退出。"""

    import curses

    def boom(*args, **kwargs):
        raise curses.error("no tty")

    monkeypatch.setattr(curses, "initscr", boom)
    assert tui.run(None) == 0
    out = capsys.readouterr().out
    assert "没有可用 TTY" in out or "GrouPig 面板（文本模式）" in out
