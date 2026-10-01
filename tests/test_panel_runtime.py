"""grouppig.runtime 的 --panel 选项：机器人与面板必须同进程共存。

只读面板不参与业务，但它是运维入口；这里验证的是**装配路径**：
``python -m grouppig.runtime --check --panel`` 之外，真正跑起来时
面板端点在同一个进程里可访问，且快照反映的就是这个 app。
"""

from __future__ import annotations

import json
import socket
import threading
import time
import urllib.request

import pytest

from grouppig.infra.runtime.registry import Registry
from grouppig.panel import web


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def test_panel_serves_the_same_app_it_is_given():
    """面板端点的内容必须来自传入的 app（域、泵、注册表一致）。"""

    from grouppig.runtime.app import build_app

    app = build_app(registry=Registry(), dsn="sqlite+aiosqlite:///:memory:", connect=False)
    await app.start()
    httpd = web.serve(app, host="127.0.0.1", port=_free_port())
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    try:
        time.sleep(0.2)
        url = f"http://127.0.0.1:{httpd.server_address[1]}/api/snapshot"
        with urllib.request.urlopen(url, timeout=10) as response:
            snapshot = json.loads(response.read().decode("utf-8"))
        assert snapshot["app"]["started"] is True
        assert snapshot["registry"]["total"] == len(app.container.registry.names())
        assert snapshot["domains"] == {name: True for name in app.domains}
        assert [pump["name"] for pump in snapshot["pumps"]] == [pump.name for pump in app.pumps]
        assert snapshot["contract"]["missing"] == []
    finally:
        httpd.shutdown()
        httpd.server_close()
        await app.aclose()


async def test_panel_records_bus_events():
    """事件总线发布后，面板事件流能看见（观测面与主链路同源）。"""

    from grouppig.panel.snapshot import EVENTS
    from grouppig.runtime.app import build_app

    app = build_app(registry=Registry(), dsn="sqlite+aiosqlite:///:memory:", connect=False)
    await app.start()
    try:
        before = len(EVENTS)
        await app.container.bus.publish(
            "kafka:grouppig.qq.message.received",
            {"group_id": 1, "sender_id": 2, "text": "hi"},
            source="test",
        )
        assert len(EVENTS) > before
        topics = [event["topic"] for event in EVENTS.tail(10)]
        assert "kafka:grouppig.qq.message.received" in topics
    finally:
        await app.aclose()


def test_runtime_parser_accepts_panel_flags():
    from grouppig.runtime.app import _parser

    args = _parser().parse_args(["--panel", "--panel-host", "127.0.0.1", "--panel-port", "9999"])
    assert args.panel is True
    assert args.panel_port == 9999


@pytest.mark.parametrize("flag", ["--panel-port"])
def test_runtime_panel_defaults_to_localhost(flag):
    from grouppig.runtime.app import _parser

    args = _parser().parse_args([])
    assert args.panel is False
    assert args.panel_host == "127.0.0.1"  # 默认只监听本机
