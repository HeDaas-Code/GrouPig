"""只读面板的访问控制与内容脱敏。

面板以前是「绑什么地址都行、谁都能读、事件摘要原样回显群消息」：
``http://<host>:8848/api/snapshot`` 就能把 ``{"group_id":1,"text":"我的身份证号是…"}``
原封不动读出来。这里把三条底线钉住：

1. 非本机绑定必须显式 opt-in（并打醒目警告）；
2. 设了共享密钥就必须带（头或 ``?token=``），否则 401；
3. ``Host`` 头不在白名单内 → 403（钝化 DNS rebinding）；
4. 事件摘要默认不原样回显聊天正文。
"""

from __future__ import annotations

import http.client
import json
import threading
import urllib.error
import urllib.request

import pytest

from grouppig.panel import web
from grouppig.panel.snapshot import EventRing


# ---- 绑定策略 ----------------------------------------------------------
def test_remote_bind_is_refused_without_opt_in():
    with pytest.raises(web.PanelBindError):
        web.serve(None, host="0.0.0.0", port=0)


def test_remote_bind_warns_loudly_when_allowed(capsys: pytest.CaptureFixture[str]):
    httpd = web.serve(None, host="0.0.0.0", port=0, allow_remote=True)
    try:
        err = capsys.readouterr().err
        assert "非本机地址" in err
        assert "⚠" in err
    finally:
        httpd.server_close()


def test_loopback_bind_needs_no_opt_in_and_prints_nothing(capsys: pytest.CaptureFixture[str]):
    httpd = web.serve(None, host="127.0.0.1", port=0)
    try:
        assert capsys.readouterr().err == ""
    finally:
        httpd.server_close()


@pytest.mark.parametrize(
    ("host", "expected"),
    [
        ("127.0.0.1", True),
        ("127.0.0.5", True),
        ("localhost", True),
        ("::1", True),
        ("0.0.0.0", False),
        ("192.168.1.10", False),
        ("example.com", False),
    ],
)
def test_is_loopback_host(host: str, expected: bool):
    assert web.is_loopback_host(host) is expected


# ---- 共享密钥 ----------------------------------------------------------
def _server(**kwargs):
    httpd = web.serve(None, port=0, **kwargs)
    port = httpd.server_address[1]
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    return f"http://127.0.0.1:{port}", thread, httpd


def _stop(httpd, thread) -> None:
    httpd.shutdown()
    httpd.server_close()
    thread.join(timeout=5)


def test_token_is_required_when_configured():
    base, thread, httpd = _server(token="s3cret")
    try:
        with pytest.raises(urllib.error.HTTPError) as excinfo:
            urllib.request.urlopen(base + "/api/snapshot", timeout=10)
        assert excinfo.value.code == 401

        with pytest.raises(urllib.error.HTTPError) as excinfo:
            urllib.request.urlopen(base + "/api/snapshot?token=wrong", timeout=10)
        assert excinfo.value.code == 401

        # 浏览器路径：?token=
        with urllib.request.urlopen(base + "/api/snapshot?token=s3cret", timeout=10) as response:
            assert response.status == 200
            assert json.loads(response.read().decode("utf-8"))["version"] == 1

        # 脚本路径：请求头
        request = urllib.request.Request(base + "/api/health", headers={web.TOKEN_HEADER: "s3cret"})
        with urllib.request.urlopen(request, timeout=10) as response:
            assert response.status == 200

        # 页面本身也要密钥，且 JS 会把 token 带进后续请求
        with urllib.request.urlopen(base + "/?token=s3cret", timeout=10) as response:
            page = response.read().decode("utf-8")
        assert "TOKEN" in page and "location.search" in page
    finally:
        _stop(httpd, thread)


def test_no_token_means_public_readonly_panel():
    base, thread, httpd = _server()
    try:
        with urllib.request.urlopen(base + "/api/snapshot", timeout=10) as response:
            assert response.status == 200
    finally:
        _stop(httpd, thread)


# ---- Host 头（DNS rebinding） -------------------------------------------
def test_host_header_outside_allowlist_is_403():
    httpd = web.serve(None, port=0)
    port = httpd.server_address[1]
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        conn.putrequest("GET", "/api/snapshot", skip_host=True, skip_accept_encoding=True)
        conn.putheader("Host", "evil.example.com")
        conn.endheaders()
        response = conn.getresponse()
        body = response.read().decode("utf-8")
        conn.close()
        assert response.status == 403
        assert "Host" in body
    finally:
        _stop(httpd, thread)


def test_host_header_with_port_is_allowed_for_loopback():
    httpd = web.serve(None, port=0)
    port = httpd.server_address[1]
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        conn.putrequest("GET", "/api/health", skip_host=True, skip_accept_encoding=True)
        conn.putheader("Host", f"127.0.0.1:{port}")
        conn.endheaders()
        response = conn.getresponse()
        response.read()
        conn.close()
        assert response.status == 200
    finally:
        _stop(httpd, thread)


# ---- 事件摘要脱敏 -------------------------------------------------------
def test_event_summary_does_not_echo_chat_text():
    ring = EventRing()
    secret = "我的身份证号是110101199001011234"
    item = ring.record("kafka:grouppig.qq.message.received", {"group_id": 1, "sender_id": 2, "text": secret})
    assert secret not in item["summary"]
    assert "group_id=1" in item["summary"]
    assert "已脱敏" in item["summary"]
    assert str(len(secret)) in item["summary"]


def test_event_summary_keeps_short_markers():
    """短标记（测试、心跳、状态词）仍然可读：观测面不能变成盲盒。"""

    ring = EventRing()
    assert ring.record("t", {"index": 3, "text": "x" * 10})["summary"] == "index=3 text=xxxxxxxxxx"
    assert ring.record("t2", "raw text")["summary"] == "raw text"


def test_event_summary_redacts_long_bare_payload():
    ring = EventRing()
    item = ring.record("t", "群消息正文" * 20)
    assert "群消息正文" not in item["summary"]
    assert "已脱敏" in item["summary"]


# ---- 只读保证 ----------------------------------------------------------
def test_writes_are_still_not_implemented():
    base, thread, httpd = _server(token="s3cret")
    try:
        request = urllib.request.Request(
            base + "/api/snapshot?token=s3cret", method="POST", data=b"{}", headers={web.TOKEN_HEADER: "s3cret"}
        )
        with pytest.raises(urllib.error.HTTPError) as excinfo:
            urllib.request.urlopen(request, timeout=10)
        assert excinfo.value.code in (501, 405)
    finally:
        _stop(httpd, thread)


# ---- CLI 参数 ----------------------------------------------------------
def test_runtime_parser_accepts_panel_security_flags():
    from grouppig.runtime.app import _parser

    args = _parser().parse_args(
        ["--panel", "--panel-host", "0.0.0.0", "--panel-allow-remote", "--panel-token", "s3cret"]
    )
    assert args.panel_allow_remote is True
    assert args.panel_token == "s3cret"

    defaults = _parser().parse_args([])
    assert defaults.panel_allow_remote is False
    assert defaults.panel_token is None


def test_panel_settings_read_token_from_config():
    from grouppig.infra.config.loader import load_config

    config = load_config("config/grouppig.toml", use_env=False, use_local=False).with_overrides(
        {"panel": {"token": "from-config"}}
    )
    settings = web.PanelSettings.from_config(config, host="127.0.0.1")
    assert settings.token == "from-config"
    assert settings.check_token("from-config") is True
    assert settings.check_token("nope") is False
