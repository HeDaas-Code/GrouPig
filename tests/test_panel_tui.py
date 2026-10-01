"""``python -m grouppig.panel tui`` 必须渲染**已装配**的运行时。

以前 CLI 走的是 ``tui.run(None, …)``：每一帧都拿 ``None`` 去 ``build_snapshot``，
于是面板恒为空 —— ``启动: False  运行时长: Nones  注册名: 0``。
"""

from __future__ import annotations

import pytest

from grouppig.panel import tui


@pytest.fixture
def isolated_registry(monkeypatch):
    """CLI 会自己装配 app：把全局 registry 换成本次调用专用的实例，避免污染。"""

    from grouppig.infra.runtime import di as di_module
    from grouppig.infra.runtime import registry as registry_module

    isolated = registry_module.Registry()
    monkeypatch.setattr(registry_module, "default_registry", isolated)
    monkeypatch.setattr(di_module, "default_registry", isolated)
    return isolated


def test_tui_once_renders_a_started_runtime(capsys: pytest.CaptureFixture[str], isolated_registry):
    from grouppig.panel.__main__ import main

    assert main(["tui", "--once", "--dsn", "sqlite+aiosqlite:///:memory:"]) == 0
    out = capsys.readouterr().out
    assert "启动: True" in out
    assert "运行时长: None" not in out
    assert "注册名: 0" not in out
    assert "已装配" in out


def test_tui_run_uses_the_provider_snapshot(capsys: pytest.CaptureFixture[str]):
    """``provider`` 是 CLI 注入的取数函数；TUI 必须用它而不是自己 build。"""

    calls: list[int] = []

    def provider():
        calls.append(1)
        return {"app": {"started": True, "uptime": 1.5}, "registry": {"total": 3}}

    assert tui.run(None, once=True, provider=provider) == 0
    assert calls == [1]
    out = capsys.readouterr().out
    assert "启动: True" in out
    assert "注册名: 3" in out
