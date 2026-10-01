"""``python -m grouppig.runtime`` 命令行契约。

``--check`` 的 help 承诺是「只装配并打印契约自检结果，然后退出」，
所以它必须是一个**无副作用**的检查：不连 OneBot、不跑迁移、不起周期泵；
失败时给一行可操作的错误（而不是裸 traceback），并且不留没关的容器。
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from grouppig.infra.runtime.errors import GrouPigError
from grouppig.infra.runtime.registry import Registry
from grouppig.runtime.app import build_app, main

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPO_ROOT / "config" / "grouppig.toml"


def _tables(db_path: Path) -> list[str]:
    with sqlite3.connect(db_path) as conn:
        return sorted(row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'"))


def test_check_succeeds_without_a_reachable_onebot(capsys: pytest.CaptureFixture[str]):
    """本机没有 NapCat 时 ``--check`` 仍应成功退出（它本来就不该去连）。"""

    code = main(["--config", str(CONFIG_PATH), "--check"], registry=Registry())
    assert code == 0
    out = capsys.readouterr().out
    assert "已装配" in out


def test_check_does_not_run_migrations(tmp_path: Path):
    """检查不是启动：不许在运维的真实库里建表。"""

    db_path = tmp_path / "fresh.db"
    code = main(
        [
            "--config",
            str(CONFIG_PATH),
            "--check",
            "--dsn",
            f"sqlite+aiosqlite:///{db_path}",
            "--quiet",
        ],
        registry=Registry(),
    )
    assert code == 0
    assert _tables(db_path) == []


def test_startup_failure_reports_one_line_and_exits_nonzero(capsys: pytest.CaptureFixture[str]):
    """装配失败要一行可操作的错误 + 非零退出，不要裸 traceback。"""

    code = main(
        [
            "--config",
            str(CONFIG_PATH),
            "--check",
            # 同步驱动 → StoreError：用最少的依赖制造一次真实的启动失败
            "--dsn",
            "sqlite:///not-async.db",
            "--quiet",
        ]
    )
    assert code != 0
    captured = capsys.readouterr()
    assert "Traceback" not in captured.err
    assert "Traceback" not in captured.out
    lines = [line for line in captured.err.splitlines() if line.strip()]
    assert len(lines) == 1, lines
    assert "启动失败" in lines[0]


def test_missing_config_reports_one_line_and_exits_nonzero(capsys: pytest.CaptureFixture[str], tmp_path: Path):
    """配置文件不存在时同理（这条以前也是裸 traceback）。"""

    code = main(["--config", str(tmp_path / "nope.toml"), "--check", "--quiet"])
    assert code != 0
    captured = capsys.readouterr()
    assert "Traceback" not in captured.err
    lines = [line for line in captured.err.splitlines() if line.strip()]
    assert len(lines) == 1, lines
    assert "配置文件不存在" in lines[0]


async def test_failed_start_still_closes_the_container():
    """装配中途失败时容器也要关（路由器 / 总线 / 日志句柄不能留）。"""

    # 显式传入独立注册表：build_app 默认用全局 default_registry，
    # 会把会话层的名字注册进全局，污染 test_session_contract 的隔离断言。
    app = build_app(
        config_path=CONFIG_PATH,
        dsn="sqlite:///not-async.db",
        connect=False,
        pumps=False,
        registry=Registry(),
    )
    with pytest.raises(GrouPigError):
        await app.start()
    assert app.container.started is True  # 容器已经起来了，只是后面的域没装完
    await app.aclose()
    assert app.container.started is False
