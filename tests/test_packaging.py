"""打包与「离开源码树」的可用性。

三件事必须成立，否则包只能在本仓库的 cwd 里跑：

1. 有 ``[project.scripts]`` 控制台入口（装完就能敲 ``grouppig``）；
2. ``api-index.json`` 的解析在**仓库走查失败**时回落到随包分发的副本
   （``infra/runtime/di.py`` 在 import 期就断言契约名，回落失败 = 连 import 都不行）；
3. 默认配置路径不是裸的 cwd 相对路径（换个目录启动就找不到 ``config/grouppig.toml``）。

约束：仓库里那份 ``normify-grouppig/api-index.json`` 必须原地不动 ——
``test_contract_alignment`` / ``test_skeleton`` 的计数都从它推导。
"""

from __future__ import annotations

import importlib
import json
import os
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC = REPO_ROOT / "src"
API_INDEX = REPO_ROOT / "normify-grouppig" / "api-index.json"
BUNDLED_API_INDEX = SRC / "grouppig" / "_data" / "api-index.json"

_PROXY_KEYS = (
    "no_proxy",
    "NO_PROXY",
    "ALL_PROXY",
    "all_proxy",
    "HTTP_PROXY",
    "http_proxy",
    "HTTPS_PROXY",
    "https_proxy",
)


def _clean_env(**extra: str) -> dict[str, str]:
    """子进程环境：去掉宿主代理（本机 `no_proxy` 里的 `[::1]` 会让 httpx 直接炸）。"""

    env = {key: value for key, value in os.environ.items() if key not in _PROXY_KEYS}
    env.pop("GROUPPIG_API_INDEX", None)
    env.pop("GROUPPIG_REPO_ROOT", None)
    env.pop("GROUPPIG_CONFIG", None)
    env.update(extra)
    return env


def test_console_script_target_is_importable_and_help_exits_zero():
    """`[project.scripts]` 必须存在，且目标可导入、`--help` 正常退出。"""

    data = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    scripts = data["project"].get("scripts")
    assert scripts, "缺少 [project.scripts]：装完之后没有 grouppig 命令"
    assert "grouppig" in scripts

    module_name, _, attr = scripts["grouppig"].partition(":")
    assert module_name and attr
    module = importlib.import_module(module_name)
    entry = getattr(module, attr)
    with pytest.raises(SystemExit) as excinfo:
        entry(["--help"])
    assert excinfo.value.code == 0


def test_bundled_api_index_is_in_sync_with_the_repo_copy():
    """随包副本不能悄悄过期（设计树变了就得同步重拷）。"""

    assert BUNDLED_API_INDEX.is_file(), f"缺少随包 api-index 副本：{BUNDLED_API_INDEX}"
    assert json.loads(BUNDLED_API_INDEX.read_text(encoding="utf-8")) == json.loads(
        API_INDEX.read_text(encoding="utf-8")
    )


def test_api_index_falls_back_to_package_data_outside_the_repo(tmp_path: Path):
    """只把 `src/grouppig` 拷出去也要能 import（仓库走查失败 → 用随包副本）。"""

    package = tmp_path / "grouppig"
    shutil.copytree(SRC / "grouppig", package, ignore=shutil.ignore_patterns("__pycache__"))
    code = (
        "import json, grouppig;"
        "from grouppig.infra.runtime import contract;"
        "print(json.dumps({'file': grouppig.__file__, 'names': len(contract.api_index()),"
        " 'known': contract.is_known_name('rpc:model.system1')}))"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=tmp_path,
        env=_clean_env(PYTHONPATH=str(tmp_path)),
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout.strip().splitlines()[-1])
    assert payload["file"].startswith(str(tmp_path)), payload  # 确认用的是拷贝，不是仓库里的包
    assert payload["names"] > 0
    assert payload["known"] is True


def test_default_config_path_resolves_outside_cwd(tmp_path: Path):
    """换个工作目录启动，默认配置仍应找得到（顺着包的位置回到仓库）。"""

    from grouppig.infra.config.loader import resolve_config_path

    here = Path.cwd()
    try:
        os.chdir(tmp_path)
        resolved = resolve_config_path()
    finally:
        os.chdir(here)
    assert resolved.is_file(), f"默认配置不可解析：{resolved}"
    assert resolved.resolve() == (REPO_ROOT / "config" / "grouppig.toml").resolve()


def test_check_runs_from_another_working_directory(tmp_path: Path):
    """`python -m grouppig.runtime --check` 在别处也要跑得起来。"""

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "grouppig.runtime",
            "--check",
            "--quiet",
            "--dsn",
            "sqlite+aiosqlite:///:memory:",
        ],
        cwd=tmp_path,
        env=_clean_env(PYTHONPATH=str(SRC)),
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert result.returncode == 0, result.stdout + result.stderr
