"""共享测试夹具。"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

from grouppig.infra.config.loader import Config, load_config  # noqa: E402
from grouppig.infra.runtime.di import Container, build_container  # noqa: E402
from helpers import FakeTransport  # noqa: E402

if TYPE_CHECKING:  # pragma: no cover - 仅供类型标注
    from grouppig.memory.runtime.stores import MemoryStores

CONFIG_PATH = REPO_ROOT / "config" / "grouppig.toml"


@pytest.fixture(scope="session")
def config_text() -> str:
    return CONFIG_PATH.read_text(encoding="utf-8")


@pytest.fixture
def config_file(tmp_path: Path, config_text: str) -> Path:
    """仓库配置的临时副本（可安全改写）。"""

    path = tmp_path / "grouppig.toml"
    path.write_text(
        config_text.replace("threshold = 0.26", "threshold = 0.55").replace("renormalize = true", "renormalize = false"),
        encoding="utf-8",
    )
    return path


@pytest.fixture
def config(config_file: Path) -> Config:
    return load_config(config_file, use_env=False, use_local=False)


@pytest.fixture
async def container(config: Config) -> Container:
    c = await build_container(config=config).start()
    try:
        yield c
    finally:
        await c.aclose()


@pytest.fixture
def fake_transport() -> FakeTransport:
    return FakeTransport()


# ---- memory 域夹具（各域测试共用；放在 conftest 里避免测试模块重复导入同名夹具）----

pytest_plugins = ["memory_helpers", "session_helpers"]


@pytest.fixture
async def memory_container(config: Config) -> Container:
    """infra 容器 + 独立注册表 + memory 已挂载（不污染全局默认注册表）。"""

    from grouppig.infra.logger import RuntimeLogger
    from grouppig.infra.model_gateway.router import ModelRouter
    from grouppig.infra.runtime.bus import EventBus
    from grouppig.infra.runtime.registry import Registry
    from grouppig.infra.token_budget.meter import TokenMeter
    from grouppig.memory.runtime.di import attach_memory

    registry = Registry()
    logger = RuntimeLogger()
    meter = TokenMeter(logger=logger, config=config)
    c = Container(
        config=config,
        registry=registry,
        logger=logger,
        bus=EventBus(logger=logger, strict_topics=True),
        meter=meter,
        router=ModelRouter(config, logger=logger, meter=meter),
    )
    await c.start()
    await attach_memory(c, dsn="sqlite+aiosqlite:///:memory:")
    try:
        yield c
    finally:
        await c.aclose()


@pytest.fixture
async def stores() -> MemoryStores:
    """建好表的 SQLite 内存库（六类存储全装配）。"""

    from grouppig.memory.runtime.stores import MemoryStores

    instance = MemoryStores.from_dsn("sqlite+aiosqlite:///:memory:")
    await instance.migrate()
    try:
        yield instance
    finally:
        await instance.aclose()


# ---- session 域夹具（各域测试共用；放在 conftest 里避免测试模块重复导入同名夹具）----

pytest_plugins = ["memory_helpers", "session_helpers"]


@pytest.fixture
async def session_container(config: Config):
    """infra 容器 + 独立注册表 + memory 已挂载 + 会话层已挂载。"""

    from session_helpers import session_container as build

    async with build(config) as pair:
        yield pair


@pytest.fixture
async def session_layer(config: Config):
    """只要会话层（容器仍可从 layer.container 取到）。"""

    from session_helpers import session_container as build

    async with build(config) as (_container, layer):
        yield layer


# ---- perception 域夹具（t4）------------------------------------------------

pytest_plugins = ["memory_helpers", "perception_helpers"]


@pytest.fixture
async def perception_container(config: Config) -> Container:
    """infra 容器 + 独立注册表 + memory + perception 已装配（不污染全局默认注册表）。"""

    from grouppig.memory.runtime.di import attach_memory
    from perception_helpers import make_container, make_perception

    c = make_container(config)
    await c.start()
    await attach_memory(c, dsn="sqlite+aiosqlite:///:memory:")
    # 感知层的滚动时间窗挂到容器上，便于测试直接取用（也等价于 t10 集成时的注入方式）
    c.perception = make_perception(config, c)  # type: ignore[attr-defined]
    try:
        yield c
    finally:
        await c.perception.aclose()  # type: ignore[attr-defined]
        await c.aclose()


@pytest.fixture
async def perception(config: Config) -> Any:
    """只装感知层（不挂 memory）：验证「下游缺席也能跑」。"""

    from perception_helpers import make_container, make_perception

    c = make_container(config)
    await c.start()
    instance = make_perception(config, c)
    try:
        yield instance
    finally:
        await instance.aclose()
        await c.aclose()
