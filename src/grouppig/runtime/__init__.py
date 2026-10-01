"""grouppig.runtime —— 单进程集成层（闭环装配 + 驱动泵 + 启动入口）。

设计树里每个叶子都只声明「我依赖谁」，没人声明「谁在什么时候调用我」；
本包补的就是那一层：

* :mod:`grouppig.runtime.app` —— `GrouppigApp` / `build_app` / `create_app` / `main`
* :mod:`grouppig.runtime.pumps` —— 四个驱动泵（感知排空 / 心流推进 / 画像刷新 / 会话收尾）
* :mod:`grouppig.runtime.errors` —— `IntegrationError`

normify id: `grouppig.runtime`（运行时补充模块，设计树暂无）。
"""

from __future__ import annotations

from grouppig.runtime.app import (
    DEFAULT_CONFIG_PATH,
    DOMAIN_SCOPES,
    WIRING_ORDER,
    GrouppigApp,
    IntegrationOptions,
    build_app,
    create_app,
    main,
    run,
)
from grouppig.runtime.errors import IntegrationError
from grouppig.runtime.pumps import (
    CallTally,
    DrainPump,
    FlowDriver,
    ProfilePump,
    SessionSweeper,
    best_effort,
)

__all__ = [
    "DEFAULT_CONFIG_PATH",
    "DOMAIN_SCOPES",
    "WIRING_ORDER",
    "CallTally",
    "DrainPump",
    "FlowDriver",
    "GrouppigApp",
    "IntegrationError",
    "IntegrationOptions",
    "ProfilePump",
    "SessionSweeper",
    "best_effort",
    "build_app",
    "create_app",
    "main",
    "run",
]
