"""grouppig.infra.config —— 配置中心（加载 / 校验 / 热更新）。

normify id: ``grouppig.infra.config``（容器模块）。

叶子：

* :mod:`grouppig.infra.config.loader` —— ``rpc:config.get``
* :mod:`grouppig.infra.config.validator` —— ``rpc:config.validate``
* :mod:`grouppig.infra.config.reloader` —— ``rpc:config.reload``
"""

from __future__ import annotations

from grouppig.infra.config.loader import Config, get_config, load_config, resolve_secret, set_config
from grouppig.infra.config.reloader import ConfigReloader
from grouppig.infra.config.validator import Issue, ValidationReport, validate_config

__all__ = [
    "Config",
    "ConfigReloader",
    "Issue",
    "ValidationReport",
    "get_config",
    "load_config",
    "resolve_secret",
    "set_config",
    "validate_config",
]
