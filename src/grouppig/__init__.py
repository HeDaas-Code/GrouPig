"""GrouPig —— 原子化 QQ AI 群友。

目录结构逐字镜像 ``normify-grouppig`` 的模块路径：``grouppig.<domain>.<area>.<leaf>``
对应 ``src/grouppig/<domain>/<area>/<leaf>.py``（normify 段名里的 ``-`` 在 Python 侧
写作 ``_``，例如 ``grouppig.infra.model-gateway.router`` → ``grouppig/infra/model_gateway/router.py``）。

跨模块通信只允许通过 ``normify-grouppig/api-index.json`` 中的名字：
``rpc:<name>``（同步调用）、``kafka:<topic>``（事件总线）、``mysql:<table>``（存储）。
"""

from __future__ import annotations

__version__ = "0.1.0"

__all__ = ["__version__"]
