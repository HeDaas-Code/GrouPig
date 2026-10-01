"""grouppig.infra.model-gateway —— 模型网关（对话 / 嵌入 / 轻量分类）。

normify id: ``grouppig.infra.model-gateway``（容器模块）。

叶子：

* :mod:`grouppig.infra.model_gateway.router` —— ``rpc:model.chat`` / ``rpc:model.embed`` / ``rpc:model.classify``
* :mod:`grouppig.infra.model_gateway.codec` —— ``rpc:model.encode`` / ``rpc:model.decode``
* :mod:`grouppig.infra.model_gateway.retry` —— ``rpc:model.retry``
"""

from __future__ import annotations

from grouppig.infra.model_gateway.codec import ModelRequest, ModelResponse, decode_response, encode_request
from grouppig.infra.model_gateway.retry import RetryPolicy, execute_with_retry
from grouppig.infra.model_gateway.router import ModelRouter, get_router, set_router

__all__ = [
    "ModelRequest",
    "ModelResponse",
    "ModelRouter",
    "RetryPolicy",
    "decode_response",
    "encode_request",
    "execute_with_retry",
    "get_router",
    "set_router",
]
