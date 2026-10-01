"""grouppig.infra.runtime.transport —— 模型 HTTP 传输层。

是 ``grouppig.infra.model-gateway.router`` 的下游：只负责把编码好的 payload POST 出去、
把 JSON 拿回来，不含任何重试 / 编解码逻辑（重试在 ``model-gateway.retry``，编解码在
``model-gateway.codec``）。

协议：OpenAI 兼容的 ``/chat/completions`` 与 ``/embeddings``（OpenAI 协议族），
``base_url`` / 密钥全部来自配置与环境变量，密钥不入库、不落盘。

normify id: ``grouppig.infra.runtime.transport``（运行时补充模块，设计树暂无）。
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from grouppig.infra.runtime.errors import TransportError
from grouppig.infra.runtime.laya_system1 import LayaSystemOneTransport
from grouppig.infra.runtime.local_embed import LocalEmbedTransport

DEFAULT_CHAT_PATH = "/chat/completions"
#: 本地嵌入 provider 名（[model.tasks.embed].provider = "local"）。
LOCAL_EMBED_PROVIDER = "local"
#: LAY A System-1 决策模型 provider 名（[model.tasks.*].provider = "laya"）。
#: 端点 POST {base_url}/v1/systemone 非 OpenAI 兼容，见 runtime/laya_system1.py。
LAYA_PROVIDER = "laya"

DEFAULT_EMBED_PATH = "/embeddings"

#: 是否让 httpx 读取环境里的代理变量（``HTTP_PROXY`` / ``ALL_PROXY`` / ``NO_PROXY`` …）。
#:
#: 默认 **False**，理由是这里的行为必须可预测：
#:
#: * 模型服务商是公网 HTTPS 端点，宿主环境里的代理变量通常是**别的工具**留下的，
#:   静默劫持模型调用会表现为「连接超时 / 502」，而配置里看不出任何异常；
#: * ``NO_PROXY`` 里一个不合法的条目就足以让 httpx 在**构造客户端时**抛异常。
#:   实测宿主 ``no_proxy=localhost,127.0.0.1,::1,[::1]`` 时，
#:   httpx 把 ``[::1]`` 当端口解析 → ``InvalidURL: Invalid port: ':1]'``，
#:   于是**所有** HTTP 传输调用在发出请求前就炸掉（本仓库 48 个用例因此变红）。
#:
#: 需要走代理的部署显式设 ``model.trust_env = true``。
DEFAULT_TRUST_ENV = False


@runtime_checkable
class ModelTransport(Protocol):
    """传输层协议：实现它即可替换真实 HTTP（测试用假传输）。"""

    async def complete(
        self,
        payload: dict[str, Any],
        *,
        path: str = DEFAULT_CHAT_PATH,
        timeout: float | None = None,
        provider: str = "",
    ) -> dict[str, Any]: ...

    async def aclose(self) -> None: ...


@dataclass
class HttpTransport:
    """基于 httpx 的 OpenAI 兼容 HTTP 传输。"""

    base_url: str
    api_key: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    timeout: float = 30.0
    provider: str = ""
    max_connections: int = 20
    trust_env: bool = DEFAULT_TRUST_ENV
    _client: Any = None

    def __post_init__(self) -> None:
        self.base_url = (self.base_url or "").rstrip("/")

    # ---- 客户端 --------------------------------------------------------
    def _ensure_client(self) -> Any:
        if self._client is not None:
            return self._client
        if not self.base_url:
            raise TransportError(
                f"provider {self.provider or '?'} 的 base_url 为空：请在 [model.providers.*] 中配置", retryable=False
            )
        try:
            import httpx
        except ImportError as exc:  # pragma: no cover - httpx 是运行依赖
            raise TransportError("缺少 httpx 依赖，请运行 `uv sync`", retryable=False) from exc
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=self.timeout,
            headers=self._headers(),
            limits=httpx.Limits(max_connections=self.max_connections),
            trust_env=self.trust_env,
        )
        return self._client

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json", **self.headers}
        if self.api_key:
            headers.setdefault("Authorization", f"Bearer {self.api_key}")
        return headers

    # ---- 调用 ----------------------------------------------------------
    async def complete(
        self,
        payload: dict[str, Any],
        *,
        path: str = DEFAULT_CHAT_PATH,
        timeout: float | None = None,
        provider: str = "",
    ) -> dict[str, Any]:
        client = self._ensure_client()
        try:
            response = await client.post(path, json=payload, timeout=timeout or self.timeout)
        except Exception as exc:
            raise TransportError(f"模型请求失败（{path}）：{exc!r}", retryable=True) from exc
        if response.status_code >= 400:
            body = _short(response.text)
            retryable = response.status_code in (408, 409, 425, 429) or response.status_code >= 500
            raise TransportError(
                f"模型接口返回 {response.status_code}（{path}）：{body}",
                status=response.status_code,
                retryable=retryable,
                body=body,
            )
        try:
            data = response.json()
        except Exception as exc:
            raise TransportError(
                f"模型响应不是合法 JSON（{path}）：{exc!r}", status=response.status_code, retryable=False
            ) from exc
        if not isinstance(data, dict):
            raise TransportError(f"模型响应应为 JSON 对象，得到 {type(data).__name__}", retryable=False)
        return data

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None


def _short(text: str, limit: int = 300) -> str:
    text = (text or "").strip().replace("\n", " ")
    return text[:limit]


def provider_spec(config: Any, provider: str) -> dict[str, Any]:
    """从配置里取服务商定义。"""

    spec = config.section(f"model.providers.{provider}")
    return spec


def resolve_api_key(config: Any, provider: str, *, environ: dict[str, str] | None = None) -> str | None:
    """按 ``api_key_env`` → ``api_key_field``（``~/.dsh/.credentials.yaml``）→ ``GROUPPIG_MODEL_API_KEY`` 解析密钥。"""

    from grouppig.infra.config.loader import resolve_secret

    environ = dict(os.environ if environ is None else environ)
    spec = provider_spec(config, provider)
    env_name = str(spec.get("api_key_env") or "").strip()
    cred_field = str(spec.get("api_key_field") or "model.api_key").strip()
    names = [n for n in (env_name, "GROUPPIG_MODEL_API_KEY", cred_field) if n]
    # ~/.dsh/.credentials.yaml 的引用统一放在 refs: 段下，故同时按 refs.<name> 解析
    # （含点号路径，例如 api_key_field = "model.api_key" → refs.model.api_key）
    names = [*names, *[f"refs.{n}" for n in names]]
    value = resolve_secret(*names, environ=environ)
    text = str(value).strip() if value is not None else ""
    # 取不到就是 None：绝不返回空串，调用方才能区分「没配密钥」与「配了空值」
    return text or None


def resolve_trust_env(config: Any, *, default: bool = DEFAULT_TRUST_ENV) -> bool:
    """读 ``model.trust_env``（缺省 / 非布尔值一律回落默认值）。"""

    value = config.get("model.trust_env", default)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        text = value.strip().lower()
        if text in ("1", "true", "yes", "on"):
            return True
        if text in ("0", "false", "no", "off"):
            return False
    return default


def build_transport(
    config: Any,
    *,
    provider: str | None = None,
    api_key: str | None = None,
    environ: dict[str, str] | None = None,
    timeout: float | None = None,
) -> ModelTransport:
    """按配置构造传输层：local → 本地嵌入、laya → LAY A 决策模型，其余走 OpenAI 兼容 HTTP。"""

    provider = provider or str(config.get("model.default_provider", ""))
    if provider.strip().lower() == LOCAL_EMBED_PROVIDER:
        # 本地嵌入：无网络、无密钥、无额外依赖（见 infra/runtime/local_embed.py）
        return LocalEmbedTransport()
    spec = provider_spec(config, provider)
    retry_cfg = config.section("model.retry")
    resolved_timeout = float(timeout if timeout is not None else retry_cfg.get("timeout", 30.0))
    resolved_key = api_key if api_key is not None else resolve_api_key(config, provider, environ=environ)
    trust_env = resolve_trust_env(config)
    if provider.strip().lower() == LAYA_PROVIDER:
        # LAY A：唯一端点 /v1/systemone，非 OpenAI 兼容（见 infra/runtime/laya_system1.py）
        return LayaSystemOneTransport(
            base_url=str(spec.get("base_url") or ""),
            api_key=resolved_key,
            timeout=resolved_timeout,
            provider=provider,
            trust_env=trust_env,
        )
    return HttpTransport(
        base_url=str(spec.get("base_url") or ""),
        api_key=resolved_key,
        timeout=resolved_timeout,
        provider=provider,
        trust_env=trust_env,
    )


__all__ = [
    "DEFAULT_CHAT_PATH",
    "DEFAULT_EMBED_PATH",
    "DEFAULT_TRUST_ENV",
    "HttpTransport",
    "LAYA_PROVIDER",
    "LOCAL_EMBED_PROVIDER",
    "LayaSystemOneTransport",
    "LocalEmbedTransport",
    "ModelTransport",
    "build_transport",
    "provider_spec",
    "resolve_api_key",
    "resolve_trust_env",
]
