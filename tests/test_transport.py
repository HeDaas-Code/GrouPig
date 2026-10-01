"""HTTP 传输层测试（用本机假 HTTP 服务端，不依赖外网）。"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

import pytest

from grouppig.infra.runtime.errors import TransportError
from grouppig.infra.runtime.transport import HttpTransport, build_transport, provider_spec, resolve_api_key


class FakeHttpServer:
    """极简 HTTP/1.1 服务端：按路径返回预设响应。"""

    def __init__(self, routes: dict[str, tuple[int, dict]]) -> None:
        self.routes = routes
        self.requests: list[dict] = []
        self._server: asyncio.AbstractServer | None = None
        self.port = 0

    async def __aenter__(self) -> FakeHttpServer:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *exc: object) -> None:
        assert self._server is not None
        self._server.close()
        await self._server.wait_closed()

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = await reader.readuntil(b"\r\n\r\n")
            lines = head.decode("latin-1").split("\r\n")
            method, path, _ = lines[0].split(" ", 2)
            headers = {}
            for line in lines[1:]:
                if ":" in line:
                    key, value = line.split(":", 1)
                    headers[key.strip().lower()] = value.strip()
            body = b""
            length = int(headers.get("content-length", 0) or 0)
            if length:
                body = await reader.readexactly(length)
            self.requests.append(
                {"method": method, "path": path, "headers": headers, "body": json.loads(body or b"{}")}
            )
            status, payload = self.routes.get(path, (404, {"error": "not found"}))
            blob = json.dumps(payload).encode("utf-8")
            writer.write(
                b"HTTP/1.1 %d X\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n"
                % (status, len(blob))
                + blob
            )
            await writer.drain()
        finally:
            writer.close()


@pytest.fixture
async def http_server() -> AsyncIterator[FakeHttpServer]:
    routes = {
        "/chat/completions": (
            200,
            {
                "model": "m1",
                "choices": [{"message": {"content": "在的"}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 3, "completion_tokens": 2},
            },
        ),
        "/embeddings": (200, {"model": "e1", "data": [{"embedding": [0.1, 0.2]}]}),
        "/broken": (503, {"error": "overloaded"}),
        "/rejected": (400, {"error": "bad request"}),
        "/not-json": (200, None),
    }
    async with FakeHttpServer(routes) as server:
        yield server


async def test_http_transport_posts_and_parses_json(http_server: FakeHttpServer):
    transport = HttpTransport(base_url=http_server.base_url, api_key="sk-test")
    data = await transport.complete({"model": "m1", "messages": []})
    assert data["choices"][0]["message"]["content"] == "在的"
    request = http_server.requests[-1]
    assert request["method"] == "POST"
    assert request["headers"]["authorization"] == "Bearer sk-test"
    assert request["body"]["model"] == "m1"
    await transport.aclose()


async def test_http_transport_uses_embedding_path(http_server: FakeHttpServer):
    transport = HttpTransport(base_url=http_server.base_url)
    data = await transport.complete({"model": "e1", "input": ["a"]}, path="/embeddings")
    assert data["data"][0]["embedding"] == [0.1, 0.2]
    assert http_server.requests[-1]["path"] == "/embeddings"
    await transport.aclose()


async def test_http_transport_error_semantics(http_server: FakeHttpServer):
    transport = HttpTransport(base_url=http_server.base_url)
    with pytest.raises(TransportError) as overloaded:
        await transport.complete({}, path="/broken")
    assert overloaded.value.status == 503 and overloaded.value.retryable is True

    with pytest.raises(TransportError) as rejected:
        await transport.complete({}, path="/rejected")
    assert rejected.value.status == 400 and rejected.value.retryable is False

    with pytest.raises(TransportError) as missing:
        await transport.complete({}, path="/nowhere")
    assert missing.value.status == 404
    await transport.aclose()


async def test_http_transport_rejects_non_json(http_server: FakeHttpServer):
    transport = HttpTransport(base_url=http_server.base_url)
    with pytest.raises(TransportError) as excinfo:
        await transport.complete({}, path="/not-json")
    assert "JSON" in str(excinfo.value)
    await transport.aclose()


async def test_http_transport_requires_base_url():
    transport = HttpTransport(base_url="", provider="a6api")
    with pytest.raises(TransportError) as excinfo:
        await transport.complete({})
    assert "base_url" in str(excinfo.value)
    assert excinfo.value.retryable is False


async def test_http_transport_connection_failure_is_retryable():
    transport = HttpTransport(base_url="http://127.0.0.1:1", timeout=0.2)
    with pytest.raises(TransportError) as excinfo:
        await transport.complete({})
    assert excinfo.value.retryable is True


async def test_build_transport_reads_config(config):
    transport = build_transport(config, provider="a6api", api_key="k", environ={})
    assert transport.provider == "a6api"
    assert transport.api_key == "k"
    assert transport.timeout == 30.0
    assert provider_spec(config, "a6api")["api_key_env"] == "GROUPPIG_MODEL_API_KEY"
    await transport.aclose()


def test_resolve_api_key_precedence(config, tmp_path):
    creds = tmp_path / "credentials.yaml"
    creds.write_text("model:\n  api_key: from-file\n", encoding="utf-8")
    from grouppig.infra.config import loader as config_loader

    original = config_loader.DEFAULT_CREDENTIALS_FILE
    config_loader.DEFAULT_CREDENTIALS_FILE = creds
    try:
        assert resolve_api_key(config, "a6api", environ={"GROUPPIG_MODEL_API_KEY": "from-env"}) == "from-env"
        assert resolve_api_key(config, "a6api", environ={}) == "from-file"
    finally:
        config_loader.DEFAULT_CREDENTIALS_FILE = original


async def test_router_against_fake_http_server(http_server: FakeHttpServer, config):
    """端到端：router → codec → retry → HttpTransport → 假服务端。"""
    from grouppig.infra.model_gateway.router import ModelRouter

    # chat 与 embed 都要走假服务端：embed 的 provider 一并指回 a6api（仓库默认是本地嵌入）
    overridden = config.with_overrides(
        {
            "model": {
                "providers": {"a6api": {"base_url": http_server.base_url, "api_key_env": "GROUPPIG_MODEL_API_KEY"}},
                "tasks": {"embed": {"provider": "a6api", "model": "text-embedding-3-small"}},
            }
        }
    )
    router = ModelRouter(overridden, environ={"GROUPPIG_MODEL_API_KEY": "sk-local"})
    try:
        response = await router.chat([{"role": "user", "content": "在吗"}])
        assert response.text == "在的"
        assert response.usage.total_tokens == 5
        embedded = await router.embed(["你好"])
        assert embedded.embedding == (0.1, 0.2)
    finally:
        await router.aclose()


# ---- provider 分派：laya（LAY A System-1） -------------------------------
async def test_build_transport_returns_laya_transport(config):
    from grouppig.infra.runtime.laya_system1 import LayaSystemOneTransport
    from grouppig.infra.runtime.transport import LAYA_PROVIDER

    transport = build_transport(config, provider="laya", api_key="sk-test", environ={})
    assert isinstance(transport, LayaSystemOneTransport)
    assert transport.provider == "laya"
    assert transport.base_url == config.section("model.providers.laya")["base_url"]
    assert transport.path == "/v1/systemone"
    assert transport.api_key == "sk-test"
    assert LAYA_PROVIDER == "laya"
    await transport.aclose()


def test_resolve_api_key_for_laya_uses_env_then_refs(config, tmp_path):
    """laya 密钥来源：专属环境变量 → 凭据 refs.model.api_key（支持 refs.<name>）。"""

    creds = tmp_path / "credentials.yaml"
    creds.write_text("refs:\n  model:\n    api_key: from-refs\n", encoding="utf-8")
    from grouppig.infra.config import loader as config_loader

    original = config_loader.DEFAULT_CREDENTIALS_FILE
    config_loader.DEFAULT_CREDENTIALS_FILE = creds
    try:
        assert resolve_api_key(config, "laya", environ={"GROUPPIG_LAYA_API_KEY": "from-env"}) == "from-env"
        # 环境变量存在但为空 → 继续往下找，不被空值截断
        assert resolve_api_key(config, "laya", environ={"GROUPPIG_LAYA_API_KEY": ""}) == "from-refs"
    finally:
        config_loader.DEFAULT_CREDENTIALS_FILE = original


def test_resolve_api_key_never_returns_empty_string(config, tmp_path):
    """凭据里写了空值时返回 None，绝不返回空串。"""

    creds = tmp_path / "credentials.yaml"
    creds.write_text("refs:\n  model:\n    api_key: ''\n", encoding="utf-8")
    from grouppig.infra.config import loader as config_loader

    original = config_loader.DEFAULT_CREDENTIALS_FILE
    config_loader.DEFAULT_CREDENTIALS_FILE = creds
    try:
        assert resolve_api_key(config, "laya", environ={}) is None
        assert resolve_api_key(config, "a6api", environ={}) is None
    finally:
        config_loader.DEFAULT_CREDENTIALS_FILE = original
