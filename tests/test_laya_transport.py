"""LAY A System-1 传输层测试（用本机假服务端，不依赖真实端点与密钥）。"""

from __future__ import annotations

import ast
import asyncio
import dataclasses
import inspect
import json
import re
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any

import pytest

from grouppig.infra.model_gateway.codec import ModelRequest, encode_request
from grouppig.infra.runtime import laya_system1 as laya
from grouppig.infra.runtime.errors import TransportError
from grouppig.infra.runtime.laya_system1 import (
    LAY_A_MODELS,
    SYSTEM_ONE_PATH,
    LayaSystemOneTransport,
    build_request,
    classify_request,
    normalize_answers,
    normalize_usage,
    payload_task,
)
from grouppig.infra.runtime.transport import (
    HttpTransport,
    ModelTransport,
    provider_spec,
    resolve_api_key,
)

LABELS = ("提问", "闲聊", "求助")


# ---- 假服务端 ----------------------------------------------------------
class FakeLayaServer:
    """极简 HTTP/1.1 服务端：只服务 /v1/systemone，记录每次请求。"""

    def __init__(self, response: dict[str, Any] | None = None) -> None:
        self.response: dict[str, Any] = dict(response or {"json": laya_response({"label": choice_answer("提问")})})
        self.requests: list[dict[str, Any]] = []
        self._server: asyncio.AbstractServer | None = None
        self.port = 0

    @property
    def hits(self) -> int:
        return len(self.requests)

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    @property
    def last_body(self) -> dict[str, Any]:
        return self.requests[-1]["body"] if self.requests else {}

    @property
    def last_headers(self) -> dict[str, str]:
        return self.requests[-1]["headers"] if self.requests else {}

    async def start(self) -> FakeLayaServer:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = await reader.readuntil(b"\r\n\r\n")
            lines = head.decode("latin-1").split("\r\n")
            method, path, _ = lines[0].split(" ", 2)
            headers: dict[str, str] = {}
            for line in lines[1:]:
                if ":" in line:
                    key, value = line.split(":", 1)
                    headers[key.strip().lower()] = value.strip()
            length = int(headers.get("content-length", 0) or 0)
            raw = await reader.readexactly(length) if length else b""
            self.requests.append({"method": method, "path": path, "headers": headers, "body": json.loads(raw or b"{}")})
            delay = float(self.response.get("delay") or 0.0)
            if delay:
                await asyncio.sleep(delay)
            status = int(self.response.get("status", 200))
            if "text" in self.response:
                blob = str(self.response["text"]).encode("utf-8")
            else:
                blob = json.dumps(self.response.get("json")).encode("utf-8")
            writer.write(
                b"HTTP/1.1 %d X\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n"
                % (status, len(blob))
                + blob
            )
            await writer.drain()
        finally:
            writer.close()


def choice_answer(label: str, *, confidence: float = 0.68, probabilities: dict[str, float] | None = None) -> dict:
    return {
        "type": "choice",
        "choice": label,
        "probabilities": probabilities or {label: 0.72, "其它": 0.28},
        "confidence": confidence,
        "action": {"kind": "label"},
    }


def score_answer(value: float, *, confidence: float = 0.51) -> dict:
    return {"type": "score", "score": value, "probabilities": {}, "confidence": confidence}


def noul_answer(*, confidence: float = 0.9) -> dict:
    return {"type": "noul", "noul": True, "probabilities": {}, "confidence": confidence}


def laya_response(answers: dict[str, dict], *, model: str = "typed-decisions", input_tokens: int = 42) -> dict:
    return {
        "model": model,
        "answers": answers,
        "usage": {"input_tokens": input_tokens, "output_tokens": 0},
        "routing": {"provider": "laya", "ms": 169},
    }


def make_transport(server: FakeLayaServer | None = None, **kwargs: Any) -> LayaSystemOneTransport:
    options: dict[str, Any] = {
        "base_url": server.base_url if server is not None else "",
        "api_key": "sk-test",
        "timeout": 2.0,
        "provider": "laya",
    }
    options.update(kwargs)
    return LayaSystemOneTransport(**options)


@pytest.fixture
async def server_factory() -> AsyncIterator[Callable[..., Any]]:
    created: list[FakeLayaServer] = []

    async def make(response: dict[str, Any] | None = None) -> FakeLayaServer:
        server = await FakeLayaServer(response).start()
        created.append(server)
        return server

    try:
        yield make
    finally:
        for server in created:
            await server.stop()


# ---- 接口对齐 ----------------------------------------------------------
def test_complete_and_aclose_match_http_transport_signatures():
    for name in ("complete", "aclose"):
        expected = inspect.signature(getattr(HttpTransport, name))
        actual = inspect.signature(getattr(LayaSystemOneTransport, name))
        assert [(p.name, p.kind) for p in actual.parameters.values()] == [
            (p.name, p.kind) for p in expected.parameters.values()
        ], name


def test_can_be_returned_by_build_transport(config):
    """与 transport.build_transport 构造 HttpTransport 的入参完全一致，可直接被它返回。"""

    spec = provider_spec(config, "a6api")
    retry_cfg = config.section("model.retry")
    transport = LayaSystemOneTransport(
        base_url=str(spec.get("base_url") or "http://127.0.0.1:7858"),
        api_key=resolve_api_key(config, "a6api", environ={}),
        timeout=float(retry_cfg.get("timeout", 30.0)),
        provider="a6api",
    )
    assert isinstance(transport, ModelTransport)
    assert transport.provider == "a6api"
    assert transport.path == SYSTEM_ONE_PATH
    assert transport.default_model == "auto"
    assert all(model in LAY_A_MODELS for model in ("auto", "english", "multilingual", "typed-decisions"))


# ---- 三原语 ------------------------------------------------------------
async def test_choice_primitive_round_trip(server_factory):
    server = await server_factory({"json": laya_response({"label": choice_answer("提问")})})
    transport = make_transport(server)
    try:
        answers = await transport.ask(
            "在吗", {"label": laya.choice_question("这句话在做什么？", dict.fromkeys(LABELS, "x"))}
        )
    finally:
        await transport.aclose()

    assert server.hits == 1
    assert server.requests[0]["method"] == "POST"
    assert server.requests[0]["path"] == SYSTEM_ONE_PATH
    assert server.last_headers["authorization"] == "Bearer sk-test"
    assert server.last_body["state"] == "在吗"
    assert server.last_body["model"] == "auto"
    assert server.last_body["questions"]["label"]["type"] == "choice"
    assert sorted(server.last_body["questions"]["label"]["criteria"]) == sorted(LABELS)

    answer = answers["label"]
    assert answer["type"] == "choice"
    assert answer["choice"] == "提问"
    assert answer["probabilities"] == {"提问": 0.72, "其它": 0.28}
    assert answer["confidence"] == pytest.approx(0.68)
    assert answer["action"] == {"kind": "label"}


async def test_score_primitive_round_trip(server_factory):
    server = await server_factory({"json": laya_response({"urge": score_answer(0.73)})})
    transport = make_transport(server)
    try:
        answers = await transport.ask(
            {"messages": ["在吗"]},
            {"urge": laya.score_question("这句话的催促程度", ["不催", "一般", "很急"])},
        )
    finally:
        await transport.aclose()

    assert server.last_body["state"] == {"messages": ["在吗"]}
    assert server.last_body["questions"]["urge"]["criteria"] == ["不催", "一般", "很急"]
    assert answers["urge"]["type"] == "score"
    assert answers["urge"]["score"] == pytest.approx(0.73)
    assert answers["urge"]["probabilities"] == {}
    assert answers["urge"]["confidence"] == pytest.approx(0.51)


async def test_noul_primitive_round_trip(server_factory):
    server = await server_factory({"json": laya_response({"should_speak": noul_answer()})})
    transport = make_transport(server)
    try:
        answers = await transport.ask("在吗", {"should_speak": laya.noul_question("现在适合插话吗")})
    finally:
        await transport.aclose()

    assert "criteria" not in server.last_body["questions"]["should_speak"]
    assert answers["should_speak"]["type"] == "noul"
    assert answers["should_speak"]["noul"] is True
    assert answers["should_speak"]["confidence"] == pytest.approx(0.9)


async def test_batch_questions_use_one_http_request(server_factory):
    questions = {
        "label": laya.choice_question("意图？", dict.fromkeys(LABELS, "x")),
        "urgency": laya.score_question("紧急度", ["低", "中", "高"]),
        "should_speak": laya.noul_question("现在适合插话吗"),
        "topic": laya.choice_question("话题？", {"游戏": "x", "工作": "y"}),
        "mood": laya.score_question("情绪强度", ["弱", "强"]),
        "ask": laya.noul_question("在提问吗"),
        "q7": laya.choice_question("q7", {"a": "x"}),
        "q8": laya.score_question("q8", ["a", "b"]),
        "q9": laya.noul_question("q9"),
        "q10": laya.choice_question("q10", {"a": "x"}),
        "q11": laya.score_question("q11", ["a"]),
        "q12": laya.noul_question("q12"),
    }
    answers = {qid: noul_answer() for qid in questions}
    answers["label"] = choice_answer("提问")
    server = await server_factory({"json": laya_response(answers)})
    transport = make_transport(server)
    try:
        result = await transport.ask("一大段群聊上下文", questions)
    finally:
        await transport.aclose()

    assert server.hits == 1, "批量提问必须只发一次 HTTP"
    assert len(server.last_body["questions"]) == 12
    assert len(result) == 12
    assert result["label"]["choice"] == "提问"


# ---- 本地校验（不浪费往返） ---------------------------------------------
async def test_invalid_model_is_rejected_locally_without_http(server_factory):
    server = await server_factory()
    transport = make_transport(server)
    try:
        with pytest.raises(TransportError) as excinfo:
            await transport.ask("在吗", {"label": laya.choice_question("意图？", {"提问": "x"})}, model="laya")
    finally:
        await transport.aclose()
    assert excinfo.value.retryable is False
    assert "model" in str(excinfo.value)
    assert server.hits == 0, "非法 model 必须本地报错，不发 HTTP"


def test_build_request_validates_locally():
    with pytest.raises(TransportError) as empty:
        build_request("x", {})
    assert empty.value.retryable is False

    with pytest.raises(TransportError) as bad_type:
        build_request("x", {"q": {"type": "yesno", "instructions": "?"}})
    assert "yesno" in str(bad_type.value)

    with pytest.raises(TransportError) as no_instructions:
        build_request("x", {"q": {"type": "choice", "criteria": {"a": "b"}}})
    assert "instructions" in str(no_instructions.value)

    with pytest.raises(TransportError):
        build_request("x", {"q": {"type": "choice", "instructions": "?", "criteria": {}}})

    with pytest.raises(TransportError):
        build_request("x", {"q": {"type": "score", "instructions": "?", "criteria": "不是数组"}})

    with pytest.raises(TransportError) as state_error:
        build_request({"bad": {1, 2}}, {"q": laya.noul_question("?")})
    assert "state" in str(state_error.value)

    with pytest.raises(TransportError) as model_error:
        build_request("x", {"q": laya.noul_question("?")}, model="grok-4.6")
    assert model_error.value.retryable is False


async def test_unsupported_tasks_are_rejected(server_factory):
    server = await server_factory()
    transport = make_transport(server)
    try:
        chat = {"model": "auto", "messages": [{"role": "user", "content": "在吗"}]}
        with pytest.raises(TransportError) as chat_error:
            await transport.complete(chat)
        assert chat_error.value.retryable is False
        assert "classify" in str(chat_error.value)

        embed = {"model": "auto", "input": ["你好"]}
        with pytest.raises(TransportError) as embed_error:
            await transport.complete(embed)
        assert embed_error.value.retryable is False
    finally:
        await transport.aclose()
    assert server.hits == 0

    assert payload_task({"task": "chat", "messages": []}) == "chat"
    assert payload_task({"task": "embed", "input": []}) == "embed"
    assert payload_task({"questions": {"q": {}}}) == "system1"
    assert payload_task({"task": "system1", "state": "x", "questions": {"q": {}}}) == "system1"


# ---- 错误映射 ----------------------------------------------------------
@pytest.mark.parametrize("status", [401, 422])
async def test_auth_and_format_errors_are_not_retryable(server_factory, status: int):
    server = await server_factory({"status": status, "json": {"error": "nope"}})
    transport = make_transport(server)
    try:
        with pytest.raises(TransportError) as excinfo:
            await transport.ask("在吗", {"label": laya.noul_question("?")})
    finally:
        await transport.aclose()
    assert excinfo.value.status == status
    assert excinfo.value.retryable is False


async def test_server_error_is_retryable(server_factory):
    server = await server_factory({"status": 503, "json": {"error": "model unavailable"}})
    transport = make_transport(server)
    try:
        with pytest.raises(TransportError) as excinfo:
            await transport.ask("在吗", {"label": laya.noul_question("?")})
    finally:
        await transport.aclose()
    assert excinfo.value.status == 503
    assert excinfo.value.retryable is True


async def test_timeout_is_retryable(server_factory):
    server = await server_factory({"json": laya_response({"label": noul_answer()}), "delay": 0.5})
    transport = make_transport(server, timeout=0.1)
    try:
        with pytest.raises(TransportError) as excinfo:
            await transport.ask("在吗", {"label": laya.noul_question("?")})
    finally:
        await transport.aclose()
    assert excinfo.value.retryable is True
    assert "超时" in str(excinfo.value)


async def test_connection_error_is_retryable():
    transport = make_transport(None, base_url="http://127.0.0.1:1", timeout=0.3)
    try:
        with pytest.raises(TransportError) as excinfo:
            await transport.ask("在吗", {"label": laya.noul_question("?")})
    finally:
        await transport.aclose()
    assert excinfo.value.retryable is True


async def test_missing_base_url_is_not_retryable():
    transport = make_transport(None, base_url="", provider="laya")
    with pytest.raises(TransportError) as excinfo:
        await transport.ask("在吗", {"label": laya.noul_question("?")})
    assert "base_url" in str(excinfo.value)
    assert excinfo.value.retryable is False


async def test_non_json_and_non_object_responses_are_not_retryable(server_factory):
    broken = await server_factory({"text": "<html>not json</html>"})
    transport = make_transport(broken)
    try:
        with pytest.raises(TransportError) as excinfo:
            await transport.ask("在吗", {"label": laya.noul_question("?")})
        assert excinfo.value.retryable is False
        assert "JSON" in str(excinfo.value)
    finally:
        await transport.aclose()

    scalar = await server_factory({"json": None})
    other = make_transport(scalar)
    try:
        with pytest.raises(TransportError) as excinfo:
            await other.ask("在吗", {"label": laya.noul_question("?")})
        assert excinfo.value.retryable is False
        assert "对象" in str(excinfo.value)
    finally:
        await other.aclose()


async def test_missing_answers_is_not_retryable(server_factory):
    server = await server_factory({"json": {"model": "auto", "usage": {}}})
    transport = make_transport(server)
    try:
        data = await transport.complete({"questions": {"q": laya.noul_question("?")}, "state": "x", "model": "auto"})
        with pytest.raises(TransportError) as excinfo:
            normalize_answers(data)
    finally:
        await transport.aclose()
    assert excinfo.value.retryable is False


# ---- classify 复用 -----------------------------------------------------
def test_classify_request_builds_a_choice_question():
    body = classify_request("在吗", LABELS)
    assert body["state"] == "在吗"
    assert body["model"] == "auto"
    question = body["questions"]["label"]
    assert question["type"] == "choice"
    assert sorted(question["criteria"]) == sorted(LABELS)
    assert question["instructions"]

    with pytest.raises(TransportError):
        classify_request("在吗", [])


async def test_transport_classify_round_trip(server_factory):
    payload = laya_response(
        {"label": choice_answer("闲聊", confidence=0.44, probabilities={"闲聊": 0.44, "提问": 0.56})}
    )
    server = await server_factory({"json": payload})
    transport = make_transport(server)
    try:
        result = await transport.classify("在吗", LABELS)
    finally:
        await transport.aclose()

    assert result["label"] == "闲聊"
    assert result["probabilities"] == {"闲聊": 0.44, "提问": 0.56}
    assert result["scores"] == {"闲聊": 0.44, "提问": 0.56}
    assert result["confidence"] == pytest.approx(0.44)
    assert result["in_candidates"] is True
    assert server.last_body["questions"]["label"]["instructions"] == laya.DEFAULT_CLASSIFY_INSTRUCTIONS


async def test_codec_classify_payload_is_served(server_factory):
    """rpc:model.classify 现有的 codec 请求体可以直接喂进来（无需改 codec）。"""

    payload = encode_request(
        ModelRequest(
            task="classify",
            model="auto",
            messages=({"role": "user", "content": "在吗"},),
            labels=LABELS,
            params={"temperature": 0.0},
        )
    )
    assert "questions" not in payload and "labels" not in payload  # codec 把候选标签写进了提示词

    server = await server_factory({"json": laya_response({"label": choice_answer("提问")})})
    transport = make_transport(server)
    try:
        data = await transport.complete(payload)
    finally:
        await transport.aclose()

    assert server.hits == 1
    body = server.last_body
    assert body["model"] == "auto"
    assert body["state"] == "在吗"
    assert sorted(body["questions"]["label"]["criteria"]) == sorted(LABELS)
    assert normalize_answers(data)["label"]["choice"] == "提问"


# ---- 工具与安全 --------------------------------------------------------
def test_normalize_usage_maps_tokens():
    assert normalize_usage({"usage": {"input_tokens": 42, "output_tokens": 0}}) == {
        "prompt_tokens": 42,
        "completion_tokens": 0,
        "total_tokens": 42,
    }
    assert normalize_usage({}) == {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}


async def test_aclose_is_idempotent(server_factory):
    server = await server_factory()
    transport = make_transport(server)
    await transport.ask("在吗", {"label": laya.noul_question("?")})
    assert transport._client is not None
    await transport.aclose()
    assert transport._client is None
    await transport.aclose()


def test_module_has_no_hardcoded_secret_and_no_new_dependency():
    source = Path(laya.__file__).read_text(encoding="utf-8")
    assert not re.search(r"sk-[A-Za-z0-9]{8,}", source)
    assert not re.search(r"Bearer\s+[A-Za-z0-9]", source)
    assert "Bearer {self.api_key}" in source

    tree = ast.parse(source)
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            imported.add(node.module.split(".")[0])
    allowed = {"__future__", "collections", "dataclasses", "json", "re", "typing", "grouppig", "httpx"}
    assert imported <= allowed, sorted(imported - allowed)

    fields = {f.name: f for f in dataclasses.fields(LayaSystemOneTransport)}
    assert fields["api_key"].default is None
    assert fields["base_url"].default is dataclasses.MISSING  # base_url 必须显式传入
