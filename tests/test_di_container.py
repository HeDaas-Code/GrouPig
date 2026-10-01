"""依赖注入容器与装配入口测试。"""

from __future__ import annotations

from pathlib import Path

import pytest

from grouppig.infra.config.loader import get_config
from grouppig.infra.model_gateway.router import get_router
from grouppig.infra.runtime import contract
from grouppig.infra.runtime.di import (
    Container,
    build_container,
    get_container,
    register_infra_handlers,
    set_container,
    start,
)
from grouppig.infra.runtime.errors import ConfigError, HandlerNotRegistered
from grouppig.infra.token_budget.meter import get_meter
from helpers import FakeTransport, design_tree_facts

TOPIC = "kafka:grouppig.event.routed"
INFRA = "grouppig.infra"


def infra_contract_names() -> set[str]:
    return design_tree_facts().names_of(INFRA)


def infra_registered(container) -> set[str]:
    return {name for name in container.registry.names() if (contract.owner(name) or "").startswith(INFRA)}


def test_build_container_is_not_started_until_start(config):
    container = build_container(config=config)
    assert container.started is False
    assert container.router is not None and container.meter is not None and container.bus is not None
    assert infra_registered(container) == infra_contract_names()


def test_build_container_rejects_invalid_config(config):
    with pytest.raises(ConfigError):
        build_container(config=config.with_overrides({"storage": {"driver": "oracle"}}))
    build_container(config=config.with_overrides({"storage": {"driver": "oracle"}}), validate=False)  # 跳过校验可用


async def test_start_binds_global_singletons(config):
    container = build_container(config=config)
    await container.start()
    try:
        assert container.started is True
        assert get_config().get("app.name") == "grouppig"
        assert get_meter() is container.meter
        assert get_router() is container.router
    finally:
        await container.aclose()
    assert container.started is False


async def test_health_reports_components_and_contract(container):
    health = container.health()
    assert health["started"] is True
    assert health["registry"]["infra_missing"] == []
    assert health["registry"]["unknown"] == []
    facts = design_tree_facts()
    assert health["registry"]["total"] >= len(facts.names_of(INFRA))
    assert len(health["bus"]["topics"]) == len(facts.topics)
    assert health["token"]["calls"] == 0
    assert health["contract"]["names"] == facts.counts["names"]
    assert health["contract"]["modules"] == facts.counts["modules"]
    assert health["model"]["meter"] is True


async def test_container_call_unknown_name_raises(container):
    with pytest.raises(HandlerNotRegistered):
        await container.call("rpc:session.does-not-exist")


async def test_container_register_and_call_custom_handler(config):
    container = await build_container(config=config).start()
    try:

        async def handler(value: int) -> int:
            return value * 2

        container.register("rpc:token.policy", handler, replace=True)
        assert await container.call("rpc:token.policy", 21) == 42
    finally:
        await container.aclose()


async def test_container_rejects_duplicate_registration(container):
    with pytest.raises(ValueError):
        container.register("rpc:config.get", lambda: {})


async def test_injected_transport_is_used_by_router(config):
    transport = FakeTransport(reply="hi")
    container = await build_container(config=config, transport=transport).start()
    try:
        payload = await container.call("rpc:model.chat", [{"role": "user", "content": "在吗"}])
        assert payload["text"] == "hi"
        assert len(transport.calls) == 1
    finally:
        await container.aclose()
    assert transport.closed is True


async def test_publish_uses_designed_topics_only(container):
    from grouppig.infra.runtime.errors import UnknownTopicError

    seen: list[dict] = []
    container.subscribe(TOPIC, lambda event: seen.append(event.payload))
    await container.publish(TOPIC, {"ok": True})
    assert seen == [{"ok": True}]
    with pytest.raises(UnknownTopicError):
        await container.publish("kafka:grouppig.made.up", {})


async def test_config_reload_through_container(config_file: Path):
    container = build_container(config_file)
    await container.start()
    try:
        assert container.config.get("app.env") == "dev"
        config_file.write_text(
            config_file.read_text(encoding="utf-8").replace('env = "dev"', 'env = "prod"'), encoding="utf-8"
        )
        result = await container.call("rpc:config.reload")
        assert result["ok"] is True and result["changed"] is True
        assert container.config.get("app.env") == "prod"
        assert get_config().get("app.env") == "prod"
        assert (await container.call("rpc:config.get", "app.env")) == "prod"
    finally:
        await container.aclose()


async def test_config_reload_keeps_previous_when_invalid(config_file: Path):
    container = build_container(config_file)
    await container.start()
    try:
        config_file.write_text(
            config_file.read_text(encoding="utf-8").replace('ws_url = "ws://127.0.0.1:3001"', 'ws_url = "http://nope"'),
            encoding="utf-8",
        )
        result = await container.call("rpc:config.reload")
        assert result["ok"] is False
        assert container.config.get("onebot.ws_url") == "ws://127.0.0.1:3001"
    finally:
        await container.aclose()


async def test_start_helper_binds_global_container(config):
    previous = set_container(None)
    try:
        container = await start(config=config)
        assert get_container() is container
        assert container.started is True
        await container.aclose()
    finally:
        set_container(previous)


def test_register_infra_handlers_is_idempotent(config):
    container = Container(config=config)
    register_infra_handlers(container)
    first = set(container.registry.names())
    register_infra_handlers(container)
    assert set(container.registry.names()) == first
    assert first == {name for name, module in contract.api_index().items() if module.startswith("grouppig.infra")}
    assert len(container.registry) == len(first)


async def test_close_is_idempotent(config, fake_transport):
    container = await build_container(config=config, transport=fake_transport).start()
    await container.aclose()
    await container.aclose()
    assert container.started is False


# ---- rpc:model.system1（LAY A 接入） --------------------------------------
class _FakeLaya:
    """假 LAY A 传输：直接返回 answers 结构，不连真实端点。"""

    def __init__(self, *, confidence: float = 0.9, choice: str = "提问") -> None:
        self.confidence = confidence
        self.choice = choice
        self.calls: list[dict] = []

    async def complete(self, payload, *, path="/chat/completions", timeout=None, provider=""):
        self.calls.append(dict(payload))
        return {
            "model": "auto",
            "answers": {
                "label": {
                    "type": "choice",
                    "choice": self.choice,
                    "probabilities": {self.choice: self.confidence, "闲聊": round(1 - self.confidence, 4)},
                    "confidence": self.confidence,
                }
            },
            "usage": {"input_tokens": 30, "output_tokens": 0},
        }

    async def aclose(self) -> None:
        return None


#: rpc:model.classify 的返回形态（codec.ModelResponse.as_dict() 的键，回归护栏）。
CLASSIFY_KEYS = {
    "task",
    "model",
    "provider",
    "text",
    "embedding_dim",
    "label",
    "scores",
    "usage",
    "finish_reason",
    "latency_ms",
    "request_id",
    "attempts",
}


async def test_rpc_model_system1_is_registered_and_returns_answers(config):
    """rpc:model.system1 已登记（模块归属 router），一次调用返回 answers/confidences/source。"""

    container = await build_container(config=config).start()
    try:
        assert "rpc:model.system1" in set(container.registry.names())
        handler = container.registry.get("rpc:model.system1")
        assert handler.module == contract.owner("rpc:model.system1")
        assert contract.owner("rpc:model.system1") == "grouppig.infra.model-gateway.router"

        laya = _FakeLaya()
        container.router.set_transport(laya, provider="laya")
        result = await container.call(
            "rpc:model.system1",
            {"text": "在吗"},
            {"label": {"type": "choice", "instructions": "分类", "criteria": {"提问": "在提问", "闲聊": "在闲聊"}}},
        )

        assert result["task"] == "system1"
        assert result["source"] == "system1"
        assert result["answers"]["label"]["choice"] == "提问"
        assert result["confidences"] == {"label": 0.9}
        assert result["aggregate"] == "min"
        assert result["usage"]["total_tokens"] == 30
        assert len(laya.calls) == 1  # 一次 HTTP
        assert set(laya.calls[0]["questions"]) == {"label"}
    finally:
        await container.aclose()


async def test_rpc_model_classify_shape_is_unchanged(config):
    """rpc:model.classify 的返回形态（label/scores）未改变。"""

    container = await build_container(config=config).start()
    try:
        container.router.set_transport(_FakeLaya(), provider="laya")
        result = await container.call("rpc:model.classify", "在吗", ["提问", "闲聊"])
        assert set(result) == CLASSIFY_KEYS
        assert result["task"] == "classify"
        assert result["label"] == "提问"
        assert result["scores"] == {"提问": 0.9, "闲聊": 0.1}
    finally:
        await container.aclose()


async def test_container_contract_check_reports_no_infra_gap(container):
    """装配根的契约自检：infra 子树没有缺口，也没有契约外的名字。"""

    report = container.contract_check()
    assert report["missing"] == []
    assert report["unknown"] == []
    assert report["expected_rpc"] == len(contract.rpc_names())
    assert report["registered"] >= len(design_tree_facts().names_of(INFRA))
    assert "rpc:model.system1" in container.registry.names()
