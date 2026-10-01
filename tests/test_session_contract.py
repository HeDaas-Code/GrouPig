"""session 契约对齐测试：25 个 ``rpc:`` 名字、2 个 ``kafka:`` 主题、模块路径与归属必须逐字对齐 normify 设计。"""

from __future__ import annotations

from pathlib import Path

import pytest

from grouppig.infra.runtime import contract
from grouppig.infra.runtime.errors import UnknownNameError
from grouppig.infra.runtime.registry import Registry
from grouppig.session.runtime.di import (
    SESSION_RPC,
    SESSION_SCOPE,
    SESSION_TOPICS,
    SessionLayer,
    register_session_handlers,
)
from session_helpers import SESSION_RPC as EXPECTED_RPC
from session_helpers import SESSION_TOPICS as EXPECTED_TOPICS

REPO_ROOT = Path(__file__).resolve().parents[1]

#: 本域 16 个设计叶子的模块 id（逐字来自 normify 设计树）。
EXPECTED_MODULES = (
    "grouppig.session",
    "grouppig.session.lifecycle",
    "grouppig.session.lifecycle.archive-trigger",
    "grouppig.session.lifecycle.event-emitter",
    "grouppig.session.lifecycle.heat",
    "grouppig.session.lifecycle.state-machine",
    "grouppig.session.threads",
    "grouppig.session.threads.cross",
    "grouppig.session.threads.cross.matcher",
    "grouppig.session.threads.cross.reference-parser",
    "grouppig.session.threads.weaver",
    "grouppig.session.threads.weaver.linker",
    "grouppig.session.threads.weaver.outliner",
    "grouppig.session.threads.weaver.segmenter",
    "grouppig.session.topic",
    "grouppig.session.topic.detector",
    "grouppig.session.topic.detector.boundary",
    "grouppig.session.topic.detector.candidate",
    "grouppig.session.topic.detector.ranker",
    "grouppig.session.topic.embedder",
    "grouppig.session.topic.embedder.cache",
    "grouppig.session.topic.embedder.similarity",
    "grouppig.session.wake",
    "grouppig.session.wake.buffer",
    "grouppig.session.wake.restorer",
)

#: 设计里归属 ``grouppig.session`` 的 16 个叶子（去掉 9 个容器）。
EXPECTED_LEAVES = tuple(
    module
    for module in EXPECTED_MODULES
    if module
    not in {
        "grouppig.session",
        "grouppig.session.lifecycle",
        "grouppig.session.threads",
        "grouppig.session.threads.cross",
        "grouppig.session.threads.weaver",
        "grouppig.session.topic",
        "grouppig.session.topic.detector",
        "grouppig.session.topic.embedder",
        "grouppig.session.wake",
    }
)


def test_session_names_in_api_index_match_expectation():
    expected = {name for name, module in contract.api_index().items() if module.startswith(SESSION_SCOPE)}
    assert expected == set(EXPECTED_RPC) | set(EXPECTED_TOPICS)
    assert len(expected) == 27
    assert all(contract.owner(name).startswith(SESSION_SCOPE) for name in expected)


def test_module_constants_match_contract():
    assert set(SESSION_RPC) == set(EXPECTED_RPC)
    assert tuple(SESSION_TOPICS) == EXPECTED_TOPICS
    # 域导出的常量 == 契约里本域全部 rpc 名字
    designed = {n for n, m in contract.api_index().items() if m.startswith(SESSION_SCOPE) and n.startswith("rpc:")}
    assert set(SESSION_RPC) == designed
    assert len(designed) == 25


def test_every_session_leaf_source_path_exists_and_mirrors_design():
    modules = contract.modules()
    for module_id in EXPECTED_MODULES:
        assert module_id in modules, module_id
        spec = modules[module_id]
        path = contract.module_to_path(module_id, root=REPO_ROOT)
        assert path.is_file(), f"{module_id} → {path} 不存在"
        if spec.is_container:
            assert path.name == "__init__.py"
    # 每个设计叶子的契约名字都必须已被注册常量覆盖
    for module_id in EXPECTED_LEAVES:
        spec = modules[module_id]
        for name in (*spec.rpc_names, *spec.topic_names):
            assert name in set(SESSION_RPC) | set(SESSION_TOPICS), f"{module_id} 的 {name} 不在常量表里"


def test_registered_session_handlers_exactly_cover_contract(session_container):
    container, layer = session_container
    registered = set(container.registry.names())
    check = contract.check_registry(registered, scope=SESSION_SCOPE)
    missing_rpc = [name for name in check["missing"] if name.startswith("rpc:")]
    assert missing_rpc == [], f"契约里属于 session 但没注册：{missing_rpc}"
    session_registered = {name for name in registered if (contract.owner(name) or "").startswith(SESSION_SCOPE)}
    assert session_registered == set(EXPECTED_RPC)
    assert layer.contract_check() == {"missing": [], "unknown": []}


def test_handler_module_attribution_is_the_design_leaf(session_container):
    container, _layer = session_container
    for name in EXPECTED_RPC:
        registration = container.registry.get(name)
        assert registration.module == contract.owner(name), name
        assert registration.is_async, f"{name} 应为协程处理器"


def test_register_session_handlers_is_idempotent_and_rejects_foreign_registry(config):
    layer = SessionLayer()
    registry = Registry()
    register_session_handlers(registry, layer)
    first = set(registry.names())
    register_session_handlers(registry, layer)  # replace=True，可重复注册
    assert set(registry.names()) == first
    assert set(EXPECTED_RPC) <= first
    with pytest.raises(UnknownNameError):
        registry.register("rpc:session.self-invented", lambda: None)


def test_isolated_registry_does_not_leak_into_global():
    from grouppig.infra.runtime.registry import default_registry

    layer = SessionLayer()
    registry = Registry()
    layer.register(registry)
    assert not (set(EXPECTED_RPC) & set(default_registry.names()))


async def test_attach_session_registers_and_exposes_layer(config):
    from grouppig.infra.logger import RuntimeLogger
    from grouppig.infra.runtime.bus import EventBus
    from grouppig.infra.runtime.di import Container
    from grouppig.session.runtime.di import TOPIC_MESSAGE_RECEIVED, attach_session, get_session

    container = Container(
        config=config, registry=Registry(), logger=RuntimeLogger(), bus=EventBus(logger=RuntimeLogger())
    )
    layer = await attach_session(container)
    assert get_session(container) is layer
    assert set(EXPECTED_RPC) <= set(container.registry.names())
    assert layer.subscription is not None
    assert container.bus.subscribers(TOPIC_MESSAGE_RECEIVED) != ()
    await layer.aclose()
    await container.aclose()


def test_session_topics_are_publishable_on_strict_bus(session_container):
    _container, layer = session_container
    assert layer.health()["topics"] == list(EXPECTED_TOPICS)
    assert all(contract.is_known_name(topic) for topic in EXPECTED_TOPICS)
