"""social 契约测试：名字逐字对齐设计、模块路径与源码一一对应、装配后契约零缺口。"""

from __future__ import annotations

from pathlib import Path

import pytest

from grouppig.infra.runtime import contract
from grouppig.social import (
    GRAPH_MANAGER_RPC,
    GRAPH_RELATIONSHIP_RPC,
    PROFILE_EXTRACTOR_RPC,
    PROFILE_MANAGER_RPC,
    SOCIAL_RPC,
    SOCIAL_SCOPE,
    SOCIAL_TOPICS,
    SPEECH_PROFILER_RPC,
    SPEECH_RESPONDER_RPC,
    build_social,
)
from grouppig.social.graph.manager import egonet as egonet_module
from grouppig.social.graph.manager import tiering as tiering_module
from grouppig.social.graph.relationship import decay as decay_module
from grouppig.social.graph.relationship import rules as rules_module
from grouppig.social.profile.extractor import conflict_resolver as conflict_module
from grouppig.social.profile.extractor import fact_extractor as fact_module
from grouppig.social.profile.extractor import stance_extractor as stance_module
from grouppig.social.profile.manager import lookup as lookup_module
from grouppig.social.profile.manager import versioning as versioning_module
from grouppig.social.speech.profiler import lexicon as lexicon_module
from grouppig.social.speech.profiler import style_metrics as metrics_module
from grouppig.social.speech.profiler import temper as temper_module
from grouppig.social.speech.responder import adapter as adapter_module
from grouppig.social.speech.responder import validator as validator_module
from social_helpers import social_contract_coverage, social_env

REPO_ROOT = Path(__file__).resolve().parents[1]

#: 设计里归属 social 的 15 个叶子模块（逐字抄自 normify 树）。
EXPECTED_MODULES: tuple[str, ...] = (
    "grouppig.social",
    "grouppig.social.profile",
    "grouppig.social.profile.extractor",
    "grouppig.social.profile.extractor.conflict-resolver",
    "grouppig.social.profile.extractor.fact-extractor",
    "grouppig.social.profile.extractor.stance-extractor",
    "grouppig.social.profile.manager",
    "grouppig.social.profile.manager.events",
    "grouppig.social.profile.manager.lookup",
    "grouppig.social.profile.manager.versioning",
    "grouppig.social.speech",
    "grouppig.social.speech.profiler",
    "grouppig.social.speech.profiler.lexicon",
    "grouppig.social.speech.profiler.style-metrics",
    "grouppig.social.speech.profiler.temper",
    "grouppig.social.speech.responder",
    "grouppig.social.speech.responder.adapter",
    "grouppig.social.speech.responder.validator",
    "grouppig.social.graph",
    "grouppig.social.graph.manager",
    "grouppig.social.graph.manager.egonet",
    "grouppig.social.graph.manager.events",
    "grouppig.social.graph.manager.tiering",
    "grouppig.social.graph.relationship",
    "grouppig.social.graph.relationship.decay",
    "grouppig.social.graph.relationship.rules",
)

#: 15 个叶子 → 实现模块（Python 用下划线，设计用连字符）。
LEAF_MODULES = (lookup_module, versioning_module, fact_module, stance_module, conflict_module)


def test_social_names_in_api_index_match_expectation():
    expected = {name for name, module in contract.api_index().items() if module.startswith(SOCIAL_SCOPE)}
    assert expected == set(SOCIAL_RPC) | set(SOCIAL_TOPICS)
    assert len(SOCIAL_RPC) == 17
    assert len(SOCIAL_TOPICS) == 2
    assert all(contract.owner(name).startswith(SOCIAL_SCOPE) for name in expected)


def test_social_module_ids_and_source_paths_exist():
    modules = contract.modules()
    for module_id in EXPECTED_MODULES:
        assert module_id in modules, module_id
        spec = modules[module_id]
        path = contract.module_to_path(module_id)
        assert path.is_file(), f"{module_id} → {path} 不存在"
        if spec.is_container:
            assert path.name == "__init__.py"


def test_every_leaf_rpc_name_lives_in_its_declared_constant():
    """每个叶子的契约名字必须出现在它自己的 ``RPC_NAMES`` 常量里（防漏挂/挂错）。"""

    for module in (
        lookup_module,
        versioning_module,
        fact_module,
        stance_module,
        conflict_module,
        lexicon_module,
        temper_module,
        metrics_module,
        validator_module,
        adapter_module,
        egonet_module,
        tiering_module,
        rules_module,
        decay_module,
    ):
        assert module.RPC_NAMES, module.__name__
        for name in module.RPC_NAMES:
            assert name in SOCIAL_RPC, f"{module.__name__} 的 {name} 不在 SOCIAL_RPC 常量表里"
            assert (
                contract.owner(name) == module.MODULE_ID.replace("_", "-")
                or contract.owner(name).replace("-", "_") == module.MODULE_ID
            ), f"{name} 的 owner 不是 {module.MODULE_ID}"


def test_leaves_cover_all_seventeen_rpc_names():
    groups = (
        PROFILE_MANAGER_RPC,
        PROFILE_EXTRACTOR_RPC,
        SPEECH_PROFILER_RPC,
        SPEECH_RESPONDER_RPC,
        GRAPH_MANAGER_RPC,
        GRAPH_RELATIONSHIP_RPC,
    )
    flat = [name for group in groups for name in group]
    assert sorted(flat) == sorted(SOCIAL_RPC)
    assert len(set(flat)) == len(flat) == 17


def test_event_leaves_declare_design_topics():
    from grouppig.social.graph.manager import events as graph_events
    from grouppig.social.profile.manager import events as profile_events

    assert profile_events.TOPIC_PROFILE_UPDATED == "kafka:grouppig.profile.updated"
    assert graph_events.TOPIC_SOCIAL_CHANGED == "kafka:grouppig.social.changed"
    assert profile_events.NAMES == (profile_events.TOPIC_PROFILE_UPDATED,)
    assert graph_events.NAMES == (graph_events.TOPIC_SOCIAL_CHANGED,)
    # 事件叶子不注册 rpc: 处理器
    assert profile_events.make_handlers(None, None) == {}
    assert graph_events.make_handlers(None, None) == {}


def test_each_design_dependency_name_is_used_by_its_leaf():
    """设计依赖 (from_api → to_api) 必须在实现里作为常量被引用。"""

    pairs = {
        (lookup_module.RPC_GET, lookup_module.DEP_PROFILE_STORE_GET),
        (versioning_module.RPC_UPDATE, versioning_module.DEP_PROFILE_GET),
        (versioning_module.RPC_UPDATE, versioning_module.DEP_PROFILE_STORE_PUT),
        (versioning_module.RPC_UPDATE, versioning_module.DEP_TOPIC_PROFILE_UPDATED),
        (fact_module.RPC_EXTRACT, fact_module.DEP_CHAT_QUERY),
        (fact_module.RPC_EXTRACT, fact_module.DEP_PROFILE_UPDATE),
        (stance_module.RPC_EXTRACT, stance_module.DEP_THREAD_LOAD),
        (stance_module.RPC_EXTRACT, stance_module.DEP_PROFILE_UPDATE),
        (conflict_module.RPC_RESOLVE, conflict_module.DEP_PROFILE_UPDATE),
        (lexicon_module.RPC_LEXICON, lexicon_module.DEP_CHAT_QUERY),
        (temper_module.RPC_TEMPER, temper_module.DEP_CHAT_QUERY),
        (metrics_module.RPC_PROFILE, metrics_module.DEP_CHAT_QUERY),
        (metrics_module.RPC_PROFILE, metrics_module.DEP_PROFILE_GET),
        (metrics_module.RPC_PROFILE, metrics_module.DEP_SPEECH_LEXICON),
        (metrics_module.RPC_PROFILE, metrics_module.DEP_SPEECH_TEMPER),
        (adapter_module.RPC_ADVISE, adapter_module.DEP_SPEECH_STYLE),
        (adapter_module.RPC_TAILOR, adapter_module.DEP_SPEECH_VALIDATE),
        (egonet_module.RPC_EGONET, egonet_module.DEP_PROFILE_GET),
        (egonet_module.RPC_EGONET, egonet_module.DEP_SOCIAL_GET_EDGES),
        (tiering_module.RPC_TIERING, tiering_module.DEP_EGONET),
        (tiering_module.RPC_TIERING, tiering_module.DEP_SOCIAL_PUT_EDGE),
        (tiering_module.RPC_TIERING, tiering_module.DEP_TOPIC_SOCIAL_CHANGED),
        (rules_module.RPC_ADJUST, rules_module.DEP_DECAY),
        (rules_module.RPC_ADJUST, rules_module.DEP_TIERING),
        (decay_module.RPC_DECAY, decay_module.DEP_SOCIAL_GET_EDGES),
    }
    for from_api, to_api in pairs:
        assert contract.is_known_name(from_api), from_api
        assert contract.is_known_name(to_api), to_api


def test_design_dependencies_match_normify_frontmatter():
    """把实现里的 from_api→to_api 对与 normify 设计文件逐条对照。"""

    import re

    from grouppig.infra.runtime.errors import ContractError

    expected: set[tuple[str, str]] = set()
    for path in (REPO_ROOT / "normify-grouppig" / "modules" / "grouppig" / "social").rglob("*.md"):
        text = path.read_text(encoding="utf-8")
        for block in re.findall(r"deps:\n((?:\s+- .*\n|\s+.*\n)*)", text):
            pairs = re.findall(r'from_api:\s*"([^"]+)"\s*\n\s*to_api:\s*"([^"]+)"', block)
            expected.update(pairs)
    implemented = {
        (lookup_module.RPC_GET, lookup_module.DEP_PROFILE_STORE_GET),
        (versioning_module.RPC_UPDATE, versioning_module.DEP_PROFILE_GET),
        (versioning_module.RPC_UPDATE, versioning_module.DEP_PROFILE_STORE_PUT),
        (versioning_module.RPC_UPDATE, versioning_module.DEP_TOPIC_PROFILE_UPDATED),
        (fact_module.RPC_EXTRACT, fact_module.DEP_CHAT_QUERY),
        (fact_module.RPC_EXTRACT, fact_module.DEP_PROFILE_UPDATE),
        (stance_module.RPC_EXTRACT, stance_module.DEP_THREAD_LOAD),
        (stance_module.RPC_EXTRACT, stance_module.DEP_PROFILE_UPDATE),
        (conflict_module.RPC_RESOLVE, conflict_module.DEP_PROFILE_UPDATE),
        (lexicon_module.RPC_LEXICON, lexicon_module.DEP_CHAT_QUERY),
        (temper_module.RPC_TEMPER, temper_module.DEP_CHAT_QUERY),
        (metrics_module.RPC_PROFILE, metrics_module.DEP_CHAT_QUERY),
        (metrics_module.RPC_PROFILE, metrics_module.DEP_PROFILE_GET),
        (metrics_module.RPC_PROFILE, metrics_module.DEP_SPEECH_LEXICON),
        (metrics_module.RPC_PROFILE, metrics_module.DEP_SPEECH_TEMPER),
        (adapter_module.RPC_ADVISE, adapter_module.DEP_SPEECH_STYLE),
        (adapter_module.RPC_TAILOR, adapter_module.DEP_SPEECH_VALIDATE),
        (egonet_module.RPC_EGONET, egonet_module.DEP_PROFILE_GET),
        (egonet_module.RPC_EGONET, egonet_module.DEP_SOCIAL_GET_EDGES),
        (tiering_module.RPC_TIERING, tiering_module.DEP_EGONET),
        (tiering_module.RPC_TIERING, tiering_module.DEP_SOCIAL_PUT_EDGE),
        (tiering_module.RPC_TIERING, tiering_module.DEP_TOPIC_SOCIAL_CHANGED),
        (rules_module.RPC_ADJUST, rules_module.DEP_DECAY),
        (rules_module.RPC_ADJUST, rules_module.DEP_TIERING),
        (decay_module.RPC_DECAY, decay_module.DEP_SOCIAL_GET_EDGES),
    }
    assert expected, "没有从设计文件里解析到 deps（正则或路径失效）"
    missing = expected - implemented
    assert not missing, f"设计依赖未实现：{sorted(missing)}"
    assert ContractError  # 保持导入（contract 错误类型在别处断言）


def test_contract_names_are_registered_after_install(config):
    """装配后本域 17 个 rpc 名字全部注册，且没有契约外的名字。"""

    layer = build_social(container=None)
    assert layer.registry is not None
    layer.register()
    registered = set(layer.registry.names())
    assert set(SOCIAL_RPC) <= registered
    check = contract.check_registry(registered, scope=SOCIAL_SCOPE)
    # kafka: 主题是发布/订阅，不进注册表；rpc 名字必须全覆盖且无自造名
    assert [name for name in check["missing"] if name.startswith("rpc:")] == []
    assert check["unknown"] == []


async def test_install_social_registers_and_subscribes(config):
    env = await social_env(config)
    try:
        check = env.layer.contract_check()
        assert check["missing"] == []
        assert check["unknown"] == []
        coverage = social_contract_coverage(env.registry)
        assert coverage == {"missing": [], "unknown": [], "topics_missing": []}
        health = await env.layer.health()
        assert health["contract"]["expected_rpc"] == 17
        assert health["contract"]["expected_topics"] == 2
        assert health["started"] is False  # subscribe=False 时不算启动
    finally:
        await env.aclose()


async def test_start_subscribes_design_topics_and_close_unsubscribes(config):
    env = await social_env(config)
    try:
        await env.layer.start(bus=env.bus)
        topics = set(env.bus.known_topics())
        assert set(SOCIAL_TOPICS) <= topics
        assert env.layer.started is True
        own = {getattr(sub, "name", "") for topic in SOCIAL_TOPICS for sub in env.bus.subscribers(topic)}
        assert any(name.startswith("social:") for name in own)
        await env.layer.close()
        assert env.layer.started is False
        remaining = {getattr(sub, "name", "") for topic in SOCIAL_TOPICS for sub in env.bus.subscribers(topic)}
        assert not any(name.startswith("social:") for name in remaining)
    finally:
        await env.aclose()


async def test_publishing_design_topics_is_allowed_by_strict_bus(config):
    """严格主题总线下，本域两个 kafka: 主题必须能正常发布（不是自造名字）。"""

    env = await social_env(config)
    try:
        for topic in SOCIAL_TOPICS:
            await env.bus.publish(topic, {"probe": True})
        assert len(await env.events(SOCIAL_TOPICS[0])) == 1
        assert len(await env.events(SOCIAL_TOPICS[1])) == 1
    finally:
        await env.aclose()


@pytest.mark.parametrize("module_id", [m for m in EXPECTED_MODULES if m.count(".") >= 3])
def test_leaf_source_path_uses_underscores_for_dashes(module_id: str):
    """设计里的连字符叶子在 Python 侧必须用下划线文件名。"""

    path = contract.module_to_path(module_id)
    assert "-" not in path.name
    assert path.is_file()
