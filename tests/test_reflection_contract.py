"""reflection 域契约测试：模块路径镜像、名字逐字对齐、依赖边与装配纪律。

这些测试是反思层的「防漂移护栏」：设计树（`normify-grouppig`）是唯一事实来源，
任何名字/路径/依赖与设计不一致都应当在这里红掉。
"""

from __future__ import annotations

import importlib
import re
from pathlib import Path

import pytest

from grouppig.infra.runtime import contract
from grouppig.infra.runtime.errors import HandlerNotRegistered
from grouppig.infra.runtime.registry import Registry
from grouppig.reflection import (
    EVALUATOR_RPC,
    PRESETS_RPC,
    REFLECTION_SCOPE,
    SESSION_REVIEW_RPC,
    STRATEGY_RPC,
    build_reflection,
)
from reflection_helpers import (
    REFLECTION_RPC,
    TOPIC_SESSION_COMPLETED,
    expected_reflection_names,
    reflection_contract_coverage,
    reflection_env,
)

#: 设计与源码的镜像关系（模块 id → 源码路径）。设计里的连字符在 Python 侧是下划线。
EXPECTED_MODULES: dict[str, str] = {
    "grouppig.reflection.presets.registry": "src/grouppig/reflection/presets/registry.py",
    "grouppig.reflection.presets.matcher": "src/grouppig/reflection/presets/matcher.py",
    "grouppig.reflection.strategy.synthesizer": "src/grouppig/reflection/strategy/synthesizer.py",
    "grouppig.reflection.strategy.validator": "src/grouppig/reflection/strategy/validator.py",
    "grouppig.reflection.strategy.rollback": "src/grouppig/reflection/strategy/rollback.py",
    "grouppig.reflection.evaluator.scoring": "src/grouppig/reflection/evaluator/scoring.py",
    "grouppig.reflection.evaluator.ab-test": "src/grouppig/reflection/evaluator/ab_test.py",
    "grouppig.reflection.session-review.timeline": "src/grouppig/reflection/session_review/timeline.py",
    "grouppig.reflection.session-review.metrics": "src/grouppig/reflection/session_review/metrics.py",
    "grouppig.reflection.session-review.insights": "src/grouppig/reflection/session_review/insights.py",
}

#: 设计的叶子 → 它负责的契约名字（逐字取自 api-index.json）。
EXPECTED_APIS: dict[str, tuple[str, ...]] = {
    "grouppig.reflection.presets.registry": ("rpc:presets.load", "rpc:presets.register"),
    "grouppig.reflection.presets.matcher": ("rpc:presets.match",),
    "grouppig.reflection.strategy.synthesizer": ("rpc:strategy.generate",),
    "grouppig.reflection.strategy.validator": ("rpc:strategy.validate",),
    "grouppig.reflection.strategy.rollback": ("rpc:strategy.rollback",),
    "grouppig.reflection.evaluator.scoring": ("rpc:strategy.score",),
    "grouppig.reflection.evaluator.ab-test": ("rpc:strategy.evaluate",),
    "grouppig.reflection.session-review.timeline": (
        "rpc:review.on-session-completed",
        "rpc:review.timeline",
    ),
    "grouppig.reflection.session-review.metrics": ("rpc:review.metrics",),
    "grouppig.reflection.session-review.insights": ("rpc:review.analyze",),
}

#: 设计 frontmatter 里的依赖对（`from_api` → `to_api`），实现里必须有对应常量。
EXPECTED_DEPENDENCIES: tuple[tuple[str, str, str], ...] = (
    ("grouppig.reflection.presets.matcher", "rpc:presets.match", "rpc:presets.load"),
    ("grouppig.reflection.strategy.synthesizer", "rpc:strategy.generate", "rpc:strategy.validate"),
    ("grouppig.reflection.strategy.synthesizer", "rpc:strategy.generate", "rpc:strategy.evaluate"),
    ("grouppig.reflection.strategy.synthesizer", "rpc:strategy.generate", "rpc:presets.register"),
    ("grouppig.reflection.strategy.validator", "rpc:strategy.validate", "rpc:presets.load"),
    ("grouppig.reflection.strategy.rollback", "rpc:strategy.rollback", "rpc:presets.register"),
    ("grouppig.reflection.evaluator.scoring", "rpc:strategy.score", "rpc:chat.query"),
    ("grouppig.reflection.evaluator.scoring", "rpc:strategy.score", "rpc:presets.match"),
    ("grouppig.reflection.evaluator.ab-test", "rpc:strategy.evaluate", "rpc:strategy.score"),
    ("grouppig.reflection.session-review.timeline", "rpc:review.timeline", "rpc:archive.load"),
    ("grouppig.reflection.session-review.timeline", "rpc:review.timeline", "rpc:chat.query"),
    ("grouppig.reflection.session-review.metrics", "rpc:review.metrics", "rpc:review.timeline"),
    ("grouppig.reflection.session-review.insights", "rpc:review.analyze", "rpc:review.metrics"),
    ("grouppig.reflection.session-review.insights", "rpc:review.analyze", "rpc:strategy.generate"),
)

#: 设计叶子模块 id → 源码模块名（importlib 用）。
LEAF_MODULES: tuple[tuple[str, str], ...] = (
    ("grouppig.reflection.presets.registry", "grouppig.reflection.presets.registry"),
    ("grouppig.reflection.presets.matcher", "grouppig.reflection.presets.matcher"),
    ("grouppig.reflection.strategy.synthesizer", "grouppig.reflection.strategy.synthesizer"),
    ("grouppig.reflection.strategy.validator", "grouppig.reflection.strategy.validator"),
    ("grouppig.reflection.strategy.rollback", "grouppig.reflection.strategy.rollback"),
    ("grouppig.reflection.evaluator.scoring", "grouppig.reflection.evaluator.scoring"),
    ("grouppig.reflection.evaluator.ab-test", "grouppig.reflection.evaluator.ab_test"),
    ("grouppig.reflection.session-review.timeline", "grouppig.reflection.session_review.timeline"),
    ("grouppig.reflection.session-review.metrics", "grouppig.reflection.session_review.metrics"),
    ("grouppig.reflection.session-review.insights", "grouppig.reflection.session_review.insights"),
)


# ---------------------------------------------------------------------------
# 设计对齐
# ---------------------------------------------------------------------------


def test_design_modules_exist_at_mirrored_paths():
    root = Path(contract.repo_root())
    for module_id, relative in EXPECTED_MODULES.items():
        assert (root / relative).is_file(), f"{module_id} 的源码不在 {relative}"
        assert module_id.startswith(REFLECTION_SCOPE)


def test_each_design_module_owns_its_declared_apis():
    index = contract.api_index()
    for module_id, names in EXPECTED_APIS.items():
        for name in names:
            assert index.get(name) == module_id, f"{name} 的 owner 不是 {module_id}"


def test_reflection_rpc_names_are_exactly_the_design_set():
    assert set(REFLECTION_RPC) == expected_reflection_names()
    assert len(REFLECTION_RPC) == 12


def test_subdomain_tuples_cover_every_rpc_name():
    grouped = [*PRESETS_RPC, *STRATEGY_RPC, *EVALUATOR_RPC, *SESSION_REVIEW_RPC]
    assert sorted(grouped) == sorted(REFLECTION_RPC)
    assert len(grouped) == len(set(grouped)), "子域之间不应有重复名字"


def test_reflection_owns_no_kafka_topics_but_subscribes_session_completed():
    index = contract.api_index()
    owned = {
        name for name, module in index.items() if name.startswith("kafka:") and module.startswith(REFLECTION_SCOPE)
    }
    assert owned == set(), f"设计里反思域不拥有主题，却出现了 {sorted(owned)}"
    assert TOPIC_SESSION_COMPLETED in index, "会话完成事件必须存在于契约里"


def test_leaf_modules_declare_their_names():
    for module_id, module_name in LEAF_MODULES:
        module = importlib.import_module(module_name)
        declared = set(getattr(module, "NAMES", ()))
        assert declared == set(EXPECTED_APIS[module_id]), f"{module_name} 的 NAMES 与设计不一致"


def test_leaf_modules_declare_design_dependencies():
    """设计依赖必须在实现里有对应常量（防止实现偷偷改依赖）。"""

    for module_id, _from_api, to_api in EXPECTED_DEPENDENCIES:
        module_name = module_id.replace("ab-test", "ab_test").replace("session-review", "session_review")
        module = importlib.import_module(module_name)
        constants = {
            getattr(module, name)
            for name in dir(module)
            if name.startswith("DEP_") and isinstance(getattr(module, name), str)
        }
        assert to_api in constants, f"{module_name} 缺少指向 {to_api} 的依赖常量（设计里是必需的）"


def test_design_dependencies_match_normify_frontmatter():
    """直接解析设计 frontmatter，逐条核对依赖对。"""

    root = Path(contract.repo_root()) / "normify-grouppig" / "modules" / "grouppig" / "reflection"
    found: set[tuple[str, str, str]] = set()
    for path in sorted(root.rglob("*.md")):
        text = path.read_text(encoding="utf-8")
        match = re.match(r"\A---\n(.*?)\n---\n", text, re.S)
        if not match:
            continue
        front = match.group(1)
        module_id = re.search(r"^id:\s*(\S+)$", front, re.M)
        if not module_id:
            continue
        for block in re.findall(
            r"- kind:\s*call\n(?:\s+.*\n)*?\s+to:\s*(\S+)\n(?:\s+.*\n)*?\s+from_api:\s*\"?([^\"\n]+)\"?\n(?:\s+.*\n)*?\s+to_api:\s*\"?([^\"\n]+)\"?",
            front,
        ):
            if block[1].startswith("rpc:"):
                found.add((module_id.group(1), block[1].strip(), block[2].strip()))
    assert len(found) >= len(EXPECTED_DEPENDENCIES), (
        f"设计文档里只解析出 {len(found)} 条依赖，少于期望的 {len(EXPECTED_DEPENDENCIES)} 条：{sorted(found)}"
    )
    missing = sorted(set(EXPECTED_DEPENDENCIES) - found)
    assert not missing, f"测试里登记的依赖在设计文档里找不到：{missing}"


def test_dash_to_underscore_path_mapping():
    """设计用连字符，Python 用下划线；两者必须指向同一个模块。"""

    module = importlib.import_module("grouppig.reflection.session_review.timeline")
    assert module.MODULE == "grouppig.reflection.session-review.timeline"
    module = importlib.import_module("grouppig.reflection.evaluator.ab_test")
    assert module.MODULE == "grouppig.reflection.evaluator.ab-test"


# ---------------------------------------------------------------------------
# 装配纪律
# ---------------------------------------------------------------------------


def test_build_reflection_registers_every_design_name():
    registry = Registry()
    layer = build_reflection(registry=registry)
    layer.register()
    assert set(REFLECTION_RPC) <= set(registry.names())
    assert layer.contract_check() == {"missing": [], "unknown": []}


def test_register_is_idempotent():
    registry = Registry()
    layer = build_reflection(registry=registry)
    layer.register()
    before = len(registry.names())
    layer.register()
    assert len(registry.names()) == before


def test_contract_coverage_matches_design():
    registry = Registry()
    layer = build_reflection(registry=registry)
    layer.register()
    coverage = reflection_contract_coverage(registry)
    assert coverage["missing"] == []
    assert coverage["unknown"] == []
    assert coverage["owned_topics"] == [], "反思域不拥有任何 kafka: 主题"
    assert coverage["subscribed_topics"] == [TOPIC_SESSION_COMPLETED]


def test_build_reflection_uses_default_registry_when_none_given():
    layer = build_reflection()
    assert layer.registry is not None
    assert layer.presets is not None and layer.matcher is not None and layer.insights is not None


def test_every_leaf_module_exposes_register_and_make_handlers():
    for _module_id, module_name in LEAF_MODULES:
        module = importlib.import_module(module_name)
        assert callable(module.register), f"{module_name} 缺少 register"
        assert callable(module.make_handlers), f"{module_name} 缺少 make_handlers"


async def test_install_reflection_subscribes_session_completed(config):
    env = await reflection_env(config, listen=False)
    try:
        holder = env.layer
        subs = holder.subscribe(env.bus)
        assert len(subs) == 1
        names = {getattr(sub, "name", "") for sub in env.bus.subscribers(TOPIC_SESSION_COMPLETED)}
        assert any("reflection" in name for name in names)
        await holder.close()
    finally:
        await env.aclose()


async def test_close_unsubscribes_reflection_topics(config):
    env = await reflection_env(config, listen=False)
    try:
        env.layer.subscribe(env.bus)
        await env.layer.close()
        names = {getattr(sub, "name", "") for sub in env.bus.subscribers(TOPIC_SESSION_COMPLETED)}
        assert not any("reflection" in name for name in names)
    finally:
        await env.aclose()


async def test_health_reports_contract_and_leaf_counts(config):
    env = await reflection_env(config)
    try:
        health = await env.layer.health()
        assert health["scope"] == REFLECTION_SCOPE
        assert health["contract"]["expected_rpc"] == 12
        assert health["contract"]["missing"] == []
        assert health["contract"]["unknown"] == []
        for section in ("presets", "strategy", "evaluator", "session_review"):
            assert section in health
    finally:
        await env.aclose()


def test_missing_downstream_is_tolerated_not_fatal():
    """反思域对下游缺失是容错的（复盘不该因为某个下游没挂而整体失败）。"""

    registry = Registry()
    layer = build_reflection(registry=registry)
    layer.register()
    assert layer.timeline is not None
    # 直接调用叶子的 _call：下游没注册时应返回 None 而不是抛错
    import asyncio

    assert asyncio.run(layer.timeline._call("rpc:chat.query", {})) is None


async def test_registry_still_raises_for_unregistered_names():
    """容错发生在叶子内部，注册表本身仍然严格（契约纪律不放松）。"""

    registry = Registry()
    layer = build_reflection(registry=registry)
    layer.register()
    with pytest.raises(HandlerNotRegistered):
        await registry.acall("rpc:chat.query", {})
