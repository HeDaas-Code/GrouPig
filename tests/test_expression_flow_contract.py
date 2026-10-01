"""expression 编排域（t9）契约测试：模块路径镜像、名字逐字对齐、设计依赖与装配纪律。

设计树（``normify-grouppig``）是唯一事实来源：任何名字 / 路径 / 依赖与设计不一致都应在这里红掉。

**范围纪律**：只断言 t9 的 12 个叶子、14 个 ``rpc:`` 名字与 1 个 ``kafka:`` 主题。
t8（persona + generator）的 6 个名字与 6 个叶子不在本文件断言范围内
（它们由 ``tests/test_expression_contract.py`` 守护），因此两边互不牵连。
"""

from __future__ import annotations

import importlib
import re
from pathlib import Path
from typing import Any

import pytest

from expression_flow_helpers import (
    EXPECTED_APIS,
    EXPECTED_DEPENDENCIES,
    FLOW_RPC,
    LEAF_MODULES,
    flow_contract_coverage,
    flow_env,
    message,
)
from grouppig.expression.runtime import (
    EXPRESSION_FLOW_RPC,
    EXPRESSION_FLOW_TOPICS,
    FLOW_SCOPES,
    IDENTITY_RPC,
    ORCHESTRATOR_RPC,
    SLANG_RPC,
    build_expression_flow,
)
from grouppig.infra.runtime import contract
from grouppig.infra.runtime.registry import Registry

REPO_ROOT = Path(__file__).resolve().parents[1]

#: 设计与源码的镜像关系（模块 id → 源码路径）。
EXPECTED_MODULES: dict[str, str] = {
    "grouppig.expression.orchestrator.flow.state": "src/grouppig/expression/orchestrator/flow/state.py",
    "grouppig.expression.orchestrator.flow.transition": "src/grouppig/expression/orchestrator/flow/transition.py",
    "grouppig.expression.orchestrator.flow.emitter": "src/grouppig/expression/orchestrator/flow/emitter.py",
    "grouppig.expression.orchestrator.planner.structure": "src/grouppig/expression/orchestrator/planner/structure.py",
    "grouppig.expression.orchestrator.planner.reviser": "src/grouppig/expression/orchestrator/planner/reviser.py",
    "grouppig.expression.orchestrator.selector.templates": "src/grouppig/expression/orchestrator/selector/templates.py",
    "grouppig.expression.orchestrator.selector.cost": "src/grouppig/expression/orchestrator/selector/cost.py",
    "grouppig.expression.identity.denial": "src/grouppig/expression/identity/denial.py",
    "grouppig.expression.identity.deflector": "src/grouppig/expression/identity/deflector.py",
    "grouppig.expression.slang.recognizer": "src/grouppig/expression/slang/recognizer.py",
    "grouppig.expression.slang.learner": "src/grouppig/expression/slang/learner.py",
    "grouppig.expression.slang.injector": "src/grouppig/expression/slang/injector.py",
}

# ---------------------------------------------------------------------------
# 设计对齐
# ---------------------------------------------------------------------------


def test_design_modules_exist_at_mirrored_paths():
    root = Path(contract.repo_root())
    for module_id, relative in EXPECTED_MODULES.items():
        assert (root / relative).is_file(), f"{module_id} 的源码不在 {relative}"
        assert any(module_id.startswith(scope) for scope in FLOW_SCOPES), module_id


def test_each_design_module_owns_its_declared_apis():
    index = contract.api_index()
    for module_id, names in EXPECTED_APIS.items():
        for name in names:
            assert index.get(name) == module_id, f"{name} 的 owner 不是 {module_id}"


def test_flow_rpc_names_are_exactly_the_design_set():
    index = contract.api_index()
    expected = {
        name
        for name, module in index.items()
        if name.startswith("rpc:") and any(module == scope or module.startswith(scope + ".") for scope in FLOW_SCOPES)
    }
    assert set(EXPRESSION_FLOW_RPC) == expected
    assert set(EXPRESSION_FLOW_RPC) == set(FLOW_RPC)
    assert len(EXPRESSION_FLOW_RPC) == 14


def test_flow_owns_exactly_one_topic():
    index = contract.api_index()
    expected = {
        name
        for name, module in index.items()
        if name.startswith("kafka:") and any(module == scope or module.startswith(scope + ".") for scope in FLOW_SCOPES)
    }
    assert expected == {"kafka:grouppig.reply.composed"}
    assert set(EXPRESSION_FLOW_TOPICS) == expected


def test_subdomain_tuples_cover_every_flow_rpc_name():
    grouped = [*ORCHESTRATOR_RPC, *IDENTITY_RPC, *SLANG_RPC]
    assert sorted(grouped) == sorted(EXPRESSION_FLOW_RPC)
    assert len(grouped) == len(set(grouped)), "子域之间不应有重复名字"


def test_leaf_modules_declare_their_names():
    for module_id, module_name in LEAF_MODULES:
        module = importlib.import_module(module_name)
        declared = set(getattr(module, "NAMES", ()))
        assert declared == set(EXPECTED_APIS[module_id]), f"{module_name} 的 NAMES 与设计不一致"


def test_leaf_modules_declare_their_module_id():
    """源码里的 ``MODULE_ID`` 必须逐字等于设计模块 id（连字符形式）。"""

    for module_id, module_name in LEAF_MODULES:
        module = importlib.import_module(module_name)
        assert module.MODULE_ID == module_id, module_name


def test_leaf_modules_declare_design_dependencies():
    """设计依赖必须在实现里有对应常量（防止实现偷偷改依赖）。"""

    for module_id, _from_api, to_api in EXPECTED_DEPENDENCIES:
        module_name = module_id.replace("prompt-builder", "prompt_builder")
        module = importlib.import_module(module_name)
        constants = {
            getattr(module, name)
            for name in dir(module)
            if name.startswith("DEP_") and isinstance(getattr(module, name), str)
        }
        assert to_api in constants, f"{module_name} 缺少指向 {to_api} 的依赖常量（设计里是必需的）"


def test_design_dependencies_match_normify_frontmatter():
    """直接解析设计 frontmatter，逐条核对 t9 三个子树的依赖对。"""

    found: set[tuple[str, str, str]] = set()
    base = Path(contract.repo_root()) / "normify-grouppig" / "modules" / "grouppig" / "expression"
    for subtree in ("orchestrator", "identity", "slang"):
        for path in sorted((base / subtree).rglob("*.md")):
            text = path.read_text(encoding="utf-8")
            match = re.match(r"\A---\n(.*?)\n---\n", text, re.S)
            if not match:
                continue
            front = match.group(1)
            module_id = re.search(r"^id:\s*(\S+)$", front, re.M)
            if not module_id:
                continue
            pattern = (
                r"- kind:\s*(?:call|event)\n(?:\s+.*\n)*?\s+to:\s*(\S+)\n"
                r"(?:\s+.*\n)*?\s+from_api:\s*\"?([^\"\n]+)\"?\n"
                r"(?:\s+.*\n)*?\s+to_api:\s*\"?([^\"\n]+)\"?"
            )
            for block in re.findall(pattern, front):
                found.add((module_id.group(1), block[1].strip(), block[2].strip()))
    missing = sorted(set(EXPECTED_DEPENDENCIES) - found)
    assert not missing, f"测试里登记的依赖在设计文档里找不到：{missing}"
    assert len(found) >= len(EXPECTED_DEPENDENCIES)


def test_emitter_publishes_reply_composed_topic():
    """emitter 是发布方：它只有主题常量，不占用任何 ``rpc:`` 名字。"""

    from grouppig.expression.orchestrator.flow import emitter as emitter_module

    assert emitter_module.NAMES == ("kafka:grouppig.reply.composed",)
    assert emitter_module.make_handlers(None) == {}


# ---------------------------------------------------------------------------
# 装配纪律
# ---------------------------------------------------------------------------


def test_build_expression_flow_registers_every_flow_name():
    registry = Registry()
    layer = build_expression_flow(registry=registry)
    layer.register()
    assert set(EXPRESSION_FLOW_RPC) <= set(registry.names())
    assert layer.contract_check() == {"missing": [], "unknown": []}


def test_register_is_idempotent():
    registry = Registry()
    layer = build_expression_flow(registry=registry)
    layer.register()
    before = len(registry.names())
    layer.register()
    assert len(registry.names()) == before


def test_contract_coverage_matches_design():
    registry = Registry()
    layer = build_expression_flow(registry=registry)
    layer.register()
    coverage = flow_contract_coverage(registry)
    assert coverage["missing"] == []
    assert coverage["unknown"] == []
    assert coverage["expected"] == sorted(EXPRESSION_FLOW_RPC)


def test_flow_registration_does_not_touch_core_names():
    """t9 的装配**不注册** t8 的 6 个名字（两个入口各自负责，互不越界）。"""

    from grouppig.expression import EXPRESSION_CORE_RPC

    registry = Registry()
    layer = build_expression_flow(registry=registry)
    layer.register()
    assert not (set(EXPRESSION_CORE_RPC) & set(registry.names()))


def test_build_expression_flow_uses_default_registry_when_none_given():
    layer = build_expression_flow()
    assert layer.registry is not None
    assert layer.store is not None and layer.injector is not None and layer.deflector is not None


def test_every_leaf_module_exposes_register_and_make_handlers():
    for _module_id, module_name in LEAF_MODULES:
        module = importlib.import_module(module_name)
        assert callable(module.register), f"{module_name} 缺少 register"
        assert callable(module.make_handlers), f"{module_name} 缺少 make_handlers"


def test_flow_layer_is_a_separate_entry_from_core_layer():
    """t8 的 ``install_expression`` 仍然只装 6 个名字；t9 走自己的入口。"""

    from grouppig.expression import install_expression

    registry = Registry()
    holder = type("H", (), {"registry": registry, "bus": None, "logger": None, "config": None})()
    import asyncio

    asyncio.run(install_expression(holder))
    assert "rpc:generator.compose" in registry.names()
    assert "rpc:flow.start" not in registry.names()


async def test_health_reports_contract_and_leaf_sections(config):
    env = await flow_env(config)
    try:
        health = await env.flow.health()
        assert health["scopes"] == list(FLOW_SCOPES)
        assert health["contract"]["expected_rpc"] == 14
        assert health["contract"]["topics"] == ["kafka:grouppig.reply.composed"]
        assert health["contract"]["missing"] == []
        assert health["contract"]["unknown"] == []
        for section in ("orchestrator", "identity", "slang"):
            assert section in health
    finally:
        await env.aclose()


def test_design_declares_runtime_as_expression_assembly_entry():
    """设计树把 ``grouppig.expression.runtime`` 定为整域装配入口，源码路径必须镜像它。"""

    index = contract.api_index()
    assert index.get("rpc:flow.start") == "grouppig.expression.orchestrator.flow.state"
    runtime_md = Path(contract.repo_root()) / "normify-grouppig" / "modules" / "grouppig" / "expression" / "runtime.md"
    assert runtime_md.is_file()
    assert "grouppig.expression.runtime" in runtime_md.read_text(encoding="utf-8")
    assert (Path(contract.repo_root()) / "src" / "grouppig" / "expression" / "runtime.py").is_file()


async def test_install_expression_domain_wires_both_layers(config):
    """一行装整域：t8 的 6 个 + t9 的 14 个名字全部注册，整域自检为绿。"""

    from grouppig.expression.runtime import install_expression_domain
    from grouppig.infra.runtime.registry import Registry

    registry = Registry()
    holder = type("H", (), {"registry": registry, "bus": None, "logger": None, "config": None})()
    domain = await install_expression_domain(holder)
    try:
        assert domain.contract_check() == {"missing": [], "unknown": []}
        assert set(EXPRESSION_FLOW_RPC) <= set(registry.names())
        assert "rpc:generator.compose" in registry.names()
        health = await domain.health()
        assert health["flow"]["contract"]["missing"] == []
        assert health["core"]["contract"]["missing"] == []
    finally:
        await domain.aclose()


async def test_flow_installs_without_memory_or_core(config):
    """t9 不强依赖 memory / t8：只注册自己能跑起来（编排会记降级但不抛）。"""

    env = await flow_env(config, memory=False, core=False)
    try:
        assert env.flow.contract_check() == {"missing": [], "unknown": []}
        result = await env.call("rpc:flow.start", 100200300, messages=[{"sender_id": 1002, "content": "打本吗"}])
        assert result["state"] == "acknowledge"
        ended = await env.call("rpc:flow.end", result["flow_id"])
        assert ended["found"] is True
        assert ended["text"] == ""
        assert "compose_unavailable" in ended["degraded_paths"]
    finally:
        await env.aclose()


async def test_every_design_dependency_is_exercised_at_runtime(config):
    """设计依赖不是「有常量」就算数：跑一轮真实链路，逐条验证它**真的被调用**。

    设计 frontmatter 给 t9 的 14 条依赖全部落在这里；``kafka:grouppig.reply.composed``
    由 emitter 发布（不经过注册表），单独用事件总线核对。
    """

    from expression_flow_helpers import seed_slang

    env = await flow_env(config)
    calls: list[str] = []
    # 叶子的跨域调用都走各自 ctx.call（装配时绑定的），因此要在 ctx 上装探针
    layers = [layer for layer in (env.flow, env.core) if layer is not None]
    originals = [(layer, layer.ctx.call) for layer in layers]

    def probe(original: Any) -> Any:
        async def recording(name: str, *args: Any, **kwargs: Any) -> Any:
            calls.append(name)
            return await original(name, *args, **kwargs)

        return recording

    for layer, original in originals:
        layer.ctx.call = probe(original)
    try:
        await seed_slang(env)  # 让 slang.lookup 有东西可查
        picked = await env.call("rpc:selector.pick-preset", {"heat": 0.7}, scenario="chat")
        start = await env.call("rpc:flow.start", 100200300, messages=[message(0)], keyword="打本", preset=picked)
        await env.call("rpc:planner.revise", start["plan"], new_messages=[{"content": "等下"}])
        for _ in range(start["step_count"]):
            await env.call("rpc:flow.next", start["flow_id"])
        await env.call("rpc:flow.end", start["flow_id"])
        await env.call("rpc:slang.learn", [{"term": "开荒", "context": "开荒就是第一次打"}], group_id=100200300)

        required = {to_api for _module, _from_api, to_api in EXPECTED_DEPENDENCIES if to_api.startswith("rpc:")}
        missing = sorted(required - set(calls))
        assert not missing, f"这些设计依赖没有被真正调用：{missing}"
        assert env.events(), "kafka:grouppig.reply.composed 必须真的发布"
    finally:
        for layer, original in originals:
            layer.ctx.call = original
        await env.aclose()


async def test_domain_contract_check_covers_twenty_rpc_names(config):
    """整域自检覆盖 t8 的 6 个 + t9 的 14 个 = 20 个 rpc 名字。"""

    from grouppig.expression import EXPRESSION_CORE_RPC
    from grouppig.expression.runtime import ExpressionDomain, install_expression_domain
    from grouppig.infra.runtime.registry import Registry

    assert len(EXPRESSION_CORE_RPC) + len(EXPRESSION_FLOW_RPC) == 20
    registry = Registry()
    holder = type("H", (), {"registry": registry, "bus": None, "logger": None, "config": None})()
    domain = await install_expression_domain(holder)
    try:
        assert isinstance(domain, ExpressionDomain)
        assert domain.contract_check() == {"missing": [], "unknown": []}
        assert len(registry.names()) == 20
    finally:
        await domain.aclose()


@pytest.mark.parametrize("scope", FLOW_SCOPES)
def test_scope_prefixes_are_design_subtrees(scope: str):
    index = contract.api_index()
    assert any(module.startswith(scope) for module in index.values()), scope
