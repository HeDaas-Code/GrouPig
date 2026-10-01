"""expression 域契约测试：模块路径镜像、名字逐字对齐、设计依赖与装配纪律。

这些测试是表达核心（t8：persona + generator）的「防漂移护栏」：
设计树（``normify-grouppig``）是唯一事实来源，任何名字/路径/依赖与设计不一致都应当在这里红掉。

**范围纪律**：只断言 t8 的 6 个名字与 6 个叶子。orchestrator / identity / slang 属 t9，
本文件的用例不得因为 t9 未落地而变红（也因此不断言整个 expression 域的完整覆盖）。
"""

from __future__ import annotations

import importlib
import re
from pathlib import Path

from expression_helpers import (
    CORE_RPC,
    EXPECTED_APIS,
    EXPECTED_DEPENDENCIES,
    LEAF_MODULES,
    FakeModel,
    expected_core_names,
    expression_contract_coverage,
    expression_env,
)
from grouppig.expression import (
    EXPRESSION_CORE_RPC,
    EXPRESSION_SCOPE,
    GENERATOR_RPC,
    PERSONA_RPC,
    build_expression,
)
from grouppig.infra.runtime import contract
from grouppig.infra.runtime.registry import Registry

REPO_ROOT = Path(__file__).resolve().parents[1]

#: 设计与源码的镜像关系（模块 id → 源码路径）。
EXPECTED_MODULES: dict[str, str] = {
    "grouppig.expression.persona.profile": "src/grouppig/expression/persona/profile.py",
    "grouppig.expression.persona.prompt-builder": "src/grouppig/expression/persona/prompt_builder.py",
    "grouppig.expression.generator.context": "src/grouppig/expression/generator/context.py",
    "grouppig.expression.generator.compressor": "src/grouppig/expression/generator/compressor.py",
    "grouppig.expression.generator.writer": "src/grouppig/expression/generator/writer.py",
    "grouppig.expression.generator.polisher": "src/grouppig/expression/generator/polisher.py",
}


# ---------------------------------------------------------------------------
# 设计对齐
# ---------------------------------------------------------------------------


def test_design_modules_exist_at_mirrored_paths():
    root = Path(contract.repo_root())
    for module_id, relative in EXPECTED_MODULES.items():
        assert (root / relative).is_file(), f"{module_id} 的源码不在 {relative}"
        assert module_id.startswith(EXPRESSION_SCOPE)


def test_each_design_module_owns_its_declared_apis():
    index = contract.api_index()
    for module_id, names in EXPECTED_APIS.items():
        for name in names:
            assert index.get(name) == module_id, f"{name} 的 owner 不是 {module_id}"


def test_core_rpc_names_are_exactly_the_design_set():
    assert set(EXPRESSION_CORE_RPC) == expected_core_names()
    assert len(EXPRESSION_CORE_RPC) == 6
    assert set(EXPRESSION_CORE_RPC) == set(CORE_RPC)


def test_subdomain_tuples_cover_every_core_rpc_name():
    grouped = [*PERSONA_RPC, *GENERATOR_RPC]
    assert sorted(grouped) == sorted(EXPRESSION_CORE_RPC)
    assert len(grouped) == len(set(grouped)), "子域之间不应有重复名字"


def test_expression_core_owns_no_kafka_topics():
    """``kafka:grouppig.reply.composed`` 归 t9 的 flow.emitter，t8 不拥有任何主题。"""

    index = contract.api_index()
    owned = {
        name
        for name, module in index.items()
        if name.startswith("kafka:")
        and (module.startswith("grouppig.expression.persona") or module.startswith("grouppig.expression.generator"))
    }
    assert owned == set(), f"t8 的叶子不该拥有主题，却出现了 {sorted(owned)}"


def test_leaf_modules_declare_their_names():
    for module_id, module_name in LEAF_MODULES:
        module = importlib.import_module(module_name)
        declared = set(getattr(module, "NAMES", ()))
        assert declared == set(EXPECTED_APIS[module_id]), f"{module_name} 的 NAMES 与设计不一致"


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
    """直接解析设计 frontmatter，逐条核对依赖对（t8 两个子树）。"""

    found: set[tuple[str, str, str]] = set()
    for subtree in ("persona", "generator"):
        root = Path(contract.repo_root()) / "normify-grouppig" / "modules" / "grouppig" / "expression" / subtree
        for path in sorted(root.rglob("*.md")):
            text = path.read_text(encoding="utf-8")
            match = re.match(r"\A---\n(.*?)\n---\n", text, re.S)
            if not match:
                continue
            front = match.group(1)
            module_id = re.search(r"^id:\s*(\S+)$", front, re.M)
            if not module_id:
                continue
            pattern = (
                r"- kind:\s*call\n(?:\s+.*\n)*?\s+to:\s*(\S+)\n"
                r"(?:\s+.*\n)*?\s+from_api:\s*\"?([^\"\n]+)\"?\n"
                r"(?:\s+.*\n)*?\s+to_api:\s*\"?([^\"\n]+)\"?"
            )
            for block in re.findall(pattern, front):
                if block[1].startswith("rpc:"):
                    found.add((module_id.group(1), block[1].strip(), block[2].strip()))
    assert len(found) >= len(EXPECTED_DEPENDENCIES), (
        f"设计文档里只解析出 {len(found)} 条依赖，少于期望的 {len(EXPECTED_DEPENDENCIES)} 条：{sorted(found)}"
    )
    missing = sorted(set(EXPECTED_DEPENDENCIES) - found)
    assert not missing, f"测试里登记的依赖在设计文档里找不到：{missing}"


def test_dash_to_underscore_path_mapping():
    """设计用连字符，Python 用下划线；两者必须指向同一个模块。"""

    module = importlib.import_module("grouppig.expression.persona.prompt_builder")
    assert module.MODULE_ID == "grouppig.expression.persona.prompt-builder"


# ---------------------------------------------------------------------------
# 装配纪律
# ---------------------------------------------------------------------------


def test_build_expression_registers_every_core_name():
    registry = Registry()
    layer = build_expression(registry=registry)
    layer.register()
    assert set(EXPRESSION_CORE_RPC) <= set(registry.names())
    assert layer.contract_check() == {"missing": [], "unknown": []}


def test_register_is_idempotent():
    registry = Registry()
    layer = build_expression(registry=registry)
    layer.register()
    before = len(registry.names())
    layer.register()
    assert len(registry.names()) == before


def test_contract_coverage_matches_design():
    registry = Registry()
    layer = build_expression(registry=registry)
    layer.register()
    coverage = expression_contract_coverage(registry)
    assert coverage["missing"] == []
    assert coverage["unknown"] == []


def test_build_expression_uses_default_registry_when_none_given():
    layer = build_expression()
    assert layer.registry is not None
    assert layer.persona is not None and layer.packer is not None and layer.writer is not None


def test_every_leaf_module_exposes_register_and_make_handlers():
    for _module_id, module_name in LEAF_MODULES:
        module = importlib.import_module(module_name)
        assert callable(module.register), f"{module_name} 缺少 register"
        assert callable(module.make_handlers), f"{module_name} 缺少 make_handlers"


async def test_health_reports_contract_and_leaf_counts(config):
    env = await expression_env(config)
    try:
        health = await env.layer.health()
        assert health["scope"] == EXPRESSION_SCOPE
        assert health["contract"]["expected_rpc"] == 6
        assert health["contract"]["missing"] == []
        assert health["contract"]["unknown"] == []
        for section in ("persona", "generator"):
            assert section in health
    finally:
        await env.aclose()


async def test_install_expression_accepts_missing_downstream(config):
    """表达层不强依赖 memory / social：只注册自己能跑起来。"""

    env = await expression_env(config, memory=False, model=FakeModel(fail=True))
    try:
        assert env.layer.contract_check() == {"missing": [], "unknown": []}
        result = await env.compose(messages=[{"sender_id": 1002, "content": "打本吗"}])
        assert result["degraded"] is True
    finally:
        await env.aclose()
