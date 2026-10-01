"""契约对齐测试：infra 的 API 名字与源码路径必须逐字对齐 normify 设计。

计数与名字集合全部从 normify-grouppig/tree.json 动态推导，
设计树随各域落地增长时这些断言不会失效（不写死任何数字）。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from grouppig.infra.runtime import contract
from grouppig.infra.runtime.di import build_container
from grouppig.infra.runtime.errors import UnknownNameError
from grouppig.infra.runtime.registry import Registry
from helpers import design_tree_facts

REPO_ROOT = Path(__file__).resolve().parents[1]
INFRA = "grouppig.infra"
COUNT_KEYS = ("names", "rpc", "kafka", "mysql", "modules", "containers", "leaves")


def infra_contract_names() -> set[str]:
    """设计树里归属 grouppig.infra 子树的名字（动态推导）。"""

    return design_tree_facts().names_of(INFRA)


def test_contract_view_matches_design_tree():
    facts = design_tree_facts()
    summary = contract.summary()
    assert summary["api_index"] == str(contract.api_index_path())
    for key in COUNT_KEYS:
        assert summary[key] == facts.counts[key], key
    # api-index.json + modules/*.md 与 tree.json 两个视图必须完全一致
    assert dict(contract.api_index()) == facts.api_index
    assert set(contract.module_ids()) == set(facts.module_ids)
    assert set(contract.container_ids()) == set(facts.container_ids)
    assert set(contract.leaf_ids()) == set(facts.leaf_ids)
    assert facts.counts["containers"] + facts.counts["leaves"] == facts.counts["modules"]
    assert facts.counts["rpc"] + facts.counts["kafka"] + facts.counts["mysql"] == facts.counts["names"]


def test_infra_names_in_api_index_are_owned_by_infra_modules():
    facts = design_tree_facts()
    expected = infra_contract_names()
    assert expected, "设计树里应至少有一个 infra 名字"
    for name in sorted(expected):
        assert contract.owner(name) == facts.owner_of(name), name
        assert name in contract.names_for_module(facts.owner_of(name)), name


def test_registered_infra_handlers_exactly_cover_contract(config):
    container = build_container(config=config)
    registered = set(container.registry.names())
    check = contract.check_registry(registered, scope=INFRA)
    assert check["missing"] == [], "契约里属于 infra 但没注册：" + str(check["missing"])
    assert check["unknown"] == [], "注册了契约外的名字：" + str(check["unknown"])
    expected = infra_contract_names()
    infra_registered = {n for n in registered if (contract.owner(n) or "").startswith(INFRA)}
    assert infra_registered == expected
    for name in sorted(expected):
        assert container.registry.get(name).module == contract.owner(name), name


def test_topic_names_match_design_tree():
    facts = design_tree_facts()
    topics = set(contract.topic_names())
    assert topics == facts.topics
    assert topics, "设计树里应有 kafka 主题"
    for topic in sorted(topics):
        assert topic.startswith("kafka:grouppig."), topic
        owner = contract.owner(topic)
        assert owner is not None and owner in facts.module_ids, topic


def test_table_names_match_design_tree():
    facts = design_tree_facts()
    assert set(contract.table_names()) == facts.tables


def test_registry_rejects_names_outside_contract():
    registry = Registry()
    with pytest.raises(UnknownNameError):
        registry.register("rpc:not.in.contract", lambda: None)


def test_module_path_mapping_mirrors_normify():
    modules = contract.modules()
    for module_id in contract.module_ids():
        py_id = contract.module_to_py(module_id)
        assert py_id == module_id.replace("-", "_"), module_id
        path = contract.module_to_path(module_id, root=REPO_ROOT)
        assert path.is_relative_to(REPO_ROOT / "src"), module_id
        segment = contract.python_segment(module_id.split(".")[-1])
        if modules[module_id].is_container:
            assert path.name == "__init__.py", module_id
            assert path.parent.name == segment, module_id
        else:
            assert path.name == segment + ".py", module_id
    hyphenated = sorted(mid for mid in contract.module_ids() if "-" in mid)
    if hyphenated:
        assert "_" in contract.module_to_path(hyphenated[0], root=REPO_ROOT).as_posix()


def test_every_infra_module_has_source_path():
    for module_id in contract.module_ids():
        if not module_id.startswith(INFRA):
            continue
        path = contract.module_to_path(module_id, root=REPO_ROOT)
        assert path.is_file(), module_id + " 缺少源码文件：" + str(path.relative_to(REPO_ROOT))


def test_all_container_packages_exist():
    for module_id in contract.container_ids():
        path = contract.module_to_path(module_id, root=REPO_ROOT)
        assert path.is_file(), "容器包缺失：" + str(path.relative_to(REPO_ROOT))


def test_api_index_owners_all_exist_as_modules():
    modules = contract.modules()
    for name, module_id in contract.api_index().items():
        assert module_id in modules, name + " 的归属模块 " + module_id + " 不在设计树中"


def test_no_leaf_module_id_ends_with_index():
    assert not [mid for mid in contract.leaf_ids() if mid.endswith(".index")]


def test_check_registry_reports_missing_for_empty_registry():
    expected = infra_contract_names()
    check = contract.check_registry(set(), scope=INFRA)
    assert sorted(check["missing"]) == sorted(expected)
    assert check["unknown"] == []


def test_contract_summary_is_json_friendly():
    facts = design_tree_facts()
    summary = json.loads(json.dumps(contract.summary()))
    assert summary["names"] == facts.counts["names"]
    assert summary["modules"] == facts.counts["modules"]
