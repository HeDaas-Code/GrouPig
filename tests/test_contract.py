"""契约模块（``grouppig.infra.runtime.contract``）的语义测试。

锁死 t35 收敛后的**有效状态**语义：normify 的 ``state`` 是**可选**字段，未声明即 active；
``contract.ModuleSpec.state`` 是这条规则的**唯一**翻译点，消费方直接读它，不再各自兜底。
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "tools"))

import gen_skeleton  # noqa: E402

from grouppig.infra.runtime import contract  # noqa: E402
from helpers import design_tree_facts  # noqa: E402

_STATE_LINE = re.compile("^state:(.*)$", re.M)
#: 「未声明 ⇒ active」的翻译若被写进代码，形态通常是 state 与字面 unknown 出现在同一行。
_QUOTES = chr(34) + chr(39)
_UNKNOWN_PAIR = re.compile(
    "state.{0,60}[" + _QUOTES + "]unknown[" + _QUOTES + "]|[" + _QUOTES + "]unknown[" + _QUOTES + "].{0,60}state",
    re.I,
)


def declared_state(module_id: str) -> str | None:
    """frontmatter 里**显式**声明的 state（未声明返回 None）。"""

    path = contract.modules()[module_id].source_path
    if path is None:
        return None
    found = _STATE_LINE.search(path.read_text(encoding="utf-8"))
    return found.group(1).strip().strip(chr(34)).strip(chr(39)) if found else None


def test_default_state_constant_matches_normify_semantics():
    """normify 的 state 是可选字段、未声明即 active：contract 只提供这一个常量。"""

    assert contract.DEFAULT_STATE == "active"


def test_undeclared_state_reports_active():
    """未声明 state 的模块，ModuleSpec.state == "active"（不再是字面 unknown）。"""

    specs = contract.modules()
    undeclared = [mid for mid in specs if declared_state(mid) is None]
    # 非空转：设计树里绝大多数模块都不写 state（当前 185/198）
    assert len(undeclared) >= 100, len(undeclared)
    for mid in undeclared:
        assert specs[mid].state == "active", mid


def test_undeclared_modules_are_landed_not_pending():
    """语义证据：未声明 state 的模块都带真实 fingerprint（只有 planned 才写 pending）。"""

    facts = design_tree_facts()
    undeclared = [mid for mid in contract.modules() if declared_state(mid) is None]
    pending = [mid for mid in undeclared if str(facts.modules[mid].get("fingerprint")) == "pending"]
    assert undeclared and pending == [], pending


def test_declared_states_are_preserved():
    """显式声明的 state 原样返回：计划态不会被默认值吃掉。"""

    specs = contract.modules()
    declared = {mid: declared_state(mid) for mid in specs}
    declared = {mid: state for mid, state in declared.items() if state is not None}
    assert declared, "设计树里应有显式声明 state 的模块（否则本用例空转）"
    assert set(declared.values()) == {"planned"}, declared
    for mid, state in declared.items():
        assert specs[mid].state == state, mid


def test_state_matches_design_tree_json():
    """与 tree.json 交叉校验：缺失 state 键 = active。"""

    facts = design_tree_facts()
    specs = contract.modules()
    assert len(facts.modules) == len(specs)
    for mid, entry in facts.modules.items():
        expected = str(entry.get("state") or contract.DEFAULT_STATE)
        assert specs[mid].state == expected, mid


def test_no_second_translation_of_default_state():
    """全仓不应再有第二处「未声明 ⇒ active」的翻译（t34 的 gen_skeleton.state_of 已删除）。"""

    for name in ("state_of", "UNSET_STATE", "DEFAULT_STATE"):
        assert not hasattr(gen_skeleton, name), name

    offenders: list[str] = []
    for base in ("src", "tools", "tests"):
        for path in sorted((REPO_ROOT / base).rglob("*.py")):
            for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                if _UNKNOWN_PAIR.search(line):
                    offenders.append(str(path.relative_to(REPO_ROOT)) + ":" + str(lineno))
    assert offenders == [], offenders


def test_module_map_state_column_uses_spec_state():
    """模块地图的状态列直接来自 spec.state，不再二次翻译。"""

    text = (REPO_ROOT / "docs" / "MODULE_MAP.md").read_text(encoding="utf-8")
    assert "| unknown |" not in text
    rows = {
        line.split("|")[1].strip().strip(chr(96)): line for line in text.splitlines() if line.startswith("| " + chr(96))
    }
    specs = contract.modules()
    assert len(rows) == len(specs)
    for mid, spec in specs.items():
        assert "| " + spec.state + " |" in rows[mid], rows[mid]
