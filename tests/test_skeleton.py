"""仓库骨架与模块地图测试。

计数与名字集合全部从 normify-grouppig/tree.json 动态推导，不写死设计树规模；
「生成器不建叶子文件」改为在临时目录校验生成器自身行为，因此各域落地实现后依然成立。
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "tools"))

import gen_skeleton  # noqa: E402

from grouppig.infra.runtime import contract  # noqa: E402
from helpers import design_tree_facts  # noqa: E402

MODULE_MAP = REPO_ROOT / "docs" / "MODULE_MAP.md"
MARK = chr(96)  # 反引号：模块地图里的行内代码标记


def quoted(text: str) -> str:
    return MARK + text + MARK


def test_generated_skeleton_is_complete():
    assert gen_skeleton.main(["--check"]) == 0


def test_module_map_lists_every_module():
    facts = design_tree_facts()
    text = MODULE_MAP.read_text(encoding="utf-8")
    body = [line for line in text.splitlines() if line.startswith("| " + MARK)]
    assert len(body) == facts.counts["modules"]
    for module_id in sorted(facts.module_ids):
        assert quoted(module_id) in text, module_id


def test_module_map_header_uses_derived_counts():
    facts = design_tree_facts()
    text = MODULE_MAP.read_text(encoding="utf-8")
    for key in ("names", "rpc", "kafka", "mysql", "modules", "containers", "leaves"):
        assert str(facts.counts[key]) in text, key + "=" + str(facts.counts[key])


def test_module_map_paths_match_module_to_path():
    facts = design_tree_facts()
    text = MODULE_MAP.read_text(encoding="utf-8")
    for module_id in sorted(facts.module_ids):
        relative = contract.module_to_path(module_id, root=REPO_ROOT).relative_to(REPO_ROOT).as_posix()
        assert quoted(relative) in text, module_id + " -> " + relative


def test_module_map_is_in_sync_with_generator():
    before = MODULE_MAP.read_text(encoding="utf-8")
    assert gen_skeleton.generate() == 0
    assert MODULE_MAP.read_text(encoding="utf-8") == before


def test_owner_mapping_covers_every_module():
    for module_id in contract.module_ids():
        owner = gen_skeleton.owner_of(module_id)
        assert isinstance(owner, str) and owner, module_id
    assert gen_skeleton.owner_of("grouppig.unknown.thing") == "unassigned"


@pytest.mark.parametrize(
    ("module_id", "owner"),
    [
        ("grouppig.infra.config.loader", "infra-engineer"),
        ("grouppig.memory.chat-store.dao", "memory-engineer"),
        ("grouppig.gateway.adapter.onebot", "gateway-engineer"),
    ],
)
def test_owner_mapping_examples(module_id: str, owner: str):
    if module_id not in contract.modules():
        pytest.skip(module_id + " 已不在设计树中（改名或移动），跳过示例断言")
    assert gen_skeleton.owner_of(module_id) == owner


def test_generator_only_creates_container_packages(tmp_path, monkeypatch):
    """默认只建容器包、不建叶子文件；显式 with_leaves 才落地占位文件。"""

    repo = tmp_path / "repo"
    (repo / "src").mkdir(parents=True)
    monkeypatch.setattr(gen_skeleton, "ROOT", repo)
    monkeypatch.setenv(contract.API_INDEX_ENV, str(REPO_ROOT / "normify-grouppig" / "api-index.json"))

    assert gen_skeleton.generate() == 0
    for module_id in contract.container_ids():
        assert contract.module_to_path(module_id, root=repo).is_file(), module_id
    for module_id in contract.leaf_ids():
        assert not contract.module_to_path(module_id, root=repo).exists(), module_id
    assert (repo / "docs" / "MODULE_MAP.md").is_file()

    assert gen_skeleton.generate(with_leaves=True) == 0
    for module_id in contract.leaf_ids():
        assert contract.module_to_path(module_id, root=repo).is_file(), module_id


def test_check_mode_detects_missing_skeleton(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    (repo / "src").mkdir(parents=True)
    monkeypatch.setattr(gen_skeleton, "ROOT", repo)
    assert gen_skeleton.main(["--check"]) == 1
    assert gen_skeleton.generate() == 0
    assert gen_skeleton.main(["--check"]) == 0


def test_project_metadata_files_exist():
    for name in ("pyproject.toml", "README.md", "Makefile", ".gitignore", "config/grouppig.toml"):
        assert (REPO_ROOT / name).is_file(), name
    assert (REPO_ROOT / "src" / "grouppig" / "__init__.py").is_file()


# ---- t34：容器 __init__.py 的 state 必须与设计树同步 ----------------------
#: 手写装配入口（不是 PACKAGE_INIT 产物）：生成器绝不能覆盖它们，这里做逐字回归。
HANDWRITTEN_INITS = (
    "src/grouppig/__init__.py",
    "src/grouppig/perception/__init__.py",
    "src/grouppig/infra/runtime/__init__.py",
    "src/grouppig/memory/runtime/__init__.py",
    "src/grouppig/panel/__init__.py",
    "src/grouppig/session/runtime/__init__.py",
)


def container_id_for(path: Path) -> str:
    """路径反查容器模块 id（查不到说明测试自己写错了路径）。"""

    for module_id in contract.container_ids():
        if contract.module_to_path(module_id, root=REPO_ROOT) == path:
            return module_id
    raise AssertionError("路径不对应任何容器模块：" + path.as_posix())


def tree_json_states() -> dict[str, str]:
    """设计树（tree.json）里每个模块的 state；未显式声明即 normify 默认的 active。"""

    return {mid: str(entry.get("state", "active")) for mid, entry in design_tree_facts().modules.items()}


def test_template_container_inits_match_design_tree_state():
    """凡是与 PACKAGE_INIT 同构的容器 __init__.py，其 state= 必等于设计树的 state。"""

    states = tree_json_states()
    specs = contract.modules()
    checked: list[str] = []
    for module_id in contract.container_ids():
        path = contract.module_to_path(module_id, root=REPO_ROOT)
        found = gen_skeleton.template_fields(path, mid=module_id)
        if found is None:
            continue  # 手写文件：生成器不碰，也不在这里断言
        checked.append(module_id)
        assert found.group("state") == specs[module_id].state, (module_id, path)
        assert found.group("state") == states[module_id], (module_id, path)

    # 覆盖既有 planned 容器，并允许设计树新增 planned 容器。
    planned = [mid for mid in contract.container_ids() if specs[mid].state == "planned"]
    assert len(checked) >= 40, checked
    assert len(planned) >= 13
    assert set(planned) - set(checked) <= {"grouppig.perception.runtime"}
    assert gen_skeleton.stale_container_inits(REPO_ROOT) == []


def test_state_detector_is_not_vacuous():
    """判据不是空转：把模板产物改成错误的 state，同一个判据必须报出它。"""

    corrupted = "grouppig.memory"  # 模板产物，设计树里是 active
    path = contract.module_to_path(corrupted, root=REPO_ROOT)
    text = path.read_text(encoding="utf-8")
    assert gen_skeleton.template_fields(path, mid=corrupted) is not None
    assert "state=active" in text
    try:
        path.write_text(text.replace("state=active", "state=planned"), encoding="utf-8")
        stale = {mid for mid, _file_state, _tree_state in gen_skeleton.stale_container_inits(REPO_ROOT)}
        assert corrupted in stale, stale
    finally:
        path.write_text(text, encoding="utf-8")
    assert gen_skeleton.stale_container_inits(REPO_ROOT) == []


def test_handwritten_package_inits_are_never_touched():
    """手写装配入口不是模板产物；生成器跑一遍后必须逐字不变。"""

    paths = [REPO_ROOT / rel for rel in HANDWRITTEN_INITS]
    for path in paths:
        assert path.is_file(), path
        assert gen_skeleton.template_fields(path, mid=container_id_for(path)) is None, path
    before = {path: path.read_bytes() for path in paths}
    assert gen_skeleton.generate() == 0
    assert {path: path.read_bytes() for path in paths} == before


def test_generator_refreshes_stale_state_and_is_idempotent(tmp_path, monkeypatch):
    """人为改坏模板产物 → --check 报错、生成器原地修回；内容已正确时不写盘。"""

    repo = tmp_path / "repo"
    (repo / "src").mkdir(parents=True)
    monkeypatch.setattr(gen_skeleton, "ROOT", repo)
    monkeypatch.setenv(contract.API_INDEX_ENV, str(REPO_ROOT / "normify-grouppig" / "api-index.json"))
    assert gen_skeleton.generate() == 0

    module_id = "grouppig.memory"  # 设计树里是 active（frontmatter 未写 state）
    path = contract.module_to_path(module_id, root=repo)
    path.write_text(gen_skeleton.PACKAGE_INIT.format(name="测试容器", mid=module_id, state="planned"), encoding="utf-8")

    assert gen_skeleton.stale_container_inits(repo) == [(module_id, "planned", "active")]
    assert gen_skeleton.main(["--check"]) == 1

    assert gen_skeleton.generate() == 0
    assert "state=active" in path.read_text(encoding="utf-8")
    assert gen_skeleton.stale_container_inits(repo) == []
    assert gen_skeleton.main(["--check"]) == 0

    stamp = path.stat().st_mtime_ns
    assert gen_skeleton.generate() == 0
    assert path.stat().st_mtime_ns == stamp, "内容已正确时不该写盘（幂等）"


def test_handwritten_init_with_state_wording_is_left_alone(tmp_path, monkeypatch):
    """手写文件里也可能出现 state=planned 字样：结构判据必须跳过它，而不是见字样就改。"""

    repo = tmp_path / "repo"
    (repo / "src").mkdir(parents=True)
    monkeypatch.setattr(gen_skeleton, "ROOT", repo)
    monkeypatch.setenv(contract.API_INDEX_ENV, str(REPO_ROOT / "normify-grouppig" / "api-index.json"))
    assert gen_skeleton.generate() == 0

    module_id = "grouppig.memory"
    path = contract.module_to_path(module_id, root=repo)
    handwritten = (
        '"""grouppig.memory —— 记忆层装配入口（手写，不是生成器产物）。\n'
        "\n"
        f"normify id: {MARK}{MARK}{module_id}{MARK}{MARK}（容器模块，state=planned）。\n"
        "\n"
        "多出来的段落让它整文件对不上模板句式。\n"
        '"""\n'
    )
    path.write_text(handwritten, encoding="utf-8")

    assert gen_skeleton.template_fields(path, mid=module_id) is None
    assert gen_skeleton.stale_container_inits(repo) == []
    assert gen_skeleton.main(["--check"]) == 0
    assert gen_skeleton.generate() == 0
    assert path.read_text(encoding="utf-8") == handwritten


def test_module_map_state_column_is_effective_state():
    """模块地图的 state 列写有效状态（未显式声明 = active），不再出现 unknown。"""

    text = MODULE_MAP.read_text(encoding="utf-8")
    assert "| unknown |" not in text
    specs = contract.modules()
    for module_id in ("grouppig.memory", "grouppig.infra", "grouppig.expression.slang"):
        row = next(line for line in text.splitlines() if line.startswith("| " + quoted(module_id) + " |"))
        assert "| " + specs[module_id].state + " |" in row, row


# ---- 方案 A：易变的值不进人写的文件 ---------------------------------------
def module_id_for(path: Path) -> str:
    """路径反查任意模块 id（容器与叶子都覆盖）。"""

    for module_id in contract.module_ids():
        if contract.module_to_path(module_id, root=REPO_ROOT) == path:
            return module_id
    raise AssertionError("路径不对应任何模块：" + path.as_posix())


def test_handwritten_module_markers_carry_no_state():
    """非模板产物的模块标记行不得写 state：state 会变，副本没人维护（曾产生 74 处假话）。"""

    pattern = re.compile("（(?:容器|叶子)模块，state=[a-z]+）")
    template_products, offenders = [], []
    for path in sorted((REPO_ROOT / "src").rglob("*.py")):
        if not pattern.search(path.read_text(encoding="utf-8")):
            continue
        if gen_skeleton.template_fields(path, mid=module_id_for(path)) is not None:
            template_products.append(path)
            continue
        offenders.append(path.relative_to(REPO_ROOT).as_posix())

    assert offenders == []
    # 非空转：生成器模板产物确实带着 state（否则上面的循环可能一个文件都没走到）
    assert len(template_products) >= 40, len(template_products)
