#!/usr/bin/env python3
"""生成 ``src/grouppig`` 目录骨架与 ``docs/MODULE_MAP.md``。

骨架逐字镜像 ``normify-grouppig`` 的模块路径：

* 容器模块（57 个）→ Python 包 ``<path>/__init__.py``
* 叶子模块（109 个）→ ``<path>.py``（默认只登记到 MODULE_MAP，不生成空文件；
  加 ``--with-leaves`` 才会落地带 ``NotImplementedError`` 的占位文件）

normify 段名里的 ``-`` 在 Python 侧写作 ``_``（``model-gateway`` → ``model_gateway``）。

容器包的 docstring 里写着设计树的 state=，而 state 会随模块落地从 planned 变 active；
本脚本因此**每次运行都会把仍是模板产物的 __init__.py 刷新到当前 state**：

* 判据是**整文件结构同构**（把 PACKAGE_INIT 的三个占位符换成捕获组后全文匹配，且
  ``normify id`` 行就是本模块）—— 手写装配入口（``src/grouppig/perception/__init__.py`` 等）
  整文件对不上句式，一律逐字不动；
* 内容已正确时不写盘（幂等，不制造无意义的 mtime 变化）。

用法::

    python tools/gen_skeleton.py                # 生成包目录 + 刷新 state + 模块地图
    python tools/gen_skeleton.py --with-leaves  # 额外生成叶子占位文件
    python tools/gen_skeleton.py --check        # 只校验：骨架齐全 **且** state 与设计树同步（CI 用）
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from grouppig.infra.runtime import contract  # noqa: E402

OWNERS = (
    ("grouppig.infra", "infra-engineer"),
    ("grouppig.memory", "memory-engineer"),
    ("grouppig.gateway", "gateway-engineer"),
    ("grouppig.perception", "perception-engineer"),
    ("grouppig.session", "session-engineer"),
    ("grouppig.social", "social-reflect-engineer"),
    ("grouppig.reflection", "social-reflect-engineer"),
    ("grouppig.expression.persona", "expression-core-engineer"),
    ("grouppig.expression.generator", "expression-core-engineer"),
    ("grouppig.expression.orchestrator", "expression-flow-engineer"),
    ("grouppig.expression.identity", "expression-flow-engineer"),
    ("grouppig.expression.slang", "expression-flow-engineer"),
)

PACKAGE_INIT = '''"""{name}。

normify id: ``{mid}``（容器模块，state={state}）。
"""
'''

LEAF_STUB = '''"""{name}。

normify id: ``{mid}``（叶子模块，state={state}）。

TODO(占位)：由 {owner} 实现；契约 API：{apis}
"""

from __future__ import annotations


def _not_implemented(*args, **kwargs):
    raise NotImplementedError("{mid} 尚未实现")


__all__ = ["_not_implemented"]
'''


#: 有效状态的**唯一事实来源**是 ``contract.DEFAULT_STATE``（未声明 state 即 active）；
#: 本脚本直接读 ``spec.state``，不再自己翻译一次。

#: 模板里的三个占位符，以及「一行内任意文本」的捕获片段。
_PLACEHOLDERS = ("{name}", "{mid}", "{state}")
_LINE = r"[^\n]*"


def _package_init_pattern() -> re.Pattern[str]:
    """把 PACKAGE_INIT 编译成结构同构正则（三个占位符各捕获一行内文本）。"""

    splitter = "(" + "|".join(re.escape(item) for item in _PLACEHOLDERS) + ")"
    pieces: list[str] = []
    for part in re.split(splitter, PACKAGE_INIT):
        if part in _PLACEHOLDERS:
            pieces.append("(?P<" + part[1:-1] + ">" + _LINE + ")")
        else:
            pieces.append(re.escape(part))
    return re.compile("".join(pieces))


#: 判断「这个 __init__.py 还是不是生成器写的」的**结构**判据。
#: 手写文件哪怕 docstring 里恰好出现 state=planned 字样，整文件也对不上这个句式。
PACKAGE_INIT_PATTERN = _package_init_pattern()


def template_fields(path: Path, *, mid: str) -> re.Match[str] | None:
    """文件仍是模板产物时返回捕获组（name / mid / state），否则 None。

    判据是**整文件结构同构** + ``normify id`` 行必须正是本模块；手写文件返回 None，
    生成器据此绝不覆盖它们。
    """

    if not path.is_file():
        return None
    found = PACKAGE_INIT_PATTERN.fullmatch(path.read_text(encoding="utf-8"))
    if found is None or found.group("mid") != mid:
        return None
    return found


def refresh_package_init(path: Path, *, mid: str, state: str) -> bool:
    """把模板产物的 state= 原地刷新为设计树当前 state；返回是否写盘。

    手写文件（template_fields 为 None）直接跳过；内容已正确时不写盘（幂等）。
    """

    found = template_fields(path, mid=mid)
    if found is None or found.group("state") == state:
        return False
    path.write_text(PACKAGE_INIT.format(name=found.group("name"), mid=mid, state=state), encoding="utf-8")
    return True


def stale_container_inits(root: Path | None = None) -> list[tuple[str, str, str]]:
    """模板同构但 state= 与设计树不一致的容器：[(mid, 文件里的 state, 有效 state)]。"""

    base = root or ROOT
    specs = contract.modules()
    stale: list[tuple[str, str, str]] = []
    for mid in contract.container_ids():
        found = template_fields(contract.module_to_path(mid, root=base), mid=mid)
        if found is None:
            continue
        state = specs[mid].state
        if found.group("state") != state:
            stale.append((mid, found.group("state"), state))
    return stale


def owner_of(module_id: str) -> str:
    for prefix, owner in OWNERS:
        if module_id == prefix or module_id.startswith(prefix + "."):
            return owner
    return "unassigned"


def _name_of(spec: contract.ModuleSpec) -> str:
    path = spec.source_path
    if path is None:
        return spec.id
    text = path.read_text(encoding="utf-8")
    found = re.search(r"^name:\s*\{(.*)\}\s*$", text, re.M)
    return found.group(1).strip() if found else spec.id


def generate(*, with_leaves: bool = False, check: bool = False) -> int:
    specs = contract.modules()
    missing: list[Path] = []
    created: list[Path] = []
    refreshed: list[Path] = []
    stale: list[Path] = []
    rows: list[str] = []

    for mid in contract.module_ids():
        spec = specs[mid]
        target = contract.module_to_path(mid, root=ROOT)
        apis = ", ".join(f"{proto}:{p}" for proto, p in spec.apis) or "-"
        kind = "容器" if spec.is_container else "叶子"
        state = spec.state
        relative = target.relative_to(ROOT)
        rows.append(f"| `{mid}` | {kind} | {state} | `{relative}` | {apis} | {owner_of(mid)} |")
        if spec.is_container:
            init = target
            if not init.exists():
                if check:
                    missing.append(init)
                    continue
                init.parent.mkdir(parents=True, exist_ok=True)
                init.write_text(PACKAGE_INIT.format(name=_name_of(spec), mid=mid, state=state), encoding="utf-8")
                created.append(init)
            elif check:
                found = template_fields(init, mid=mid)
                if found is not None and found.group("state") != state:
                    stale.append(init)
            elif refresh_package_init(init, mid=mid, state=state):
                refreshed.append(init)
        elif with_leaves and not target.exists():
            if check:
                missing.append(target)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(
                LEAF_STUB.format(name=_name_of(spec), mid=mid, state=state, owner=owner_of(mid), apis=apis),
                encoding="utf-8",
            )
            created.append(target)

    if check:
        if missing:
            print(f"骨架缺失（{len(missing)}）：")
            for path in missing:
                print("  -", path.relative_to(ROOT))
            return 1
        if stale:
            print(f"state 陈旧（{len(stale)}）：模板产物的 __init__.py 里写的 state 与设计树不一致")
            for path in stale:
                print("  `", path.relative_to(ROOT))
            print("跑 python tools/gen_skeleton.py 原地刷新")
            return 1
        print(f"骨架完整且 state 同步：{len(contract.container_ids())} 个容器包、{len(contract.leaf_ids())} 个叶子模块")
        return 0

    docs = ROOT / "docs" / "MODULE_MAP.md"
    docs.parent.mkdir(parents=True, exist_ok=True)
    summary = contract.summary()
    header = [
        "# GrouPig 模块地图（由 `tools/gen_skeleton.py` 生成，请勿手改）",
        "",
        f"契约来源：`normify-grouppig/api-index.json`（{summary['names']} 个名字："
        f"{summary['rpc']} rpc / {summary['kafka']} kafka / {summary['mysql']} mysql）、"
        f"`normify-grouppig/modules/**/*.md`（{summary['modules']} 个模块："
        f"{summary['containers']} 容器 / {summary['leaves']} 叶子）。",
        "",
        "路径映射规则：`grouppig.<domain>.<area>.<leaf>` → `src/grouppig/<domain>/<area>/<leaf>.py`，"
        "段名中的 `-` 在 Python 侧写作 `_`（`grouppig.infra.model-gateway.router` → "
        "`src/grouppig/infra/model_gateway/router.py`）；容器模块对应同名包的 `__init__.py`。",
        "",
        "| 模块 id | 类型 | 状态 | 源码路径 | 契约 API | 负责人 |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    docs.write_text("\n".join([*header, *rows, ""]), encoding="utf-8")
    print(f"生成 {len(created)} 个文件/目录，刷新 {len(refreshed)} 个 state，模块地图：{docs.relative_to(ROOT)}")
    for path in created:
        print("  +", path.relative_to(ROOT))
    for path in refreshed:
        print("  `", path.relative_to(ROOT))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="生成 src/grouppig 目录骨架")
    parser.add_argument("--with-leaves", action="store_true", help="同时生成叶子模块占位文件")
    parser.add_argument("--check", action="store_true", help="只校验，不写文件")
    args = parser.parse_args(argv)
    return generate(with_leaves=args.with_leaves, check=args.check)


if __name__ == "__main__":
    raise SystemExit(main())
