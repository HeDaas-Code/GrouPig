"""grouppig.infra.runtime.contract —— normify 契约的唯一事实来源。

读取 ``normify-grouppig/api-index.json`` 与 ``normify-grouppig/modules/**/*.md``，
向全项目提供：

* ``rpc:`` / ``kafka:`` / ``mysql:`` 名字集合（逐字对齐设计，不允许自造名字）
* 模块 id ↔ 源码路径的映射（``grouppig.infra.model-gateway.router`` → ``src/grouppig/infra/model_gateway/router.py``）
* 契约断言（``assert_known_name`` / ``check_registry``）

normify id: ``grouppig.infra.runtime.contract``（运行时补充模块，设计树暂无）。
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

from grouppig.infra.runtime.errors import ContractError, UnknownNameError

API_INDEX_ENV = "GROUPPIG_API_INDEX"
_REPO_MARKER = Path("normify-grouppig") / "api-index.json"
_FRONTMATTER = re.compile(r"\A---\n(.*?)\n---\n", re.S)

#: 有效状态：normify 的 state 是**可选**字段，未声明即 active。单点事实，消费方不要再翻译一次。
#:
#: 依据一（normify_help topic:fields）：state(active|planned|deprecated) 属可选字段，只有计划态需要
#: 显式写 state=planned（并配 fingerprint=pending）；落地后用 normify_module_refresh(activate=true) 转 active。
#: 依据二（设计树实际形态）：198 个模块里只有 13 个写了 state: planned，其余 185 个没有 state 行、
#: 且带真实 fingerprint（非 pending）—— 即「未声明 = active」。
#: 依据三（激活变更的写法）：2026-09-24-activate-landed-modules 把 19 个已落地模块转 active 的方式是
#: 删掉 state 行，全树 0 个模块写 state: active。
DEFAULT_STATE = "active"


@dataclass(frozen=True)
class ModuleSpec:
    """normify 模块的元数据（来自 Markdown frontmatter）。

    ``state`` 一律是**有效状态**（见 :data:`DEFAULT_STATE`）：frontmatter 未声明时即
    ``"active"``。消费方直接读它即可，不要再自己把「未声明」翻译一次。
    """

    id: str
    parent: str | None
    state: str
    apis: tuple[tuple[str, str], ...] = ()
    source_path: Path | None = None
    children: tuple[str, ...] = field(default=())

    @property
    def is_container(self) -> bool:
        return bool(self.children)

    @property
    def rpc_names(self) -> tuple[str, ...]:
        return tuple(f"rpc:{p}" for proto, p in self.apis if proto == "rpc")

    @property
    def topic_names(self) -> tuple[str, ...]:
        return tuple(f"kafka:{p}" for proto, p in self.apis if proto == "kafka")

    @property
    def table_names(self) -> tuple[str, ...]:
        return tuple(f"mysql:{p}" for proto, p in self.apis if proto == "mysql")


def repo_root(start: Path | None = None) -> Path:
    """向上查找仓库根（含 ``normify-grouppig/api-index.json`` 的目录）。"""

    env = os.environ.get("GROUPPIG_REPO_ROOT")
    if env:
        return Path(env).expanduser().resolve()
    here = (start or Path(__file__)).resolve()
    for candidate in (here, *here.parents):
        if (candidate / _REPO_MARKER).is_file():
            return candidate
    raise ContractError(f"找不到仓库根：从 {here} 向上没有 {_REPO_MARKER}")


def api_index_path() -> Path:
    env = os.environ.get(API_INDEX_ENV)
    if env:
        return Path(env).expanduser().resolve()
    return repo_root() / _REPO_MARKER


def modules_dir() -> Path:
    return api_index_path().parent / "modules"


@lru_cache(maxsize=1)
def api_index() -> dict[str, str]:
    """``名字 → 归属模块 id`` 的完整索引（164 条：147 rpc / 9 kafka / 8 mysql）。"""

    path = api_index_path()
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ContractError(f"{path} 不是 JSON 对象")
    return {str(k): str(v) for k, v in data.items()}


def _names(prefix: str) -> tuple[str, ...]:
    return tuple(sorted(n for n in api_index() if n.startswith(prefix)))


def rpc_names() -> tuple[str, ...]:
    return _names("rpc:")


def topic_names() -> tuple[str, ...]:
    return _names("kafka:")


def table_names() -> tuple[str, ...]:
    return _names("mysql:")


def owner(name: str) -> str | None:
    """名字的归属模块 id；不在契约中则返回 ``None``。"""

    return api_index().get(name)


def is_known_name(name: str) -> bool:
    return name in api_index()


def assert_known_name(name: str) -> str:
    if not is_known_name(name):
        raise UnknownNameError(f"名字 {name!r} 不在 normify-grouppig/api-index.json 中（禁止自造契约名）")
    return name


def names_for_module(module_id: str) -> tuple[str, ...]:
    return tuple(sorted(n for n, m in api_index().items() if m == module_id))


def python_segment(normify_segment: str) -> str:
    """normify 段名 → Python 标识符（``model-gateway`` → ``model_gateway``）。"""

    return normify_segment.replace("-", "_")


def module_to_py(module_id: str) -> str:
    """模块 id → Python 点分路径（``grouppig.infra.model-gateway.router`` → ``grouppig.infra.model_gateway.router``）。"""

    return ".".join(python_segment(part) for part in module_id.split("."))


def module_to_path(module_id: str, *, root: Path | None = None) -> Path:
    """模块 id → 源码路径（容器 → ``<path>/__init__.py``，叶子 → ``<path>.py``）。"""

    base = (root or repo_root()) / "src"
    parts = [python_segment(p) for p in module_id.split(".")]
    spec = modules().get(module_id)
    if spec is not None and spec.is_container:
        return base.joinpath(*parts, "__init__.py")
    return base.joinpath(*parts[:-1], parts[-1] + ".py")


def _parse_frontmatter(text: str) -> dict[str, Any]:
    match = _FRONTMATTER.match(text)
    if not match:
        return {}
    body = match.group(1)
    meta: dict[str, Any] = {}
    for key in ("id", "parent", "state"):
        found = re.search(rf"^{key}:\s*(.+)$", body, re.M)
        if found:
            meta[key] = found.group(1).strip().strip('"').strip("'")
    apis: list[tuple[str, str]] = []
    for proto, path in re.findall(r'protocol:\s*(\S+)\s*\n\s*path:\s*"?([^"\n]+)"?', body):
        apis.append((proto, path.strip()))
    meta["apis"] = tuple(apis)
    return meta


@lru_cache(maxsize=1)
def modules() -> dict[str, ModuleSpec]:
    """全部 166 个 normify 模块（57 容器 / 109 叶子）。"""

    root = modules_dir()
    if not root.is_dir():
        raise ContractError(f"模块目录不存在：{root}")
    raw: dict[str, dict[str, Any]] = {}
    for md in sorted(root.rglob("*.md")):
        meta = _parse_frontmatter(md.read_text(encoding="utf-8"))
        mid = meta.get("id")
        if not mid:
            continue
        raw[mid] = {**meta, "source_path": md}
    specs: dict[str, ModuleSpec] = {}
    for mid, meta in raw.items():
        children = tuple(sorted(k for k, v in raw.items() if v.get("parent") == mid))
        specs[mid] = ModuleSpec(
            id=mid,
            parent=meta.get("parent"),
            state=meta.get("state") or DEFAULT_STATE,
            apis=meta.get("apis", ()),
            source_path=meta.get("source_path"),
            children=children,
        )
    return specs


def container_ids() -> tuple[str, ...]:
    return tuple(sorted(m for m, s in modules().items() if s.is_container))


def leaf_ids() -> tuple[str, ...]:
    return tuple(sorted(m for m, s in modules().items() if not s.is_container))


def module_ids() -> tuple[str, ...]:
    return tuple(sorted(modules()))


def check_registry(registry_names: set[str], *, scope: str | None = None) -> dict[str, list[str]]:
    """比对已注册名字与契约。

    返回 ``{"missing": [...], "unknown": [...]}``；``scope`` 为模块 id 前缀
    （如 ``grouppig.infra``），只校验该子树。
    """

    if scope is None:
        expected = set(api_index())
    else:
        expected = {n for n, m in api_index().items() if m == scope or m.startswith(scope + ".")}
    return {
        "missing": sorted(expected - registry_names),
        "unknown": sorted(registry_names - set(api_index())),
    }


def summary() -> dict[str, Any]:
    """契约概览，供启动日志与健康检查使用。"""

    idx = api_index()
    return {
        "api_index": str(api_index_path()),
        "names": len(idx),
        "rpc": len(rpc_names()),
        "kafka": len(topic_names()),
        "mysql": len(table_names()),
        "modules": len(modules()),
        "containers": len(container_ids()),
        "leaves": len(leaf_ids()),
    }


__all__ = [
    "API_INDEX_ENV",
    "ModuleSpec",
    "api_index",
    "api_index_path",
    "assert_known_name",
    "check_registry",
    "container_ids",
    "is_known_name",
    "leaf_ids",
    "module_ids",
    "module_to_path",
    "module_to_py",
    "modules",
    "modules_dir",
    "names_for_module",
    "owner",
    "python_segment",
    "repo_root",
    "rpc_names",
    "summary",
    "table_names",
    "topic_names",
]
