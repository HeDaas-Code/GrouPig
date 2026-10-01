"""grouppig.infra.config.loader —— 配置加载器（``rpc:config.get``）。

normify id: ``grouppig.infra.config.loader``。

职责：把 TOML 文件、本地覆盖文件、环境变量与显式 overrides 合并成不可变
:class:`Config`，提供点分路径读取；密钥只从环境变量或 ``~/.dsh/.credentials.yaml``
读取，绝不写进配置对象或落盘。
"""

from __future__ import annotations

import hashlib
import json
import os
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from grouppig.infra.runtime.errors import ConfigError

CONFIG_ENV = "GROUPPIG_CONFIG"
ENV_PREFIX = "GROUPPIG__"  # 双下划线表示层级：GROUPPIG__MODEL__TASKS__CHAT__MODEL
DEFAULT_CONFIG_FILE = Path("config") / "grouppig.toml"
LOCAL_CONFIG_FILE = Path("config") / "grouppig.local.toml"
EXAMPLE_CONFIG_FILE = Path("config") / "grouppig.example.toml"
DEFAULT_CREDENTIALS_FILE = Path.home() / ".dsh" / ".credentials.yaml"


def _package_search_roots() -> tuple[Path, ...]:
    """从本模块自身位置向上找「可能是仓库根」的目录。

    默认配置路径以前是裸的 cwd 相对路径：换个工作目录启动就报「配置文件不存在」，
    而包明明还装在同一棵树里。这里给出候选根，让默认路径可解析。
    """

    here = Path(__file__).resolve()
    return (here.parent, *here.parents)


def _deep_merge(base: dict[str, Any], overlay: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for key, value in overlay.items():
        if isinstance(value, Mapping) and isinstance(out.get(key), Mapping):
            out[key] = _deep_merge(dict(out[key]), value)
        else:
            out[key] = value
    return out


def _parse_scalar(text: str) -> Any:
    lowered = text.strip()
    if lowered.lower() in ("true", "false"):
        return lowered.lower() == "true"
    if lowered.lower() in ("null", "none"):
        return None
    try:
        return json.loads(lowered)
    except (ValueError, TypeError):
        return text


def _env_overrides(environ: Mapping[str, str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in environ.items():
        if not key.startswith(ENV_PREFIX):
            continue
        path = [seg.lower() for seg in key[len(ENV_PREFIX) :].split("__") if seg]
        if not path:
            continue
        node = out
        for seg in path[:-1]:
            node = node.setdefault(seg, {})
        node[path[-1]] = _parse_scalar(value)
    return out


def _load_toml(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as fh:
            data = tomllib.load(fh)
    except FileNotFoundError as exc:
        raise ConfigError(f"配置文件不存在：{path}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"配置文件 TOML 解析失败：{path}：{exc}") from exc
    if not isinstance(data, dict):  # pragma: no cover - tomllib 保证返回 dict
        raise ConfigError(f"配置文件根节点必须是表：{path}")
    return data


@dataclass(frozen=True)
class Config:
    """不可变配置快照。"""

    raw: Mapping[str, Any] = field(default_factory=dict)
    source: str | None = None
    sources: tuple[str, ...] = ()

    # ---- 读取 ----------------------------------------------------------
    def get(self, path: str, default: Any = None) -> Any:
        node: Any = self.raw
        for seg in path.split("."):
            if isinstance(node, Mapping) and seg in node:
                node = node[seg]
            else:
                return default
        return node

    def require(self, path: str) -> Any:
        sentinel = object()
        value = self.get(path, sentinel)
        if value is sentinel:
            raise ConfigError(f"缺少必需配置项：{path}")
        return value

    def section(self, path: str) -> dict[str, Any]:
        value = self.get(path, {})
        if not isinstance(value, Mapping):
            raise ConfigError(f"配置项 {path} 不是表（table），得到 {type(value).__name__}")
        return dict(value)

    def sections(self, path: str) -> dict[str, dict[str, Any]]:
        value = self.get(path, {})
        if not isinstance(value, Mapping):
            raise ConfigError(f"配置项 {path} 不是表（table），得到 {type(value).__name__}")
        return {str(k): dict(v) for k, v in value.items() if isinstance(v, Mapping)}

    def to_dict(self) -> dict[str, Any]:
        return json.loads(json.dumps(self.raw, default=str))

    @property
    def fingerprint(self) -> str:
        blob = json.dumps(self.raw, sort_keys=True, default=str).encode("utf-8")
        return hashlib.sha256(blob).hexdigest()[:16]

    def with_overrides(self, overrides: Mapping[str, Any]) -> Config:
        return Config(
            raw=_deep_merge(dict(self.raw), overrides),
            source=self.source,
            sources=self.sources,
        )

    # ---- 便捷视图 ------------------------------------------------------
    @property
    def env(self) -> str:
        return str(self.get("app.env", "dev"))

    @property
    def storage_driver(self) -> str:
        return str(self.get("storage.driver", "sqlite"))

    @property
    def storage_dsn(self) -> str:
        return str(self.get("storage.dsn", ""))

    def as_log_fields(self) -> dict[str, Any]:
        return {"source": self.source, "fingerprint": self.fingerprint, "env": self.env}


def resolve_config_path(path: str | os.PathLike[str] | None = None, *, root: Path | None = None) -> Path:
    """解析配置路径：显式参数 → ``$GROUPPIG_CONFIG`` → ``config/grouppig.toml`` → ``config/grouppig.example.toml``。

    「默认」不是裸的 cwd 相对路径：cwd 找不到时，会顺着包自身的位置向上找同一个
    ``config/grouppig.toml``（换目录启动、或从别处调库都能用）。显式给了 ``root``
    就以 ``root`` 为准，不做这层回落 —— 调用方明确指定的根不该被悄悄绕过。
    """

    base = root or Path.cwd()
    if path is not None:
        candidate = Path(path).expanduser()
        return candidate if candidate.is_absolute() else (base / candidate)
    env_path = os.environ.get(CONFIG_ENV)
    if env_path:
        candidate = Path(env_path).expanduser()
        return candidate if candidate.is_absolute() else (base / candidate)
    candidates = [base / DEFAULT_CONFIG_FILE, base / EXAMPLE_CONFIG_FILE]
    if root is None:
        for parent in _package_search_roots():
            candidates.append(parent / DEFAULT_CONFIG_FILE)
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return base / DEFAULT_CONFIG_FILE


def load_config(
    path: str | os.PathLike[str] | None = None,
    *,
    overrides: Mapping[str, Any] | None = None,
    use_env: bool = True,
    use_local: bool = True,
    root: Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> Config:
    """加载配置（TOML → 本地覆盖 → 环境变量 → 显式 overrides）。"""

    environ = os.environ if environ is None else environ
    resolved = resolve_config_path(path, root=root)
    raw = _load_toml(resolved) if resolved.is_file() else {}
    sources = [str(resolved)] if resolved.is_file() else []

    if use_local:
        local = (root or Path.cwd()) / LOCAL_CONFIG_FILE
        if local.is_file() and local.resolve() != resolved.resolve():
            raw = _deep_merge(raw, _load_toml(local))
            sources.append(str(local))
    if use_env:
        env_over = _env_overrides(environ)
        if env_over:
            raw = _deep_merge(raw, env_over)
            sources.append("env:GROUPPIG__*")
    if overrides:
        raw = _deep_merge(raw, overrides)
        sources.append("overrides")

    if not raw:
        raise ConfigError(f"没有加载到任何配置（查找路径：{resolved}）")
    return Config(raw=raw, source=str(resolved), sources=tuple(sources))


# ---- 进程内当前配置 ------------------------------------------------------
_current: Config | None = None


def set_config(config: Config | None) -> Config | None:
    """设置进程内当前配置（供 ``rpc:config.get`` 与各模块读取）。"""

    global _current
    previous, _current = _current, config
    return previous


def get_config(*, reload: bool = False) -> Config:
    """取当前配置；未初始化时按默认路径加载一次。"""

    global _current
    if _current is None or reload:
        _current = load_config()
    return _current


def resolve_secret(
    *names: str,
    credentials_path: str | os.PathLike[str] | None = None,
    environ: Mapping[str, str] | None = None,
    default: str | None = None,
) -> str | None:
    """按顺序解析密钥：环境变量 → ``~/.dsh/.credentials.yaml`` → default。

    密钥只在此处短暂存在，绝不写入 :class:`Config`。
    """

    environ = os.environ if environ is None else environ
    for name in names:
        if not name:
            continue
        flat = name.replace(".", "_").replace("-", "_")
        leaf = flat.split("_")[-1]
        for candidate in (
            name,
            name.upper(),
            flat,
            flat.upper(),
            f"GROUPPIG_{flat.upper()}",
            leaf,
            leaf.upper(),
            f"GROUPPIG_{leaf.upper()}",
        ):
            value = environ.get(candidate)
            if value:
                return value
    path = Path(credentials_path).expanduser() if credentials_path else DEFAULT_CREDENTIALS_FILE
    if path.is_file():
        try:
            import yaml  # 延迟导入，缺依赖时退化为轻量解析
        except ImportError:  # pragma: no cover
            return _scan_credentials(path, names) or default
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except Exception:  # pragma: no cover - 凭据文件损坏不应导致崩溃
            return default
        for name in names:
            value = _dig(data, name)
            if value:
                return str(value)
    return default


def _dig(data: Any, dotted: str) -> Any:
    node = data
    for seg in dotted.split("."):
        if isinstance(node, Mapping) and seg in node:
            node = node[seg]
        else:
            return None
    return node


def _scan_credentials(path: Path, names: tuple[str, ...]) -> str | None:
    """极简回退：在文本里找 ``<name>: <value>``。"""

    text = path.read_text(encoding="utf-8", errors="ignore")
    for name in names:
        needle = name.split(".")[-1]
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith(f"{needle}:"):
                return stripped.split(":", 1)[1].strip().strip("'\"") or None
    return None


def config_get(path: str | None = None, default: Any = None, *, config: Config | None = None) -> Any:
    """``rpc:config.get`` —— 读取配置。

    ``path`` 为 ``None`` 时返回整份配置字典，否则按点分路径读取（缺失时返回 ``default``）。
    """

    cfg = config if config is not None else get_config()
    if path in (None, "", "."):
        return cfg.to_dict()
    return cfg.get(str(path), default)


__all__ = [
    "CONFIG_ENV",
    "DEFAULT_CONFIG_FILE",
    "DEFAULT_CREDENTIALS_FILE",
    "ENV_PREFIX",
    "LOCAL_CONFIG_FILE",
    "Config",
    "config_get",
    "get_config",
    "load_config",
    "resolve_config_path",
    "resolve_secret",
    "set_config",
]
