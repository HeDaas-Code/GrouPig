"""grouppig.reflection.presets.registry —— 预设注册表（`rpc:presets.load` / `rpc:presets.register`）。

职责（对应设计 `grouppig.reflection.presets.registry`「持久化预设的加载与注册，维护预设版本」）：

* `rpc:presets.register` —— 注册/更新一个行为预设，写入 `presets/<preset_id>.json`，
  每次写入把旧版本压进 `history`（版本自增，可回滚、可审计）；
* `rpc:presets.load` —— 加载预设：默认读磁盘目录，也支持 `presets=[...]` 直接喂内存预设
  （离线/单测用）与 `include_builtin` 兜底内置预设。

**预设结构**（`Preset`）::

    {
      "preset_id": "calm_discussion",
      "name": "平静讨论",
      "version": 3,
      "scenario": "calm",          # 场景键，matcher 按它筛选
      "triggers": {                # 行为特征条件，matcher 逐条比对
        "heat_max": 0.45,          # 热度上限
        "topic_max": 3,            # 同时活跃话题数上限
        "flood_max": 0.2,          # 刷屏分上限
        "interrupt_min": 0.0,      # 插话时机分下限（越高越该等）
        "phase": ["discussion"]    # 阶段白名单（可省）
      },
      "actions": {                 # 表达层要执行的动作（本域只描述，不执行）
        "reply_probability": 0.35,
        "max_replies_per_minute": 2,
        "max_length": 80,
        "wait_seconds": [3, 12],
        "tone": "平和",
        "use_slang": false,
        "mention_reply": true
      },
      "notes": "…",
      "source": "builtin" | "reflection" | "manual",
      "created_at": 1700000000.0,
      "updated_at": 1700000000.0
    }

**为什么落文件**：设计给本叶子的依赖只有「持久化预设的加载与注册」，没有 `rpc:`/`mysql:`
存储契约名；因此默认用磁盘 JSON（`GROUPPIG_PRESETS_DIR` 可覆盖），并支持 `writer`/`reader`
注入接任意后端。表/存储名字一旦进设计树，只需换掉这两个钩子。

设计：`grouppig.reflection.presets.registry`（叶子模块）。
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

#: normify 模块 id。
MODULE = "grouppig.reflection.presets.registry"

#: 本叶子负责的契约名字（逐字取自 api-index.json）。
NAMES: tuple[str, ...] = ("rpc:presets.load", "rpc:presets.register")

#: 预设场景键（设计描述里点名的四类 + 兜底）。
SCENARIOS: tuple[str, ...] = ("calm", "exposition", "smalltalk", "flooding", "conflict", "silence", "default")

#: 预设动作字段（白名单，未知键丢弃以免把脏数据写进表达层）。
ACTION_KEYS: tuple[str, ...] = (
    "reply_probability",
    "max_replies_per_minute",
    "max_length",
    "min_interval_seconds",
    "wait_seconds",
    "tone",
    "use_slang",
    "use_emoji",
    "mention_reply",
    "stance",
    "risk",
)

#: 触发器字段（matcher 逐条比对；`*_max` 是上限，`*_min` 是下限）。
TRIGGER_KEYS: tuple[str, ...] = (
    "heat_max",
    "heat_min",
    "topic_max",
    "topic_min",
    "flood_max",
    "flood_min",
    "interrupt_max",
    "interrupt_min",
    "reply_rate_max",
    "reply_rate_min",
    "focus_max",
    "focus_min",
    "phase",
    "keyword_any",
    "keyword_all",
)

#: 环境变量：预设目录。
ENV_PRESETS_DIR = "GROUPPIG_PRESETS_DIR"

#: 默认预设目录（仓库内 `var/presets`，与 config 的 storage 同级）。
DEFAULT_PRESETS_DIR = Path("var") / "presets"


def default_presets_dir() -> Path:
    """预设目录（`GROUPPIG_PRESETS_DIR` 优先）。"""

    override = os.environ.get(ENV_PRESETS_DIR)
    return Path(override) if override else DEFAULT_PRESETS_DIR


def new_preset_id(scenario: str = "default", *, suffix: str = "") -> str:
    """生成预设 id（`<scenario>_<时间戳>`，可加后缀）。"""

    stamp = time.strftime("%Y%m%d%H%M%S", time.gmtime())
    tail = f"_{suffix}" if suffix else ""
    return f"{scenario or 'default'}_{stamp}{tail}"


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, float(value)))


def normalize_actions(actions: Mapping[str, Any] | None) -> dict[str, Any]:
    """规整动作字段：白名单过滤 + 类型/范围收敛。"""

    source = dict(actions or {})
    payload: dict[str, Any] = {}
    for key in ACTION_KEYS:
        if key not in source:
            continue
        value = source[key]
        if key == "reply_probability":
            payload[key] = round(_clamp(value, 0.0, 1.0), 4)
        elif key == "max_replies_per_minute":
            payload[key] = max(0, int(value))
        elif key == "max_length":
            payload[key] = max(1, int(value))
        elif key == "min_interval_seconds":
            payload[key] = max(0.0, float(value))
        elif key == "wait_seconds":
            pair = list(value or ())
            if len(pair) == 2:
                low, high = float(pair[0]), float(pair[1])
                payload[key] = [min(low, high), max(low, high)]
        elif key in {"use_slang", "use_emoji", "mention_reply"}:
            payload[key] = bool(value)
        elif key == "tone":
            payload[key] = str(value or "")
        elif key == "risk":
            payload[key] = str(value or "").lower()
        else:
            payload[key] = value
    return payload


def normalize_triggers(triggers: Mapping[str, Any] | None) -> dict[str, Any]:
    """规整触发器字段：白名单过滤 + 数值/列表收敛。"""

    source = dict(triggers or {})
    payload: dict[str, Any] = {}
    for key in TRIGGER_KEYS:
        if key not in source:
            continue
        value = source[key]
        if key in {"phase", "keyword_any", "keyword_all"}:
            payload[key] = [str(item) for item in (value or ())]
        else:
            payload[key] = round(float(value), 4)
    return payload


def normalize_preset(preset: Mapping[str, Any], *, now: float | None = None) -> dict[str, Any]:
    """把任意来源的预设补全成规范结构（幂等）。"""

    stamp = float(now if now is not None else time.time())
    scenario = str(preset.get("scenario") or "default")
    if scenario not in SCENARIOS:
        scenario = "default"
    return {
        "preset_id": str(preset.get("preset_id") or preset.get("id") or new_preset_id(scenario)),
        "name": str(preset.get("name") or scenario),
        "version": max(1, int(preset.get("version", 1) or 1)),
        "scenario": scenario,
        "triggers": normalize_triggers(preset.get("triggers")),
        "actions": normalize_actions(preset.get("actions")),
        "notes": str(preset.get("notes") or ""),
        "source": str(preset.get("source") or "manual"),
        "created_at": float(preset.get("created_at", stamp) or stamp),
        "updated_at": float(preset.get("updated_at", stamp) or stamp),
        "history": [dict(item) for item in (preset.get("history") or ()) if isinstance(item, Mapping)],
        "active": bool(preset.get("active", True)),
    }


def builtin_presets(now: float | None = None) -> list[dict[str, Any]]:
    """内置预设（设计点名的四类场景 + 冲突/冷场兜底），离线可用。"""

    raw: list[dict[str, Any]] = [
        {
            "preset_id": "calm_discussion",
            "name": "平静讨论",
            "scenario": "calm",
            "triggers": {"heat_max": 0.45, "topic_max": 3, "flood_max": 0.2, "interrupt_min": 0.0},
            "actions": {
                "reply_probability": 0.35,
                "max_replies_per_minute": 2,
                "max_length": 80,
                "wait_seconds": [3, 12],
                "tone": "平和",
                "use_slang": False,
                "mention_reply": True,
            },
            "notes": "热度不高且有明确话题时，正常参与讨论。",
            "source": "builtin",
        },
        {
            "preset_id": "group_exposition",
            "name": "群体阐述",
            "scenario": "exposition",
            "triggers": {"heat_min": 0.3, "topic_max": 4, "flood_max": 0.35, "interrupt_max": 1.0},
            "actions": {
                "reply_probability": 0.2,
                "max_replies_per_minute": 1,
                "max_length": 60,
                "wait_seconds": [5, 20],
                "tone": "克制",
                "use_slang": False,
                "mention_reply": True,
            },
            "notes": "多人在讲话时少插话，只在被点名或话题直接相关时接。",
            "source": "builtin",
        },
        {
            "preset_id": "smalltalk",
            "name": "闲聊",
            "scenario": "smalltalk",
            "triggers": {"heat_max": 0.6, "topic_max": 2, "flood_max": 0.25},
            "actions": {
                "reply_probability": 0.5,
                "max_replies_per_minute": 4,
                "max_length": 60,
                "wait_seconds": [1, 6],
                "tone": "热络",
                "use_slang": True,
                "use_emoji": True,
                "mention_reply": False,
            },
            "notes": "轻松闲聊，可以用梗与表情，短句为主。",
            "source": "builtin",
        },
        {
            "preset_id": "flood_silence",
            "name": "刷屏退避",
            "scenario": "flooding",
            "triggers": {"flood_min": 0.5, "heat_min": 0.6},
            "actions": {
                "reply_probability": 0.05,
                "max_replies_per_minute": 1,
                "max_length": 40,
                "wait_seconds": [15, 60],
                "tone": "克制",
                "use_slang": False,
                "mention_reply": True,
                "risk": "low",
            },
            "notes": "刷屏时不抢话，避免成为噪声的一部分。",
            "source": "builtin",
        },
        {
            "preset_id": "conflict_deescalate",
            "name": "冲突降温",
            "scenario": "conflict",
            "triggers": {"keyword_any": ["吵", "别骂", "生气", "垃圾", "滚"], "heat_min": 0.3},
            "actions": {
                "reply_probability": 0.25,
                "max_replies_per_minute": 1,
                "max_length": 50,
                "wait_seconds": [8, 30],
                "tone": "中性",
                "use_slang": False,
                "stance": "neutral",
                "risk": "medium",
            },
            "notes": "有人起冲突时降温，不站队、不复读对方措辞。",
            "source": "builtin",
        },
        {
            "preset_id": "silence_break",
            "name": "冷场破冰",
            "scenario": "silence",
            "triggers": {"heat_max": 0.15, "topic_max": 1, "interrupt_max": 0.5},
            "actions": {
                "reply_probability": 0.4,
                "max_replies_per_minute": 1,
                "max_length": 40,
                "wait_seconds": [10, 40],
                "tone": "轻松",
                "use_slang": True,
                "use_emoji": True,
                "mention_reply": False,
            },
            "notes": "长时间冷场时给一个轻话题，但不追问。",
            "source": "builtin",
        },
    ]
    return [normalize_preset(item, now=now) for item in raw]


class PresetRegistry:
    """预设注册表：加载 / 注册 / 版本历史。"""

    def __init__(
        self,
        ctx: Any = None,
        *,
        directory: str | os.PathLike[str] | None = None,
        writer: Callable[[str, Mapping[str, Any]], Any] | None = None,
        reader: Callable[[str], Any] | None = None,
        presets: Sequence[Mapping[str, Any]] | None = None,
        use_builtin: bool = True,
        max_history: int = 10,
    ) -> None:
        self.ctx = ctx
        self.directory = Path(directory) if directory is not None else default_presets_dir()
        self.writer = writer
        self.reader = reader
        self.use_builtin = bool(use_builtin)
        self.max_history = max(0, int(max_history))
        self._memory: dict[str, dict[str, Any]] = {}
        self.loads = 0
        self.registers = 0
        self.failures: list[dict[str, Any]] = []
        for item in presets or ():
            preset = normalize_preset(item, now=self._now())
            self._memory[preset["preset_id"]] = preset

    # ---- 工具 ----------------------------------------------------------
    def _now(self) -> float:
        clock = getattr(self.ctx, "now", None)
        return float(clock()) if callable(clock) else time.time()

    def path_of(self, preset_id: str) -> Path:
        return self.directory / f"{preset_id}.json"

    def _log(self, level: str, event: str, **fields: Any) -> None:
        log = getattr(self.ctx, "log", None)
        if callable(log):
            log(level, event, **fields)

    # ---- 读 ------------------------------------------------------------
    def load_local(self) -> list[dict[str, Any]]:
        """读磁盘目录里的预设（坏文件记进 `failures`，不中断加载）。"""

        if self.reader is not None:
            payload = self.reader(str(self.directory))
            items = payload.get("presets") if isinstance(payload, Mapping) else payload
            return [normalize_preset(item, now=self._now()) for item in (items or ())]
        if not self.directory.exists():
            return []
        found: list[dict[str, Any]] = []
        for path in sorted(self.directory.glob("*.json")):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except Exception as error:  # noqa: BLE001 - 坏文件不该让整域挂掉
                self.failures.append({"path": str(path), "error": str(error)})
                self._log("warning", "presets.load_failed", path=str(path), error=str(error))
                continue
            if isinstance(data, Mapping):
                found.append(normalize_preset(data, now=self._now()))
        return found

    def all(self) -> list[dict[str, Any]]:
        """当前已知的全部预设（内存覆盖磁盘，磁盘覆盖内置）。"""

        merged: dict[str, dict[str, Any]] = {}
        if self.use_builtin:
            for preset in builtin_presets(now=self._now()):
                merged[preset["preset_id"]] = preset
        for preset in self.load_local():
            merged[preset["preset_id"]] = preset
        merged.update(self._memory)
        return [merged[key] for key in sorted(merged)]

    def get(self, preset_id: str) -> dict[str, Any] | None:
        return next((item for item in self.all() if item["preset_id"] == preset_id), None)

    async def load(
        self,
        *,
        preset_id: str = "",
        scenario: str = "",
        presets: Sequence[Mapping[str, Any]] | None = None,
        active_only: bool = False,
        include_builtin: bool | None = None,
    ) -> dict[str, Any]:
        """`rpc:presets.load` —— 加载预设（可带过滤条件）。"""

        self.loads += 1
        previous_builtin = self.use_builtin
        if include_builtin is not None:
            self.use_builtin = bool(include_builtin)
        try:
            items = self.all()
        finally:
            self.use_builtin = previous_builtin
        if presets is not None:
            by_id = {item["preset_id"]: item for item in items}
            for item in presets:
                normalized = normalize_preset(item, now=self._now())
                by_id[normalized["preset_id"]] = normalized
            items = [by_id[key] for key in sorted(by_id)]
        if preset_id:
            items = [item for item in items if item["preset_id"] == preset_id]
        if scenario:
            wanted = {part.strip() for part in str(scenario).split(",") if part.strip()}
            items = [item for item in items if item["scenario"] in wanted]
        if active_only:
            items = [item for item in items if item.get("active", True)]
        self._log("debug", "presets.loaded", count=len(items), scenario=scenario)
        return {
            "presets": items,
            "count": len(items),
            "scenarios": sorted({item["scenario"] for item in items}),
            "directory": str(self.directory),
            "failures": list(self.failures),
        }

    # ---- 写 ------------------------------------------------------------
    def _write_local(self, preset: Mapping[str, Any]) -> str:
        if self.writer is not None:
            self.writer(str(preset["preset_id"]), preset)
            return "writer"
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path_of(str(preset["preset_id"])).write_text(
            json.dumps(preset, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8"
        )
        return "file"

    async def register(
        self,
        preset: Mapping[str, Any] | None = None,
        *,
        persist: bool = True,
        now: float | None = None,
        **fields: Any,
    ) -> dict[str, Any]:
        """`rpc:presets.register` —— 注册/更新预设（版本自增 + 历史留档）。"""

        self.registers += 1
        stamp = float(now if now is not None else self._now())
        merged = dict(preset) if isinstance(preset, Mapping) else {}
        merged.update({key: value for key, value in fields.items() if value is not None})
        candidate = normalize_preset(merged, now=stamp)
        preset_id = str(candidate["preset_id"])
        previous = self._memory.get(preset_id) or self.get(preset_id)
        if previous is not None:
            candidate["version"] = int(previous.get("version", 1)) + 1
            candidate["created_at"] = float(previous.get("created_at", stamp) or stamp)
            history = list(candidate.get("history") or [])
            history.append({**previous, "history": [], "superseded_at": stamp, "superseded_by": candidate["version"]})
            candidate["history"] = history[-self.max_history :] if self.max_history else []
        candidate["updated_at"] = stamp
        self._memory[preset_id] = candidate
        storage = ""
        if persist:
            try:
                storage = self._write_local(candidate)
            except Exception as error:  # noqa: BLE001 - 磁盘不可写时保留内存版本
                self.failures.append({"preset_id": preset_id, "error": str(error)})
                self._log("warning", "presets.persist_failed", preset_id=preset_id, error=str(error))
                storage = "memory"
        self._log("info", "presets.registered", preset_id=preset_id, version=candidate["version"], storage=storage)
        return {
            "preset": candidate,
            "created": previous is None,
            "version": candidate["version"],
            "previous_version": int(previous.get("version", 0)) if previous else 0,
            "storage": storage,
            "persisted": bool(storage and storage != "memory"),
        }

    # ---- 诊断 ----------------------------------------------------------
    def status(self) -> dict[str, Any]:
        return {
            "module": MODULE,
            "loads": self.loads,
            "registers": self.registers,
            "cached": len(self._memory),
            "directory": str(self.directory),
            "use_builtin": self.use_builtin,
            "failures": len(self.failures),
        }


def make_handlers(registry: PresetRegistry) -> dict[str, Any]:
    """本叶子的 `{契约名: 处理器}`（装配层逐个注册）。"""

    return {
        "rpc:presets.load": registry.load,
        "rpc:presets.register": registry.register,
    }


def register(target: Any, instance: PresetRegistry | None = None) -> Any:
    """把本叶子的处理器注册进 `target`（`Registry` 或容器）。"""

    instance = instance or PresetRegistry()
    for name, handler in make_handlers(instance).items():
        target.register(name, handler, module=MODULE, replace=True)
    return target


__all__ = [
    "ACTION_KEYS",
    "DEFAULT_PRESETS_DIR",
    "ENV_PRESETS_DIR",
    "MODULE",
    "NAMES",
    "SCENARIOS",
    "TRIGGER_KEYS",
    "PresetRegistry",
    "builtin_presets",
    "default_presets_dir",
    "make_handlers",
    "new_preset_id",
    "normalize_actions",
    "normalize_preset",
    "normalize_triggers",
    "register",
]
