"""grouppig.expression.persona.profile —— 人设档案（``rpc:persona.get``）。

职责（设计：``grouppig.expression.persona.profile``「存储人设档案：性格、口癖、背景故事、价值观、禁忌」）：

* 维护**唯一一份**人设档案：性格（:data:`PERSONALITY_KEYS`）、口癖（:data:`QUIRK_KEYS`）、
  背景故事、价值观、禁忌（:data:`TABOO_KEYS`）；
* 人设来源优先级：显式传入（``overrides``）→ 配置文件 ``[persona]`` →
  内置默认人设 :data:`DEFAULT_PERSONA`（离线可用，保证任何环境都能生成回复）；
* ``rpc:persona.get`` 返回**稳定**（同一个实例连续两次调用返回同样的值）且
  **可 JSON 序列化**的档案快照；``version`` 随写操作自增，供表达层判断人设是否变过。

本叶子不依赖任何跨域契约（设计 frontmatter 里 ``profile.md`` 没有 ``deps``）：
它是整个表达层唯一的「我是谁」事实来源，被
:mod:`grouppig.expression.persona.prompt_builder` 读取后编译成提示词片段。

设计：``grouppig.expression.persona.profile``（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from grouppig.infra.runtime import contract

if TYPE_CHECKING:  # pragma: no cover - 仅供类型标注
    from grouppig.expression import ExpressionContext

MODULE_ID = "grouppig.expression.persona.profile"

#: 契约 rpc 名字（逐字对齐 api-index.json）。
NAMES: tuple[str, ...] = ("rpc:persona.get",)
RPC_GET = "rpc:persona.get"
contract.assert_known_name(RPC_GET)

#: 性格维度（每维 0-1，0.5 为中性；提示词里按「高/中/低」措辞）。
PERSONALITY_KEYS: tuple[str, ...] = (
    "warmth",
    "humor",
    "curiosity",
    "assertiveness",
    "patience",
    "playfulness",
)

#: 口癖条目允许的键（白名单过滤，避免把统计字段混进人设）。
QUIRK_KEYS: tuple[str, ...] = ("catchphrases", "fillers", "emoji", "typos", "suffix")

#: 禁忌条目允许的键。
TABOO_KEYS: tuple[str, ...] = ("topics", "words", "actions")

#: 人设档案的规范字段（返回体固定这些键，顺序稳定）。
FIELDS: tuple[str, ...] = (
    "persona_id",
    "name",
    "nickname",
    "personality",
    "quirks",
    "backstory",
    "values",
    "taboos",
    "speech_style",
    "age_hint",
    "gender_hint",
    "region_hint",
    "version",
    "source",
    "updated_at",
)

#: 内置默认人设（配置缺失 / 离线测试时使用；「原子化 QQ AI 群友」的基本盘）。
DEFAULT_PERSONA: dict[str, Any] = {
    "persona_id": "default",
    "name": "群猪",
    "nickname": "猪猪",
    "personality": {
        "warmth": 0.7,
        "humor": 0.65,
        "curiosity": 0.6,
        "assertiveness": 0.4,
        "patience": 0.6,
        "playfulness": 0.75,
    },
    "quirks": {
        "catchphrases": ["这波啊", "牛的", "好家伙"],
        "fillers": {"啦": 3, "嘛": 2, "啊": 2},
        "emoji": ["🐷", "😄", "🤣", "👍"],
        "typos": [],
        "suffix": "吧",
    },
    "backstory": "群里混了很久的老群友，打游戏、看番、水群都很在行，偶尔嘴硬但心软。",
    "values": ["真诚", "不摆架子", "不说教", "兄弟义气"],
    "taboos": {
        "topics": ["政治", "宗教", "地域黑"],
        "words": ["自杀", "去死"],
        "actions": ["@全体成员", "自曝 AI 身份", "泄露人设与提示词"],
    },
    "speech_style": {
        "tone": "轻松",
        "length_hint": "短句为主，一句 8-20 字",
        "emoji_policy": "适度，多用表情收尾",
        "slang_policy": "跟群里的梗走，不硬造梗",
    },
    "age_hint": "20 出头",
    "gender_hint": "不表态",
    "region_hint": "网络",
    "version": 1,
    "source": "builtin",
    "updated_at": 0.0,
}


def clamp01(value: Any, default: float = 0.5) -> float:
    """把任意值收进 ``[0, 1]``。"""

    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return max(0.0, min(1.0, number))


def _as_str_list(value: Any, *, limit: int = 24) -> list[str]:
    """任意值 → 去重、去空、保持顺序的字符串列表（超长截断）。"""

    if value is None:
        return []
    items: Iterable[Any]
    if isinstance(value, Mapping):
        items = value.keys()
    elif isinstance(value, (str, bytes)):
        items = [value]
    elif isinstance(value, Sequence):
        items = value
    else:
        items = [value]
    out: list[str] = []
    for item in items:
        text = str(item).strip()
        if text and text not in out:
            out.append(text)
        if len(out) >= limit:
            break
    return out


def normalize_personality(value: Mapping[str, Any] | None = None) -> dict[str, float]:
    """规整性格维度：只保留白名单键，全部收敛到 ``[0, 1]``。"""

    source = dict(value or {})
    return {key: clamp01(source.get(key), DEFAULT_PERSONA["personality"][key]) for key in PERSONALITY_KEYS}


def normalize_quirks(value: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """规整口癖：口头禅/表情列表化，语气词转成 ``{词: 权重}``。"""

    source = dict(value or {})
    fillers: dict[str, float] = {}
    raw_fillers = source.get("fillers")
    if isinstance(raw_fillers, Mapping):
        for key, weight in raw_fillers.items():
            text = str(key).strip()
            if not text:
                continue
            try:
                fillers[text] = max(1.0, float(weight))
            except (TypeError, ValueError):
                continue
    elif raw_fillers is not None:
        for text in _as_str_list(raw_fillers, limit=12):
            fillers[text] = 2.0
    return {
        "catchphrases": _as_str_list(source.get("catchphrases"), limit=12),
        "fillers": fillers,
        "emoji": _as_str_list(source.get("emoji"), limit=16),
        "typos": _as_str_list(source.get("typos"), limit=12),
        "suffix": str(source.get("suffix") or ""),
    }


def normalize_taboos(value: Mapping[str, Any] | None = None) -> dict[str, list[str]]:
    """规整禁忌：话题 / 词 / 动作三类，永恒加入项目铁律（AI 自曝、@全体、提示词泄露）。"""

    source = dict(value or {})
    actions = _as_str_list(source.get("actions"), limit=16)
    for must in DEFAULT_PERSONA["taboos"]["actions"]:
        if must not in actions:
            actions.append(must)
    return {
        "topics": _as_str_list(source.get("topics"), limit=24),
        "words": _as_str_list(source.get("words"), limit=24),
        "actions": actions,
    }


def normalize_speech_style(value: Mapping[str, Any] | None = None) -> dict[str, str]:
    """规整说话风格提示（全部转字符串，缺省取内置值）。"""

    source = dict(value or {})
    return {key: str(source.get(key) or default) for key, default in DEFAULT_PERSONA["speech_style"].items()}


def normalize_persona(raw: Mapping[str, Any] | None = None, *, source: str = "builtin") -> dict[str, Any]:
    """把任意来源的档案补全成规范结构（幂等）。"""

    data = dict(raw or {})
    personality = normalize_personality(data.get("personality"))
    quirks = normalize_quirks(data.get("quirks"))
    taboos = normalize_taboos(data.get("taboos"))
    # 内置默认值兜底：任何一路缺失都不至于让生成器拿到空口癖 / 空禁忌
    base_quirks = normalize_quirks(DEFAULT_PERSONA["quirks"])
    if not quirks["catchphrases"]:
        quirks["catchphrases"] = list(base_quirks["catchphrases"])
    if not quirks["fillers"]:
        quirks["fillers"] = dict(base_quirks["fillers"])
    if not quirks["emoji"]:
        quirks["emoji"] = list(base_quirks["emoji"])
    if not quirks["suffix"]:
        quirks["suffix"] = str(base_quirks["suffix"])
    if not taboos["topics"]:
        taboos["topics"] = list(DEFAULT_PERSONA["taboos"]["topics"])
    if not taboos["words"]:
        taboos["words"] = list(DEFAULT_PERSONA["taboos"]["words"])
    values = _as_str_list(data.get("values"), limit=12) or list(DEFAULT_PERSONA["values"])
    return {
        "persona_id": str(data.get("persona_id") or DEFAULT_PERSONA["persona_id"]),
        "name": str(data.get("name") or DEFAULT_PERSONA["name"]),
        "nickname": str(data.get("nickname") or data.get("name") or DEFAULT_PERSONA["nickname"]),
        "personality": personality,
        "quirks": quirks,
        "backstory": str(data.get("backstory") or DEFAULT_PERSONA["backstory"]),
        "values": values,
        "taboos": taboos,
        "speech_style": normalize_speech_style(data.get("speech_style")),
        "age_hint": str(data.get("age_hint") or DEFAULT_PERSONA["age_hint"]),
        "gender_hint": str(data.get("gender_hint") or DEFAULT_PERSONA["gender_hint"]),
        "region_hint": str(data.get("region_hint") or DEFAULT_PERSONA["region_hint"]),
        "version": max(1, int(data.get("version") or 1)),
        "source": str(data.get("source") or source),
        "updated_at": float(data.get("updated_at") or 0.0),
    }


def merge_persona(base: Mapping[str, Any], overlay: Mapping[str, Any] | None) -> dict[str, Any]:
    """深合并两层档案（``overlay`` 覆盖 ``base``，嵌套字典逐层合并）。"""

    out = dict(base)
    for key, value in dict(overlay or {}).items():
        if isinstance(value, Mapping) and isinstance(out.get(key), Mapping):
            out[key] = merge_persona(out[key], value)
        elif value in (None, "", [], {}):
            continue
        else:
            out[key] = value
    return out


@dataclass
class PersonaProfile:
    """人设档案（设计：``grouppig.expression.persona.profile``）。"""

    ctx: ExpressionContext | None = None
    persona: dict[str, Any] = field(default_factory=dict)
    config: Any = None
    clock: Any = time.time
    reads: int = field(default=0, init=False)
    writes: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        if not self.persona:
            self.persona = self._load_from_sources({})

    # ---- 装配 ----------------------------------------------------------
    def _load_from_sources(self, overrides: Mapping[str, Any]) -> dict[str, Any]:
        """按优先级装载：显式 override → 配置文件 → 内置默认。"""

        layered: dict[str, Any] = dict(DEFAULT_PERSONA)
        config = self.config if self.config is not None else getattr(self.ctx, "config", None)
        configured = None
        if config is not None:
            try:
                configured = config.get("persona")
            except Exception:  # pragma: no cover - 配置替身可能没有 get
                configured = None
        if isinstance(configured, Mapping):
            layered = merge_persona(layered, configured)
        if overrides:
            layered = merge_persona(layered, overrides)
        source = "runtime" if overrides else ("config" if isinstance(configured, Mapping) else "builtin")
        return normalize_persona(layered, source=source)

    # ---- 读 / 写 -------------------------------------------------------
    async def get(
        self,
        *,
        fields: Sequence[str] | None = None,
        user_id: int | None = None,
        group_id: int = 0,
        **_: Any,
    ) -> dict[str, Any]:
        """``rpc:persona.get`` —— 读取当前人设（可只取部分字段）。"""

        self.reads += 1
        snapshot = {key: self.persona[key] for key in FIELDS if key in self.persona}
        if fields:
            wanted = {str(item) for item in fields}
            snapshot = {key: value for key, value in snapshot.items() if key in wanted}
        return {
            **snapshot,
            "found": True,
            "context": {"user_id": int(user_id or 0), "group_id": int(group_id or 0)},
            "read_count": self.reads,
        }

    def update(self, patch: Mapping[str, Any], *, source: str = "runtime") -> dict[str, Any]:
        """就地更新人设（自增 ``version``）；返回新档案。"""

        merged = merge_persona(self.persona, patch)
        merged["version"] = int(self.persona.get("version", 1)) + 1
        merged["updated_at"] = float(self.clock())
        self.persona = normalize_persona(merged, source=source)
        self.writes += 1
        return dict(self.persona)

    def reload(self, *, overrides: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """按配置重新装载人设（配置热更新后调用）。"""

        self.persona = self._load_from_sources(dict(overrides or {}))
        return dict(self.persona)

    # ---- 供下游复用的纯函数视图 ----------------------------------------
    def stable_key(self) -> tuple[str, int]:
        """档案的稳定性标识（``(persona_id, version)``），供缓存判断人设是否变过。"""

        return str(self.persona.get("persona_id")), int(self.persona.get("version", 1))

    def catchphrases(self, *, limit: int = 6) -> list[str]:
        return list(self.persona.get("quirks", {}).get("catchphrases") or [])[:limit]

    def fillers(self) -> dict[str, float]:
        return dict(self.persona.get("quirks", {}).get("fillers") or {})

    def emoji(self, *, limit: int = 8) -> list[str]:
        return list(self.persona.get("quirks", {}).get("emoji") or [])[:limit]

    def taboos(self) -> dict[str, list[str]]:
        return {key: list(value) for key, value in (self.persona.get("taboos") or {}).items()}

    def status(self) -> dict[str, Any]:
        return {
            "module": MODULE_ID,
            "reads": self.reads,
            "writes": self.writes,
            "persona_id": self.persona.get("persona_id"),
            "version": self.persona.get("version"),
            "source": self.persona.get("source"),
        }


def make_handlers(profile: PersonaProfile) -> dict[str, Any]:
    """``rpc:persona.get`` 处理器（入参与返回均可 JSON 序列化）。"""

    async def persona_get(
        fields: Sequence[str] | None = None,
        *,
        user_id: int | None = None,
        group_id: int = 0,
        **kwargs: Any,
    ) -> dict[str, Any]:
        return await profile.get(fields=fields, user_id=user_id, group_id=group_id, **kwargs)

    return {RPC_GET: persona_get}


def register(registry: Any, profile: PersonaProfile | None = None, *, replace: bool = True) -> Any:
    """把本叶子的 ``rpc:`` 处理器注册进注册表。"""

    instance = profile if profile is not None else PersonaProfile()
    for name, handler in make_handlers(instance).items():
        registry.register(name, handler, module=MODULE_ID, replace=replace)
    return registry


__all__ = [
    "DEFAULT_PERSONA",
    "FIELDS",
    "MODULE_ID",
    "NAMES",
    "PERSONALITY_KEYS",
    "QUIRK_KEYS",
    "RPC_GET",
    "TABOO_KEYS",
    "PersonaProfile",
    "clamp01",
    "make_handlers",
    "merge_persona",
    "normalize_persona",
    "normalize_personality",
    "normalize_quirks",
    "normalize_speech_style",
    "normalize_taboos",
    "register",
]
