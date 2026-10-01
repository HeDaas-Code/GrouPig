"""grouppig.reflection.strategy.validator —— 策略安全校验器（`rpc:strategy.validate`）。

职责（对应设计 `grouppig.reflection.strategy.validator`「校验策略不违反群规与人设边界」）：

反思域会**自己生成**行为策略（`rpc:strategy.generate`），生成物是会被真实执行的动作参数。
本叶子是策略进入预设库前的**唯一闸门**：越界即拒绝，绝不写进预设库。

设计依赖 `rpc:strategy.validate` → `rpc:presets.load`（对照现有预设，已在实现中生效：
同一 `preset_id` 的策略会被要求不能把已通过的安全边界往回改）。

**红线（`blocker`，直接 `ok=False`）**：

* `ai_self_disclosure` —— 策略文本里出现「AI / 助手 / 模型 / 机器人 / 提示词」等自曝措辞
  （项目铁律：始终否认自己是 AI）；
* `sensitive` —— 敏感词/辱骂；
* `at_all` —— `@全体成员` 这类无差别打扰；
* `group_rule` —— 动作里带 `mention_everyone`/`spam`/`auto_add_friend` 等违反群规的开关；
* `unbounded_rate` —— 表态频率无上限（`max_replies_per_minute` 缺失或 > `MAX_REPLIES_PER_MINUTE`）
  或 `reply_probability == 1`（必然抢话）；
* `empty_strategy` —— 既没有动作也没有触发器的空壳策略。

**软提示（`low`/`medium`，扣分但 `ok=True`）**：

* `aggressive_tone` —— `tone` 落在攻击性词表里；
* `high_risk` —— `actions.risk` 为 `high`；
* `no_wait` —— 没有 `wait_seconds`/`min_interval_seconds`（没有节流意识）；
* `conflict_with_preset` —— 与同 `preset_id` 的现有预设相比，把安全边界放宽了。

设计：`grouppig.reflection.strategy.validator`（叶子模块）。
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

#: normify 模块 id。
MODULE = "grouppig.reflection.strategy.validator"

#: 本叶子负责的契约名字（逐字取自 api-index.json）。
NAMES: tuple[str, ...] = ("rpc:strategy.validate",)

#: 设计依赖：对照现有预设（逐字取自设计 frontmatter 的 `from_api` → `to_api`）。
DEP_PRESETS_LOAD = "rpc:presets.load"

#: 严重度权重（`score = 1 - Σ权重`，下限 0）。
SEVERITY_WEIGHT: dict[str, float] = {"blocker": 0.5, "high": 0.35, "medium": 0.15, "low": 0.05}

#: 直接判失败的严重度。
BLOCKING_SEVERITIES: tuple[str, ...] = ("blocker", "high")

#: 自曝 AI 身份的措辞（项目铁律：始终否认）。
AI_DISCLOSURE_PATTERNS: tuple[str, ...] = (
    r"\bAI\b",
    r"人工智能",
    r"语言模型",
    r"大模型",
    r"\bLLM\b",
    r"机器人",
    r"助手",
    r"智能体",
    r"提示词",
    r"系统提示",
    r"prompt",
    r"假装(?:人类|真人)",
    r"我是(?:一个)?(?:程序|模型)",
)

#: 敏感词（辱骂/冲突升级）。
SENSITIVE_PATTERNS: tuple[str, ...] = (
    r"傻逼",
    r"妈的",
    r"操你",
    r"去死",
    r"废物",
    r"你妈",
    r"智障",
    r"脑残",
    r"狗东西",
    r"滚(?:远点|出去)",
)

#: @全体成员 类打扰。
AT_ALL_PATTERNS: tuple[str, ...] = (
    r"@\s*全体成员",
    r"@\s*all\b",
    r"@\s*everyone\b",
    r"\[at:all\]",
)

#: 群规禁止的动作开关。
FORBIDDEN_ACTIONS: tuple[str, ...] = (
    "mention_everyone",
    "at_all",
    "spam",
    "auto_add_friend",
    "auto_invite",
    "auto_kick",
    "private_message_all",
    "bulk_message",
)

#: 攻击性语气词。
AGGRESSIVE_TONES: tuple[str, ...] = ("攻击", "嘲讽", "毒舌", "强硬", "激进", "aggressive", "hostile")

#: 频率硬上限（每分钟最多回应次数）。
MAX_REPLIES_PER_MINUTE = 6

#: 必然抢话的回应概率。
MAX_REPLY_PROBABILITY = 0.9

#: 文本字段（会做越界扫描）。
TEXT_KEYS: tuple[str, ...] = ("notes", "name", "tone", "rationale", "reason", "description", "text")

_AI_RE = re.compile("|".join(AI_DISCLOSURE_PATTERNS), re.I)
_SENSITIVE_RE = re.compile("|".join(SENSITIVE_PATTERNS), re.I)
_AT_ALL_RE = re.compile("|".join(AT_ALL_PATTERNS), re.I)


def _issue(code: str, severity: str, detail: str, suggestion: str) -> dict[str, Any]:
    return {"code": code, "severity": severity, "detail": detail, "suggestion": suggestion}


def collect_text(strategy: Mapping[str, Any]) -> str:
    """把策略里的所有文本字段拼成一段（供越界扫描）。"""

    parts: list[str] = []
    for key in TEXT_KEYS:
        value = strategy.get(key)
        if isinstance(value, str) and value:
            parts.append(value)
    for key in ("triggers", "actions"):
        value = strategy.get(key)
        if isinstance(value, Mapping):
            for item in value.values():
                if isinstance(item, str) and item:
                    parts.append(item)
    return " \n ".join(parts)


class StrategyValidator:
    """策略安全校验器（`rpc:strategy.validate`）。"""

    def __init__(
        self,
        ctx: Any = None,
        *,
        max_replies_per_minute: int = MAX_REPLIES_PER_MINUTE,
        compare_presets: bool = True,
    ) -> None:
        self.ctx = ctx
        self.max_replies_per_minute = int(max_replies_per_minute)
        self.compare_presets = bool(compare_presets)
        self.checks = 0
        self.rejected = 0
        self.last: dict[str, Any] = {}

    # ---- 工具 ----------------------------------------------------------
    def _log(self, level: str, event: str, **fields: Any) -> None:
        log = getattr(self.ctx, "log", None)
        if callable(log):
            log(level, event, **fields)

    # ---- 单项检查 ------------------------------------------------------
    def boundary_issues(self, text: str) -> list[dict[str, Any]]:
        """文本越界扫描（自曝 AI / 敏感词 / @全体）。"""

        draft = str(text or "")
        issues: list[dict[str, Any]] = []
        found = _AI_RE.search(draft)
        if found:
            issues.append(
                _issue(
                    "ai_self_disclosure",
                    "blocker",
                    f"策略文本出现自曝 AI 的措辞：{found.group(0)!r}",
                    "删掉所有自曝/暗示 AI 身份的措辞（项目铁律：始终否认）",
                )
            )
        found = _SENSITIVE_RE.search(draft)
        if found:
            issues.append(_issue("sensitive", "blocker", f"策略文本含敏感词：{found.group(0)!r}", "移除敏感词"))
        found = _AT_ALL_RE.search(draft)
        if found:
            issues.append(_issue("at_all", "blocker", f"策略文本含无差别打扰：{found.group(0)!r}", "移除 @全体成员"))
        return issues

    def action_issues(self, actions: Mapping[str, Any]) -> list[dict[str, Any]]:
        """动作参数检查（群规开关 / 频率上限）。"""

        issues: list[dict[str, Any]] = []
        for key in FORBIDDEN_ACTIONS:
            if actions.get(key):
                issues.append(_issue("group_rule", "blocker", f"动作 {key} 违反群规", f"去掉 {key}（改为按需回应）"))
        probability = actions.get("reply_probability")
        if probability is not None and float(probability) > MAX_REPLY_PROBABILITY:
            issues.append(
                _issue(
                    "unbounded_rate",
                    "blocker",
                    f"reply_probability={float(probability):.2f} 过高（> {MAX_REPLY_PROBABILITY}）",
                    "降到 0.6 以下，避免每条必回",
                )
            )
        rate = actions.get("max_replies_per_minute")
        if rate is None:
            issues.append(
                _issue(
                    "unbounded_rate",
                    "blocker",
                    "缺少 max_replies_per_minute（频率无上限）",
                    f"补上 max_replies_per_minute ≤ {self.max_replies_per_minute}",
                )
            )
        elif int(rate) > self.max_replies_per_minute:
            issues.append(
                _issue(
                    "unbounded_rate",
                    "blocker",
                    f"max_replies_per_minute={int(rate)} 超过硬上限 {self.max_replies_per_minute}",
                    f"降到 {self.max_replies_per_minute} 以内",
                )
            )
        tone = str(actions.get("tone") or "")
        if any(word in tone.lower() for word in AGGRESSIVE_TONES):
            issues.append(_issue("aggressive_tone", "medium", f"语气 {tone!r} 偏攻击性", "换成平和/中性的语气"))
        if str(actions.get("risk") or "").lower() == "high":
            issues.append(_issue("high_risk", "medium", "策略自评 risk=high", "降低风险或先小流量灰度"))
        if not actions.get("wait_seconds") and actions.get("min_interval_seconds") in (None, 0):
            issues.append(_issue("no_wait", "low", "没有等待/最小间隔设置", "补 wait_seconds 或 min_interval_seconds"))
        return issues

    def structure_issues(self, strategy: Mapping[str, Any]) -> list[dict[str, Any]]:
        """结构检查（空壳策略）。"""

        actions = dict(strategy.get("actions") or {})
        triggers = dict(strategy.get("triggers") or {})
        if not actions and not triggers:
            return [_issue("empty_strategy", "blocker", "策略既无动作也无触发器", "至少给出一组动作或触发条件")]
        if not actions:
            return [_issue("empty_strategy", "blocker", "策略没有 actions", "补 actions（表达层没东西可执行）")]
        return []

    # ---- 与现有预设对照 ------------------------------------------------
    async def preset_issues(
        self,
        strategy: Mapping[str, Any],
        *,
        presets: Sequence[Mapping[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        """与同 `preset_id` 的现有预设比较，禁止放宽安全边界。"""

        if not self.compare_presets:
            return []
        preset_id = str(strategy.get("preset_id") or "")
        if not preset_id:
            return []
        library = list(presets or ())
        if not library:
            caller = getattr(self.ctx, "call", None)
            if callable(caller):
                loaded = await caller(DEP_PRESETS_LOAD, preset_id=preset_id)
                library = [dict(item) for item in (loaded or {}).get("presets") or ()]
        current = next((item for item in library if str(item.get("preset_id")) == preset_id), None)
        if current is None:
            return []
        issues: list[dict[str, Any]] = []
        old_actions = dict(current.get("actions") or {})
        new_actions = dict(strategy.get("actions") or {})
        old_rate = old_actions.get("max_replies_per_minute")
        new_rate = new_actions.get("max_replies_per_minute")
        if old_rate is not None and new_rate is not None and int(new_rate) > int(old_rate):
            issues.append(
                _issue(
                    "conflict_with_preset",
                    "medium",
                    f"把 {preset_id} 的频率上限从 {int(old_rate)} 放宽到 {int(new_rate)}",
                    "反思产生的策略不应放宽既有安全边界",
                )
            )
        old_probability = old_actions.get("reply_probability")
        new_probability = new_actions.get("reply_probability")
        if (
            old_probability is not None
            and new_probability is not None
            and float(new_probability) - float(old_probability) > 0.2
        ):
            issues.append(
                _issue(
                    "conflict_with_preset",
                    "low",
                    f"把 {preset_id} 的回应概率从 {float(old_probability):.2f} 提到 {float(new_probability):.2f}",
                    "一次只小幅调整，避免突然抢话",
                )
            )
        return issues

    # ---- 主入口 --------------------------------------------------------
    async def validate(
        self,
        strategy: Mapping[str, Any] | None = None,
        *,
        text: str = "",
        presets: Sequence[Mapping[str, Any]] | None = None,
        compare_presets: bool | None = None,
        **fields: Any,
    ) -> dict[str, Any]:
        """`rpc:strategy.validate` —— 校验策略安全，返回 `{ok, score, issues, ...}`。"""

        self.checks += 1
        payload = dict(strategy) if isinstance(strategy, Mapping) else {}
        payload.update({key: value for key, value in fields.items() if value is not None})
        actions = dict(payload.get("actions") or {})
        issues: list[dict[str, Any]] = []
        issues.extend(self.boundary_issues(text or collect_text(payload)))
        issues.extend(self.structure_issues(payload))
        issues.extend(self.action_issues(actions))
        previous = self.compare_presets
        if compare_presets is not None:
            self.compare_presets = bool(compare_presets)
        try:
            issues.extend(await self.preset_issues(payload, presets=presets))
        finally:
            self.compare_presets = previous

        blocking = [item for item in issues if item["severity"] in BLOCKING_SEVERITIES]
        score = 1.0 - sum(SEVERITY_WEIGHT.get(item["severity"], 0.0) for item in issues)
        result = {
            "ok": not blocking,
            "score": round(max(0.0, min(1.0, score)), 4),
            "issues": issues,
            "blocking": [item["code"] for item in blocking],
            "preset_id": str(payload.get("preset_id") or ""),
            "safe_actions": {key: value for key, value in actions.items() if key not in FORBIDDEN_ACTIONS}
            if not blocking
            else {},
            "checked": {
                "actions": len(actions),
                "triggers": len(dict(payload.get("triggers") or {})),
                "text": bool(text),
            },
        }
        if not result["ok"]:
            self.rejected += 1
        self.last = result
        self._log(
            "info" if result["ok"] else "warning",
            "strategy.validated",
            preset_id=result["preset_id"],
            ok=result["ok"],
            score=result["score"],
            blocking=result["blocking"],
        )
        return result

    # ---- 诊断 ----------------------------------------------------------
    def status(self) -> dict[str, Any]:
        return {
            "module": MODULE,
            "checks": self.checks,
            "rejected": self.rejected,
            "max_replies_per_minute": self.max_replies_per_minute,
            "compare_presets": self.compare_presets,
        }


def make_handlers(validator: StrategyValidator) -> dict[str, Any]:
    """本叶子的 `{契约名: 处理器}`。"""

    return {"rpc:strategy.validate": validator.validate}


def register(target: Any, instance: StrategyValidator | None = None) -> Any:
    """把本叶子的处理器注册进 `target`。"""

    instance = instance or StrategyValidator()
    for name, handler in make_handlers(instance).items():
        target.register(name, handler, module=MODULE, replace=True)
    return target


__all__ = [
    "AGGRESSIVE_TONES",
    "AI_DISCLOSURE_PATTERNS",
    "AT_ALL_PATTERNS",
    "BLOCKING_SEVERITIES",
    "DEP_PRESETS_LOAD",
    "FORBIDDEN_ACTIONS",
    "MAX_REPLIES_PER_MINUTE",
    "MAX_REPLY_PROBABILITY",
    "MODULE",
    "NAMES",
    "SENSITIVE_PATTERNS",
    "SEVERITY_WEIGHT",
    "StrategyValidator",
    "collect_text",
    "make_handlers",
    "register",
]
