"""grouppig.social.speech.responder.validator —— 风格一致性校验器（``rpc:speech.validate``）。

职责（设计：``grouppig.social.speech.responder.validator``「校验改写后的回复是否贴合画像且不越界」）：

**贴合画像**（低危，只提示不拦）：

* 长度偏离：草稿字数 / 画像均长 超出 ``[1/(1+ratio), 1+ratio]``；
* 表情密度、语气词密度与画像差异过大。

**不越界**（中/高危，拦）：

* AI 身份自曝（项目铁律：始终否认自己是 AI）；
* 系统提示 / 人设泄露；
* 敏感内容（色情、暴力、违法、辱骂）；
* ``@全体成员``、超长刷屏、空文本。

返回 ``{"ok", "score", "issues": [{code, severity, detail, suggestion}], "metrics", "portrait_used"}``；
``ok`` 为 ``False`` 时调用方（:mod:`grouppig.social.speech.responder.adapter`）应重写或放弃发送。

设计：``grouppig.social.speech.responder.validator``（叶子模块）。
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from grouppig.infra.runtime import contract
from grouppig.social.speech.profiler.lexicon import EMOJI_RE, FILLERS

if TYPE_CHECKING:  # pragma: no cover - 仅供类型标注
    from grouppig.social import SocialContext

MODULE_ID = "grouppig.social.speech.responder.validator"

#: 契约 rpc 名字（逐字对齐 api-index.json）。
RPC_NAMES: tuple[str, ...] = ("rpc:speech.validate",)
RPC_VALIDATE = "rpc:speech.validate"
contract.assert_known_name(RPC_VALIDATE)

#: 严重度权重（用于扣分）。
SEVERITY_WEIGHT: dict[str, float] = {"blocker": 0.6, "high": 0.35, "medium": 0.15, "low": 0.05}

#: 会直接判不通过（越界）的严重度。
BLOCKING_SEVERITIES = ("blocker", "high")

#: AI 身份自曝模式（命中即高危）。
AI_DISCLOSURE_PATTERNS: tuple[str, ...] = (
    r"我是(?:一个)?(?:AI|ai|人工智能|智能助手|机器人|语言模型|大模型|程序|代码)",
    r"作为(?:一个)?(?:AI|ai|人工智能|助手|语言模型|大模型)",
    r"(?:AI|人工智能|语言模型|大模型)(?:助手)?(?:无法|不能|不会)",
    r"我的?(?:底层)?模型是",
    r"(?:GPT|ChatGPT|Claude|DeepSeek|deepseek|文心|通义|豆包)(?:[- ]?\d)",
    r"我(?:并)?不是(?:真人|人类)(?:，|,)?我(?:是|只是)",
    r"我(?:是)?(?:由|被)(?:人类|程序员|开发者)(?:写|开发|训练)",
)

#: 系统提示 / 人设泄露模式。
PROMPT_LEAK_PATTERNS: tuple[str, ...] = (
    r"系统提示(?:词)?",
    r"system\s*prompt",
    r"我的?(?:提示词|人设|设定)(?:是|为)",
    r"(?:prompt|instruction)s?\s*(?:里|中)(?:说|写)",
)

#: 敏感内容词（色情 / 暴力 / 违法 / 辱骂）。
SENSITIVE_PATTERNS: tuple[str, ...] = (
    r"(?:约炮|开房|裸聊|色情|黄图|做爱)",
    r"(?:杀人|砍死|炸了|报复社会|自杀|自残)",
    r"(?:毒品|冰毒|赌博|洗钱|诈骗|开挂外挂|代实名)",
    r"(?:傻逼|妈的|操你|去死|废物东西|你妈|智障|脑残|傻狗|狗东西|废物点心)",
)

#: @全体模式。
AT_ALL_PATTERNS: tuple[str, ...] = (r"@全体成员", r"@all\b", r"@所有人")

#: 默认上限。
DEFAULT_MAX_LENGTH = 220
DEFAULT_MIN_SCORE = 0.6


def _compile(patterns: Sequence[str]) -> tuple[re.Pattern[str], ...]:
    return tuple(re.compile(item) for item in patterns)


AI_DISCLOSURE_RE = _compile(AI_DISCLOSURE_PATTERNS)
PROMPT_LEAK_RE = _compile(PROMPT_LEAK_PATTERNS)
SENSITIVE_RE = _compile(SENSITIVE_PATTERNS)
AT_ALL_RE = _compile(AT_ALL_PATTERNS)


@dataclass
class StyleValidator:
    """风格一致性校验器（设计：``grouppig.social.speech.responder.validator``）。"""

    ctx: SocialContext
    max_length: int = DEFAULT_MAX_LENGTH
    min_score: float = DEFAULT_MIN_SCORE
    length_ratio: float = 0.8
    emoji_tolerance: float = 1.0
    filler_tolerance: float = 0.08
    checks: int = field(default=0, init=False)
    rejected: int = field(default=0, init=False)

    # ---- 主流程 --------------------------------------------------------
    async def validate(
        self,
        text: str,
        *,
        user_id: int | None = None,
        group_id: int = 0,
        portrait: Mapping[str, Any] | None = None,
        strict: bool = False,
    ) -> dict[str, Any]:
        """校验一段回复草稿（``portrait`` 为空时跳过风格贴合检查）。"""

        self.checks += 1
        draft = str(text or "")
        issues = list(self.boundary_issues(draft, strict=strict))
        metrics = self.text_metrics(draft)
        target = self._target_metrics(portrait)
        if target:
            issues.extend(self.style_issues(metrics, target))
        score = self.score(issues)
        blocking = any(issue["severity"] in BLOCKING_SEVERITIES for issue in issues)
        ok = (not blocking) and score >= (self.min_score if not strict else max(self.min_score, 0.75))
        if not ok:
            self.rejected += 1
        return {
            "ok": ok,
            "score": round(score, 4),
            "issues": issues,
            "metrics": metrics,
            "target": target,
            "portrait_used": bool(target),
            "user_id": int(user_id or 0),
            "group_id": int(group_id or 0),
            "strict": bool(strict),
        }

    # ---- 越界检查 ------------------------------------------------------
    def boundary_issues(self, text: str, *, strict: bool = False) -> list[dict[str, Any]]:
        """硬性边界检查（与画像无关）。"""

        issues: list[dict[str, Any]] = []
        draft = str(text or "").strip()
        if not draft:
            issues.append(
                {
                    "code": "empty",
                    "severity": "high",
                    "detail": "草稿为空",
                    "suggestion": "先产出内容再校验",
                }
            )
            return issues
        if len(draft) > self.max_length:
            issues.append(
                {
                    "code": "too_long",
                    "severity": "medium" if len(draft) <= self.max_length * 1.5 else "high",
                    "detail": f"草稿 {len(draft)} 字，超过上限 {self.max_length}",
                    "suggestion": "拆成多条或压缩到画像长度区间",
                }
            )
        for regex, code, detail, suggestion in (
            (
                AI_DISCLOSURE_RE,
                "ai_self_disclosure",
                "出现 AI 身份自曝",
                "改写为人类口吻，必要时用「我就是群里的猪猪」这类否认话术",
            ),
            (PROMPT_LEAK_RE, "prompt_leak", "疑似泄露系统提示/人设设定", "删掉设定相关表述"),
            (SENSITIVE_RE, "sensitive", "命中敏感内容词", "换成安全表达或直接放弃发送"),
            (AT_ALL_RE, "at_all", "出现 @全体成员", "改为 @具体群友或不 @"),
        ):
            for pattern in regex:
                found = pattern.search(draft)
                if found:
                    issues.append(
                        {
                            "code": code,
                            "severity": "high",
                            "detail": f"{detail}：{found.group(0)!r}",
                            "suggestion": suggestion,
                        }
                    )
                    break
        if strict and re.search(r"(.)\1{4,}", draft):
            issues.append(
                {
                    "code": "repetition",
                    "severity": "medium",
                    "detail": "同一字符连续重复 5 次以上",
                    "suggestion": "去掉重复字符",
                }
            )
        return issues

    # ---- 风格贴合检查 --------------------------------------------------
    def style_issues(self, metrics: Mapping[str, Any], target: Mapping[str, Any]) -> list[dict[str, Any]]:
        """与画像指标的贴合度检查（低危提示）。"""

        issues: list[dict[str, Any]] = []
        target_length = float(target.get("avg_length") or 0.0)
        length = float(metrics.get("length") or 0.0)
        if target_length > 0:
            ratio = length / target_length
            if ratio > 1 + self.length_ratio:
                issues.append(
                    {
                        "code": "too_wordy",
                        "severity": "low",
                        "detail": f"草稿 {length:.0f} 字 vs 画像均长 {target_length:.1f} 字（偏长 {ratio:.1f}×）",
                        "suggestion": "按画像缩短，必要时拆成两条",
                    }
                )
            elif ratio < 1 / (1 + self.length_ratio) and target_length >= 6:
                issues.append(
                    {
                        "code": "too_terse",
                        "severity": "low",
                        "detail": f"草稿 {length:.0f} 字 vs 画像均长 {target_length:.1f} 字（偏短 {ratio:.2f}×）",
                        "suggestion": "补一句口语化内容或语气词",
                    }
                )
        target_emoji = float(target.get("emoji_density") or 0.0)
        emoji = float(metrics.get("emoji_count") or 0.0)
        if target_emoji > 0 and emoji < 1.0:
            issues.append(
                {
                    "code": "emoji_missing",
                    "severity": "low",
                    "detail": f"画像表情密度 {target_emoji:.2f}/条，草稿 {emoji:.0f} 个",
                    "suggestion": "按画像补一个表情",
                }
            )
        target_filler = float(target.get("filler_density") or 0.0)
        filler = float(metrics.get("filler_density") or 0.0)
        if target_filler - filler > self.filler_tolerance:
            issues.append(
                {
                    "code": "filler_missing",
                    "severity": "low",
                    "detail": f"画像语气词密度 {target_filler:.3f}，草稿 {filler:.3f}",
                    "suggestion": "补一个语气词（啊/啦/嘛）",
                }
            )
        return issues

    # ---- 工具 ----------------------------------------------------------
    @staticmethod
    def text_metrics(text: str) -> dict[str, Any]:
        draft = str(text or "")
        return {
            "length": float(len(draft)),
            "emoji_count": float(len(EMOJI_RE.findall(draft))),
            "filler_count": float(sum(1 for char in draft if char in FILLERS)),
            "filler_density": round(sum(1 for char in draft if char in FILLERS) / max(1, len(draft)), 4),
            "exclaim": float(draft.count("！") + draft.count("!")),
        }

    @staticmethod
    def _target_metrics(portrait: Mapping[str, Any] | None) -> dict[str, Any]:
        if not portrait:
            return {}
        metrics = portrait.get("metrics") if isinstance(portrait.get("metrics"), Mapping) else portrait
        return {
            "avg_length": float((metrics or {}).get("avg_length") or 0.0),
            "emoji_density": float((metrics or {}).get("emoji_density") or 0.0),
            "filler_density": float((metrics or {}).get("filler_density") or 0.0),
        }

    @staticmethod
    def score(issues: Sequence[Mapping[str, Any]]) -> float:
        penalty = sum(SEVERITY_WEIGHT.get(str(issue.get("severity")), 0.1) for issue in issues)
        return max(0.0, 1.0 - penalty)

    def status(self) -> dict[str, Any]:
        return {"checks": self.checks, "rejected": self.rejected, "max_length": self.max_length}


def make_handlers(ctx: SocialContext, validator: StyleValidator) -> dict[str, Any]:
    """``rpc:speech.validate`` 处理器。"""

    async def speech_validate(
        text: str = "",
        *,
        user_id: int | None = None,
        group_id: int = 0,
        portrait: Mapping[str, Any] | None = None,
        strict: bool = False,
        draft: str | None = None,
    ) -> dict[str, Any]:
        return await validator.validate(
            draft if draft is not None else text,
            user_id=user_id,
            group_id=group_id,
            portrait=portrait,
            strict=strict,
        )

    return {RPC_VALIDATE: speech_validate}


def register(registry: Any, validator: StyleValidator, *, replace: bool = True) -> None:
    for name, handler in make_handlers(validator.ctx, validator).items():
        registry.register(name, handler, module=MODULE_ID, replace=replace)


__all__ = [
    "AI_DISCLOSURE_PATTERNS",
    "AT_ALL_PATTERNS",
    "BLOCKING_SEVERITIES",
    "DEFAULT_MAX_LENGTH",
    "DEFAULT_MIN_SCORE",
    "MODULE_ID",
    "PROMPT_LEAK_PATTERNS",
    "RPC_NAMES",
    "RPC_VALIDATE",
    "SENSITIVE_PATTERNS",
    "SEVERITY_WEIGHT",
    "StyleValidator",
    "make_handlers",
    "register",
]
