"""grouppig.reflection.strategy.rollback —— 回滚管理器（`rpc:strategy.rollback`）。

职责（对应设计 `grouppig.reflection.strategy.rollback`「回滚效果变差的策略并恢复上一版本」）：

评估器给出「建议回滚」之后，由本叶子执行回滚：把预设恢复到**上一个版本**（从
`preset["history"]` 里取最近一版），并以**新版本号**重新注册（不删除任何历史，
可再次回滚、可审计）。设计依赖 `rpc:strategy.rollback` → `rpc:presets.register`。

**为什么恢复是「新版本」而不是「剪切历史」**：预设库是唯一事实来源，反思域可能反复试错。
只追加不删除，才能回答「第 3 版为什么被推翻」「回滚了哪一版」；这也是
`strategy.score` 里历史均分能反映「这版被回滚过」的前提（回滚时在预设上打
`rolled_back=True` + `rolled_back_at` + `rolled_back_reason`，matcher 会据此扣分）。

**回滚决策**（`should_rollback`）::

    score < threshold（默认 0.45）且 samples >= min_samples（默认 2）→ 建议回滚
    也可直接传 `force=True` 立即回滚（人工干预/验证用）

设计：`grouppig.reflection.strategy.rollback`（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

from grouppig.reflection.presets.registry import normalize_preset

#: normify 模块 id。
MODULE = "grouppig.reflection.strategy.rollback"

#: 本叶子负责的契约名字（逐字取自 api-index.json）。
NAMES: tuple[str, ...] = ("rpc:strategy.rollback",)

#: 设计依赖：写回旧版（逐字取自设计 frontmatter 的 `from_api` → `to_api`）。
DEP_PRESETS_REGISTER = "rpc:presets.register"

#: 回滚阈值（低于它且样本足够就建议回滚）。
DEFAULT_THRESHOLD = 0.45

#: 做出回滚判断所需的最少评估样本数。
DEFAULT_MIN_SAMPLES = 2

#: 无法从 history 取到上一版时的兜底：这些字段一定回到保守值。
SAFE_FALLBACK_ACTIONS: dict[str, Any] = {
    "reply_probability": 0.2,
    "max_replies_per_minute": 1,
    "wait_seconds": [8, 30],
    "tone": "克制",
}


def snapshot_of(preset: Mapping[str, Any]) -> dict[str, Any]:
    """把一个预设压成可回放的快照（去掉嵌套 history，避免无限递归）。"""

    return {
        key: value
        for key, value in dict(preset).items()
        if key not in {"history", "rolled_back_at", "rolled_back_reason", "rolled_back_from"}
    }


def previous_version(preset: Mapping[str, Any], *, version: int | None = None) -> dict[str, Any] | None:
    """取历史里的上一版（`version` 指定则取那一版，否则取最近一版）。"""

    history = [item for item in (preset.get("history") or ()) if isinstance(item, Mapping)]
    if not history:
        return None
    if version is None:
        return snapshot_of(history[-1])
    matched = next((item for item in reversed(history) if int(item.get("version", 0) or 0) == int(version)), None)
    return snapshot_of(matched) if matched is not None else None


def should_rollback(
    score: float | None,
    *,
    samples: int = 1,
    threshold: float = DEFAULT_THRESHOLD,
    min_samples: int = DEFAULT_MIN_SAMPLES,
) -> dict[str, Any]:
    """按分数与样本数判断是否建议回滚。"""

    if score is None:
        return {"rollback": False, "reason": "no_score"}
    if int(samples) < int(min_samples):
        return {
            "rollback": False,
            "reason": "insufficient_samples",
            "detail": f"样本 {int(samples)} < {int(min_samples)}",
        }
    if float(score) < float(threshold):
        return {
            "rollback": True,
            "reason": "underperforming",
            "detail": f"均分 {float(score):.3f} < 阈值 {float(threshold):.3f}",
        }
    return {"rollback": False, "reason": "healthy", "detail": f"均分 {float(score):.3f} 达标"}


class RollbackManager:
    """预设回滚（`rpc:strategy.rollback`）。"""

    def __init__(
        self,
        ctx: Any = None,
        *,
        threshold: float = DEFAULT_THRESHOLD,
        min_samples: int = DEFAULT_MIN_SAMPLES,
    ) -> None:
        self.ctx = ctx
        self.threshold = float(threshold)
        self.min_samples = max(1, int(min_samples))
        self.rollbacks = 0
        self.refusals = 0
        self.last: dict[str, Any] = {}

    # ---- 工具 ----------------------------------------------------------
    def _now(self) -> float:
        clock = getattr(self.ctx, "now", None)
        return float(clock()) if callable(clock) else time.time()

    def _log(self, level: str, event: str, **fields: Any) -> None:
        log = getattr(self.ctx, "log", None)
        if callable(log):
            log(level, event, **fields)

    async def _call(self, name: str, *args: Any, **kwargs: Any) -> Any:
        """跨域调用；下游没挂时返回 `None`（复盘绝不因为某个下游缺失而整体失败）。"""

        caller = getattr(self.ctx, "call", None)
        if not callable(caller):
            return None
        try:
            return await caller(name, *args, **kwargs)
        except Exception as error:  # noqa: BLE001 - 下游缺失/失败都退化为本地计算
            self._log("warning", "reflection.dependency_unavailable", name=name, error=str(error))
            return None

    # ---- 主入口 --------------------------------------------------------
    async def rollback(
        self,
        preset_id: str = "",
        *,
        preset: Mapping[str, Any] | None = None,
        version: int | None = None,
        score: float | None = None,
        samples: int = 1,
        reason: str = "",
        force: bool = False,
        persist: bool = True,
        presets: Sequence[Mapping[str, Any]] | None = None,
        now: float | None = None,
    ) -> dict[str, Any]:
        """`rpc:strategy.rollback` —— 回滚效果变差的策略并恢复上一版本。"""

        stamp = float(now if now is not None else self._now())
        current = dict(preset) if isinstance(preset, Mapping) else None
        pool = [dict(item) for item in (presets or ())]
        if current is None and preset_id:
            if not pool:
                loader = getattr(self.ctx, "call", None)
                if callable(loader):
                    loaded = await loader("rpc:presets.load", preset_id=preset_id)
                    pool = [dict(item) for item in (loaded or {}).get("presets") or ()]
            current = next((item for item in pool if str(item.get("preset_id")) == preset_id), None)
        if current is None:
            self.refusals += 1
            return {
                "rolled_back": False,
                "reason": "preset_not_found",
                "preset_id": str(preset_id),
                "now": stamp,
            }

        target_id = str(current.get("preset_id"))
        decision = (
            {"rollback": True, "reason": "forced"}
            if force
            else should_rollback(score, samples=samples, threshold=self.threshold, min_samples=self.min_samples)
        )
        if not decision["rollback"]:
            self.refusals += 1
            result = {
                "rolled_back": False,
                "reason": decision["reason"],
                "detail": decision.get("detail", ""),
                "preset_id": target_id,
                "score": score,
                "samples": int(samples),
                "now": stamp,
            }
            self.last = result
            return result

        restored = previous_version(current, version=version)
        restored_from = int(restored.get("version", 0) or 0) if restored else 0
        if restored is None:
            restored = snapshot_of(
                {**current, "actions": {**dict(current.get("actions") or {}), **SAFE_FALLBACK_ACTIONS}}
            )
            restored["actions"] = {**dict(restored.get("actions") or {}), **SAFE_FALLBACK_ACTIONS}
        restored["preset_id"] = target_id
        restored["source"] = "rollback"
        restored["rolled_back"] = True
        restored["rolled_back_at"] = stamp
        restored["rolled_back_from"] = int(current.get("version", 1) or 1)
        restored["rolled_back_reason"] = str(reason or decision.get("reason") or "underperforming")
        restored["history"] = [
            {**snapshot_of(item), "superseded_at": stamp, "superseded_by": "rollback"}
            if isinstance(item, Mapping)
            else item
            for item in (current.get("history") or ())
        ]
        candidate = normalize_preset(restored, now=stamp)
        candidate["version"] = int(current.get("version", 1) or 1) + 1

        registration = await self._call(DEP_PRESETS_REGISTER, candidate, persist=persist)
        if registration is None:
            self.refusals += 1
            result = {
                "rolled_back": False,
                "reason": "presets_unavailable",
                "preset_id": target_id,
                "candidate": candidate,
                "now": stamp,
            }
            self.last = result
            return result

        self.rollbacks += 1
        registered = registration.get("preset") or candidate
        result = {
            "rolled_back": True,
            "preset": registered,
            "preset_id": target_id,
            "version": int(registration.get("version", 0) or 0),
            "restored_from": restored_from,
            "restored_version": int(registered.get("version", 0) or 0),
            "rolled_back_from": int(current.get("version", 1) or 1),
            "reason": str(reason or decision.get("reason") or "underperforming"),
            "detail": decision.get("detail", ""),
            "score": score,
            "samples": int(samples),
            "storage": registration.get("storage", ""),
            "now": stamp,
        }
        self.last = result
        self._log(
            "info",
            "strategy.rolled_back",
            preset_id=target_id,
            version=result["version"],
            restored_from=restored_from,
            score=score,
        )
        return result

    async def maybe_rollback(
        self,
        preset_id: str,
        *,
        score: float | None,
        samples: int = 1,
        reason: str = "",
        persist: bool = True,
        now: float | None = None,
    ) -> dict[str, Any]:
        """评估器可直接调用的便捷入口（自动判断是否该回滚）。"""

        return await self.rollback(
            preset_id,
            score=score,
            samples=samples,
            reason=reason,
            persist=persist,
            now=now,
        )

    # ---- 诊断 ----------------------------------------------------------
    def status(self) -> dict[str, Any]:
        return {
            "module": MODULE,
            "rollbacks": self.rollbacks,
            "refusals": self.refusals,
            "threshold": self.threshold,
            "min_samples": self.min_samples,
        }


def make_handlers(manager: RollbackManager) -> dict[str, Any]:
    """本叶子的 `{契约名: 处理器}`。"""

    return {"rpc:strategy.rollback": manager.rollback}


def register(target: Any, instance: RollbackManager | None = None) -> Any:
    """把本叶子的处理器注册进 `target`。"""

    instance = instance or RollbackManager()
    for name, handler in make_handlers(instance).items():
        target.register(name, handler, module=MODULE, replace=True)
    return target


__all__ = [
    "DEFAULT_MIN_SAMPLES",
    "DEFAULT_THRESHOLD",
    "DEP_PRESETS_REGISTER",
    "MODULE",
    "NAMES",
    "SAFE_FALLBACK_ACTIONS",
    "RollbackManager",
    "make_handlers",
    "previous_version",
    "register",
    "should_rollback",
    "snapshot_of",
]
