"""grouppig.infra.config.reloader —— 配置热更新器（``rpc:config.reload``）。

normify id: ``grouppig.infra.config.reloader``；依赖方向与设计一致：
``config.reload`` → ``config.get``（重新加载）→ ``config.validate``（先校验）。

热更新通过 ``on_change`` 回调通知订阅方；**不新增事件主题**——设计契约只允许
``api-index.json`` 中的 9 条 ``kafka:grouppig.*`` 主题。
"""

from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from grouppig.infra.config.loader import Config, load_config, set_config
from grouppig.infra.config.validator import ValidationReport, validate_config

ChangeHook = Callable[["ReloadResult"], Any | Awaitable[Any]]


@dataclass
class ReloadResult:
    """一次热更新的结果。"""

    ok: bool
    config: Config
    previous_fingerprint: str | None = None
    report: ValidationReport | None = None
    error: str | None = None
    elapsed_ms: float = 0.0
    reloaded_at: float = field(default_factory=time.time)

    @property
    def fingerprint(self) -> str:
        return self.config.fingerprint

    @property
    def changed(self) -> bool:
        return self.previous_fingerprint != self.fingerprint

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "fingerprint": self.fingerprint,
            "previous_fingerprint": self.previous_fingerprint,
            "changed": self.changed,
            "error": self.error,
            "elapsed_ms": round(self.elapsed_ms, 3),
            "reloaded_at": self.reloaded_at,
            "validation": self.report.as_dict() if self.report else None,
        }


class ConfigReloader:
    """加载 → 校验 → 原子替换当前配置。"""

    def __init__(
        self,
        path: str | Path | None = None,
        *,
        root: Path | None = None,
        on_change: ChangeHook | None = None,
        validate: bool = True,
        logger: Any | None = None,
        use_env: bool = True,
    ) -> None:
        self.path = path
        self.root = root
        self.on_change = on_change
        self.validate = validate
        self.logger = logger
        self.use_env = use_env
        self._config: Config | None = None
        self._history: list[ReloadResult] = []
        self._watching = False
        self._last_mtime: float | None = None

    # ---- 状态 ----------------------------------------------------------
    @property
    def config(self) -> Config:
        if self._config is None:
            self._config = self.load_initial()
        return self._config

    @property
    def history(self) -> tuple[ReloadResult, ...]:
        return tuple(self._history)

    @property
    def watching(self) -> bool:
        return self._watching

    def load_initial(self) -> Config:
        config = load_config(self.path, root=self.root, use_env=self.use_env)
        if self.validate:
            validate_config(config).raise_if_invalid()
        self._config = config
        set_config(config)
        self._remember_mtime()
        return config

    # ---- 热更新 --------------------------------------------------------
    async def reload(self, *, force: bool = False) -> ReloadResult:
        """重新加载配置；校验失败时保留旧配置并返回 ``ok=False``。"""

        started = time.perf_counter()
        previous = self._config
        previous_fp = previous.fingerprint if previous else None
        try:
            candidate = load_config(self.path, root=self.root, use_env=self.use_env)
        except Exception as exc:
            result = ReloadResult(
                ok=False, config=previous or Config(), previous_fingerprint=previous_fp, error=str(exc)
            )
            result.elapsed_ms = (time.perf_counter() - started) * 1000
            self._history.append(result)
            self._log("error", "config.reload_failed", error=str(exc))
            return result

        report = validate_config(candidate) if self.validate else None
        if report is not None and not report.ok:
            result = ReloadResult(
                ok=False,
                config=previous or candidate,
                previous_fingerprint=previous_fp,
                report=report,
                error="; ".join(str(i) for i in report.errors),
            )
            result.elapsed_ms = (time.perf_counter() - started) * 1000
            self._history.append(result)
            self._log("error", "config.reload_rejected", reason=result.error)
            return result

        if not force and previous is not None and candidate.fingerprint == previous.fingerprint:
            result = ReloadResult(ok=True, config=previous, previous_fingerprint=previous_fp, report=report)
            result.elapsed_ms = (time.perf_counter() - started) * 1000
            return result

        self._config = candidate
        set_config(candidate)
        self._remember_mtime()
        result = ReloadResult(ok=True, config=candidate, previous_fingerprint=previous_fp, report=report)
        result.elapsed_ms = (time.perf_counter() - started) * 1000
        self._history.append(result)
        self._log("info", "config.reloaded", **candidate.as_log_fields(), changed=result.changed)
        await self._notify(result)
        return result

    async def watch(self, interval: float = 2.0, *, max_iterations: int | None = None) -> int:
        """按 mtime 轮询的看护循环；返回执行的轮数（``max_iterations`` 便于测试）。"""

        self._watching = True
        iterations = 0
        try:
            while self._watching:
                if max_iterations is not None and iterations >= max_iterations:
                    break
                iterations += 1
                await asyncio.sleep(interval)
                mtime = self._mtime()
                if mtime is not None and self._last_mtime is not None and mtime != self._last_mtime:
                    await self.reload()
        finally:
            self._watching = False
        return iterations

    def stop(self) -> None:
        self._watching = False

    # ---- 内部 ----------------------------------------------------------
    def _resolved_path(self) -> Path:
        from grouppig.infra.config.loader import resolve_config_path

        return resolve_config_path(self.path, root=self.root)

    def _mtime(self) -> float | None:
        path = self._resolved_path()
        try:
            return path.stat().st_mtime
        except OSError:
            return None

    def _remember_mtime(self) -> None:
        self._last_mtime = self._mtime()

    async def _notify(self, result: ReloadResult) -> None:
        if self.on_change is None:
            return
        try:
            outcome = self.on_change(result)
            if inspect.isawaitable(outcome):
                await outcome
        except Exception as exc:  # pragma: no cover - 回调异常不影响主流程
            self._log("error", "config.change_hook_failed", error=str(exc))

    def _log(self, level: str, event: str, **fields: Any) -> None:
        if self.logger is None:
            return
        try:
            self.logger.log(level, event, **fields)
        except Exception:  # pragma: no cover
            pass


__all__ = ["ChangeHook", "ConfigReloader", "ReloadResult"]
