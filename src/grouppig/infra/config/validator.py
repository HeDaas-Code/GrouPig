"""grouppig.infra.config.validator —— 配置校验器（``rpc:config.validate``）。

normify id: ``grouppig.infra.config.validator``。

只做结构 / 类型 / 取值域 / 跨字段一致性校验，不修改配置。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from grouppig.infra.config.loader import Config
from grouppig.infra.runtime.errors import ConfigError

LEVELS = ("error", "warning")
REQUIRED_SECTIONS = ("app", "logging", "storage", "onebot", "model", "token")
REQUIRED_MODEL_TASKS = ("chat", "classify", "embed")
KNOWN_SCENARIOS = ("smalltalk", "chat", "discussion", "reflection", "classify", "embed")
STORAGE_DSN_PREFIX = {"sqlite": ("sqlite",), "mysql": ("mysql",)}
KNOWN_TOP_LEVEL = (
    "app",
    "logging",
    "storage",
    "onebot",
    "model",
    "token",
    "bus",
)


@dataclass(frozen=True, slots=True)
class Issue:
    level: str
    code: str
    path: str
    message: str

    def as_dict(self) -> dict[str, str]:
        return {"level": self.level, "code": self.code, "path": self.path, "message": self.message}

    def __str__(self) -> str:
        return f"[{self.level}] {self.path}: {self.message} ({self.code})"


@dataclass
class ValidationReport:
    issues: list[Issue] = field(default_factory=list)

    # ---- 构造 ----------------------------------------------------------
    def error(self, code: str, path: str, message: str) -> None:
        self.issues.append(Issue("error", code, path, message))

    def warn(self, code: str, path: str, message: str) -> None:
        self.issues.append(Issue("warning", code, path, message))

    # ---- 查询 ----------------------------------------------------------
    @property
    def errors(self) -> list[Issue]:
        return [i for i in self.issues if i.level == "error"]

    @property
    def warnings(self) -> list[Issue]:
        return [i for i in self.issues if i.level == "warning"]

    @property
    def ok(self) -> bool:
        return not self.errors

    def raise_if_invalid(self) -> None:
        if not self.ok:
            detail = "; ".join(str(i) for i in self.errors)
            raise ConfigError(f"配置校验未通过（{len(self.errors)} 个错误）：{detail}", issues=self.errors)

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "errors": [i.as_dict() for i in self.errors],
            "warnings": [i.as_dict() for i in self.warnings],
            "counts": {"error": len(self.errors), "warning": len(self.warnings)},
        }


def _check_type(report: ValidationReport, config: Config, path: str, types: tuple[type, ...]) -> Any:
    value = config.get(path)
    if value is None:
        return None
    if not isinstance(value, types):
        names = "/".join(t.__name__ for t in types)
        report.error("type", path, f"应为 {names}，得到 {type(value).__name__}")
        return None
    return value


def _check_range(
    report: ValidationReport,
    config: Config,
    path: str,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
    types: tuple[type, ...] = (int, float),
) -> None:
    value = _check_type(report, config, path, types)
    if value is None:
        return
    if minimum is not None and value < minimum:
        report.error("range", path, f"应 >= {minimum}，得到 {value}")
    if maximum is not None and value > maximum:
        report.error("range", path, f"应 <= {maximum}，得到 {value}")


def validate_config(config: Config) -> ValidationReport:
    """校验配置；返回报告（不抛异常，调用方决定是否 ``raise_if_invalid()``）。"""

    report = ValidationReport()

    for section in REQUIRED_SECTIONS:
        if not isinstance(config.get(section), dict):
            report.error("missing-section", section, f"缺少必需的配置表 [{section}]")

    for key in config.raw:
        if key not in KNOWN_TOP_LEVEL:
            report.warn("unknown-key", key, f"未知的顶层配置表 [{key}]（拼写错误？）")

    # app
    if not str(config.get("app.name", "")).strip():
        report.error("required", "app.name", "不能为空")

    # logging
    level = str(config.get("logging.level", "INFO")).upper()
    if level not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
        report.error("enum", "logging.level", f"非法日志级别 {level}")
    _check_type(report, config, "logging.json", (bool,))
    _check_type(report, config, "logging.file", (str,))

    # storage
    driver = str(config.get("storage.driver", "")).lower()
    if driver not in STORAGE_DSN_PREFIX:
        report.error("enum", "storage.driver", f"应为 sqlite 或 mysql，得到 {driver!r}")
    dsn = str(config.get("storage.dsn", ""))
    if not dsn:
        report.error("required", "storage.dsn", "不能为空")
    elif driver in STORAGE_DSN_PREFIX and not dsn.startswith(STORAGE_DSN_PREFIX[driver]):
        report.error(
            "mismatch", "storage.dsn", f"driver={driver} 要求 dsn 以 {STORAGE_DSN_PREFIX[driver]} 开头，得到 {dsn!r}"
        )

    # onebot
    ws_url = str(config.get("onebot.ws_url", ""))
    if not ws_url:
        report.error("required", "onebot.ws_url", "不能为空")
    elif not ws_url.startswith(("ws://", "wss://")):
        report.error("format", "onebot.ws_url", f"应为 ws:// 或 wss:// 地址，得到 {ws_url!r}")
    _check_type(report, config, "onebot.self_id", (int,))
    _check_range(report, config, "onebot.reconnect_interval", minimum=0.1)
    _check_range(report, config, "onebot.heartbeat_interval", minimum=0.0)

    # model
    provider = str(config.get("model.default_provider", ""))
    providers = config.sections("model.providers")
    if not provider:
        report.error("required", "model.default_provider", "不能为空")
    elif provider not in providers:
        report.error("reference", "model.default_provider", f"未在 [model.providers.{provider}] 中定义")
    if not providers:
        report.error("missing-section", "model.providers", "至少需要一个模型服务商")
    for name, spec in providers.items():
        if not str(spec.get("base_url", "")).strip():
            report.warn("empty-base-url", f"model.providers.{name}.base_url", "为空：使用 http transport 时会失败")
        if not str(spec.get("api_key_env", "")).strip():
            report.warn("empty-api-key-env", f"model.providers.{name}.api_key_env", "为空：无法从环境变量取密钥")

    tasks = config.sections("model.tasks")
    for task in REQUIRED_MODEL_TASKS:
        spec = tasks.get(task)
        if spec is None:
            report.error("missing-section", f"model.tasks.{task}", "缺少必需的任务配置")
            continue
        if not str(spec.get("model", "")).strip():
            report.error("required", f"model.tasks.{task}.model", "不能为空")
        task_provider = str(spec.get("provider", provider))
        if task_provider not in providers:
            report.error("reference", f"model.tasks.{task}.provider", f"未在 [model.providers.{task_provider}] 中定义")
        _check_range(report, config, f"model.tasks.{task}.temperature", minimum=0.0, maximum=2.0)
        _check_range(report, config, f"model.tasks.{task}.max_tokens", minimum=0, types=(int,))
        fallbacks = spec.get("fallback_models", [])
        if not isinstance(fallbacks, list):
            report.error("type", f"model.tasks.{task}.fallback_models", "应为字符串数组")

    _check_range(report, config, "model.retry.max_attempts", minimum=1, types=(int,))
    _check_range(report, config, "model.retry.base_delay", minimum=0.0)
    _check_range(report, config, "model.retry.max_delay", minimum=0.0)
    _check_range(report, config, "model.retry.timeout", minimum=0.1)
    base_delay = config.get("model.retry.base_delay")
    max_delay = config.get("model.retry.max_delay")
    if isinstance(base_delay, (int, float)) and isinstance(max_delay, (int, float)) and max_delay < base_delay:
        report.error("mismatch", "model.retry.max_delay", f"应 >= base_delay（{base_delay}），得到 {max_delay}")

    # token
    _check_range(report, config, "token.daily_limit", minimum=0, types=(int,))
    policies = config.sections("token.policies")
    if not policies:
        report.warn("missing-section", "token.policies", "未配置任何预算策略，将使用内置默认值")
    default_scenario = str(config.get("token.default_scenario", "chat"))
    if policies and default_scenario not in policies:
        report.warn("reference", "token.default_scenario", f"{default_scenario!r} 未在 [token.policies] 中定义")
    for scenario, spec in policies.items():
        if scenario not in KNOWN_SCENARIOS:
            report.warn(
                "unknown-scenario", f"token.policies.{scenario}", f"未知场景（已知：{', '.join(KNOWN_SCENARIOS)}）"
            )
        _check_range(report, config, f"token.policies.{scenario}.max_input_tokens", minimum=0, types=(int,))
        _check_range(report, config, f"token.policies.{scenario}.max_output_tokens", minimum=0, types=(int,))
        on_exceed = str(spec.get("on_exceed", "clamp"))
        if on_exceed not in ("clamp", "raise"):
            report.error("enum", f"token.policies.{scenario}.on_exceed", f"应为 clamp 或 raise，得到 {on_exceed!r}")

    # bus
    _check_type(report, config, "bus.strict_topics", (bool,))
    _check_range(report, config, "bus.handler_timeout", minimum=0.0)

    return report


def validate_config_or_raise(config: Config) -> Config:
    report = validate_config(config)
    report.raise_if_invalid()
    return config


__all__ = [
    "KNOWN_SCENARIOS",
    "LEVELS",
    "REQUIRED_MODEL_TASKS",
    "REQUIRED_SECTIONS",
    "Issue",
    "ValidationReport",
    "validate_config",
    "validate_config_or_raise",
]
