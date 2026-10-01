"""配置加载 / 校验 / 热更新测试。"""

from __future__ import annotations

from pathlib import Path

import pytest

from grouppig.infra.config.loader import (
    Config,
    config_get,
    load_config,
    resolve_config_path,
    resolve_secret,
    set_config,
)
from grouppig.infra.config.reloader import ConfigReloader
from grouppig.infra.config.validator import validate_config
from grouppig.infra.runtime.errors import ConfigError

REPO_ROOT = Path(__file__).resolve().parents[1]


# ---- loader --------------------------------------------------------------
def test_load_config_reads_toml(config: Config, config_file: Path):
    assert config.source == str(config_file)
    assert config.get("app.name") == "grouppig"
    assert config.get("onebot.ws_url") == "ws://127.0.0.1:3001"
    assert config.storage_driver == "sqlite"
    spec = config.section("model.tasks.chat")
    assert spec["provider"] == "a6api"
    assert spec["model"]  # 模型名由部署方在 config/grouppig.toml 里选定，不写死
    assert config.section("model.providers.a6api")["base_url"]
    assert config.section("model.tasks.embed")["provider"] == "local"  # 无 embedding 服务商时用本地确定性嵌入


def test_dotted_get_and_require(config: Config):
    assert config.get("model.tasks.chat.temperature") == 0.8
    assert config.get("does.not.exist", "fallback") == "fallback"
    assert config.require("app.env") == "dev"
    with pytest.raises(ConfigError):
        config.require("does.not.exist")
    with pytest.raises(ConfigError):
        config.section("app.name")  # 不是表


def test_sections_returns_mapping_of_tables(config: Config):
    tasks = config.sections("model.tasks")
    assert set(tasks) == {"chat", "classify", "embed"}
    assert all(isinstance(spec, dict) for spec in tasks.values())


def test_fingerprint_is_stable_and_sensitive(config: Config):
    same = load_config(config.source, use_env=False, use_local=False)
    assert same.fingerprint == config.fingerprint
    changed = config.with_overrides({"app": {"env": "prod"}})
    assert changed.fingerprint != config.fingerprint
    assert changed.get("app.env") == "prod"
    assert changed.get("app.name") == "grouppig"  # 深合并不丢字段


def test_env_overrides_nested(config_file: Path):
    environ = {"GROUPPIG__MODEL__TASKS__CHAT__MODEL": "custom-model", "GROUPPIG__TOKEN__DAILY_LIMIT": "1234"}
    config = load_config(config_file, environ=environ, use_local=False)
    assert config.get("model.tasks.chat.model") == "custom-model"
    assert config.get("token.daily_limit") == 1234
    assert "env:GROUPPIG__*" in config.sources


def test_missing_file_raises(tmp_path: Path):
    with pytest.raises(ConfigError):
        load_config(tmp_path / "nope.toml", use_env=False, use_local=False)


def test_invalid_toml_raises(tmp_path: Path):
    bad = tmp_path / "bad.toml"
    bad.write_text("[app\nname = ", encoding="utf-8")
    with pytest.raises(ConfigError):
        load_config(bad, use_env=False, use_local=False)


def test_resolve_config_path_prefers_explicit_then_env(config_file: Path, monkeypatch):
    monkeypatch.setenv("GROUPPIG_CONFIG", str(config_file))
    assert resolve_config_path() == config_file
    assert resolve_config_path(REPO_ROOT / "config/grouppig.toml") == REPO_ROOT / "config/grouppig.toml"
    monkeypatch.delenv("GROUPPIG_CONFIG")


def test_resolve_secret_prefers_env_then_credentials(tmp_path: Path):
    creds = tmp_path / "credentials.yaml"
    creds.write_text("model:\n  api_key: from-file\n", encoding="utf-8")
    assert resolve_secret("model.api_key", credentials_path=creds, environ={}) == "from-file"
    assert (
        resolve_secret("model.api_key", credentials_path=creds, environ={"GROUPPIG_MODEL_API_KEY": "from-env"})
        == "from-env"
    )
    assert resolve_secret("nope", credentials_path=tmp_path / "missing.yaml", environ={}, default="d") == "d"


def test_config_get_handler_shapes(config: Config):
    set_config(config)
    assert config_get()["app"]["name"] == "grouppig"
    assert config_get("app.env", config=config) == "dev"
    assert config_get("nope", "fallback", config=config) == "fallback"


# ---- validator -----------------------------------------------------------
def test_repo_config_is_valid(config: Config):
    report = validate_config(config)
    assert report.ok, [str(i) for i in report.errors]
    assert report.as_dict()["counts"]["error"] == 0


def test_validation_reports_missing_sections():
    report = validate_config(Config(raw={"app": {"name": "x"}}))
    assert not report.ok
    codes = {issue.code for issue in report.errors}
    assert "missing-section" in codes
    with pytest.raises(ConfigError):
        report.raise_if_invalid()


@pytest.mark.parametrize(
    ("overrides", "path"),
    [
        ({"storage": {"driver": "postgres"}}, "storage.driver"),
        ({"storage": {"driver": "sqlite", "dsn": "mysql+aiomysql://x"}}, "storage.dsn"),
        ({"onebot": {"ws_url": "http://127.0.0.1:3001"}}, "onebot.ws_url"),
        ({"logging": {"level": "LOUD"}}, "logging.level"),
        ({"model": {"tasks": {"chat": {"model": ""}}}}, "model.tasks.chat.model"),
        ({"model": {"tasks": {"chat": {"provider": "ghost"}}}}, "model.tasks.chat.provider"),
        ({"model": {"retry": {"max_attempts": 0}}}, "model.retry.max_attempts"),
        ({"token": {"policies": {"chat": {"on_exceed": "explode"}}}}, "token.policies.chat.on_exceed"),
    ],
)
def test_validation_catches_bad_values(config: Config, overrides: dict, path: str):
    report = validate_config(config.with_overrides(overrides))
    assert not report.ok
    assert path in {issue.path for issue in report.errors}, [str(i) for i in report.errors]


def test_validation_warns_on_unknown_scenario(config: Config):
    report = validate_config(config.with_overrides({"token": {"policies": {"chitchat": {"max_output_tokens": 10}}}}))
    assert report.ok
    assert any(issue.code == "unknown-scenario" for issue in report.warnings)


# ---- reloader ------------------------------------------------------------
async def test_reloader_initial_and_reload_detects_change(config_file: Path, tmp_path: Path):
    reloader = ConfigReloader(config_file)
    first = reloader.load_initial()
    assert first.get("app.env") == "dev"

    config_file.write_text(
        config_file.read_text(encoding="utf-8").replace('env = "dev"', 'env = "staging"'), encoding="utf-8"
    )
    result = await reloader.reload()
    assert result.ok and result.changed
    assert result.config.get("app.env") == "staging"
    assert reloader.config.get("app.env") == "staging"
    assert len(reloader.history) == 1


async def test_reloader_keeps_old_config_when_invalid(config_file: Path):
    reloader = ConfigReloader(config_file)
    reloader.load_initial()
    config_file.write_text(
        config_file.read_text(encoding="utf-8").replace('driver = "sqlite"', 'driver = "oracle"'), encoding="utf-8"
    )
    result = await reloader.reload()
    assert not result.ok
    assert result.error and "storage.driver" in result.error
    assert reloader.config.get("storage.driver") == "sqlite"


async def test_reloader_notifies_on_change(config_file: Path):
    seen: list[dict] = []

    async def hook(result):
        seen.append(result.as_dict())

    reloader = ConfigReloader(config_file, on_change=hook)
    reloader.load_initial()
    config_file.write_text(
        config_file.read_text(encoding="utf-8").replace('name = "grouppig"', 'name = "grouppig-x"'), encoding="utf-8"
    )
    result = await reloader.reload()
    assert result.ok and seen and seen[0]["changed"] is True


async def test_reloader_skips_when_unchanged(config_file: Path):
    reloader = ConfigReloader(config_file)
    reloader.load_initial()
    result = await reloader.reload()
    assert result.ok
    assert result.changed is False
    assert result.previous_fingerprint == result.fingerprint


async def test_reloader_watch_loop_stops(config_file: Path):
    reloader = ConfigReloader(config_file)
    reloader.load_initial()
    iterations = await reloader.watch(interval=0.01, max_iterations=2)
    assert iterations == 2
    assert reloader.watching is False


# ---- 模型 provider 分派（laya）与密钥卫生 ---------------------------------
def test_classify_task_defaults_to_laya(config: Config):
    """classify 默认走 LAY A（协议枚举 model），grok-4.6 保留为降级目标。"""

    spec = config.section("model.tasks.classify")
    assert spec["provider"] == "laya"
    assert spec["model"] == "auto"  # LAY A 的协议枚举，不是服务商模型名
    assert "grok-4.6" in spec["fallback_models"]

    provider = config.section("model.providers.laya")
    assert provider["base_url"] == "http://127.0.0.1:7780"
    assert provider["api_key_field"] == "model.api_key"
    assert provider["api_key_env"] == "GROUPPIG_LAYA_API_KEY"


def test_repo_config_contains_no_secret_literals(config_file: Path):
    """仓库配置里只允许出现密钥来源（环境变量名 / 凭据字段），不允许密钥字面量。"""

    text = config_file.read_text(encoding="utf-8")
    secret_keys = {"api_key", "apikey", "token", "secret", "password", "bearer"}
    for lineno, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key = stripped.split("=", 1)[0].strip().lower()
        assert key not in secret_keys, f"第 {lineno} 行疑似写了密钥字面量：{stripped}"
    assert "sk-" not in text


# ---- app.integration 的取值校验 ------------------------------------------
# `IntegrationOptions.from_config` 的 `_flag/_number/_integer` 是**容错**读取器：
# 非法值静默回落到默认。容错本身没错（旧配置还能跑），但静默会让「配错了」和
# 「没配」长得一模一样 —— 比如 `drain_interval = 0` 会让泵空转烧一个核。
# 所以这些键必须在校验器里**报错**，而不是靠读取器兜底。
_BAD_INTEGRATION_VALUES = (
    ("drain_interval", 0),
    ("drain_interval", -5),
    ("drain_interval", "0.5"),
    ("profile_interval", 0),
    ("profile_interval", -5.0),
    ("sweep_interval", 0),
    ("drain_batch", 0),
    ("drain_batch", -1),
    ("drain_batch", "lots"),
    ("profile_window_seconds", 0),
    ("profile_min_messages", 0),
    ("flow_max_steps", 0),
    ("migrate", "maybe"),
    ("pumps", "yes"),
    ("connect", 1),
)


@pytest.mark.parametrize(("key", "value"), _BAD_INTEGRATION_VALUES)
def test_invalid_app_integration_value_is_an_error(config: Config, key: str, value):
    """非法 `app.integration.*` 必须是 error（不是 warning，更不是静默回落）。"""

    report = validate_config(config.with_overrides({"app": {"integration": {key: value}}}))
    assert report.ok is False, f"{key}={value!r} 被静默接受了"
    assert any(issue.path == f"app.integration.{key}" for issue in report.errors), report.as_dict()


def test_valid_app_integration_values_pass(config: Config):
    """合法取值不能被误报（校验只针对真非法的）。"""

    report = validate_config(
        config.with_overrides(
            {
                "app": {
                    "integration": {
                        "drain_interval": 0.25,
                        "profile_interval": 30,
                        "sweep_interval": 60.0,
                        "drain_batch": 64,
                        "profile_window_seconds": 900,
                        "profile_min_messages": 2,
                        "flow_max_steps": 8,
                        "migrate": False,
                        "sweep": True,
                        "pumps": True,
                        "flow_send_via_topic": False,
                        "pump_first_tick_immediate": False,
                        "demux_pump": True,
                        "connect": True,
                        "dsn": "sqlite+aiosqlite:///:memory:",
                    }
                }
            }
        )
    )
    assert report.ok, report.as_dict()


def test_unknown_app_integration_key_warns(config: Config):
    """未知的 `app.integration.*` 子键要提醒（多半是拼写错误）。"""

    report = validate_config(config.with_overrides({"app": {"integration": {"drain_bat": 64}}}))
    assert any(issue.level == "warning" and "drain_bat" in issue.path for issue in report.issues), report.as_dict()


def test_shipped_config_has_no_false_unknown_section_warning(config: Config):
    """仓库配置里真实存在的段不该被报「未知顶层配置表」。"""

    report = validate_config(config)
    unknown = [issue for issue in report.warnings if issue.code == "unknown-key"]
    assert unknown == [], [str(issue) for issue in unknown]


def test_genuinely_unknown_top_level_section_still_warns(config: Config):
    """别把警告静音了：真拼错的顶层段仍要报出来。"""

    report = validate_config(config.with_overrides({"preception": {"interrupt": {"threshold": 0.5}}}))
    unknown = [issue for issue in report.warnings if issue.code == "unknown-key"]
    assert [issue.path for issue in unknown] == ["preception"]
