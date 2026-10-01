"""日志与结构化追踪测试。"""

from __future__ import annotations

import asyncio
import io
import json
from pathlib import Path

from grouppig.infra import logger as logger_module
from grouppig.infra.logger import RuntimeLogger, configure_logging, get_logger, set_logger


def make_logger(**kwargs) -> tuple[RuntimeLogger, io.StringIO]:
    sink = io.StringIO()
    return RuntimeLogger(**{"sink": sink, **kwargs}), sink


def test_json_records_include_structured_fields():
    log, sink = make_logger(level="DEBUG")
    log.info("session.started", "会话开始", session_id="s1", members=3)
    payload = json.loads(sink.getvalue().strip())
    assert payload["event"] == "session.started"
    assert payload["message"] == "会话开始"
    assert payload["level"] == "INFO"
    assert payload["fields"] == {"session_id": "s1", "members": 3}
    assert payload["logger"] == "grouppig"


def test_level_filtering_and_debug_records():
    log, sink = make_logger(level="WARNING")
    log.debug("noisy", "不该出现")
    log.info("quiet", "也不该出现")
    log.error("boom", "要出现")
    lines = [line for line in sink.getvalue().splitlines() if line]
    assert len(lines) == 1
    assert json.loads(lines[0])["event"] == "boom"
    assert len(log.records) == 3  # 记录仍全部保留，供测试断言


def test_sensitive_fields_are_redacted():
    log, sink = make_logger(level="DEBUG")
    log.info("model.call", api_key="sk-secret", nested={"access_token": "t", "model": "m"})
    fields = json.loads(sink.getvalue().strip())["fields"]
    assert fields["api_key"] == logger_module.REDACTED
    assert fields["nested"]["access_token"] == logger_module.REDACTED
    assert fields["nested"]["model"] == "m"


def test_trace_context_manager_records_duration():
    log, sink = make_logger(level="DEBUG")
    with log.trace("model.chat", model="m1") as span:
        assert span.event == "trace.model.chat"
    payload = json.loads(sink.getvalue().strip())
    assert payload["event"] == "trace.model.chat"
    assert payload["duration_ms"] >= 0
    assert payload["trace_id"] and payload["span_id"]
    assert payload["fields"]["model"] == "m1"


def test_child_logger_inherits_context_and_level():
    log, sink = make_logger(level="DEBUG")
    child = log.child("gateway.adapter", session_id="s9")
    record = child.info("event.routed")
    assert record.logger == "gateway.adapter"
    assert record.fields["session_id"] == "s9"
    assert json.loads(sink.getvalue().strip())["fields"]["session_id"] == "s9"


def test_text_output_mode():
    log, sink = make_logger(level="DEBUG", json_output=False)
    log.warning("bus.dropped", "主题积压", topic="kafka:grouppig.event.routed")
    line = sink.getvalue().strip()
    assert "WARNING" in line and "bus.dropped" in line and "kafka:grouppig.event.routed" in line
    assert not line.startswith("{")


def test_file_sink_writes_and_closes(tmp_path: Path):
    target = tmp_path / "logs" / "grouppig.log"
    log = RuntimeLogger(level="DEBUG", sink=io.StringIO(), file=target)
    log.info("container.started")
    assert target.is_file() and "container.started" in target.read_text(encoding="utf-8")
    log.close()


def test_configure_logging_applies_config(config):
    sink = io.StringIO()
    log = configure_logging(config=config, sink=sink, bridge_stdlib=False)
    assert log.level == 20
    assert log.json_output is True
    log.info("configured")
    assert json.loads(sink.getvalue().strip())["event"] == "configured"
    set_logger(RuntimeLogger(level="DEBUG", sink=io.StringIO()))
    assert get_logger("x").name == "x"


async def test_rpc_logger_handlers(container):
    records = []
    container.logger.on_record = records.append
    payload = await container.call("rpc:logger.log", "INFO", "session.started", "会话开始", session_id="s1")
    assert payload["event"] == "session.started"
    assert payload["fields"]["session_id"] == "s1"

    traced = await container.call("rpc:logger.trace", "model.chat", duration_ms=12.5, model="m1")
    assert traced["event"] == "trace.model.chat"
    assert traced["duration_ms"] == 12.5
    assert [r.event for r in records] == ["session.started", "trace.model.chat"]


async def test_logging_never_blocks_business_on_broken_sink():
    class Broken(io.StringIO):
        def write(self, _data):  # type: ignore[override]
            raise OSError("disk full")

    log = RuntimeLogger(level="DEBUG", sink=Broken())
    record = log.info("still.works")
    assert record.event == "still.works"
    await asyncio.sleep(0)
