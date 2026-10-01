"""t10 端到端集成测试：单进程闭环的每一条腿都要真的接通。

链路：OneBot 入站 → 网关路由 → 感知缓冲/分类 → 话题与会话 → 画像与关系分 →
预设与生成 → 节流发送 → 会话结束触发反思。

这些用例全部走 `build_app` / `create_app`（真实装配路径），只把两处换成测试替身：

* OneBot 连接换成 `MockOneBotServer`（真实 WebSocket，不是 mock 函数）；
* 模型调用换成 `FakeTransport`（infra 的 ModelRouter 注入点）。

其余（事件总线、注册表、SQLite、各域运行时对象）都是生产实现。

## 确定性（t15 修的时序脆弱性）

生产装配里同时有**五个后台任务**会碰数据库：gateway 的入站投递泵、感知排空泵、
画像泵、会话巡检泵，以及事件驱动的心流驱动器。测试任务自己也在读写库，于是
「测试任务的事务」与「后台任务的事务」会在同一条 SQLite 内存连接上交错，
互相提交/回滚掉对方未提交的写 —— 表现为「写返回成功但查不到」，用例在 10s
轮询里超时（失败现场：buffer `ingested=3, persisted=2`，`chat.query` 却 rows=0）。

所以本文件不靠「把 interval 调大」来求安静（`_Pump._run` 是先 tick 再 sleep，
调大 interval 挡不住首拍），而是显式掐掉并发源：

1. `[app.integration] demux_pump = false` —— 入站事件只入队，由测试任务用
   `await app.gateway.demux.pump()` **顺序投递**（见 `deliver_inbound`）；
2. `[app.integration] pump_first_tick_immediate = false` —— 三个周期泵连首拍都不跑，
   只有用例显式调用 `drain_once` / `run_once` / `sweep_once` 时才动；
3. 心流驱动器是事件驱动的，它在飞的时候用 `FlowDriver.wait_idle()` 等干净，
   composer 用 `stats.pending == 0` 等干净（见 `quiesce`）。

这样每个用例里「谁在什么时候写库」都是确定的：写库的只有测试任务自己，
以及被它 await 到结束的旁路。
"""

from __future__ import annotations

import asyncio
import contextlib
import subprocess
import sys
import time
from collections.abc import AsyncIterator, Callable, Mapping
from pathlib import Path
from typing import Any

from gateway_helpers import DEFAULT_SELF_ID, EventCollector, MockOneBotServer, group_message
from grouppig.infra.config.loader import load_config
from grouppig.infra.runtime.registry import Registry
from grouppig.runtime.app import GrouppigApp, build_app, create_app
from grouppig.runtime.pumps import TOPIC_INTERRUPT_TRIGGERED, TOPIC_MESSAGE_RECEIVED
from helpers import FakeTransport

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPO_ROOT / "config" / "grouppig.toml"

GROUP_ID = 100
ALICE = 200
BOB = 201
SELF_ID = DEFAULT_SELF_ID

#: 追加到配置副本上的测试参数。
#:
#: * `onebot.rate` 压到毫秒级，避免用例被默认的 2s 最小间隔拖慢；
#: * `perception.features` 关掉分类节流，让一次 drain 就能出行为结论；
#: * `app.integration` 掐掉两个并发源：入站投递泵不自跑（用例顺序投递），
#:   周期泵连首拍也不跑（用例显式驱动）。见模块 docstring 的「确定性」一节。
EXTRA_CONFIG = """
[onebot.rate]
per_minute = 600
burst = 4
min_interval = 0.05

[perception.features]
classify_min_interval = 0
classify_min_messages = 2

[app.integration]
drain_interval = 3600
profile_interval = 3600
sweep_interval = 3600
pump_first_tick_immediate = false
demux_pump = false
"""


def _append_config(base: str, extra: str) -> str:
    """把 ``extra`` 追加到 ``base`` 上；若两边都有同名段头，先摘掉 ``base`` 的那一段。

    出厂配置现在自带 ``[app.integration]``（记录周期泵节拍与保留策略旋钮），而这里的
    覆盖项要改的正是同一段 —— 直接追加会出现两个同名段，TOML 解析当场失败
    （``Cannot declare ('app', 'integration') twice``）。
    """

    headers = {line.strip() for line in extra.splitlines() if line.strip().startswith("[")}
    if headers:
        kept, skipping = [], False
        for line in base.splitlines():
            stripped = line.strip()
            if stripped.startswith("["):
                skipping = stripped in headers
            if not skipping:
                kept.append(line)
        base = "\n".join(kept)
    return base.rstrip("\n") + "\n" + extra


#: 真实感的中文群聊（话题候选生成器要能从里面切出短语，太短的英文串切不出来）。
CHAT = (
    "周末一起去爬山吧",
    "爬山好啊我也想去爬山",
    "那就周六早上八点集合去爬山",
)

REPLY_TEXT = "喵，爬山听起来不错呀"

#: 只用来等「进程内的 mock 服务器读到我方帧」这类真实异步边界；
#: 不再承担「等某条写落库」的职责（那是并发源，已在配置里掐掉）。
WAIT_TIMEOUT = 5.0


def _write_config(tmp_path: Path, ws_url: str, extra: str = EXTRA_CONFIG) -> Path:
    """把仓库配置复制到 tmp 并改写成测试用（ws 指向 mock、self_id 定死）。"""

    text = CONFIG_PATH.read_text(encoding="utf-8")
    text = text.replace("threshold = 0.26", "threshold = 0.55").replace("renormalize = true", "renormalize = false")
    text = text.replace('ws_url = "ws://127.0.0.1:3001"', 'ws_url = "' + ws_url + '"')
    text = text.replace("self_id = 0", "self_id = " + str(SELF_ID))
    path = tmp_path / "grouppig.toml"
    path.write_text(_append_config(text, extra), encoding="utf-8")
    return path


async def wait_for(predicate: Callable[[], bool], *, timeout: float = WAIT_TIMEOUT, interval: float = 0.005) -> bool:
    """轮询等待条件成立（只用于等真实异步边界，不再用于等数据库写入）。"""

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return False


def pin_laya_transport(router: Any, transport: Any) -> None:
    """把假传输同时钉到 LAY A provider 上（测试隔离）。

    只注册 "*" 不够：``ModelRouter._transport()`` 对 laya 会绕开 "*"
    （``/v1/systemone`` 非 OpenAI 兼容），于是 classify / system1 任务会去连真实
    LAY A 端点 —— 没有密钥时 401 仍能回落通过（假绿），一旦配上有效密钥就会失败。
    显式按 provider 注入，让 LAY A 路径确定性地走假传输。
    """

    router.set_transport(transport, provider="laya")


@contextlib.asynccontextmanager
async def integration(
    tmp_path: Path,
    *,
    transport: Any = None,
    extra: str = EXTRA_CONFIG,
    **options: Any,
) -> AsyncIterator[tuple[GrouppigApp, MockOneBotServer, FakeTransport]]:
    """起一个完整的单进程 GrouPig（真实 WebSocket + 内存库），退出时收尾。"""

    server = MockOneBotServer(self_id=SELF_ID)
    url = await server.start()
    config = load_config(_write_config(tmp_path, url, extra), use_env=False, use_local=False)
    fake = transport or FakeTransport(reply=REPLY_TEXT)
    app = build_app(
        config,
        registry=Registry(),
        transport=fake,
        dsn="sqlite+aiosqlite:///:memory:",
        **options,
    )
    # 只注册 "*" 时 classify/system1（provider=laya）会绕开它去连真实端点
    pin_laya_transport(app.container.router, fake)
    await app.start(connect=True, pumps=True)
    try:
        yield app, server, fake
    finally:
        await app.aclose()
        await server.stop()


async def deliver_inbound(app: GrouppigApp, count: int, *, timeout: float = WAIT_TIMEOUT) -> int:
    """等 `count` 条入站事件入队，然后在**当前任务里**顺序投递。

    因果：`demux_pump = false` 保证没有第二个任务在投递，`pump()` 是同步循环，
    所以「感知写缓冲 + `rpc:chat.append` + 会话层入库」全部发生在当前任务里；
    投递返回后，库里已经有这批消息，断言不再需要等墙钟。
    """

    ok = await wait_for(lambda: app.gateway.demux.queue.depth >= count, timeout=timeout)
    assert ok, f"入站事件没有全部入队：{app.gateway.demux.status()}"
    delivered = await app.gateway.demux.pump()
    assert delivered == count, (delivered, app.gateway.demux.status())
    return delivered


async def wait_chat_rows(app: GrouppigApp, count: int, *, timeout: float = WAIT_TIMEOUT) -> list[dict[str, Any]] | None:
    """等聊天流水里出现至少 count 条群消息，返回它们（超时返回 None）。"""

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = await app.container.call("rpc:chat.query", {"group_id": GROUP_ID})
        rows = list(result.get("messages") or ())
        if len(rows) >= count:
            return rows
        await asyncio.sleep(0.005)
    return None


async def drain(app: GrouppigApp) -> dict[str, Any]:
    """手工排空感知缓冲（级联下游都在这一次调用里 await 完）。"""

    return await app.drain_once()


async def quiesce(app: GrouppigApp) -> None:
    """等事件驱动的旁路收尾，保证后面只有测试任务在写库。

    心流驱动器由 `kafka:grouppig.interrupt.triggered` 派生任务跑，composer 的发送
    由 `kafka:grouppig.reply.composed` 的订阅者跑 —— 两者都不是测试任务，
    不等干净就会和断言抢连接（原来靠 `asyncio.sleep(0.3)` 蒙）。
    """

    await app.flow_driver.wait_idle()
    ok = await wait_for(lambda: app.gateway.composer.stats.pending == 0)
    assert ok, f"composer 还有在飞的发送：{app.gateway.composer.stats.as_dict()}"


async def push_chat(server: MockOneBotServer, app: GrouppigApp, texts: tuple[str, ...] = CHAT) -> int:
    """把一批群消息推给机器人，并等它们走完网关 → 感知 → 会话 → 记忆。"""

    base = int(time.time()) - 30
    for index, text in enumerate(texts):
        await server.push(
            group_message(
                text,
                group_id=GROUP_ID,
                user_id=ALICE + index % 2,
                message_id=1000 + index,
                self_id=SELF_ID,
                time=base + index,
            )
        )
    await deliver_inbound(app, len(texts))
    ok = await wait_for(lambda: len(app.session.ingested) >= len(texts))
    assert ok, f"消息没有走到会话层：{app.gateway.demux.stats.as_dict()}"
    rows = await wait_chat_rows(app, len(texts))
    assert rows is not None, (
        "消息没有落到聊天流水："
        f"rows={len(rows or ())} router={app.gateway.demux.stats.as_dict()}"
        f" buffer={app.perception.buffer.snapshot()['stats']}"
        f" bus={app.container.bus.stats.as_dict()}"
    )
    return base


async def trigger_reply(app: GrouppigApp, *, score: float = 0.9) -> dict[str, Any]:
    """走真实决策入口触发一次发言。

    冷却器是「真实」的（`perception.interrupt.cooldown`），同一个群短时间内只会放行一次，
    所以这里显式传 `cooldown` 把它让开——被测的是决策之后的链路，不是冷却策略本身。
    """

    return await app.container.call(
        "rpc:interrupt.decide",
        GROUP_ID,
        score=score,
        score_detail={"band": "strong", "total": score},
        behavior="repeat",
        cooldown={"allowed": True, "reason": "integration-test"},
    )


# ---- 1. 单进程启动入口 ------------------------------------------------------


def test_cli_check_mode_reports_no_contract_gaps() -> None:
    """`python -m grouppig.runtime --check` 是运维的装配自检入口：必须 0 缺口退出。

    走**子进程**而不是 `_main_async`：CLI 不传 registry，会往 infra 的
    `default_registry` 里注册全部名字，在本进程里跑会污染其它域的「不泄漏」断言。
    子进程还顺带覆盖了 `__main__.py` 的 `sys.exit(main())`。
    """

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "grouppig.runtime",
            "--check",
            "--no-connect",
            "--dsn",
            "sqlite+aiosqlite:///:memory:",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    assert "grouppig 已装配" in result.stdout
    assert "契约缺口（rpc）：无" in result.stdout
    assert "未登记名字：无" in result.stdout
    assert "泵：[" in result.stdout


async def test_create_app_builds_every_domain_and_pump(tmp_path: Path) -> None:
    """一行入口 `create_app` 装齐八个域与六个驱动器，且契约零缺口。"""

    async with integration(tmp_path) as (app, _server, _fake):
        assert app.started is True
        assert all(app.domains.values()), app.domains
        assert {pump.name for pump in app.pumps} == {
            "perception.drain",
            "expression.flow",
            "social.profile",
            "session.sweeper",
            "maintenance.retention",
            "perception.idle_speak",
        }
        report = app.contract_check()
        assert report["missing"] == []
        assert report["unknown"] == []
        assert report["registered"] >= 140

        health = await app.health()
        assert health["app"]["started"] is True
        for name, domain_health in health["domains"].items():
            assert isinstance(domain_health, Mapping) and domain_health, name
            assert "error" not in domain_health, (name, domain_health)
        assert {pump["name"] for pump in health["pumps"]} == {pump.name for pump in app.pumps}


# ---- 2. 消息进 → 感知 → 记忆 → 会话 ----------------------------------------


async def test_inbound_message_reaches_perception_memory_and_session(tmp_path: Path) -> None:
    """一条群消息要同时落到：网关路由统计、感知缓冲、聊天流水、会话层。"""

    async with integration(tmp_path) as (app, server, _fake):
        inbound = EventCollector(app.container.bus).listen(TOPIC_MESSAGE_RECEIVED)
        await push_chat(server, app)

        envelopes = inbound.of_topic(TOPIC_MESSAGE_RECEIVED)
        assert len(envelopes) == len(CHAT)
        assert {int(item["group_id"]) for item in envelopes} == {GROUP_ID}
        inbound.close()

        router = app.gateway.demux.stats.as_dict()
        assert router["received"] == len(CHAT)
        assert router["routed"] == len(CHAT)
        assert router["dropped"] == 0
        assert router["routes"] == {"perception": len(CHAT)}

        buffered = app.perception.buffer.snapshot()
        assert buffered["buffered"] >= len(CHAT), buffered
        stats = buffered["stats"]
        assert stats["ingested"] == len(CHAT), buffered
        # 因果：投递与断言同任务，`rpc:chat.append` 已经 await 完，不需要轮询等待。
        assert stats["persisted"] == len(CHAT), buffered
        # 这两条是 t15 竞态的**取证点**：修复前是「写返回成功但行不在库里」，
        # 表现为 persisted 与真实行数对不上。计数与行数必须互相印证。
        assert stats["persist_failed"] == 0, buffered

        rows = await app.container.call("rpc:chat.query", {"group_id": GROUP_ID})
        assert rows["count"] == len(CHAT), rows
        # 「落盘计数」与「真实行数」必须一致 —— 竞态时代正是这里对不上：
        # 缓冲说 persisted=2，rpc:chat.query 却 rows=0。
        assert stats["persisted"] == rows["count"], (stats, rows["count"])
        # persist_recovered 是「下游抛错但已提交」；它不为 0 说明有跨域调用走了异常
        # 路径，即便数据没丢也要查（原来正是它把静默丢行掩盖成了「链路可用」）。
        assert stats["persist_recovered"] == 0, buffered
        assert {int(row["sender_id"]) for row in rows["messages"]} <= {ALICE, BOB}

        result = await drain(app)
        assert result["drained"] >= len(CHAT)
        assert "rpc:normalizer.clean" in result["downstream"]["calls"]
        assert result["downstream"]["failed"] == []


async def test_topic_and_session_are_keyed_by_the_real_group(tmp_path: Path) -> None:
    """会话/话题必须按真实群号归属。

    事件总线的订阅者拿到的是 `Event` 信封（payload 在 `.payload` 里），
    会话层早先只认裸字典，导致 `group_id` 恒为 0、所有群共用一个会话。
    """

    async with integration(tmp_path) as (app, server, _fake):
        await push_chat(server, app)
        await drain(app)

        records = [record for record in app.session.ingested if int(record["group_id"]) == GROUP_ID]
        assert len(records) >= len(CHAT)
        assert all(record["group_id"] == GROUP_ID for record in records)
        assert all(record["session_id"] for record in records), records
        assert all(record["thread_id"] for record in records), records

        current = await app.container.call("rpc:session.current", group_id=GROUP_ID)
        assert current["found"] is True
        assert int(current["session"]["group_id"]) == GROUP_ID

        threads = await app.container.call(
            "rpc:threads.segment",
            [{"content": text, "group_id": GROUP_ID} for text in CHAT],
            group_id=GROUP_ID,
        )
        assert threads["count"] >= 1
        assert threads["segments"][0]["message_ids"]


# ---- 3. 画像与关系分 --------------------------------------------------------


async def test_profile_pump_refreshes_profile_and_relationship(tmp_path: Path) -> None:
    """画像泵把「聊天流水」变成「档案 + 说话画像 + 关系分 + 分层」。"""

    async with integration(tmp_path) as (app, server, _fake):
        await push_chat(server, app)
        await drain(app)

        report = await app.profile_pump.run_once([GROUP_ID])
        assert report["members"] >= 1, report
        assert report["calls"]["failed"] == 0, report
        assert report["calls"]["errors"] == [], report

        speech = await app.container.call("rpc:speech.profile", ALICE, group_id=GROUP_ID)
        assert speech["sample_size"] >= 1, speech
        assert speech["summary"]

        relationship = await app.container.call("rpc:relationship.get", ALICE, group_id=GROUP_ID)
        assert relationship["found"] is True
        assert relationship["score"] is not None
        assert relationship["tier"]
        assert relationship["interactions"] >= 1


# ---- 4. 生成回复 → 节流发送 -------------------------------------------------


async def test_flow_decision_generates_and_sends_a_reply(tmp_path: Path) -> None:
    """决策说话 → 心流被驱动到 done → 表达层发布 → 节流器放行 → 真的发出群消息。"""

    async with integration(tmp_path) as (app, server, _fake):
        await push_chat(server, app)
        await drain(app)

        triggers = EventCollector(app.container.bus).listen(TOPIC_INTERRUPT_TRIGGERED)
        decision = await trigger_reply(app)
        assert decision["action"] == "speak"
        assert decision["published"] is True
        assert len(triggers.of_topic(TOPIC_INTERRUPT_TRIGGERED)) == 1
        triggers.close()

        # 心流驱动器是派生任务：等它把 flow 推完、把回复发出去（原来靠 sleep 0.3 蒙）。
        await quiesce(app)

        ok = await wait_for(lambda: len(server.actions_of("send_group_msg")) >= 1)
        assert ok, "回复没有发出去"
        # mock 服务器读帧是另一个任务；「不会再来第二条」由连接器出站队列空 +
        # composer.sent == 1 共同证明，而不是靠多等几拍。
        connector = app.gateway.connector.status()
        assert connector["outbox"] == 0 and connector["pending_actions"] == 0, connector

        sends = server.actions_of("send_group_msg")
        assert len(sends) == 1, "一条回复只能发一次（rpc:flow.end 与 reply.composed 两条边不能同时发）"
        frame = sends[0]
        assert frame["params"]["group_id"] == GROUP_ID
        texts = [seg["data"]["text"] for seg in frame["params"]["message"] if seg["type"] == "text"]
        assert texts and texts[0].startswith(REPLY_TEXT[:2])

        flow = app.expression.flow.store.status()
        assert flow["starts"] == 1
        assert flow["ends"] == 1
        assert flow["active"] == 0

        driver = app.flow_driver.status()
        assert driver["triggered"] == 1
        assert driver["driven"] == 1
        assert driver["no_flow"] == 0

        composer = app.gateway.composer.stats.as_dict()
        assert composer["sent"] == 1
        assert composer["failures"] == 0
        assert composer["recorded"] == 1

        limiter = app.gateway.limiter.snapshot()["stats"]
        assert limiter["allowed"] >= 1
        assert limiter["consumed"] >= 1

        rows = await app.container.call("rpc:chat.query", {"group_id": GROUP_ID})
        mine = [row for row in rows["messages"] if row["role"] == "self"]
        assert len(mine) == 1
        assert mine[0]["content"]


async def test_rate_limiter_delays_a_second_reply(tmp_path: Path) -> None:
    """节流器真的会拦：最小间隔内连发两次，第二次必须等。"""

    slow = EXTRA_CONFIG.replace("min_interval = 0.05", "min_interval = 0.6")
    async with integration(tmp_path, extra=slow) as (app, server, _fake):
        await push_chat(server, app)
        await drain(app)

        started = time.monotonic()
        await app.gateway.composer.send_reply({"text": "第一条", "group_id": GROUP_ID})
        await app.gateway.composer.send_reply({"text": "第二条", "group_id": GROUP_ID})
        elapsed = time.monotonic() - started

        assert elapsed >= 0.5, "第二次发送没有被节流器推迟"
        stats = app.gateway.limiter.snapshot()["stats"]
        assert stats["consumed"] >= 2
        assert len(server.actions_of("send_group_msg")) == 2


# ---- 5. 会话结束 → 反思 ----------------------------------------------------


async def test_ending_a_session_triggers_reflection(tmp_path: Path) -> None:
    """会话归档要发布 `kafka:grouppig.session.completed` 并落到反思层。"""

    async with integration(tmp_path) as (app, server, _fake):
        await push_chat(server, app)
        await drain(app)

        session_id = app.session.ingested[-1]["session_id"]
        archived = await app.end_session(GROUP_ID, reason="idle")
        assert archived.get("found") is not False

        ok = await wait_for(lambda: session_id in (app.reflection.extra.get("reviews") or {}))
        assert ok, "会话结束没有触发反思"
        review = app.reflection.extra["reviews"][session_id]
        assert isinstance(review, Mapping)
        assert review

        bus = app.container.bus.stats.as_dict()
        assert bus["published"] >= 1
        assert bus["failed"] == 0


async def test_session_sweeper_drives_the_lifecycle_entry_point(tmp_path: Path) -> None:
    """收尾泵是「会话结束」的周期入口：没有它，安静的群永远不会归档。"""

    async with integration(tmp_path) as (app, server, _fake):
        await push_chat(server, app)
        await drain(app)

        result = await app.session_sweeper.sweep_once([GROUP_ID])
        assert result["groups"] == 1
        assert app.session_sweeper.stats["missing"] == 0
        assert app.session_sweeper.stats["swept"] >= 1


# ---- 6. 驱动器本身的确定性（t15） ------------------------------------------


async def test_periodic_pumps_do_not_tick_on_their_own(tmp_path: Path) -> None:
    """`pump_first_tick_immediate = false` 时，周期泵在用例期间一拍都不跑。

    因果：`_Pump._run` 是「先 tick 再 sleep」，只把 interval 调大挡不住首拍 ——
    排空泵会立刻跑一次 `rpc:observer.buffer.drain`，与用例手工的 `drain_once()`
    抢同一批缓冲消息（现场：`drained` 计数与用例预期对不上）。
    关掉首拍之后，「什么时候排空」完全由用例决定。
    """

    async with integration(tmp_path) as (app, server, _fake):
        await push_chat(server, app)
        assert app.drain_pump is not None
        assert app.drain_pump.stats["ticks"] == 0
        assert app.profile_pump is not None and app.profile_pump.stats["ticks"] == 0
        assert app.session_sweeper is not None and app.session_sweeper.stats["ticks"] == 0

        # 手工驱动照常工作（关的是首拍，不是泵本身）。
        result = await drain(app)
        assert result["drained"] >= len(CHAT)
        assert app.drain_pump.stats["ticks"] == 0


async def test_periodic_pump_first_tick_is_controllable(tmp_path: Path) -> None:
    """默认（`pump_first_tick_immediate = true`）仍会立刻跑首拍 —— 开关是双向的。"""

    immediate = EXTRA_CONFIG.replace("pump_first_tick_immediate = false", "pump_first_tick_immediate = true")
    async with integration(tmp_path, extra=immediate) as (app, _server, _fake):
        assert app.drain_pump is not None
        ok = await wait_for(lambda: app.drain_pump.stats["ticks"] >= 1)
        assert ok, f"默认配置下首拍没有跑：{app.drain_pump.status()}"


# ---- 7. 收尾 ----------------------------------------------------------------


async def test_shutdown_stops_pumps_and_closes_the_container(tmp_path: Path) -> None:
    """`aclose` 之后泵要停、连接要断、容器要关。"""

    async with integration(tmp_path) as (app, server, _fake):
        assert all(pump.running for pump in app.pumps if pump.name != "expression.flow")
        assert app.gateway.connector.connected is True

    assert app.started is False
    assert not any(pump.running for pump in app.pumps)
    assert server.actions_of("send_group_msg") == []


async def test_start_is_idempotent_and_pumps_can_be_skipped(tmp_path: Path) -> None:
    """`start()` 幂等；`pumps=False` 时六个驱动器一个都不建。"""

    server = MockOneBotServer(self_id=SELF_ID)
    url = await server.start()
    try:
        config = load_config(_write_config(tmp_path, url), use_env=False, use_local=False)
        fake = FakeTransport(reply=REPLY_TEXT)
        app = await create_app(
            config,
            registry=Registry(),
            transport=fake,
            dsn="sqlite+aiosqlite:///:memory:",
            pumps=False,
        )
        pin_laya_transport(app.container.router, fake)
        try:
            assert app.pumps == ()
            first = await app.start()
            assert first is app
            assert app.pumps == ()
        finally:
            await app.aclose()
    finally:
        await server.stop()
