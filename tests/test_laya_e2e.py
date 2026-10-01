"""LAY A System-1 端到端回归（全程离线，用 ``tests/fakes_laya.py`` 的本机替身）。

覆盖四件事：

1. **假服务端本身**：LAY A 协议（``POST /v1/systemone`` + ``Authorization: Bearer``）、
   可编程的 401 / 422 / 503 / 非 JSON / 挂住超时。
2. **端到端可用**：config 指向假服务端后 ``rpc:model.classify`` 与 ``rpc:model.system1``
   走完整链路；6 个以上问题只产生 **1 次** HTTP 请求（服务端计数）。
3. **延迟回归**：本地回环下 classify 路径 < 2s。仓库配置原先指向内网端点
   ``http://127.0.0.1:7780``，未配置密钥时这条路会先超时再降级，实测 51–60 秒；
   阈值放宽到 2s 只为挡住「又变回几十秒」，不做性能门禁。
4. **降级与未回归**：LAY A 返回 503 / 超时 / 401 / 非 JSON 时 classify 回落 ``grok-4.6``
   且不抛错（回落那一跳用 ``StubOpenAITransport`` 替代真实网络）；行为分类与打断决策的
   判定结果与「classify 走 OpenAI 兼容服务商」的旧行为逐字一致。

5. **负控常驻**（t32 的临时脚本固化）：``wait_for_requests`` 的五项行为各有单测；
   另有一条鉴别力负控 —— 把 LAY A 指向死端口后，其余断言全过、**只有 hits 断言失败**，
   永久守住「hits 断言不是装饰」：谁把它删了或放宽，就没有东西还能证明 LAY A 那一跳被尝试过。

离线保证：所有 provider 的 ``base_url`` 都被改写成假服务端地址；密钥是注入的假值；
``loopback_only()`` 会把非本机地址的名字解析直接掐断，并有专门用例证明这道闸真的拦得住。
"""

from __future__ import annotations

import contextlib
import json
import socket
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest import mock

import httpx
import pytest

from fakes_laya import (
    SYSTEM_ONE_PATH,
    FakeLayaServer,
    StubOpenAITransport,
    choice_answer,
    laya_response,
)
from grouppig.infra.config.loader import load_config
from grouppig.infra.model_gateway.codec import ModelRequest, encode_request
from grouppig.infra.runtime.di import build_container
from grouppig.infra.runtime.errors import TransportError
from grouppig.infra.runtime.laya_system1 import LayaSystemOneTransport, choice_question
from grouppig.infra.runtime.transport import resolve_api_key

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPO_ROOT / "config" / "grouppig.toml"

#: 仓库配置里的内网 LAY A 端点：所有用例都必须把它改写掉，绝不真的连它。
INTERNAL_LAYA_BASE_URL = "http://127.0.0.1:7780"
REAL_A6API_BASE_URL = "https://api.a6api.com/v1"

#: 注入的假密钥（不是真实凭据，只用于验证 Authorization 头怎么拼）。
FAKE_KEY = "sk-fake-laya-not-a-real-secret"

LABELS = ("提问", "闲聊")

#: 本地回环下 classify 的耗时上限。放宽到 2s 只为挡住「又退回几十秒」的回归。
LATENCY_BUDGET_SECONDS = 2.0

#: 「LAY A 那一跳确实发生过」的有界观察窗口（**次要**防线，主修见 CLIENT_TIMEOUT_SECONDS）。
#: 客户端超时不会取消 handler 线程，所以「transport 超时」与「handler 落账」之间没有同步；
#: 这里把它变成有界等待：正常立即返回，只有真的没发出请求才会等到超时 —— 鉴别力不变。
#: 取值远大于「handler 被调度」所需的量级，避免把调度抖动重新引进用例。
#: 2.0s 已是「24 路并发下实测最差送达耗时」（约 200ms）的 10 倍，正常路径命中即返回；
#: 负控指向死端口时永远收不到请求，必然等满整个窗口，所以窗口不能白给太长（原 10.0s 每次白等 8s）。
REQUEST_WAIT_SECONDS = 2.0

#: 只给 ``wait_for_requests`` 的**单元**用例用的短窗口。
#: 那些用例验证的是「等满之后返回什么」，用 REQUEST_WAIT_SECONDS 只会让用例白慢好几倍；
#: （这里刻意不写死数值：数值会随常量调整而过时，参考常量本身即可。）
#: 鉴别力负控仍用 REQUEST_WAIT_SECONDS，与主用例保持同口径。
WAIT_PROBE_SECONDS = 0.3

#: 「挂住」场景的两个时间必须拉开，否则测到的是别的东西。
#: 实测「一个请求送达假服务端」耗时：空载约 47ms，24 路并发下约 70–200ms。
#: 客户端超时若贴近这个量级（本用例原值 0.25s），高并发下客户端会在**请求写出去之前**就放弃，
#: 服务端一次都收不到 —— 那测到的是「没发出去」，不是「服务端挂住」，断言 hits>=1 自然假失败。
#: 所以：客户端超时取最差送达耗时的 ~8 倍，服务端挂起再大于客户端超时（保证客户端先放弃）。
CLIENT_TIMEOUT_SECONDS = 1.5
SERVER_HANG_SECONDS = 4.0

#: 允许被解析的地址：假服务端只监听 127.0.0.1。
_LOOPBACK = frozenset({"127.0.0.1", "localhost", "::1", "0.0.0.0", ""})


# ---- 离线闸门 ----------------------------------------------------------
class ExternalNetworkBlocked(RuntimeError):
    """守卫专用异常：用例试图连接非本机地址。"""


def _blocked(host: Any) -> None:
    if str(host) not in _LOOPBACK:
        raise ExternalNetworkBlocked(f"用例试图访问非本机地址：{host}")


@contextlib.contextmanager
def loopback_only() -> Iterator[None]:
    """把非本机地址的连接掐断，用来证明用例真的没碰内网端点。

    两处都要拦，因为**只拦一处不够**（实测）：

    * ``socket.getaddrinfo`` —— 域名要解析，拦它挡得住 api.a6api.com；
    * ``socket.socket.connect`` —— 字面量 IP **不走 DNS**，httpx 直接构造地址就连接，
      而 LAY A 端点用的是**字面量** ``127.0.0.1``。只拦 getaddrinfo 时它会真连上去
      （实测返回 401），闸门形同虚设。

    注意异常可能被 anyio 的 TaskGroup 包成 ExceptionGroup，断言时要用
    ``_blocked_messages()`` 把嵌套原因摊平。
    """

    real_getaddrinfo = socket.getaddrinfo
    real_connect = socket.socket.connect

    def guarded_getaddrinfo(host: Any, port: Any, *args: Any, **kwargs: Any) -> Any:
        _blocked(host)
        return real_getaddrinfo(host, port, *args, **kwargs)

    def guarded_connect(self: Any, address: Any, *args: Any, **kwargs: Any) -> Any:
        _blocked(address[0] if isinstance(address, tuple) else address)
        return real_connect(self, address, *args, **kwargs)

    with (
        mock.patch.object(socket, "getaddrinfo", guarded_getaddrinfo),
        mock.patch.object(socket.socket, "connect", guarded_connect),
    ):
        yield


def _blocked_messages(exc: BaseException) -> list[str]:
    """把 ExceptionGroup / 异常链摊平成消息列表（anyio 会把底层异常包一层）。"""

    found = [str(exc)]
    for sub in getattr(exc, "exceptions", ()) or ():
        found.extend(_blocked_messages(sub))
    if exc.__cause__ is not None:
        found.extend(_blocked_messages(exc.__cause__))
    return found


# ---- 配置改写 ----------------------------------------------------------
def _line(url: str) -> str:
    """生成配置里的一行 ``base_url = "..."``（用 json.dumps 引号，避免手写转义）。"""

    return "base_url = " + json.dumps(url)


def config_text(*, base_url: str, timeout: float = 5.0, max_attempts: int = 1) -> str:
    """把仓库配置改写成「全部指向本机假服务端 + 毫秒级重试」。"""

    text = CONFIG_PATH.read_text(encoding="utf-8")
    # 每个被替换的片段都必须在仓库配置里唯一存在，否则说明配置漂移、用例会假绿
    replacements = [
        (_line(INTERNAL_LAYA_BASE_URL), _line(base_url)),
        (_line(REAL_A6API_BASE_URL), _line(base_url)),
        ("max_attempts = 3", f"max_attempts = {int(max_attempts)}"),
        # 失败路径必须毫秒级结束：默认 base_delay=0.5 会让降级用例又慢又抖
        ("base_delay = 0.5", "base_delay = 0.0"),
        ("jitter = 0.1", "jitter = 0.0"),
        ("timeout = 30.0", f"timeout = {float(timeout)}"),
    ]
    for needle, _replacement in replacements:
        assert text.count(needle) == 1, f"仓库配置里 {needle!r} 出现 {text.count(needle)} 次，无法安全改写"
    for needle, replacement in replacements:
        text = text.replace(needle, replacement)
    return text


@pytest.fixture(autouse=True)
def _restore_global_singletons() -> Iterator[None]:
    """``Container.start()`` 会绑定全局单例（router / logger / meter）。

    本模块反复起容器，用完必须还原：否则最后一个容器（指向已经停掉的假服务端）会留在
    全局，污染后续测试文件 —— 那正是「我的用例把别的域搞红」的经典形态。
    三个 setter 都返回旧值，所以先取旧值、结束时写回即可。
    """

    from grouppig.infra import logger as logger_module
    from grouppig.infra.config import loader as config_module
    from grouppig.infra.model_gateway import router as router_module
    from grouppig.infra.token_budget import meter as meter_module

    # 直接读写模块级变量，不走 get_*()：那些 getter 在 None 时会**顺手造一个默认实例**
    # （绑定真实配置），于是「还原」反而变成了「把 None 换成一个真端点实例」，照样污染。
    # config 也要还原：Container.start() 会 set_config(自己的临时配置)，
    # 不还原的话后续文件读到的是指向已停假服务端的配置。
    previous = (router_module._router, logger_module._default, meter_module._meter, config_module._current)
    try:
        yield
    finally:
        router_module._router = previous[0]
        logger_module._default = previous[1]
        meter_module._meter = previous[2]
        config_module._current = previous[3]


@pytest.fixture(autouse=True)
def _offline_guard() -> Iterator[None]:
    """整个模块自动生效：任何非本机连接都会被掐断。

    不依赖每个用例自觉 ``with loopback_only()`` —— 漏写一个就等于放它去连内网端点，
    而 LAY A 端点在改到本机之前是内网字面量 IP，实测是**通的**（返回 401），漏写不会被发现。
    假服务端只监听 127.0.0.1，不受影响。
    """

    with loopback_only():
        yield


@pytest.fixture
def server() -> Iterator[FakeLayaServer]:
    instance = FakeLayaServer(label="提问").start()
    try:
        yield instance
    finally:
        instance.stop()


@pytest.fixture
def make_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, server: FakeLayaServer):
    """返回 ``build(**kwargs) -> Config``：config 指向假服务端，密钥是注入的假值。"""

    monkeypatch.setenv("GROUPPIG_LAYA_API_KEY", FAKE_KEY)
    monkeypatch.setenv("GROUPPIG_MODEL_API_KEY", FAKE_KEY)

    def build(*, base_url: str | None = None, timeout: float = 5.0, max_attempts: int = 1) -> Any:
        path = tmp_path / "grouppig.toml"
        path.write_text(
            config_text(base_url=base_url or server.base_url, timeout=timeout, max_attempts=max_attempts),
            encoding="utf-8",
        )
        return load_config(path, use_env=False, use_local=False)

    return build


def transport_for(server: FakeLayaServer, *, timeout: float = 5.0) -> LayaSystemOneTransport:
    return LayaSystemOneTransport(base_url=server.base_url, api_key=FAKE_KEY, timeout=timeout, provider="laya")


def one_question_body(label: str = "label") -> dict[str, Any]:
    return {
        "state": {"text": "在吗"},
        "questions": {label: choice_question("这句话在做什么？", dict.fromkeys(LABELS, "x"))},
        "model": "auto",
    }


def apply_failure(server: FakeLayaServer, failure: str) -> None:
    """把可编程故障装到假服务端上（供降级用例参数化）。"""

    if failure == "status401":
        server.fail_with(401)
    elif failure == "status422":
        server.fail_with(422)
    elif failure == "status503":
        server.fail_with(503)
    elif failure == "non_json":
        server.respond_non_json()
    elif failure == "timeout":
        server.hang(SERVER_HANG_SECONDS)
    else:  # pragma: no cover - 参数表写错时立刻炸，而不是静默跳过
        raise AssertionError(f"未知的故障模式：{failure}")


@contextlib.contextmanager
def dead_loopback_port() -> Iterator[str]:
    """占住一个回环端口但**不监听**，返回指向它的 base_url。

    连上去会被立刻拒绝（实测 0.05ms 的 ``ConnectionRefusedError``），而不是挂住超时 ——
    这正是「LAY A 那一跳从未发生」所需的最干净形态：不是慢，是根本没送到。

    用「仍持有的 socket」而不是「绑定后释放」：释放过的端口可能被别的进程抢走，
    那样负控就退化成「连到了别人」而不是「连不上」，结论不再成立。
    """

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        yield f"http://127.0.0.1:{sock.getsockname()[1]}"
    finally:
        sock.close()


async def classify_with_fallback(config: Any, *, reply: str = "提问") -> tuple[dict[str, Any], StubOpenAITransport]:
    """跑一次「LAY A 故障 → 回落 OpenAI 兼容服务商」的 classify，返回 (结果, 回落替身)。

    主用例（④）与鉴别力负控**共用这一个函数**：两条用例的唯一差别是 LAY A 指向哪里
    （活的假服务端 vs 刚释放的死端口），所以负控证明的确实是主用例的同一件事。
    把这段抽出来而不是各写一遍，就是为了让「同口径」由构造保证，而不是靠人肉对齐。
    """

    container = await build_container(config=config).start()
    fallback = StubOpenAITransport(reply=reply)
    # 必须注入 "*"：ModelRouter.__post_init__ 已经用 build_transport 填了 "*"，
    # 而 _transport() 对非 laya/local 的 provider 优先返回 "*"，注入 "a6api" 会被它抢先。
    container.router.set_transport(fallback)
    try:
        with loopback_only():
            result = await container.call("rpc:model.classify", "今晚打本吗", list(LABELS))
    finally:
        await container.aclose()
    return result, fallback


# ---- 离线保证 ----------------------------------------------------------
def test_repo_config_endpoints_are_the_declared_ones() -> None:
    """守住改写目标：仓库配置里每个**真实**端点都必须与下面两个常量逐字一致。

    `config_text()` 是「把真实端点替换成假服务端」，替换目标一旦漂移就会静默失败 ——
    配置改了、常量没改，改写就不发生，用例会真的去连真实服务，而且**看起来还是绿的**。
    所以这里比的是「配置里的有效值 == 常量」，不是「文件里出现过这个子串」：
    注释里留一句旧地址也能让子串断言通过。
    """

    config = load_config(CONFIG_PATH, use_env=False, use_local=False)
    laya = str(config.section("model.providers.laya").get("base_url") or "")
    a6api = str(config.section("model.providers.a6api").get("base_url") or "")
    assert laya == INTERNAL_LAYA_BASE_URL, f"LAY A 端点已漂移：配置={laya} 常量={INTERNAL_LAYA_BASE_URL}"
    assert a6api == REAL_A6API_BASE_URL, f"a6api 端点已漂移：配置={a6api} 常量={REAL_A6API_BASE_URL}"


def test_laya_endpoint_is_loopback_and_that_is_a_known_compromise() -> None:
    """如实记录一个已知折衷：LAY A 端点就在本机回环上。

    离线保证的骨架是：`config_text()` 把仓库端点换成 `127.0.0.1:<假服务端>`，再用 `loopback_only()`
    掐断一切非本机连接。原设计假设「仓库端点不是本机」，这样闸门内外的差别才证明得了「没碰真服务」。
    实际部署里 LAY A 服务只监听 `127.0.0.1:7780`（填局域网 IP 连不上，实测超时 → 降级 → 93 秒），
    端点与假服务端同机，这条差别消失：改写前后逐字相等，闸门确实拦不住「真连回环上那个服务」。

    **代价与兜底**：剩余保护是 `test_repo_config_endpoints_are_the_declared_ones` 守住「常量 == 配置」，
    ——它能抓住「配置改了常量没改」这类漂移，但抓不住「配置本身就是回环」。
    哪天 LAY A 挪到别的机器上，**这条用例会红**，那时应当把它改回「必须是非回环」的反向断言。
    """

    provider = load_config(CONFIG_PATH, use_env=False, use_local=False).section("model.providers.laya")
    base = str(provider.get("base_url") or "")
    assert base, "LAY A 端点不该为空"
    host = base.split("://", 1)[-1].split("/", 1)[0].rsplit(":", 1)[0].strip("[]")
    assert host in {"127.0.0.1", "localhost", "::1"}, (
        f"LAY A 端点 {base} 不再是本机回环 —— 离线保证可以恢复成「必须是非回环」的反向断言了"
    )


def test_effective_config_has_no_external_endpoint(make_config) -> None:
    """用例实际使用的配置里，所有 provider 都指向本机。"""

    config = make_config()
    for provider in ("laya", "a6api", "deepseek", "local"):
        base = str(config.section(f"model.providers.{provider}").get("base_url") or "")
        # 假服务端端口随用例分配，只能断言「不是仓库里的真实端点」
        assert INTERNAL_LAYA_BASE_URL not in base, provider
        assert "api.a6api.com" not in base, provider
        # 未配置的 provider 允许留空（本地嵌入用 "local" 标记）
        assert base in ("", "local") or base.startswith("http://127.0.0.1:"), (provider, base)
    assert config.get("model.tasks.classify.provider") == "laya"
    assert config.get("model.tasks.classify.fallback_models") == ["grok-4.6"]


def test_injected_api_key_is_used_and_is_not_a_real_secret(make_config) -> None:
    """链路里用的密钥是注入的假值：既证明不打真实凭据，也证明 Authorization 怎么拼。"""

    config = make_config()
    assert resolve_api_key(config, "laya") == FAKE_KEY
    assert FAKE_KEY.startswith("sk-fake-")


def test_loopback_guard_rejects_external_hosts() -> None:
    """先证明这道闸本身有效，否则下面「在闸里跑通」的结论没有意义。"""

    with loopback_only():
        assert socket.getaddrinfo("127.0.0.1", 80)
        with pytest.raises(ExternalNetworkBlocked, match="非本机地址"):
            socket.getaddrinfo("api.a6api.com", 443)
        with pytest.raises(ExternalNetworkBlocked, match="非本机地址"):
            # 外部样本用文档保留地址（RFC 5737）：本机端口会随配置漂移，写死它等于把闸门测试绑在配置上。
            socket.socket().connect(("203.0.113.7", 443))


async def test_loopback_guard_actually_intercepts_httpx() -> None:
    """证明闸门拦得住真实代码路径：httpx 连内网字面量 IP 会被掐断。

    这条用例的价值在于「字面量 IP 不走 DNS」——只拦 getaddrinfo 的话，
    对字面量 IP 完全无效（改到本机前实测真连上去并拿到 401）。
    """

    with loopback_only(), pytest.raises(BaseException) as excinfo:
        async with httpx.AsyncClient(timeout=1.0) as client:
            await client.post("http://203.0.113.7:443/v1/systemone", json={})
    assert any("非本机地址" in message for message in _blocked_messages(excinfo.value))


async def test_loopback_guard_still_allows_the_fake_server(server: FakeLayaServer) -> None:
    """闸门只挡外网：本机假服务端在闸门内照常工作。"""

    transport = transport_for(server)
    try:
        with loopback_only():
            data = await transport.complete(one_question_body())
    finally:
        await transport.aclose()
    assert data["answers"]["label"]["choice"] == "提问"


# ---- ① 假服务端：协议与可编程故障 ---------------------------------------
async def test_fake_server_serves_systemone_protocol(server: FakeLayaServer) -> None:
    """假服务端实现 POST /v1/systemone，并按 LAY A 结构作答。"""

    transport = transport_for(server)
    try:
        data = await transport.complete(one_question_body())
    finally:
        await transport.aclose()

    assert server.hits == 1
    assert server.paths == [SYSTEM_ONE_PATH]
    assert server.auth_headers == [f"Bearer {FAKE_KEY}"]
    assert data["answers"]["label"]["choice"] == "提问"
    assert data["answers"]["label"]["type"] == "choice"
    assert data["answers"]["label"]["probabilities"] == {"提问": 0.72, "闲聊": 0.28}
    assert data["usage"]["input_tokens"] == 42
    assert data["routing"]["provider"] == "laya"
    # 请求体是传输层翻译后的 LAY A 形状（不是 OpenAI 的 messages）
    assert set(server.last_body) == {"state", "questions", "model"}
    assert server.models == ["auto"]
    assert server.question_counts == [1]


@pytest.mark.parametrize(("status", "retryable"), [(401, False), (422, False), (503, True)])
async def test_fake_server_returns_programmable_http_errors(
    server: FakeLayaServer, status: int, retryable: bool
) -> None:
    """可编程返回 401 / 422 / 503，并按 LAY A 的错误语义标注可重试性。"""

    server.fail_with(status)
    transport = transport_for(server)
    try:
        with pytest.raises(TransportError) as excinfo:
            await transport.complete(one_question_body())
    finally:
        await transport.aclose()

    assert excinfo.value.status == status
    assert excinfo.value.retryable is retryable
    assert server.hits == 1


async def test_fake_server_can_return_non_json(server: FakeLayaServer) -> None:
    """可编程返回非 JSON 响应体：不可重试（重试也只会拿到同样的 HTML）。"""

    server.respond_non_json("<html><body>502 Bad Gateway</body></html>")
    transport = transport_for(server)
    try:
        with pytest.raises(TransportError) as excinfo:
            await transport.complete(one_question_body())
    finally:
        await transport.aclose()

    assert excinfo.value.retryable is False
    assert "不是合法 JSON" in str(excinfo.value)
    assert server.hits == 1


async def test_fake_server_can_hang_to_trigger_client_timeout(server: FakeLayaServer) -> None:
    """可编程「挂住不响应」：客户端先超时，且超时被标为可重试。"""

    server.hang(SERVER_HANG_SECONDS)
    transport = transport_for(server, timeout=CLIENT_TIMEOUT_SECONDS)
    started = time.perf_counter()
    try:
        with pytest.raises(TransportError) as excinfo:
            await transport.complete(one_question_body())
    finally:
        await transport.aclose()
    elapsed = time.perf_counter() - started

    assert excinfo.value.retryable is True
    assert "超时" in str(excinfo.value)
    # 客户端超时必须先于服务端的挂起触发
    assert elapsed < SERVER_HANG_SECONDS, elapsed
    # 次要防线：超时**不取消** handler 线程，「已落账」与「断言」之间没有同步。
    # 主修是上面把客户端超时与挂起时长拉开，保证请求真的发得出去。
    assert server.wait_for_requests(1, timeout=REQUEST_WAIT_SECONDS) == 1, "超时后服务端应当已经收到这次请求"


async def test_fake_server_failure_is_scoped_to_n_requests(server: FakeLayaServer) -> None:
    """``times=N`` 只影响前 N 次请求，用于测「先失败后成功」。"""

    server.fail_with(503, times=2)
    transport = transport_for(server)
    try:
        for _ in range(2):
            with pytest.raises(TransportError):
                await transport.complete(one_question_body())
        data = await transport.complete(one_question_body())
    finally:
        await transport.aclose()

    assert server.hits == 3
    assert data["answers"]["label"]["choice"] == "提问"


async def test_fake_server_answers_every_question_type(server: FakeLayaServer) -> None:
    """choice / score / noul 三种原语都能作答（system1 会同时用到）。"""

    server.set_label(None)  # 不指定标签 → 走「按 criteria 取第一个候选」的派生路径
    body = {
        "state": "在吗",
        "questions": {
            "pick": {"type": "choice", "instructions": "选一个", "criteria": {"A": "a", "B": "b"}},
            "how": {"type": "score", "instructions": "打分", "criteria": ["0", "10"]},
            "unknown": {"type": "noul", "instructions": "判断不了就说 noul"},
        },
        "model": "auto",
    }
    transport = transport_for(server)
    try:
        data = await transport.complete(body)
    finally:
        await transport.aclose()

    assert data["answers"]["pick"]["type"] == "choice"
    assert data["answers"]["pick"]["choice"] == "A"
    assert data["answers"]["how"]["type"] == "score"
    assert data["answers"]["how"]["score"] == 0.5
    assert data["answers"]["unknown"]["type"] == "noul"
    assert server.hits == 1


async def test_fake_server_answers_classify_shaped_payload(server: FakeLayaServer) -> None:
    """OpenAI 兼容形状（候选标签写在提示词里）也能被假服务端正确理解。"""

    payload = encode_request(
        ModelRequest(
            task="classify", model="auto", messages=({"role": "user", "content": "今晚打本吗"},), labels=LABELS
        )
    )
    transport = transport_for(server)
    try:
        data = await transport.complete(payload)
    finally:
        await transport.aclose()

    assert server.hits == 1
    assert server.question_counts == [1]
    assert data["answers"]["label"]["choice"] == "提问"


# ---- ② 端到端：classify / system1 全链路 --------------------------------
async def test_classify_end_to_end_through_rpc(make_config, server: FakeLayaServer) -> None:
    """config 指向假服务端后，rpc:model.classify 走完整链路且只发 1 次 HTTP。"""

    config = make_config()
    container = await build_container(config=config).start()
    try:
        with loopback_only():
            result = await container.call("rpc:model.classify", "今晚打本吗", list(LABELS))
    finally:
        await container.aclose()

    assert result["task"] == "classify"
    assert result["provider"] == "laya"
    assert result["label"] == "提问"
    assert result["scores"] == {"提问": 0.72, "闲聊": 0.28}
    assert result["usage"]["total_tokens"] == 42
    assert server.hits == 1
    assert server.paths == [SYSTEM_ONE_PATH]
    assert server.auth_headers == [f"Bearer {FAKE_KEY}"]
    assert server.models == ["auto"]


def system1_questions(count: int = 6) -> dict[str, dict[str, Any]]:
    return {
        f"q{index}": {
            "type": "choice",
            "instructions": f"第 {index} 个问题",
            "criteria": {"A": "选 A", "B": "选 B"},
        }
        for index in range(count)
    }


async def test_system1_six_questions_produce_exactly_one_http_request(make_config, server: FakeLayaServer) -> None:
    """6 个以上问题只产生 1 次 HTTP 请求（服务端计数断言）。"""

    questions = system1_questions(6)
    config = make_config()
    container = await build_container(config=config).start()
    try:
        with loopback_only():
            result = await container.call("rpc:model.system1", {"text": "在吗"}, questions)
    finally:
        await container.aclose()

    assert server.hits == 1, f"6 个问题发了 {server.hits} 次请求"
    assert server.question_counts == [6]
    assert server.paths == [SYSTEM_ONE_PATH]
    assert result["task"] == "system1"
    assert result["source"] == "system1"
    assert set(result["answers"]) == set(questions)
    assert len(result["confidences"]) == 6
    assert result["aggregate"] == "min"
    assert result["lowest_confidence"] == 0.72


async def test_system1_twelve_questions_still_one_request(make_config, server: FakeLayaServer) -> None:
    """问题数翻倍也不多发请求（逐问调用等于自毁）。"""

    questions = system1_questions(12)
    config = make_config()
    container = await build_container(config=config).start()
    try:
        with loopback_only():
            result = await container.call("rpc:model.system1", "在吗", questions)
    finally:
        await container.aclose()

    assert server.hits == 1
    assert server.question_counts == [12]
    assert len(result["answers"]) == 12


# ---- ③ 延迟回归 --------------------------------------------------------
async def test_classify_latency_on_loopback_is_under_budget(make_config, server: FakeLayaServer) -> None:
    """本地回环下 classify 路径耗时 < 2s。

    仓库配置原本指向内网端点，未配密钥时「超时 → 降级」实测 51–60 秒；本用例用假服务端
    把这条路径变成纯回环，阈值放宽到 2s 只为挡住「又变回几十秒」，不做性能门禁。
    """

    config = make_config()
    container = await build_container(config=config).start()
    try:
        with loopback_only():
            started = time.perf_counter()
            result = await container.call("rpc:model.classify", "今晚打本吗", list(LABELS))
            elapsed = time.perf_counter() - started
    finally:
        await container.aclose()

    assert result["label"] == "提问"
    assert server.hits == 1
    assert elapsed < LATENCY_BUDGET_SECONDS, f"classify 耗时 {elapsed:.3f}s，超过 {LATENCY_BUDGET_SECONDS}s"


# ---- ④ 降级：LAY A 故障 → 回落 grok-4.6 --------------------------------
@pytest.mark.parametrize("failure", ["status401", "status422", "status503", "timeout", "non_json"])
async def test_classify_falls_back_to_grok_when_laya_fails(make_config, server: FakeLayaServer, failure: str) -> None:
    """LAY A 返回 401/422/503/超时/非 JSON 时，classify 回落 grok-4.6 且不抛错。

    回落那一跳用 ``StubOpenAITransport`` 替代真实网络：它是 OpenAI 兼容形状，
    正是「变更前」classify 走的传输层。
    """

    apply_failure(server, failure)
    # 只有「挂住」这一种需要把客户端超时压到服务端挂起之下；
    # 但压得不能贴近「送达耗时」，否则客户端会在请求发出去之前就放弃（见常量处注释）。
    config = make_config(timeout=CLIENT_TIMEOUT_SECONDS if failure == "timeout" else 5.0)
    # 与鉴别力负控共用同一个函数：两条用例的唯一差别是 LAY A 指向哪里，
    # 于是「同口径」由构造保证，而不是靠人肉对齐两组时序常量。
    result, fallback = await classify_with_fallback(config)

    # 不抛错，且结果来自降级目标而不是 LAY A
    assert result["provider"] == "a6api"
    assert result["model"] == "grok-4.6"
    assert result["label"] == "提问"
    assert len(fallback.calls) == 1
    assert fallback.paths == ["/chat/completions"]
    # LAY A 那一跳确实被尝试过（否则测的就不是降级）。
    # 主修：客户端超时从 0.25s 拉到 CLIENT_TIMEOUT_SECONDS —— 0.25s 贴近「请求送达」耗时
    # （实测空载 47ms / 24 路并发 70–200ms），客户端会先放弃，请求根本没发出去。
    # 这里再加一层有界等待，兜住「超时不取消 handler 线程」带来的残余不同步。
    observed = server.wait_for_requests(1, timeout=REQUEST_WAIT_SECONDS)
    assert observed >= 1, (
        f"LAY A 那一跳没有被观察到（failure={failure}）：等待 {REQUEST_WAIT_SECONDS}s 内服务端一次请求都没收到"
    )


async def test_hits_assertion_is_the_only_proof_that_laya_was_tried(make_config, server: FakeLayaServer) -> None:
    """鉴别力负控（常驻）：LAY A 那一跳**真的没发生**时，只有 hits 断言会失败。

    把 LAY A 指向一个刚占住的死端口（连接被立刻拒绝，不是慢），其余一切与主用例逐字相同 ——
    同一个 ``classify_with_fallback``、同一组 ``CLIENT_TIMEOUT_SECONDS`` / ``REQUEST_WAIT_SECONDS``。

    于是前半段的断言（provider / model / fallback.calls / paths）**全部照过**。这正好说明：
    主用例里那条 hits 断言是**唯一**还能证明「LAY A 那一跳被尝试过」的一环。
    哪天有人把它删掉或放宽，降级用例就会退化成「只要回落成功就算过」——
    连「请求压根没发出去」这种情况也会判绿。这条用例就是那件事的可执行守门人。
    """

    with dead_loopback_port() as dead_url:
        config = make_config(base_url=dead_url)
        result, fallback = await classify_with_fallback(config)

    # ── 前半段：与主用例逐字相同的断言，在「LAY A 从未被尝试」时必须**全部成立** ──
    assert result["provider"] == "a6api"
    assert result["model"] == "grok-4.6"
    assert result["label"] == "提问"
    assert len(fallback.calls) == 1
    assert fallback.paths == ["/chat/completions"]

    # ── 后半段：服务端一次都没收到（这才是与主用例唯一的差别） ──
    observed = server.wait_for_requests(1, timeout=REQUEST_WAIT_SECONDS)
    assert observed == 0, f"死端口不该有请求送达，却观察到 {observed} 次"
    assert server.hits == 0

    # ── 把主用例的 hits 断言原样抄一遍，确认它**确实**会失败 ──
    # 这条 pytest.raises 就是「删掉/放宽 hits 断言 = 丢掉唯一鉴别力」的可执行证明：
    # 上面的断言全过，只有它会炸。
    with pytest.raises(AssertionError, match="LAY A 那一跳没有被观察到"):
        assert observed >= 1, "LAY A 那一跳没有被观察到"


async def test_no_fallback_when_laya_is_healthy(make_config, server: FakeLayaServer) -> None:
    """反向对照：LAY A 正常时不发生降级（证明上面的回落是故障触发的，不是配置写错）。"""

    config = make_config()
    container = await build_container(config=config).start()
    fallback = StubOpenAITransport(reply="闲聊")
    container.router.set_transport(fallback)
    try:
        with loopback_only():
            result = await container.call("rpc:model.classify", "今晚打本吗", list(LABELS))
    finally:
        await container.aclose()

    assert result["provider"] == "laya"
    assert result["label"] == "提问"
    assert fallback.calls == []


async def test_system1_escalates_instead_of_raising_when_laya_is_unavailable(
    make_config, server: FakeLayaServer
) -> None:
    """LAY A 不可用时 system1 升级到对话模型，不把底层异常透给调用方。"""

    server.fail_with(503)
    config = make_config()
    container = await build_container(config=config).start()
    escalation = StubOpenAITransport(reply="A")
    container.router.set_transport(escalation)
    try:
        with loopback_only():
            result = await container.call("rpc:model.system1", {"text": "在吗"}, system1_questions(3))
    finally:
        await container.aclose()

    assert result["source"] == "escalated"
    assert result["escalated_to"]["provider"] == "a6api"
    assert result["escalated_to"]["model"] == config.get("model.tasks.chat.model")
    assert len(escalation.calls) == 1
    assert server.hits == 1


# ---- ⑤ 未回归：行为分类与打断决策的判定结果 -----------------------------
async def _judge_and_decide(config: Any, transport: Any, provider: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """跑「行为分类 → 打断决策」两级判定，模型侧可换成任意传输层。"""

    from perception_helpers import GROUP_ID, burst, make_container, make_perception

    container = make_container(config)
    await container.start()
    if transport is not None:
        container.router.set_transport(transport, provider=provider)
    perception = make_perception(config, container)
    try:
        judged = await container.call("rpc:behavior.llm.judge", messages=burst(6, text="打本打本"))
        decided = await container.call(
            "rpc:interrupt.decide",
            group_id=GROUP_ID,
            score=0.8,
            score_detail={"band": "high", "components": {"mentioned": 0.0}},
            behavior=judged["label"],
            now=1_700_000_100.0,
        )
        return judged, decided
    finally:
        await perception.aclose()
        await container.aclose()


async def test_behavior_judge_and_interrupt_decision_match_the_previous_provider(
    make_config, server: FakeLayaServer
) -> None:
    """行为分类与打断决策的判定结果与「classify 走 OpenAI 兼容服务商」的旧行为一致。

    先让两条模型路径报告**同一个**分类结果（同标签 flooding + 同置信度 1.0）：LAY A 侧由
    假服务端给出 ``choice=flooding, probabilities={flooding: 1.0}``，旧路径由 OpenAI 兼容替身
    给出同样的文本。然后断言下游两级判定逐字相同 —— 这正是本次接入不该改动的部分。
    """

    config = make_config()
    server.set_label("flooding")
    server.set_probabilities({"flooding": 1.0})

    # 变更后：classify 走 LAY A（假服务端）
    with loopback_only():
        judged_laya, decided_laya = await _judge_and_decide(config, None, "laya")
    # 变更前：classify 走 OpenAI 兼容传输层（替身替代真实网络）
    judged_openai, decided_openai = await _judge_and_decide(config, StubOpenAITransport(reply="flooding"), "laya")

    assert judged_laya["label"] == judged_openai["label"] == "flooding"
    assert judged_laya["raw_label"] == "flooding"
    assert judged_openai["raw_label"] == "flooding"
    assert judged_laya["ok"] is True and judged_openai["ok"] is True
    assert judged_laya["available"] is True and judged_openai["available"] is True
    assert judged_laya["confidence"] == judged_openai["confidence"] == 1.0

    # 打断决策：刷屏场景一律 HOLD，且两条路径给出同一个决策
    assert decided_laya["action"] == decided_openai["action"] == "hold"
    assert decided_laya["reason"] == decided_openai["reason"] == "flooding"
    assert decided_laya["speak"] is False
    assert decided_openai["speak"] is False
    assert decided_laya["score"] == decided_openai["score"] == 0.8
    assert decided_laya["threshold"] == decided_openai["threshold"]
    assert decided_laya["behavior"] == decided_openai["behavior"] == "flooding"


async def test_judge_normalizes_chinese_label_from_laya(make_config, server: FakeLayaServer) -> None:
    """LAY A 回中文标签（刷屏）时，判别器的别名归一仍然生效 → flooding。"""

    config = make_config()
    server.set_label("刷屏")
    with loopback_only():
        judged, decided = await _judge_and_decide(config, None, "laya")

    assert judged["raw_label"] == "刷屏"
    assert judged["label"] == "flooding"
    assert decided["action"] == "hold"
    assert decided["reason"] == "flooding"


async def test_interrupt_decision_is_identical_when_the_model_dies(make_config, server: FakeLayaServer) -> None:
    """打断决策对「模型是否可用」不敏感：同一组输入在模型全挂时给出同一个决策。

    这是 ⑤ 的另一半：决策层本身不消费模型（``rpc:interrupt.decide`` 的输入只有
    score / behavior / cooldown），接入 LAY A 不该改动它。
    """

    from perception_helpers import GROUP_ID, make_container, make_perception

    config = make_config()
    inputs: dict[str, Any] = {
        "group_id": GROUP_ID,
        "score": 0.8,
        "score_detail": {"band": "high", "components": {"mentioned": 0.0}},
        "behavior": "discussion",
        "now": 1_700_000_100.0,
    }

    async def decide_once() -> dict[str, Any]:
        container = make_container(config)
        await container.start()
        perception = make_perception(config, container)
        try:
            return await container.call("rpc:interrupt.decide", **inputs)
        finally:
            await perception.aclose()
            await container.aclose()

    with loopback_only():
        decided_up = await decide_once()

    server.reset()
    server.fail_with(503)
    decided_down = await decide_once()

    # 决策字段逐字相同（downstream 里带调用结果，只比决策本身）
    for key in ("action", "reason", "speak", "score", "threshold", "band", "margin", "behavior"):
        assert decided_up[key] == decided_down[key], key
    assert decided_up["action"] == "speak"


# ---- 假服务端自身的协议形状（供上面用例的断言有据可依）-------------------
def test_fake_server_response_helpers_build_laya_shapes() -> None:
    response = laya_response({"label": choice_answer("提问")})
    assert set(response) == {"model", "answers", "usage", "routing"}
    assert set(response["answers"]["label"]) == {"type", "choice", "probabilities", "confidence", "action"}
    assert set(response["usage"]) == {"input_tokens", "output_tokens"}


# ---- ⑥ wait_for_requests 的单元行为（t32 负控常驻化）----------------------
def _post_systemone(server: FakeLayaServer) -> None:
    """直接向假服务端发一次合法的 LAY A POST（不经过传输层，方便单测断言面）。"""

    httpx.post(server.base_url + SYSTEM_ONE_PATH, json=one_question_body(), timeout=5.0)


def test_wait_for_requests_returns_zero_when_nothing_arrives(server: FakeLayaServer) -> None:
    """① 真的没有请求时：等满有界窗口后返回 0，而且**确实等过**。"""

    started = time.perf_counter()
    observed = server.wait_for_requests(1, timeout=WAIT_PROBE_SECONDS)
    elapsed = time.perf_counter() - started

    assert observed == 0
    assert server.hits == 0
    # 必须真的等满：否则「返回 0」可能只是因为压根没等 —— 那它就是个永远返回 0 的空壳，
    # 主用例里的 hits 断言也就跟着失去意义。
    assert elapsed >= WAIT_PROBE_SECONDS * 0.8, f"只等了 {elapsed:.3f}s，不像是有界等待"


def test_wait_for_requests_returns_immediately_when_the_request_is_already_there(
    server: FakeLayaServer,
) -> None:
    """② 请求已落账时立即返回，不白等一个窗口（主用例的常态路径）。"""

    _post_systemone(server)
    assert server.hits == 1

    started = time.perf_counter()
    observed = server.wait_for_requests(1, timeout=REQUEST_WAIT_SECONDS)
    elapsed = time.perf_counter() - started

    assert observed == 1
    # 窗口是 REQUEST_WAIT_SECONDS，这里 0.5s 的上限留了数倍余量：
    # 既证明「没有白等窗口」，又不会被并发下的线程调度抖红。
    # 同样不写死数值：常量调整后这句话不会变成错的。
    assert elapsed < 0.5, f"已经有请求了却等了 {elapsed:.3f}s"


def test_wait_for_requests_waits_until_the_requested_count(server: FakeLayaServer) -> None:
    """③ count 参数生效：只到 1 个时等 2 个要等满并如实返回 1。"""

    _post_systemone(server)

    started = time.perf_counter()
    observed = server.wait_for_requests(2, timeout=WAIT_PROBE_SECONDS)
    elapsed = time.perf_counter() - started

    assert observed == 1, "只到了 1 个请求，不该报告 2 个"
    assert elapsed >= WAIT_PROBE_SECONDS * 0.8, elapsed

    # 补上第 2 个之后立刻满足
    _post_systemone(server)
    assert server.wait_for_requests(2, timeout=WAIT_PROBE_SECONDS) == 2


def test_wait_for_requests_sees_the_counter_reset(server: FakeLayaServer) -> None:
    """④ reset() 归零后，等待窗口重新从 0 开始数（清空与落账不打架）。"""

    _post_systemone(server)
    assert server.wait_for_requests(1, timeout=WAIT_PROBE_SECONDS) == 1

    server.reset()

    assert server.hits == 0
    assert server.wait_for_requests(1, timeout=WAIT_PROBE_SECONDS) == 0


def test_wait_for_requests_records_the_body_on_the_same_entry(server: FakeLayaServer) -> None:
    """⑤ 先计入、后读体：正文最终要落在**被计入的那一条**记录上。"""

    body = one_question_body()
    httpx.post(server.base_url + SYSTEM_ONE_PATH, json=body, timeout=5.0)
    assert server.wait_for_requests(1, timeout=REQUEST_WAIT_SECONDS) == 1

    # 恰好一条记录，且它的 body 就是发出去的那份 ——
    # 「计数提前」不能变成「正文另起一条」或「正文丢失」。
    assert len(server.requests) == 1
    assert server.requests[0]["body"] == body
    assert server.last_body == body
    assert server.models == ["auto"]
    assert server.question_counts == [1]
