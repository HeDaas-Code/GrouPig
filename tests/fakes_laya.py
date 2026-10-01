"""假 LAY A System-1 服务端（标准库 ``http.server``，端口 0 自动分配，全程离线）。

为什么需要它：仓库配置里的 ``[model.providers.laya].base_url`` 指向内网端点
仓库配置里的 LAY A 端点，任何走到 classify / system1 的用例都会去连它 —— 无密钥时
拿到 401、断网时拿到超时，于是用例要么假绿要么慢到几十秒。本模块提供一个**可编程的**
本机替身，让全套测试不依赖内网端点，也不依赖真实密钥。

设计要点：

* **只用标准库**：``ThreadingHTTPServer`` 每个请求一个线程，所以「挂住不响应」的用例
  不会把后续请求一起堵死（单线程 ``HTTPServer`` 会）。
* **响应按请求体生成**，而不是返回固定桩：请求里有 ``questions`` 就逐题作答；否则按
  ``classify`` 的 OpenAI 兼容形状（候选标签写在提示词里）用传输层自己的翻译函数反解。
  这样假服务端与真服务端理解的是同一份协议，用例才能真正验证翻译层。
* **可编程故障**：HTTP 状态码、非 JSON 响应体、挂住（触发客户端超时），都支持
  ``times=N`` 只作用前 N 次请求，便于测「先失败后成功」。
* **可断言**：记录每次请求的方法 / 路径 / 头 / 请求体，并给出 ``hits``、``paths``、
  ``auth_headers``、``question_counts``、``models``、``states`` 等便捷视图。
"""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Mapping, Sequence
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from grouppig.infra.runtime import laya_system1 as laya

#: LAY A 唯一端点。
SYSTEM_ONE_PATH = laya.SYSTEM_ONE_PATH

#: 假服务端默认返回的协议枚举（必须在 LAY A 的 model 白名单里）。
DEFAULT_MODEL = "typed-decisions"


def choice_answer(
    label: str,
    *,
    confidence: float = 0.72,
    probabilities: Mapping[str, float] | None = None,
) -> dict[str, Any]:
    """choice 原语答案（与 LAY A 协议同形）。"""

    return {
        "type": "choice",
        "choice": label,
        "probabilities": dict(probabilities or {label: confidence}),
        "confidence": confidence,
        "action": {"kind": "label"},
    }


def score_answer(value: float, *, confidence: float = 0.51) -> dict[str, Any]:
    """score 原语答案。"""

    return {"type": "score", "score": float(value), "probabilities": {}, "confidence": confidence}


def noul_answer(*, confidence: float = 0.9) -> dict[str, Any]:
    """noul（无法判断）原语答案。"""

    return {"type": "noul", "noul": True, "probabilities": {}, "confidence": confidence}


def laya_response(
    answers: Mapping[str, Mapping[str, Any]],
    *,
    model: str = DEFAULT_MODEL,
    input_tokens: int = 42,
    output_tokens: int = 0,
) -> dict[str, Any]:
    """组装一份完整的 LAY A 响应体。"""

    return {
        "model": model,
        "answers": {str(qid): dict(item) for qid, item in answers.items()},
        "usage": {"input_tokens": int(input_tokens), "output_tokens": int(output_tokens)},
        "routing": {"provider": "laya", "ms": 12},
    }


class _Handler(BaseHTTPRequestHandler):
    """只服务 POST /v1/systemone；记录请求，然后按可编程动作作答。"""

    protocol_version = "HTTP/1.1"
    server_version = "FakeLaya/1.0"

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler 的接口命名
        fake: FakeLayaServer = self.server.fake  # type: ignore[attr-defined]
        body = fake._record(self)
        action = fake._next_action()
        if action and action.startswith("fail:"):
            status = int(action.split(":", 1)[1])
            self._send(status, json.dumps({"error": f"fake failure {status}"}).encode("utf-8"), "application/json")
            return
        if action == "nonjson":
            self._send(200, fake.non_json_text.encode("utf-8"), "text/html")
            return
        if action == "hang":
            # 挂住不响应：客户端会先超时。线程是 daemon，stop() 不会被它拖住。
            time.sleep(fake.hang_seconds)
        elif fake.delay:
            time.sleep(fake.delay)
        payload = fake.response_for(body)
        self._send(200, json.dumps(payload, ensure_ascii=False).encode("utf-8"), "application/json")

    def do_GET(self) -> None:  # noqa: N802 - 便于排障时探活
        self._send(405, b'{"error": "only POST /v1/systemone"}', "application/json")

    def log_message(self, *args: Any) -> None:
        """静音：默认实现会往 stderr 打日志，污染 pytest 输出。"""

    def _send(self, status: int, blob: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(blob)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(blob)


class _Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class FakeLayaServer:
    """可编程的 LAY A 替身。

    典型用法::

        server = FakeLayaServer(label="提问").start()
        try:
            server.fail_with(503, times=2)   # 前两次 503，之后恢复正常
            ...
        finally:
            server.stop()
    """

    def __init__(
        self,
        *,
        label: str | None = None,
        model: str = DEFAULT_MODEL,
        confidence: float = 0.72,
        score: float = 0.5,
        input_tokens: int = 42,
        output_tokens: int = 0,
    ) -> None:
        self.label = label
        #: 显式指定的概率分布；``None`` 表示按 criteria 自动铺（选中 0.72 / 其余平分 0.28）。
        self.probabilities: dict[str, float] | None = None
        self.model = model
        self.confidence = float(confidence)
        self.score = float(score)
        self.input_tokens = int(input_tokens)
        self.output_tokens = int(output_tokens)
        self.delay = 0.0
        self.hang_seconds = 2.0
        self.non_json_text = "<html><body>not json</body></html>"
        self.requests: list[dict[str, Any]] = []
        #: 保护 requests 并在每次落账时唤醒 ``wait_for_requests`` 的等待者。
        #: 为什么需要：客户端超时**不会取消 handler 线程**，而该线程何时被调度取决于系统负载。
        #: 高并发下「transport 超时」可能先于「handler 被调度」发生，紧接着的
        #: ``assert server.hits >= 1`` 就会读到 0 —— 那是断言与落账之间缺少同步，不是真的没发请求。
        self._recorded = threading.Condition()
        self._pending: list[str] = []
        self._always: str | None = None
        self._server: _Server | None = None
        self._thread: threading.Thread | None = None

    # ---- 生命周期 ------------------------------------------------------
    def start(self) -> FakeLayaServer:
        """在 127.0.0.1 上绑定端口 0（由内核分配），后台线程里 serve_forever。"""

        server = _Server(("127.0.0.1", 0), _Handler)
        server.fake = self  # type: ignore[attr-defined]
        self._server = server
        # poll_interval 默认 0.5s，会让每次 stop() 白等最多半秒（30 个用例就是 15 秒）。
        # 调到 0.05s：shutdown() 在 50ms 内返回，测试总时长从 13.5s 降到 ~2s。
        self._thread = threading.Thread(target=server.serve_forever, args=(0.05,), name="fake-laya", daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        """幂等关闭；挂住的请求线程是 daemon，不会阻塞这里。"""

        server, self._server = self._server, None
        if server is None:
            return
        server.shutdown()
        server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None

    def __enter__(self) -> FakeLayaServer:
        return self.start()

    def __exit__(self, *exc: object) -> None:
        self.stop()

    # ---- 地址与断言面 --------------------------------------------------
    @property
    def port(self) -> int:
        assert self._server is not None, "服务端未启动"
        return int(self._server.server_address[1])

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    @property
    def hits(self) -> int:
        """收到的请求数（「6 个问题只发 1 次 HTTP」就是断言它等于 1）。"""

        return len(self.requests)

    @property
    def paths(self) -> list[str]:
        return [str(item["path"]) for item in self.requests]

    @property
    def bodies(self) -> list[dict[str, Any]]:
        return [dict(item["body"]) for item in self.requests]

    @property
    def last_body(self) -> dict[str, Any]:
        return dict(self.requests[-1]["body"]) if self.requests else {}

    @property
    def auth_headers(self) -> list[str]:
        return [str(item["headers"].get("authorization", "")) for item in self.requests]

    @property
    def question_counts(self) -> list[int]:
        """每次请求携带的问题数（非 LAY A 形状的请求体记为 0）。"""

        counts = []
        for body in self.bodies:
            questions = body.get("questions")
            counts.append(len(questions) if isinstance(questions, Mapping) else 0)
        return counts

    @property
    def models(self) -> list[Any]:
        return [body.get("model") for body in self.bodies]

    @property
    def states(self) -> list[Any]:
        return [body.get("state") for body in self.bodies]

    def reset(self) -> None:
        """清空记录与故障（不影响已绑定的端口）。"""

        with self._recorded:
            self.requests.clear()
        self._pending.clear()
        self._always = None

    def wait_for_requests(self, count: int = 1, *, timeout: float = 5.0) -> int:
        """有界等待「至少 ``count`` 次请求已被计账」，返回实际观察到的次数。

        存在的理由不是「等等看」，而是**补上缺失的同步**：客户端超时不取消 handler
        线程，所以「transport 超时」与「handler 落账」之间本来没有先后保证。
        正常情况这里立即返回（落账早已发生）；只有确实没发出请求时才会等到超时。

        用法（断言保持原样，鉴别力不降）：

            assert server.wait_for_requests(1) >= 1, "LAY A 那一跳没被观察到"
        """

        deadline = time.monotonic() + max(float(timeout), 0.0)
        with self._recorded:
            while len(self.requests) < count:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._recorded.wait(remaining)
            return len(self.requests)

    # ---- 可编程行为 ----------------------------------------------------
    def set_label(self, label: str | None) -> None:
        """指定 choice 问题的答案；``None`` 表示按请求里的 criteria 取第一个候选。"""

        self.label = label

    def set_probabilities(self, probabilities: Mapping[str, float] | None) -> None:
        """显式指定 choice 答案的概率分布（``None`` 恢复自动铺）。

        用途：让「LAY A 路径」与「旧的 OpenAI 兼容路径」报告**同一个**分类结果
        （同标签 + 同置信度），这样下游判定的一致性是真正的等价比较。
        """

        self.probabilities = dict(probabilities) if probabilities is not None else None

    def fail_with(self, status: int, *, times: int | None = None) -> None:
        """返回 HTTP 错误。``times=None`` 表示一直失败，``times=N`` 表示只前 N 次。"""

        self._schedule(f"fail:{int(status)}", times)

    def respond_non_json(self, text: str = "<html><body>not json</body></html>", *, times: int | None = None) -> None:
        """返回 200 但响应体不是 JSON（``Content-Type: text/html``）。"""

        self.non_json_text = text
        self._schedule("nonjson", times)

    def hang(self, seconds: float = 2.0, *, times: int | None = None) -> None:
        """挂住 ``seconds`` 秒才响应，用来触发客户端超时（需把客户端 timeout 调更小）。"""

        self.hang_seconds = float(seconds)
        self._schedule("hang", times)

    def _schedule(self, action: str, times: int | None) -> None:
        if times is None:
            self._always = action
            return
        if times <= 0:
            raise ValueError("times 必须为正整数或 None")
        self._pending.extend([action] * int(times))

    def _next_action(self) -> str | None:
        if self._pending:
            return self._pending.pop(0)
        return self._always

    # ---- 协议实现 ------------------------------------------------------
    def _record(self, handler: BaseHTTPRequestHandler) -> dict[str, Any]:
        """把这次请求计入 ``requests``，并读回请求体。

        计数**先于**读体：请求行与请求头此时已解析完，客户端超时又不会取消本线程，
        所以「已到达」要尽可能早地对外可见（读体是唯一可能被调度延迟拉长的部分）。
        正文随后回填到同一条记录上。
        """

        entry: dict[str, Any] = {
            "method": handler.command,
            "path": handler.path,
            "headers": {key.lower(): value for key, value in handler.headers.items()},
            "body": {},
        }
        with self._recorded:
            self.requests.append(entry)
            self._recorded.notify_all()

        length = int(handler.headers.get("Content-Length") or 0)
        raw = handler.rfile.read(length) if length else b""
        try:
            body: Any = json.loads(raw.decode("utf-8") or "{}")
        except Exception:  # noqa: BLE001 - 非 JSON 请求体也要能记录（用于排障）
            body = {"__raw__": raw.decode("utf-8", "replace")}
        with self._recorded:
            entry["body"] = body
            self._recorded.notify_all()
        return body if isinstance(body, dict) else {}

    def response_for(self, body: Mapping[str, Any]) -> dict[str, Any]:
        """按请求体生成 LAY A 响应：有 ``questions`` 就逐题作答。"""

        questions = body.get("questions") if isinstance(body, Mapping) else None
        if isinstance(questions, Mapping) and questions:
            answers = {str(qid): self.answer_for(spec) for qid, spec in questions.items()}
        else:
            answers = {laya.DEFAULT_QID: self.answer_for({"type": "choice", "criteria": self.criteria_of(body)})}
        return laya_response(
            answers, model=self.model, input_tokens=self.input_tokens, output_tokens=self.output_tokens
        )

    def answer_for(self, spec: Mapping[str, Any]) -> dict[str, Any]:
        """按问题类型给出答案；choice 默认取 criteria 的第一个候选。"""

        spec = dict(spec or {})
        kind = str(spec.get("type") or "choice").strip()
        if kind == "score":
            return score_answer(self.score, confidence=self.confidence)
        if kind == "noul":
            return noul_answer(confidence=self.confidence)
        keys = self._criteria_keys(spec.get("criteria"))
        label = self.label or (keys[0] if keys else "unknown")
        probabilities = self.probabilities or self._distribution(label, keys)
        return choice_answer(label, confidence=self.confidence, probabilities=probabilities)

    @staticmethod
    def _criteria_keys(criteria: Any) -> list[str]:
        if isinstance(criteria, Mapping):
            return [str(key) for key in criteria]
        if isinstance(criteria, Sequence) and not isinstance(criteria, (str, bytes)):
            return [str(item) for item in criteria]
        return []

    @staticmethod
    def _distribution(label: str, keys: Sequence[str]) -> dict[str, float]:
        """被选中的候选拿 0.72，其余候选平分 0.28（与真 LAY A 一样给出概率分布）。"""

        others = [key for key in keys if key != label]
        if not others:
            return {label: 0.72}
        share = round(0.28 / len(others), 4)
        return {label: 0.72, **{key: share for key in others}}

    def criteria_of(self, body: Mapping[str, Any]) -> dict[str, str]:
        """从 OpenAI 兼容的 classify 请求体里反解候选标签（复用传输层自己的翻译函数）。"""

        try:
            _text, labels = laya.extract_classify_input(body)
        except Exception:  # noqa: BLE001 - 反解不了就当没有候选，交给调用方断言
            return {}
        return {str(label): str(label) for label in labels}


class StubOpenAITransport:
    """替代真实网络的 OpenAI 兼容传输层（记录调用，返回 chat/completions 形状）。

    用途：验证「LAY A 不可用时 classify 回落 grok-4.6」。回落目标挂在默认服务商
    （``model.default_provider`` = a6api）上，直接注入本替身即可完全不碰网络。
    """

    def __init__(self, reply: str = "提问", *, model: str = "grok-4.6") -> None:
        self.reply = reply
        self.model = model
        self.calls: list[dict[str, Any]] = []
        self.paths: list[str] = []
        self.providers: list[str] = []
        self.closed = False

    async def complete(
        self,
        payload: dict[str, Any],
        *,
        path: str = "/chat/completions",
        timeout: float | None = None,
        provider: str = "",
    ) -> dict[str, Any]:
        self.calls.append(dict(payload))
        self.paths.append(path)
        self.providers.append(provider)
        return {
            "model": str(payload.get("model") or self.model),
            "choices": [{"message": {"content": self.reply}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 11, "completion_tokens": 3},
        }

    async def aclose(self) -> None:
        self.closed = True


__all__ = [
    "DEFAULT_MODEL",
    "SYSTEM_ONE_PATH",
    "FakeLayaServer",
    "StubOpenAITransport",
    "choice_answer",
    "laya_response",
    "noul_answer",
    "score_answer",
]
