# GrouPig 接入层（grouppig.gateway）落地说明

> 归属：`gateway-engineer`（任务 t3 / t10）。契约名字逐字取自 `normify-grouppig/api-index.json`，
> 本域只有 **17 个 `rpc:` + 2 个 `kafka:`**，多余的名字一律不注册（`Gateway.contract_check()` 会报 `unknown`）。

## 1. 模块与契约名字对照

| 模块 id | 文件 | 契约名字 |
| --- | --- | --- |
| `grouppig.gateway` | `src/grouppig/gateway/__init__.py` | （容器：`Gateway` / `build_gateway` / `install`） |
| `grouppig.gateway.adapter` | `.../adapter/__init__.py` | （容器） |
| `grouppig.gateway.adapter.connector` | `.../adapter/connector.py` | `rpc:connector.connect`、`rpc:connector.heartbeat` |
| `grouppig.gateway.adapter.event-codec` | `.../adapter/event_codec.py` | `rpc:codec.decode`、`rpc:codec.encode` |
| `grouppig.gateway.adapter.onebot` | `.../adapter/onebot.py` | `rpc:onebot.start`、`rpc:onebot.send` |
| `grouppig.gateway.router` | `.../router/__init__.py` | （容器） |
| `grouppig.gateway.router.command` | `.../router/command.py` | `rpc:command.recognize`、`rpc:command.execute` |
| `grouppig.gateway.router.priority` | `.../router/priority.py` | `rpc:priority.enqueue`、`rpc:priority.next` |
| `grouppig.gateway.router.demux` | `.../router/demux.py` | `rpc:demux.dispatch` |
| `grouppig.gateway.sender` | `.../sender/__init__.py` | （容器） |
| `grouppig.gateway.sender.rate-limiter` | `.../sender/rate_limiter.py` | `rpc:rate.check`、`rpc:rate.wait` |
| `grouppig.gateway.sender.composer` | `.../sender/composer.py` | `rpc:composer.wrap`、`rpc:sender.send_reply` |
| `grouppig.gateway.sender.retract` | `.../sender/retract.py` | `rpc:retract.recall`、`rpc:retract.notify` |

主题（发布/订阅，不进注册表）：

* `kafka:grouppig.qq.message.received` —— 入站事件（`QQEvent.as_dict()`）。
* `kafka:grouppig.event.routed` —— 分用结果。

## 2. 数据流

```
NapCat(OneBot v11 WS)
   │ ① 事件帧
   ▼
Connector ──► OneBotAdapter ──► kafka:grouppig.qq.message.received
                                      │ ②（EventBus 订阅）
                                      ▼
                                   EventRouter(分用/优先级/命令)
                                      ├─► rpc:observer.ingest        （感知层，t4）
                                      ├─► kafka:grouppig.event.routed （话题/聊天线，t5/t6）
                                      └─► 本地命令 → ReplyComposer.send_reply
                                                          ▲
表达层 t9 ──► kafka:grouppig.reply.composed ──────────────┘ ③
                                                          │ ④ 节流
                                                          ▼
                          rpc:onebot.send（send_group_msg / send_private_msg）
                                                          │ ⑤ 记流水
                                                          ▼
                                        rpc:chat.append（t2） + Retractor.remember
```

`install(container, start=True)` 一次完成：注册 17 个 `rpc:` 处理器 → 订阅入站主题 →
把 composer 接到 `kafka:grouppig.reply.composed` → 连上 OneBot → 启动投递泵。

## 3. 载荷契约

### 3.1 入站 `kafka:grouppig.qq.message.received`

`QQEvent.as_dict()`，键固定如下（未知事件 `known=False`，仍会发布以便观测）：

```
event_id, kind, post_type, message_type, sub_type, notice_type, request_type,
meta_event_type, time, self_id, group_id, user_id, operator_id, target_id,
message_id, raw_message, text, command_text, segments, sender, at_self, known
```

* `kind`：`message.group` / `message.private` / `notice.group_recall` / `notice.group_ban` /
  `meta_event.heartbeat` / `meta_event.lifecycle` / `unknown.*`。
* `text`：含 `@` 占位的可读文本；`command_text`：**去掉 `@` 的纯文本**（命令识别用）。
* `segments`：OneBot 消息段数组（`{"type": "text", "data": {...}}`）。

### 3.2 路由结果 `kafka:grouppig.event.routed`（也是 `rpc:demux.dispatch` 的返回）

```
{ event, route, kind, priority, queued, queue_depth, command, at_self, reason }
```

* `route`：`perception` / `command` / `persona` / `notice` / `meta` / `drop`。
* `priority`：`0` 命令、`1` @我、`2` 私聊、`3` 群消息、`4` 通知、`5` 元事件、`6` 未知。
* `command`：命中命令时为 `CommandResult.as_dict()`（`name/kind/args/route/...`），否则 `None`。
* `reason`：`unknown_event` / `unsupported_post_type` / `queue_rejected` / `queue_full` / `""`。

### 3.3 出站

* 表达层发 `kafka:grouppig.reply.composed`，payload 即 `ReplyComposer.send_reply(**payload)` 的关键字：

  ```
  text | content | message（任选其一，字符串或消息段数组）
  group_id 或 user_id（至少一个）
  reply_to、at（提及 QQ 号或列表）、emoji、source（默认 "flow"）、
  drop_if_limited、max_wait、auto_escape、record
  ```

* `rpc:onebot.send`：`action=` 直发底层动作（如 `delete_msg`，`params` 原样透传）；
  否则按 `group_id`/`user_id` 发 `send_group_msg`/`send_private_msg`，支持 `reply_to`/`at`。
* `rpc:sender.send_reply` 返回：

  ```
  { ok, sent, message_ids, chunks, waited, rate, recorded, skipped,
    error, reason, target, text, stats }
  ```

* 自发言记流水 `rpc:chat.append`（t2 契约，若签名不同只需改 `ReplyComposer` 一处）：

  ```
  { group_id, user_id, message_id, message_ids, text, segments,
    raw_message, self: true, role: "assistant", source, ts }
  ```

## 4. 配置键（`config/grouppig.toml`，全部可选，缺省用代码内默认值）

```toml
[onebot]
ws_url = "ws://127.0.0.1:3001"
access_token = ""
self_id = 0
reconnect_interval = 3.0
max_backoff = 60.0
heartbeat_interval = 30.0
connect_timeout = 10.0
outbox_limit = 500

[onebot.queue]      # 优先级队列
max_depth = 1000
watermark = 200     # 超过后开始丢元事件
pump_interval = 0.05

[onebot.sender]     # 回复包装
max_length = 400
quote = true
auto_emoji = true

[onebot.rate]       # 令牌桶 + 冷却（默认规则）
per_minute = 20
burst = 3
min_interval = 2.0

[onebot.rate.groups."123456"]   # 单群覆盖
per_minute = 6
burst = 2
min_interval = 8.0

[onebot.rate.global]            # 全局兜底
per_minute = 120
burst = 20
min_interval = 0.5
```

`onebot.rate.*` / `onebot.queue.*` / `onebot.sender.*` 是 `[onebot]` 的子表，
`validate_config` 只对未知**顶层**表告警，因此这些调参不需要改配置校验器。

## 5. 用模拟 OneBot 服务端验证（无需真 NapCat）

`tests/gateway_helpers.py` 提供：

* `MockOneBotServer`：真 WebSocket 服务端（`websockets.asyncio.server`），校验 `Authorization: Bearer <token>`，
  记录收到的动作帧（含无 `echo` 的推送）、可 `push(事件)`、`drop_clients()` 断连、`wait_action()` 等。
* 事件构造器：`group_message` / `private_message` / `recall_notice` / `heartbeat` / `lifecycle`。
* `isolated_container(config)`：独立容器（不污染 `default_registry`，后者被 `test_di_container` 断言为 15 个）。
* `IngestSink`：替代 `rpc:observer.ingest` 的感知层桩；`EventCollector`：订阅总线主题。

跑法：

```bash
uv run python -m pytest tests/test_gateway_codec.py tests/test_gateway_connector.py \
    tests/test_gateway_onebot.py tests/test_gateway_router.py tests/test_gateway_sender.py \
    tests/test_gateway_contract.py tests/test_gateway_e2e.py -q
```

端到端覆盖（`test_gateway_e2e.py`）：消息进 → 分用 → 感知层 → `@我 + /help` 命令回复真发回服务端 →
`reply.composed` 闭合最后一跳 → 节流丢弃 → 撤回通知与 `delete_msg` → 断线重连后继续收消息 →
感知层未注册时不丢事件（计入 `undelivered`）。

## 6. 与其他域的接缝（尚未接线，由 t10 收口）

1. `rpc:observer.ingest`：感知层（t4）注册后，`EventRouter` 的默认 sink 自动生效
   （`HandlerNotRegistered` 时计入 `demux.stats.undelivered`，事件不丢）。
2. `rpc:chat.append`：内存层（t2）实现真实签名后，只需对齐 `ReplyComposer._record` 的载荷。
3. `kafka:grouppig.reply.composed`：表达层（t9）发布即自动发送，无需额外接线。
4. 撤回原因 `Retractor.REASONS`（`misfire` / `rule_violation` / `duplicate` / `hallucination` /
   `group_recall` / `friend_recall` / `manual`）
   由反思/规则域调用 `rpc:retract.recall` 或 `rpc:retract.notify` 触发。
