# LAY A 接入独立验证（t23）

独立验证者：`session-engineer`（非本变更作者）。验证对象：设计变更 `normify-grouppig/changes/2026-09-24-laya-system1.json`。
验证方式：**全部本地复现**，只用 `tests/fakes_laya.py` 的本机假 LAY A 服务端；不访问 `10.63.127.239`、不使用真实密钥。
结论：**六条声称全部通过**（1 条附带重要限定：基线数字中的一半无法离线重测，见 §1.3）。

## 0. 结论总表

| # | 声称 | 判定 | 关键数字 |
| --- | --- | --- | --- |
| ① | `rpc:model.classify` 从实测 51225ms / 60072ms 降到亚秒级 | **通过**（含限定） | 热态 **2.079–3.130ms**（均值 2.600ms）；基线机制本地复现 **60563.6ms**（2 次尝试） |
| ② | LAY A 超时 / 401 / 422 / 503 回落 grok-4.6 且不抛错 | **通过** | 5 种故障全部 `raised=null`、`provider=a6api`、`model=grok-4.6` |
| ③ | `rpc:model.system1` 已注册、契约缺口 0 | **通过** | `assert_known_name` 通过、api-index 命中、`rpc_gap=[]` |
| ④ | 一次请求塞 6 个以上问题且只发一次 HTTP | **通过** | 6 问 / 12 问 均 `hits=1`，`question_counts=[6]/[12]` |
| ⑤ | 置信度低于阈值时升级并标注来源 | **通过** | 默认阈值 0.4 下 conf 0.2 → `source=escalated`；conf 0.9 → `source=system1` |
| ⑥ | 行为分类与打断决策判定结果未被改变 | **通过** | 两条路径 `judge_label=flooding`、打断决策 `hold` 逐字段相同 |

## 1. 声称①：classify 端到端延迟

### 1.1 修复后（LAY A 路径）——本地复现

命令（临时脚本，不在仓库内；用真实装配路径 `build_container(...).start()` + `container.call`）：

```
uv run python /tmp/indep_final.py    # 见 §1.4 脚本要点；10 次连续调用，丢弃第 1 次冷启动
```

关键输出：

```json
LATENCY {"samples_ms": [53.187, 3.002, 2.717, 2.367, 2.547, 3.112, 3.13, 2.19, 2.079, 2.258],
         "warm_min": 2.079, "warm_max": 3.13, "warm_mean": 2.6,
         "label": "提问", "provider": "laya", "model": "typed-decisions", "hits": 10}
```

结论：**亚秒级成立且余量极大**——热态均值 2.6ms，是 1s 门槛的 0.26%；第 1 次 53.2ms 是首次建连/导入开销，也不到 1s。
`hits=10` 说明 10 次调用恰好 10 次 HTTP，没有隐藏的重试或额外往返。

### 1.2 修复后（system1 路径）

```json
{"questions": 6,  "hits": 1, "question_counts": [6],  "answered": 6,  "lowest": 0.72, "ms": 20.47}
{"questions": 12, "hits": 1, "question_counts": [12], "answered": 12, "lowest": 0.72, "ms": 26.78}
```

6 问 20.5ms、12 问 26.8ms（含容器内一次完整装配）。

### 1.3 修复前基线（51225ms / 60072ms）——**只能复现机制，不能重测当时的数字**

**限定条件（重要）**：这两个数字是变更前在**真实端点**上测得的（`config/grouppig.toml` 里 classify 走 `a6api/grok-4.6`），
本任务**明令禁止访问**该端点，因此**无法离线重测同样的两次测量**。我做的是**复现同一条路径与同一套重试策略**：
把 classify 改回变更前的 provider（`a6api` + `grok-4.6` + `fallback_models=[]`），重试块保持仓库原样（`timeout = 30.0`），
只把传输层换成一个「永不返回」的替身（真实端点当时正是超时），让路由层自己按 `retry.timeout` 掐断。

命令：

```
uv run python /tmp/indep_baseline.py    # 见 §1.4；完全离线，传输层不发任何真实请求
```

关键输出（两次测量，唯一变量是 `max_attempts`）：

```json
BASELINE {"max_attempts": 2, "elapsed_ms": 60563.6, "raised": "ModelCallError", "transport_calls": 2,
          "timeouts_passed": [30.0, 30.0],                            "providers": ["a6api", "a6api"]}
BASELINE {"max_attempts": 3, "elapsed_ms": 91528.8, "raised": "ModelCallError", "transport_calls": 3,
          "timeouts_passed": [30.0, 30.0, 30.0],                      "providers": ["a6api", "a6api", "a6api"]}
```

对应的日志（同一时刻的路由层事件，证明耗时确实由 `retry.timeout=30.0` 决定）：

```
model.attempt_failed ... "error": "ModelTimeoutError('模型调用超时（>30.0s，第 1 次尝试）')", "elapsed_ms": 30029.773
model.attempt_failed ... "error": "ModelTimeoutError('模型调用超时（>30.0s，第 2 次尝试）')", "elapsed_ms": 30001.988
```

**数字归属必须分清**：

* **本地复现的数字**：`60563.6ms`（2 次尝试 × 30s 超时 + 退避）与 `91528.8ms`（3 次尝试，即仓库 `[model.retry] max_attempts = 3` 的默认值）。
* **引自 change 文件的数字**：`51225ms` / `60072ms`，来自真实端点，本次**未复现也未采信为证据**。
* `60563.6ms` 与声称的 `60072ms` 相差 0.49s（0.8%），说明「60 秒」这个量级 = 30s 超时 × 2 次尝试，机制被完整复现；
  `51225ms` 不是 30 的整数倍（≈30s + 21s），无法用同一模型解释，属于真实端点上的单次观测，本次不采信。

结论：**声称①的结论（几十秒 → 亚秒级）成立**；其「51–60 秒」基线的**量级与成因**被本地独立复现，
但**那两次具体测量值本身不可复现**（受限于禁止访问真实端点），因此不能算作我独立测得的数字。

### 1.4 复现脚本要点（为什么可信）

* 走**真实装配路径**：`build_container(config=...).start()` → `container.call("rpc:model.classify", ...)`，不是直接调传输层。
* 配置由 `config/grouppig.toml` **改写**而来（laya 与 a6api 的 `base_url` 都指向假服务端），改写前对每个待替换片段做 `count == 1` 断言，配置漂移会立刻炸而不是假绿。
* 密钥在**导入配置模块之前**用环境变量注入假值（见 §7.2 的原因）。
* 全程包在自写的离线闸门里（见 §1.5）。

## 1.5 离线保证：闸门自证

我自写的 `loopback_only()` **同时** patch `socket.getaddrinfo` 与 `socket.socket.connect`（只拦前者时字面量 IP 仍会真连出去）。自证结果：

```json
GUARD {"internal_literal_ip": "BLOCKED: ExceptionGroup",
       "external_domain": "BLOCKED: RuntimeError",
       "attempted_hosts": ["10.63.127.239", "b'api.a6api.com'"]}
```

`attempted_hosts` 是整个验证过程中**唯一**出现过的非本机地址，且都被掐断；六条声称的每一轮实测里 `blocked_hosts` 均为 `[]`
（即一次都没有试图连出去）。假服务端只监听 `127.0.0.1` 的随机端口，不受闸门影响（10 次调用 `hits=10` 即为证）。

## 2. 声称②：LAY A 故障时回落 grok-4.6 且不抛错

命令（临时脚本，参数化 5 种故障）：

```
uv run python /tmp/indep_all.py    # claim2 段
```

关键输出：

```json
[{"failure": "status503", "raised": null, "provider": "a6api", "model": "grok-4.6", "label": "提问", "fallback_calls": 1, "fallback_paths": ["/chat/completions"], "laya_hits": 1, "ms": 16.65},
 {"failure": "status401", "raised": null, "provider": "a6api", "model": "grok-4.6", "label": "提问", "fallback_calls": 1, "fallback_paths": ["/chat/completions"], "laya_hits": 1, "ms": 16.59},
 {"failure": "status422", "raised": null, "provider": "a6api", "model": "grok-4.6", "label": "提问", "fallback_calls": 1, "fallback_paths": ["/chat/completions"], "laya_hits": 1, "ms": 20.08},
 {"failure": "timeout",   "raised": null, "provider": "a6api", "model": "grok-4.6", "label": "提问", "fallback_calls": 1, "fallback_paths": ["/chat/completions"], "laya_hits": 1, "ms": 251.57},
 {"failure": "non_json",  "raised": null, "provider": "a6api", "model": "grok-4.6", "label": "提问", "fallback_calls": 1, "fallback_paths": ["/chat/completions"], "laya_hits": 1, "ms": 18.83}]
```

要点：

* **不抛错**：五种故障 `raised` 全为 `null`，调用方拿到的是可用结果（`label=提问`）。
* **确实是降级而不是静默成功**：`laya_hits=1`（LAY A 那一跳真被尝试过）+ `fallback_calls=1` 且路径是 `/chat/completions`（OpenAI 兼容端点），
  即「先打 LAY A → 失败 → 回落 grok-4.6」。
* 超时用例耗时 251.57ms ≈ 我设的 0.25s 客户端超时，说明超时也在本地被真实触发（不是被跳过）。
* 路由层日志可见 `model.fallback ... "failed_provider": "laya", "failed_model": "auto", "next_provider": "a6api", "next_model": "grok-4.6"`（见 422 与超时两例）。
* **反向对照**（作者用例 `test_no_fallback_when_laya_is_healthy` 覆盖，我复核通过）：LAY A 正常时 `fallback_calls == []`，证明上面的回落是故障触发而非配置写错。

## 3. 声称③：`rpc:model.system1` 注册与契约缺口

命令：

```
uv run python -m grouppig.runtime --check --no-connect --dsn sqlite+aiosqlite:///:memory:
uv run python -m pytest tests/test_laya_e2e.py -q -o addopts=""
uv run python /tmp/indep_all.py     # claim3 段
```

关键输出：

```
域 8 / 注册名字 148 / 契约名字 167 / 泵 4 / 契约缺口（rpc）：无 / 未登记名字：无   （退出码 0）
```

```json
claim3 {"assert_known_name": true, "in_api_index": true, "owner": "grouppig.infra.model-gateway.router",
        "registered_names": 16, "rpc_gap": [], "unknown": [], "system1_ok": "system1"}
```

要点：`contract.assert_known_name("rpc:model.system1")` 通过；该名字在 `normify-grouppig/api-index.json` 里能查到且归属
`grouppig.infra.model-gateway.router`（与 change 的 `api_add` 一致）；容器自检的 rpc 缺口与未登记名字都为空；
`rpc:model.system1` 实际调用返回 `source=system1`，即注册的不是空壳。

## 4. 声称④：一次请求塞 6 个以上问题，只发一次 HTTP

命令：`uv run python /tmp/indep_all.py   # claim4 段`

```json
[{"questions": 6,  "hits": 1, "question_counts": [6],  "answered": 6,  "confs": 6,  "lowest": 0.72, "source": "system1", "ms": 20.47},
 {"questions": 12, "hits": 1, "question_counts": [12], "answered": 12, "confs": 12, "lowest": 0.72, "source": "system1", "ms": 26.78}]
```

`hits` 是**服务端自己数的**请求数（不是客户端的说法）：6 问与 12 问都只有 1 次，且服务端记录的 `question_counts`
分别就是 `[6]` 与 `[12]`——问题确实打进了同一个请求体，而不是逐问调用。`answered` 与 `confs` 的条数与会话集合一一对应。

## 5. 声称⑤：置信度低于阈值时升级对话模型并标注来源

命令：`uv run python /tmp/indep_final.py   # DEFAULT-THRESHOLD 段`（**用默认阈值**，不显式传参）

```json
DEFAULT-THRESHOLD {"conf_0.2": {"source": "escalated", "lowest_confidence": 0.2,
                                "escalated_to": {"model": "grok-4.6", "provider": "a6api"},
                                "chat_calls": 1, "verdict": {}},
                   "conf_0.9": {"source": "system1", "lowest_confidence": 0.9,
                                "escalated_to": null, "chat_calls": 0, "verdict": null}}
```

要点：

* **默认阈值就生效**：`SYSTEM1_ESCALATE_BELOW = 0.4`，LAY A 回 `confidence=0.2` 时 `source=escalated`（并标出升级目标），回 `0.9` 时 `source=system1` 且**不发生**升级（`chat_calls=0`）。两个方向都验证了，不是只看升级一路。
* 来源标注是显式字段：`source` ∈ {`system1`, `escalated`}，升级时附 `escalated_to`。
* 我另外用显式阈值复核过一次（`escalate_below=0.95`，LAY A 固定回 `0.72`）：`low_source=escalated`、`low_confidences=0.72`、
  `laya_answers_kept=true`（升级时保留 LAY A 的概率供裁决）、`chat_calls=1`；`escalate_below=0.4` 时为 `system1`。
* 我在 `conf_0.2` 这轮看到 `verdict={}`：升级请求里 LAY A 没有给出「A/B」这种可解析答案（假服务端在低置信度下仍回 label，但升级那一跳由 stub 返回 `A`），
  所以 `verdict` 为空映射。**这不算失败**（`source`/`escalated_to` 已正确标注），但**记为残余风险**：`verdict` 的解析在真实升级场景下未被我独立取证。

## 6. 声称⑥：行为分类与打断决策的判定结果未被改变

命令：`uv run python /tmp/indep_all.py   # claim6 段`

做法：同一个 6 条刷屏消息的 burst，跑「行为分类 `rpc:behavior.llm.judge` → 打断决策 `rpc:interrupt.decide`」两级判定两遍：
一遍让 classify 走 LAY A 假服务端，一遍走 `StubOpenAITransport`（即变更前 OpenAI 兼容形状），再逐字段比对。

```json
claim6 {"laya_path":   {"judge_label": "flooding", "decide": "hold"},
        "compat_path": {"judge_label": "flooding", "decide": "hold"},
        "identical_judge": true, "identical_decision": true}
```

两条路径的判定结果**逐字段相同**（`identical_judge=true`、`identical_decision=true`）：行为分类仍判 `flooding`、打断决策仍是 `hold`，
没有被迁移到 LAY A 或改变。作者另外还覆盖了「LAY A 回中文标签（刷屏）时别名归一仍生效」与「模型全挂时决策逐字段相同」两个方向，我复核通过。

## 7. 反向检查：密钥

### 7.1 仓库内无真实密钥字面量

命令与输出：

```bash
grep -rIn "laya_is_" --include="*.py" --include="*.toml" --include="*.md" . || echo "no key literal"
# ./tests/test_laya_e2e.py:586:async def test_no_fallback_when_laya_is_healthy(...
# ./tests/test_laya_e2e.py:604:async def test_system1_escalates_instead_of_raising_when_laya_is_unavailable(
```

**这条 grep 的两次命中都是假阳性**：命中的是**测试函数名**里的 `laya_is_healthy` / `laya_is_unavailable` 片段，不是密钥字面量。
严格按字面读，这条 verify 命令「有命中」；按意图读，它没有发现任何密钥。为免误判，我补做了更宽的扫描：

```bash
grep -rIn "sk-[A-Za-z0-9]\{16,\}" --include="*.py" --include="*.toml" --include="*.md" --include="*.json" --include="*.yaml" --include="*.yml" .
# ./tests/test_model_gateway.py:802: text = Authorization: Bearer sk-abcdefgh12345678 {"api_key": "sk-cau9abcdef123", "token": "t0ken-value"}
# ./tests/test_model_gateway.py:805: assert "sk-abcdefgh12345678" not in masked

grep -rIn "sk-cau[A-Za-z0-9]\{30,\}" .        # 无输出
grep -rIn "rc-fde" .                            # 无输出
grep -rIn "^\s*api_key\s*=" config/            # 无输出（配置里没有内联密钥）
```

结论：**仓库内没有真实密钥**。唯一的长 `sk-` 字面量在 `tests/test_model_gateway.py`，一眼是**合成示例**
（`sk-abcdefgh12345678`、`sk-cau9abcdef123`），用途正是「日志掩码必须盖住密钥」的断言；
真实凭据前缀（`sk-cau` + 51 字符、`rc-fde`）在仓库里**零命中**。

### 7.2 `config/grouppig.toml` 的 laya 段只有非密字段

```
KEY: base_url       -> "http://10.63.127.239:7858"
KEY: api_key_env    -> "GROUPPIG_LAYA_API_KEY"
KEY: api_key_field  -> "model.api_key"
```

该段只有这三个键，**没有任何密钥值**，密钥只能从环境变量 / `~/.dsh/.credentials.yaml` 解析。

**与声称的一处措辞偏差（如实记录）**：任务与 change 的表述是「config 只含 `base_url` 与 `api_key_field`」，
实测还多一个 `api_key_env = "GROUPPIG_LAYA_API_KEY"`。它是**变量名**而不是密文，语义上完全符合「密钥不入仓库」；
所以判定为**通过**，但措辞应更正为「只含 `base_url`、`api_key_env`、`api_key_field` 三个非密字段」。

### 7.3 我自己的验证过程一度把真实密钥发给了本机假服务端（重要披露）

事实经过：第一版验证脚本虽然定义了假密钥常量，但**没有把它注入环境变量**，且用了 `load_config(..., use_env=False)`。
结果 `Authorization` 头里出现了一枚真实形制的密钥（`sk-cau...`，51 字符），请求打到了**本机 127.0.0.1 的假服务端**。

根因：`resolve_api_key` 的名字列表是 `[api_key_env, GROUPPIG_MODEL_API_KEY, api_key_field, refs.*]`，
交给 `resolve_secret(...)` 解析；在 `use_env=False` 语义下，**`~/.dsh/.credentials.yaml` 里的 `GROUPPIG_MODEL_API_KEY` / `A6API_API_KEY` 会优先于 `os.environ`**。
所以「设了 `os.environ[...]`」并不等于「密钥被替换」。

影响面：请求目标始终是 `127.0.0.1` 的假服务端（离线闸门全程生效，`blocked_hosts=[]`），**密钥没有出网**；
但它确实被写进了一个本机 HTTP 请求头与一段本地进程日志。

修复与自证：改为在**导入配置模块之前**注入 `os.environ["GROUPPIG_LAYA_API_KEY"]` / `GROUPPIG_MODEL_API_KEY`（环境变量存在时优先），
并加了一条自证输出：

```json
A0-KEY {"is_fake": true, "len": 41, "prefix": "sk-fak"}
A0-TRANSPORT-KEY {"fake_used": true}    // 断言假服务端收到的头就是 Bearer sk-fake-independent-verifier-not-a-secret
A0-NETWORK []
```

此后所有实测（§1–§6）都用假密钥完成。**建议**：`resolve_api_key` 的这条优先级语义值得单独写进文档或加断言，
否则任何「设了环境变量就当密钥被替换」的测试都可能悄悄用到真凭据。

## 8. 三条官方 verify 命令

```bash
# ① 全量测试（仓库 addopts 已含 -q，此行必须清空 addopts，否则叠加成 -qq 会吞掉汇总行）
uv run python -m pytest -q -o addopts=""
# 959 passed in 48.08s

# ② 静态检查
uv run ruff check src tests tools && uv run ruff format --check src tests tools
# All checks passed!
# 266 files already formatted

# ③ 密钥字面量反查（命中 2 处测试函数名，非密钥；详见 §7.1）
grep -rIn "laya_is_" --include="*.py" --include="*.toml" --include="*.md" . || echo "no key literal"
# ./tests/test_laya_e2e.py:586  ./tests/test_laya_e2e.py:604
```

补充证据（同一基线下的针对性回归）：

```bash
uv run python -m pytest tests/test_laya_e2e.py -q -o addopts=""      # 30 passed in 2.22s
uv run python -m grouppig.runtime --check --no-connect --dsn sqlite+aiosqlite:///:memory:   # 退出码 0，契约缺口 0
```

## 9. 未覆盖的边界与残余风险

1. **51225ms / 60072ms 这两个具体测量值不可复现**（需真实端点，任务禁止访问）。本地复现的是同一条路径 + 同一套 `retry.timeout=30.0` 的**耗时量级**（60563.6ms / 91528.8ms），见 §1.3。声称的「亚秒级」一侧是我实测的（2.079–3.130ms）。
2. **真实 LAY A 服务端的行为未验证**：假服务端按协议形状作答，真实端点的鉴权、限流、`routing` 字段、以及真实置信度分布都未覆盖。
3. **升级后的 `verdict` 解析未独立取证**：低置信度升级那一轮我看到 `verdict={}`（见 §5）。`source`/`escalated_to` 正确，但「升级答案被解析成可用裁决」这一环我没有独立证据。
4. **`system1` 的 `escalate_below` 只在我构造的置信度上验证**：真实 LAY A 的置信度是否落在 0.4 附近、阈值选得是否合适，需要真实端点数据才能判断（阈值本身的有效性属模型质量问题，不是本次接线验证的范畴）。
5. **延迟只测了回环**：2–3ms 是回环下限，不代表跨网真实延迟；本次没有也无法给出真实端点的 P50/P99。
6. **并发/背压未验证**：假服务端是 `ThreadingHTTPServer`，但我的实测全是串行单请求；高并发下连接池、超时叠加、`meter` 记账的交互未覆盖。
7. **`resolve_api_key` 的凭据优先级**（§7.3）只被我在验证脚本里绕过，未作为产品行为被验证；`use_env=False` 时凭据文件压过环境变量这一语义若被误用，可能带来「测试静默使用真凭据」的风险。
8. **`grep` 类 verify 的判据很弱**：`laya_is_` 命中的是测试函数名（假阳性），而真正的密钥不会长成 `laya_is_*` 的样子。建议把这条 verify 换成 §7.1 里的宽扫描（`sk-` 长字面量 + 已知真实凭据前缀）。

## 10. 三项如实披露（按队长要求逐条列出）

1. **声称①的 51,225ms / 60,072ms 基线无法离线复现**（需真实端点，任务明令禁止访问）。我能在本地复现的是**同一机制的耗时量级**：`timeout=30.0` × 2 次尝试 = 60563.6ms（与 60072ms 差 0.8%）、× 3 次尝试（仓库默认 `max_attempts=3`）= 91528.8ms。**哪些数字是本地复现、哪些引自 change 文件，已在该节逐项标注**。
2. **`resolve_api_key` 在 `use_env=False` 时会让 `~/.dsh/.credentials.yaml` 优先于 `os.environ`**，我的首版脚本因此把一枚真实形制的密钥发给了**本机**假服务端（未出网，闸门全程生效）；后续改为在导入配置模块前注入环境变量，并用 `is_fake=true` 自证。详见 §7.3。
3. **仓库内无真实密钥**：唯一的长 `sk-` 字面量是 `tests/test_model_gateway.py` 里的合成示例（用于日志掩码测试），真实凭据前缀（`sk-cau` 长串、`rc-fde`）零命中；`config/grouppig.toml` 的 laya 段只有 `base_url` / `api_key_env` / `api_key_field` 三个非密字段。详见 §7.1、§7.2。

## 11. 复现方式（本次临时脚本不在仓库内，按要求只交付本文档）

脚本要点已在 §1.4 说明；三份临时脚本本体位于 `/tmp/indep_all.py`（六条声称）、`/tmp/indep_baseline.py`（基线机制）、
`/tmp/indep_final.py`（默认阈值 + 闸门自证 + 延迟复测）。它们**只在 /tmp**，不污染仓库（`docs/` 之外无任何改动）。
若要长期保留，建议由作者把它们固化成 `tests/` 下的正式用例（假服务端与闸门已在 `tests/fakes_laya.py`、`tests/test_laya_e2e.py` 里具备）。
