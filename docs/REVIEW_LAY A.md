# 审查报告：LAY A System-1 接入（t20 `rpc:model.system1` + 置信度门控）

- 审查任务：**t24**（kind=review, round 1）；被审任务：**t20**（状态 completed）
- 仓库根：`/home/hedass/文档/项目/GrouPig`（**非 git 仓库**，证据以文件内容、行号与实测脚本为准）
- 被审落点：`router.py:187 ModelRouter.system1`、`router.py:479 _escalate`、`router.py:406 _run_plan`、
  `di.py:42 / di.py:281`、`laya_system1.py`（t18 传输层）
- 本报告**整体覆写**上一轮 11:47/11:55 的快照版：其中「laya_system1.py 不存在 / 交付物整体缺失」
  的结论在 11:49 落地后已失效，本报告不沿用任何旧条目，全部结论均为本次重读代码 + 实测所得。

## 0. 结论摘要

| 问题 | 回答 |
| --- | --- |
| **审查判定** | **needs_revision**（交付物已落地且主体正确，但存在 1 条 high 门控正确性缺陷与 3 条 medium 异常契约缺陷，必须修复后复审） |
| **是否存在契约违规？** | **否**（见 ①/②：`src/**/*.py` 中所有 `rpc:` / `kafka:` 字面量均命中 api-index，自造名字 NONE；`rpc:model.system1` 的 owner 与注册 module 字符串一致） |
| **是否存在未包装异常穿透？** | **是**（3 处）：`router.py:568` 非对象响应 → 未包装 `AttributeError`；`laya_system1.py:222` 解码期 `TransportError` 绕过 fallback；`router.py:469-472` 不可重试 `TransportError` 原样抛出（无 `attempts`）。**system1 主路径（401/422/503/超时/answers 形状错）已被 `router.py:258` 覆盖，不穿透。** |
| **门控多问题聚合口径** | **已定义（min）且有测试覆盖**（`router.py:34`、`test_system1_aggregate_is_min_not_mean`）；但**实现口径与文档口径不一致**：文档说「所有问题」，实现是「已作答问题」→ **F1（high）** |

## 1. 八条审查清单逐条结论

### ① 契约纪律：新增名字是否全部来自 api-index.json，有无自造 rpc:/kafka: 名字
**结论：通过。自造名字 = NONE。**

- 实测：正则扫描 `src/**/*.py` 中全部 `"rpc:..."` / `"kafka:..."` 字符串字面量，与 `contract.api_index()` 求差集 → **`invented: NONE`**。
- `rpc:model.system1` 确实存在于设计：`normify-grouppig/api-index.json`（实测 `json.dumps(idx)` 含 `model.system1` → `True`）。
- `di.py:42` 用 `contract.assert_known_name("rpc:model.system1")` 做写时校验，名字不可能写错而不报错。

### ② 名字归属是否正确（应归属 `grouppig.infra.model-gateway.router`）
**结论：通过。设计侧与运行时侧一致。**

- 设计侧：`api-index.json` → `rpc:model.system1 -> grouppig.infra.model-gateway.router`。
- 运行时侧：`di.py:281` `registry.register("rpc:model.system1", _system1, module="grouppig.infra.model-gateway.router", replace=True)`。
- 实测 7 个 model 相关名字的「注册 module 字符串 vs `contract.owner()`」逐条比对 → **全部 MATCH**（含 chat/embed/classify/system1）。

### ③ 错误分类：401/422 不可重试、5xx/超时 可重试、有无不可重试被无限重试
**结论：通过。分类正确，不可重试的错误确实不重试。**

- `laya_system1.py:49` `RETRYABLE_STATUS = frozenset({408, 409, 425, 429})`；`:52` `FATAL_STATUS = frozenset({400,401,403,404,405,422})`。
- `laya_system1.py:416`：`retryable = response.status_code in RETRYABLE_STATUS or response.status_code >= 500` → **401/422 判为不可重试，5xx 可重试**。
- 超时：`retry.py` 的 `asyncio.wait_for` 路径独立于 `retryable`，超时天然重试（正确）。
- 实测（`max_attempts=3`）：**401 → LAY A 仅 1 次 HTTP 调用**（未重试）；**503 → 3 次**（有界，未放大）。
- 无「不可重试却无限重试」：见 F4 反而相反（不可重试的错误直接向上抛）。

### ④ 降级路径：LAY A 故障时是否抛未包装异常穿透 / 是否静默返回空结果让上层误判
**结论：system1 主路径通过；仍有 3 条未包装穿透（F2/F3/F4）；无「静默空结果」，但存在「静默部分结果」（F1）。**

- **已覆盖（正面证据）**：`router.py:258` `except (ModelCallError, TransportError)` 统一转 `_escalate`。实测 6 种故障形状：
  503 / 401 / 422 / 超时 / `answers` 缺键 / `answers` 非对象 / 答案值非对象 / `confidence` 非法 → **全部 `source=escalated`，无异常穿透**。
- **未覆盖（F2）**：LAY A 返回 JSON **数组**（非对象）→ `router.py:250` 直接调 `_decode_system_one`，绕过 `router.py:540` 的 `isinstance(value, Mapping)` 守卫，`:568` `data.get("model")` 抛 **未包装 `AttributeError`**，`:258` 不捕获该类型。
- **未覆盖（F3）**：HTTP 200 + `answers: {}` 在**解码期**抛 `TransportError`（`laya_system1.py:222`），该阶段在 `_run_plan` **之后**，因此 `fallback_models` 被跳过（实测 `chat calls=0`），原始 `TransportError` 直穿调用方。
- **未覆盖（F4）**：`router.py:469-472` 对不可重试 `TransportError` 原样 `raise`（无 `attempts` 属性）。
- **无静默空结果（正面）**：`system1` 的异常路径必走 `_escalate`（`router.py:263`）；升级也失败则抛包装过的 `ModelCallError`（`router.py:497`）。
- **但存在静默部分结果（F1）**：见 ⑤。

### ⑤ 门控正确性：confidence 阈值比较、多问题聚合口径、是否有测试
**结论：口径已定义（min）且有测试；但实现口径与文档口径不一致 → F1（high）；阈值等号边界未覆盖 → F7（low）。**

- 阈值常量：`router.py:36` `SYSTEM1_ESCALATE_BELOW = 0.4`；聚合常量：`router.py:34` `SYSTEM1_AGGREGATE = "min"`。
- 比较：`router.py:275` `lowest = min(confidences.values()) if confidences else 0.0`；`router.py:278` `if lowest < threshold`（**严格小于**，与参数名 `escalate_below` 语义一致）。
- 空答案兜底：`confidences` 为空时取 `0.0` → 必升级。实测 `answers={}` → `escalated`（正确，但**无专门用例**）。
- **测试覆盖（正面）**：`tests/test_model_gateway.py:562 test_system1_aggregate_is_min_not_mean` 明确验证「均值达标但最小值不达标仍升级」（min 而非 mean）；`:531 test_system1_escalates_when_any_question_is_uncertain` 验证「单个问题低置信即整体升级」。**口径本身定义清楚、有测试**，这一条比上一轮快照的「完全不存在」已大幅改善。
- **口径缺陷（F1）**：`router.py:204` docstring 写「取**所有问题** confidence 的最小值」，但 `router.py:273-275` 的 `answers`/`confidences` 只包含 **LAY A 实际返回的问题**。若 LAY A 只答 6 问中的 1 问，`min` 只在这 1 个上取，**未作答的问题完全不参与门控**。
  实测：`asked=6 answered=1 source=system1 lowest=0.95 ok=True chat_calls=0` —— 调用方拿到一个**看起来完全正常**（`ok=True`、`source="system1"`）的响应，却有 5 个决策静默缺失。
- 评价：聚合口径的**语义**（min）定义清楚且有测试，但**定义域**（对哪些问题取 min）实现与文档不符，且无「作答数 == 提问数」的完备性校验、无任何用例覆盖部分作答。

### ⑥ 是否真的只发一次 HTTP（有没有退化成循环调用）
**结论：通过。**

- 结构：`laya_system1.py:132 build_request` 把**所有问题装进同一个请求体**；`laya_system1.py:409` 每次 `complete` 只 `client.post` 一次；`router.py:406 _run_plan` 是「目标 × 重试」两层，无第三层放大。
- 实测：**6 个问题 → LAY A 1 次 HTTP**（`questions in body = 6`）；**12 个问题 → 仍 1 次**（`questions in body = 12`）。
- 重试有界：`max_attempts=3` + 503 → 3 次；`max_attempts=3` + 401 → 1 次。无循环退化。
- 低置信度升级是**设计内**的第 2 次 HTTP（走对话模型，`router.py:494`），不计入「一次前向」的退化。

### ⑦ 与既有 rpc:model.classify 的兼容性是否被破坏
**结论：解码与路由正确，无功能破坏；但异常契约不一致（F3/F4）与静默回落（F5）是新引入的行为变化。**

- 注册未破坏：`di.py:279` 仍注册 `rpc:model.classify`；`tests/test_model_gateway.py` 42 例 + `test_contract_alignment.py` 全绿（55 passed）。
- LAY A 解码正确：`router.py:540` 检测到 `answers` 信封后走 `_decode_system_one` → `laya_system1.py:212 normalize_classification`。实测 `label=闲聊 scores={闲聊:0.8, 提问:0.2} conf=0.8`。
- 跨服务商回落正确：`config/grouppig.toml:62-66` classify 现为 `provider="laya"` / `fallback_models=["grok-4.6"]`；`router.py:394-404 _fallback_provider` 正确地把 LAY A 的回落目标挂到 `default_provider`。实测 503 → `provider=a6api model=grok-4.6`。
- **默认 provider 已变更为 LAY A**（`config/grouppig.toml:62`），因此 classify 的行为面已整体切换，回归范围不止 router：`_decode` 的 `answers` 分支（`router.py:540`）现在是 classify 的**主路径**，其健壮性直接决定 classify 的可用性 → 见 F3。
- **上一轮假设「静默空标签」的复核结论**：`decode_response({answers:…}, task="classify")` 直接调用时**确实**静默返回 `label=None, scores={}`（实测 `ok=False`）——但 t20 的 router 主路径**不会**走到那里（`router.py:540` 已改道）。
  故：**classify 主路径不再产生静默空标签**（实测空标签场景 `label=None` 但 **`ok=False`**，可被调用方检出，非 blocker）；
  但 `rpc:model.decode`（`di.py:248` → `codec.decode` → `decode_response`）仍是该静默路径的公开入口 → **F6（low）**。

### ⑧ 是否把密钥写进代码或日志（日志脱敏）
**结论：通过（无字面量密钥、按字段名脱敏有效）；残留 1 条 low（F7）。**

- 代码/配置无密钥：`grep -rnE "sk-[A-Za-z0-9]{8,}|Bearer [A-Za-z0-9]{10,}" config/ src/grouppig/infra/` → **零命中**。
- 密钥只进请求头：`laya_system1.py:356-360 _headers()` → `Authorization: Bearer <key>`，不进请求体；密钥经 `api_key_env` 从环境解析，不落代码。
- 日志脱敏：`logger.py:24 SENSITIVE_KEYS`、`:34-45 _redact` 对 `fields` 的**键名**做子串匹配脱敏，`:160,205` 统一套用。
- 残留缺口（F7）：`_redact` 只按**键名**匹配，不扫字符串**值**。而 `laya_system1.py:313 _short` 会把响应体前 300 字拼进 `TransportError` 消息（`:418`），该消息又经 `router.py:262`（`error=repr(exc)`）与 `router.py:613`（`error=repr(error)`）以**自由文本**入日志 → 若服务商回显密钥则不会被脱敏。

## 2. Findings

| id | severity | problem | requiredFix | file:line |
| --- | --- | --- | --- | --- |
| **F1** | **high** | **门控定义域错误：部分作答静默通过。** docstring（`router.py:204`）承诺「取所有问题 confidence 的最小值」，实现只对 **LAY A 实际返回的问题**取 min。实测：提 6 问答 1 问（confidence 0.95）→ `source="system1"`、`lowest_confidence=0.95`、`ok=True`、**零次升级**，调用方拿到一个完全正常外观的响应却缺 5 个决策。无完备性校验、无任何用例覆盖。 | 在 `system1` 聚合前做**完备性校验**：对 `normalized`（提问集合）与 `answers`（作答集合）求差集，缺失的 qid 一律按 `confidence=0.0` 计入 `confidences`（等价于「未作答即不确定」→ 必然升级）；若选择「部分作答可接受」，则必须在 `ModelResponse.raw` 中显式标注 `answered` / `missing_qids` 并同步修正 `router.py:204` 的 docstring。两种做法都必须补用例：提 6 问答 1 问（期望升级或显式标注缺失）。 | `src/grouppig/infra/model_gateway/router.py:273-278`（聚合处）、`router.py:204`（docstring）、`src/grouppig/infra/runtime/laya_system1.py:183-209`（`normalize_answers` 不做完备性校验） |
| **F2** | **medium** | **非对象响应 → 未包装 `AttributeError` 穿透。** `router.py:250` 在 `system1` 内**直接**调用 `_decode_system_one`，绕过 `router.py:540` 的 `isinstance(value, Mapping)` 守卫；`router.py:568` `data.get("model")` 对 JSON 数组抛 `AttributeError`，`:258` 的 `except (ModelCallError, TransportError)` 不捕获。实测：LAY A 返回 `[1,2,3]` → 调用方收到 `AttributeError: list object has no attribute get`。这与 `router.py:211` docstring 明示的「响应不合规时不抛未包装异常」矛盾。内置 `LayaSystemOneTransport` 在 `laya_system1.py:429-432` 拦了非 dict，故生产默认路径不触发，但守卫在传输层而非契约边界，注入/替换 transport（测试与 `set_transport` 均如此）即可复现。 | 在 `_decode_system_one` 开头（或 `system1` 调用点）加 `if not isinstance(data, Mapping): raise TransportError(...)`，使其走 `:258` 的统一降级；或让 `system1` 改调 `self._decode(...)` 复用其 Mapping 守卫。补用例：LAY A 返回 JSON 数组 → 期望 `source="escalated"` 而非异常。 | `src/grouppig/infra/model_gateway/router.py:250`、`router.py:555-568`、`router.py:258` |
| **F3** | **medium** | **解码期失败绕过 fallback 且抛未包装 `TransportError`。** HTTP **200** 但 `answers` 为空对象时，`laya_system1.py:222` 在**解码阶段**抛 `TransportError`；解码在 `_run_plan`（`router.py:248`）**之后**执行，因此 `fallback_models=["grok-4.6"]` 完全不被尝试。实测：`laya calls=1  chat calls=0`（fallback 被跳过），调用方收到原始 `TransportError: LAY A 响应没有任何答案`。对 `rpc:model.classify` 而言，一个「HTTP 成功但内容不合规」的响应比网络错误更难排查，且不会触发既有的降级链。 | 把「解码/归一化失败」纳入降级：在 `_execute`（`router.py:341-345`）捕获解码期的 `TransportError`，视同该目标失败并推进 `_run_plan` 的下一个目标（或至少包装成 `ModelCallError` 并附 `attempts`）。补用例：200 + `answers:{}` → 期望回落到 grok-4.6（或抛 `ModelCallError`，而非裸 `TransportError`）。 | `src/grouppig/infra/runtime/laya_system1.py:222`、`src/grouppig/infra/model_gateway/router.py:341-345`、`router.py:406-477` |
| **F4** | **medium** | **不可重试错误原样抛出，同一 RPC 出现两种异常类型。** `router.py:469-472` 对 `TransportError` 且 `retryable=False` 的情况 `raise last_error`（注释称有意保留「配置错误直接报错」），结果是 `rpc:model.classify` 在无 `fallback_models` 时抛**裸 `TransportError`（`attempts` 属性为 `None`）**，而重试耗尽时抛 `ModelCallError`。实测：401 + 空 fallback → `RAISED TransportError | attempts attr = NONE`。调用方按网关文档写 `except ModelCallError` 会漏接 401。system1 侧已被 `:258` 覆盖，不受影响。 | 二选一并写进 docstring：① 统一包装为 `ModelCallError(..., attempts=..., last_error=exc)` 并保留 `status`（推荐，异常契约单一）；② 保留裸 `TransportError` 但在 `ModelRouter` 类 docstring 中明确声明「不可重试的传输错误以 `TransportError` 抛出」，并补用例固化该契约。 | `src/grouppig/infra/model_gateway/router.py:469-472`、`router.py:450-454` |
| **F5** | **low** | **LAY A 密钥配错时静默回落到 grok-4.6，无任何指标可观测。** 实测 classify + `fallback_models=["grok-4.6"]`：401 与 422 均**不报错**，`provider=a6api model=grok-4.6` 正常返回 `label=闲聊`，仅 `router.py:459-468` 记一条 `model.fallback` warning。回落本身是配置的意图（`config/grouppig.toml:61` 注释「grok-4.6 保留为降级目标」），但 `health()`（`router.py:642-648`）与 `_stats` 都不暴露回落次数，运维无法在不读日志的情况下发现 LAY A 已整体不可用（成本/延迟/语义已静默切换）。 | 在 `_stats` 中累加 `fallback` 计数并在 `health()` 中暴露（如 `stats["fallback"]` / `stats["laya_unavailable"]`），使 LAY A 失效可被监控发现。 | `src/grouppig/infra/model_gateway/router.py:457-468`、`router.py:642-648` |
| **F6** | **low** | **`rpc:model.decode` 对 LAY A `answers` 信封静默返回空结果。** `decode_response`（`codec.py:239`）不识别 `{answers: …}` 形状：实测 `decode_response({answers:…}, task="classify", labels=…)` → `label=None, scores={}`，**不抛异常**。该函数经 `codec.decode`（`codec.py:306-318`）由 `di.py:248` 注册为公开 RPC。router 主路径已在 `router.py:540` 改道，故 classify 不受影响；但直接调用 `rpc:model.decode` 的调用方会拿到静默空结果（`ok=False` 可检出，故降为 low）。 | 在 `decode_response` 中检测到顶层 `answers` 键时，要么转交 system1 解码逻辑，要么抛 `ModelCallError("LAY A 响应请走 system1 解码")`，避免静默空结果。 | `src/grouppig/infra/model_gateway/codec.py:239-255`、`codec.py:306-318`、`src/grouppig/infra/runtime/di.py:248` |
| **F7** | **low** | **阈值等号边界无用例。** `router.py:278` 用严格 `<`：`confidence == escalate_below` 时**不升级**（实测 `conf=0.4 threshold=0.4 → source=system1`）。语义与参数名 `escalate_below` 一致，但现有用例（`:562`）只覆盖 0.55 vs 0.5/0.6，未固定等号行为；阈值语义一旦被无意改成 `<=` 不会被测试发现。 | 补 1 条边界用例：`confidence == escalate_below` 断言不升级（固化当前语义）。 | `src/grouppig/infra/model_gateway/router.py:278`、`tests/test_model_gateway.py:562-583` |
| **F8** | **low** | **日志自由文本未脱敏（响应体入日志）。** `laya_system1.py:313 _short` 取响应体前 300 字拼进 `TransportError` 消息（`:418`），该消息经 `router.py:262`（`error=repr(exc)`）与 `router.py:613`（`error=repr(error)`）入日志；`logger.py:34-45 _redact` 只按**键名**匹配 `SENSITIVE_KEYS`，不扫字符串值 → 服务商若在错误体里回显密钥则不会被脱敏。 | 在 `_short`（或日志出口）对自由文本做正则脱敏：`Bearer\s+\S+`、`sk-[A-Za-z0-9_-]{8,}`、`"(api_?key|token)"\s*:\s*"[^"]+"`。 | `src/grouppig/infra/runtime/laya_system1.py:313-314,418`、`src/grouppig/infra/model_gateway/router.py:262,613`、`src/grouppig/infra/logger.py:34-45` |

> 未作为 finding 的类型（按任务要求）：命名/排版/注释风格、模块划分偏好、日志文案、`FATAL_STATUS` 未被引用（`laya_system1.py:52` 仅定义未使用，属死代码但无错误行为）、`SYSTEM1_AGGREGATE` 未进 `__all__`（不影响 `import`）。

## 3. 已核对且无问题的正面证据（非「看起来没问题」）

1. **无自造契约名**：正则扫描 `src/**/*.py` 全部 `rpc:`/`kafka:` 字面量对 api-index 求差集 → `invented: NONE`。
2. **归属正确**：7 个 model 相关名字的注册 `module=` 与 `contract.owner()` 逐条 MATCH（含 `rpc:model.system1`）。
3. **状态码分类正确**：`laya_system1.py:416`；`RETRYABLE_STATUS={408,409,425,429}`、`FATAL_STATUS={400,401,403,404,405,422}`，≥500 判可重试。
4. **不可重试错误不被重试**：实测 401（`max_attempts=3`）→ LAY A 仅 1 次 HTTP。
5. **重试有界**：实测 503（`max_attempts=3`）→ LAY A 3 次，随后 1 次升级，无放大。
6. **system1 主路径无异常穿透**：503 / 401 / 422 / 超时 / `answers` 缺键 / `answers` 非对象 / 答案值非对象 / `confidence` 非法 → 实测**全部** `source="escalated"`（`router.py:258`）。
7. **失败必抛、无静默空结果**：`system1` 异常路径必走 `_escalate`（`router.py:263`）；升级再失败抛包装 `ModelCallError`（`router.py:497`，实测 `test_system1_wraps_error_when_escalation_also_fails`）。
8. **一次前向确实只发一次 HTTP**：6 问 → 1 次；12 问 → 1 次（`laya_system1.py:132,409`、`router.py:406`）。
9. **门控聚合口径已定义且有测试**：`SYSTEM1_AGGREGATE="min"`（`router.py:34`）；`test_system1_aggregate_is_min_not_mean`（均值达标/最小值不达标仍升级）、`test_system1_escalates_when_any_question_is_uncertain` 均通过。
10. **classify 未出现静默空标签**：空标签场景实测 `label=None` 但 **`ok=False`**（`codec.py:86-89 ok` 定义），调用方可检出；`answers={}` 直接抛错（`laya_system1.py:222`）而非静默返回。
11. **classify 跨服务商回落正确**：实测 503 → `provider=a6api model=grok-4.6`，`_fallback_provider`（`router.py:394-404`）把 LAY A 的回落目标正确挂到 `default_provider`。
12. **预算释放写法正确**：`system1` 失败路径先 `meter.release`（`router.py:259-260`）再升级；成功路径 `meter.consume`（`router.py:277`）。
13. **密钥合规**：无字面量密钥；密钥仅进 `Authorization` 头（`laya_system1.py:356-360`）；日志按字段名脱敏（`logger.py:24,34-45`）。

## 4. Verify 命令与结果

```
$ uv run python -m pytest tests/test_model_gateway.py tests/test_contract_alignment.py -q -o addopts=""
55 passed in 9.31s

$ uv run python -c "import json;idx=json.load(open('normify-grouppig/api-index.json'));print('model.system1' in json.dumps(idx))"
True          # 名字在设计契约里存在，且已落地（di.py:281 注册、契约缺口为无）
```

## 5. 复现脚本（临时探针，位于 /tmp，未入库）

**F1 部分作答（核心发现）**：
```
laya = RawTransport({"answers": {"q0": {"type": "score", "score": 0.9, "confidence": 0.95}}, "usage": {}})
router.set_transport(laya, provider="laya")
resp = await router.system1({"t": 1}, Q6)   # Q6 = 6 个问题
# 实测: asked=6 answered=1 source=system1 lowest=0.95 ok=True chat_calls=0
```

**F2 非对象响应**：
```
router.set_transport(RawTransport([1, 2, 3]), provider="laya")
await router.system1({"t": 1}, Q)
# 实测: AttributeError: list object has no attribute get   (router.py:568)
```

**F3 解码期失败绕过 fallback**：
```
router.set_transport(RawTransport({"answers": {}, "usage": {}}), provider="laya")  # HTTP 200
await router.classify("在吗", ["闲聊", "提问"])
# 实测: TransportError: LAY A 响应没有任何答案 | laya calls=1 chat calls=0
```

**F4 / F5 classify 回落行为（复核上一轮假设）**：
```
# 401 + fallback_models=["grok-4.6"]  -> OK, provider=a6api model=grok-4.6 label=闲聊（静默回落，F5）
# 401 + fallback_models=[]            -> RAISED TransportError, attempts attr = NONE（未包装，F4）
```

## 6. 给队长的处置建议

1. **判定 needs_revision**（非 reject）：t20 交付物完整落地、契约零缺口、55 例全绿，主体正确；但 F1 是门控正确性缺陷，必须修复后复审。
2. **F1 优先级最高**：它决定「LAY A 部分作答」时上层是拿到显式升级还是静默缺失决策。修复只需在 `router.py:273-278` 聚合前用 `normalized` 补齐缺失 qid（缺失按 0.0 计），改动小、风险低。
3. **F2/F3/F4 建议合并为一次「异常契约收敛」修复**：三者都属「同一 RPC 抛出非文档化异常类型」，建议统一为 `ModelCallError` 并在 `ModelRouter` docstring 写明异常契约。
4. **F5/F6/F7/F8 可作为低优先级加固**，随下一次触碰 `router.py` / `codec.py` 时一并处理。
5. 上一轮快照的 F1/F2/F3（交付物缺失、门控不存在）**已全部失效**，不应再作为待办；本报告已整体覆写该文件。
