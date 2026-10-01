# session 话题会话层（t5 交付说明）

> 归属：`session-engineer`（任务 t5）。契约名字逐字取自 `normify-grouppig/api-index.json`，
> 本域只有 **25 个 `rpc:` + 2 个 `kafka:`**，多余的名字一律不注册（`SessionLayer.contract_check()` 会报 `unknown`）。

## 1. 交付内容

16 个设计叶子（`state=planned` → 已落地）逐字镜像 normify 模块路径：

| 设计叶子（normify id） | 源码 | 契约名字 |
| --- | --- | --- |
| `grouppig.session.topic.detector.boundary` | `src/grouppig/session/topic/detector/boundary.py` | `rpc:topic.boundary.detect` |
| `grouppig.session.topic.detector.candidate` | `.../topic/detector/candidate.py` | `rpc:topic.candidate.generate` |
| `grouppig.session.topic.detector.ranker` | `.../topic/detector/ranker.py` | `rpc:topic.detect`、`rpc:topic.resolve`、`kafka:grouppig.topic.changed` |
| `grouppig.session.topic.embedder.cache` | `.../topic/embedder/cache.py` | `rpc:topic.embed.cache.get`、`rpc:topic.embed.cache.set` |
| `grouppig.session.topic.embedder.similarity` | `.../topic/embedder/similarity.py` | `rpc:topic.embed`、`rpc:topic.similarity` |
| `grouppig.session.lifecycle.state-machine` | `.../lifecycle/state_machine.py` | `rpc:session.open`、`rpc:session.update`、`rpc:session.current`、`rpc:session.archive` |
| `grouppig.session.lifecycle.heat` | `.../lifecycle/heat.py` | `rpc:session.heat` |
| `grouppig.session.lifecycle.archive-trigger` | `.../lifecycle/archive_trigger.py` | `rpc:session.archive.check` |
| `grouppig.session.lifecycle.event-emitter` | `.../lifecycle/event_emitter.py` | `kafka:grouppig.session.completed` |
| `grouppig.session.threads.weaver.segmenter` | `.../threads/weaver/segmenter.py` | `rpc:threads.segment` |
| `grouppig.session.threads.weaver.linker` | `.../threads/weaver/linker.py` | `rpc:threads.weave`、`rpc:threads.link` |
| `grouppig.session.threads.weaver.outliner` | `.../threads/weaver/outliner.py` | `rpc:threads.outline` |
| `grouppig.session.threads.cross.reference-parser` | `.../threads/cross/reference_parser.py` | `rpc:cross.detect`、`rpc:cross.parse` |
| `grouppig.session.threads.cross.matcher` | `.../threads/cross/matcher.py` | `rpc:cross.match` |
| `grouppig.session.wake.buffer` | `.../wake/buffer.py` | `rpc:wake.buffer.push`、`rpc:wake.buffer.pop` |
| `grouppig.session.wake.restorer` | `.../wake/restorer.py` | `rpc:session.wake`、`rpc:session.sleep` |

运行时补充包（设计树暂无对应叶子，待队长登记）：

| 运行时模块 | 源码 | 作用 |
| --- | --- | --- |
| `grouppig.session.runtime.messages` | `session/runtime/messages.py` | 归一化消息视图 + 切词 / 关键词 / 余弦 / Jaccard / 新鲜度（纯函数） |
| `grouppig.session.runtime.errors` | `session/runtime/errors.py` | `SessionError` / `SessionNotFound` / `InvalidTransition` / `WakeError` / `DependencyMissing` |
| `grouppig.session.runtime.di` | `session/runtime/di.py` | `SessionLayer` 装配 + 25 个处理器注册 + `attach_session(container)` |

装配（集成入口 t10 只需一行）：

```python
from grouppig.session.runtime.di import attach_session

layer = await attach_session(container)   # 注册 25 个 rpc: + 订阅 kafka:grouppig.qq.message.received
await container.call("rpc:topic.detect", group_id=100, messages=window)
```

`SESSION_RPC` / `SESSION_TOPICS` 两个常量直接从 `contract.api_index()` 反查生成，
因此「代码登记的名字」与「契约名字」不可能漂移。

## 2. 数据流

```
gateway: kafka:grouppig.qq.message.received
   │（EventBus 订阅，SessionLayer.on_message）
   ├─► rpc:topic.detect            话题识别 → 开/续会话 → kafka:grouppig.topic.changed
   ├─► rpc:threads.weave           聊天线编织 → rpc:thread.save
   └─► rpc:session.update          并入消息 → 热度 → 状态转移 → 归档检查
                                        │（冷场 + 满足条件且 auto_archive）
                                        ▼
                                  rpc:session.archive
                                        ├─► rpc:archive.save（memory）
                                        ├─► kafka:grouppig.session.completed
                                        └─► rpc:review.on-session-completed（反思层 t7）

跨会话引用：消息里出现「上次/之前/那个」或引号片段
   → rpc:cross.detect → rpc:cross.parse
   → rpc:cross.match → rpc:thread.find-cross → rpc:session.wake
   → rpc:archive.load + rpc:chat.query → rpc:wake.buffer.push（生成器按需 pop）
```

域内协作走注入实例（不经注册表）：`state-machine` 直接持有 `HeatManager` / `ArchiveTrigger` / `SessionEventEmitter`；
`ranker` 直接持有 `TopicCandidateGenerator` / `TopicEmbedder` / `SessionStateMachine`。
跨域一律走构造期注入的 `caller`（`await caller("rpc:chat.query", ...)`），保持叶子之间不互相 import。

## 3. 25 个 `rpc:` 的入参 / 返回

### 3.1 话题（topic）

| rpc | 入参 | 返回 |
| --- | --- | --- |
| `topic.boundary.detect` | 可选 `messages`；`silence_gap`/`keyword_jump`/`now`/`group_id`/`limit` | `{boundary, reason, reasons[], at, gap, keyword_overlap, keywords{head,tail}, signals{silence,keyword_jump,reply_target}, message_count}` |
| `topic.candidate.generate` | `group_id`；可选 `messages`/`features`/`now`/`top`/`units`/`min_count`/`rank` | `{candidates[{topic_id,phrase,keywords[],message_count,first_ts,last_ts,units[]}], count, source, ranked, ranked_count}` |
| `topic.detect` | `group_id`；可选 `messages`/`features`/`now`/`open_session`/`threshold`/`top` | `{topic_id, phrase, keywords[], score, scored[], changed, previous_topic_id, session, opened, session_id, method}` |
| `topic.resolve` | `text`；可选 `candidates`/`topic_id`/`group_id`/`threshold`/`now` | `{topic_id, resolved, merged, method, score, scores[], matched, session, update, candidates}` |
| `topic.embed` | `text` 或 `texts`；可选 `model`/`use_cache`/`now` | `{vectors[][], keys[], cached[], model, via, dim, count}` |
| `topic.similarity` | `left` + `right`，或 `texts`；可选 `use_embedding`/`model`/`now`/`cosine_weight` | `{score, method, cosine, jaccard, cosine_weight, left, right}` |
| `topic.embed.cache.get` | `key` 或 `text`；可选 `prefix`/`now` | `{hit, vector, key, dim, age, ttl, hits}` |
| `topic.embed.cache.set` | `key` 或 `text` + `vector`；可选 `prefix`/`ttl`/`now` | `{stored, key, dim, ttl, size, evicted}` |

### 3.2 生命周期（lifecycle）

| rpc | 入参 | 返回 |
| --- | --- | --- |
| `session.open` | `group_id`；可选 `topic_id`/`title`/`messages`/`keywords`/`now`/`cool_others` | `{session, session_id, opened, cooled[], topic_id}` |
| `session.update` | `session_id`，或 `group_id`；可选 `messages`/`topic_id`/`title`/`keywords`/`thread_ids`/`now`/`advance`/`check_archive`/`auto_archive`/`force` | `{session, heat, archive_check, archived, archive, transition, message_count}` |
| `session.current` | 可选 `group_id`/`session_id`/`state`/`include_archived` | `{session, count, found, group_id, states}` |
| `session.archive` | `session_id`；可选 `messages`/`threads`/`reason`/`now`/`heat` | `{session, archive, saved, event, delivered[], thread_ids[]}` |
| `session.heat` | 可选 `group_id`/`messages`/`session`/`session_id`/`now`/`seconds` | `{heat, level, cooling, rate, participants, recency, message_count, last_ts, idle_seconds, source, thresholds}` |
| `session.archive.check` | 可选 `session`/`session_id`/`group_id`/`messages`/`heat`/`now` | `{archive, reasons[], checks{cooldown,heat_low,drifted,few_messages}, idle_seconds, cooldown, heat, heat_source, heat_level, heat_threshold, drift, drift_overlap, drift_threshold, message_count}` |

### 3.3 聊天线（threads）

| rpc | 入参 | 返回 |
| --- | --- | --- |
| `threads.segment` | 可选 `messages`/`session_id`/`group_id`/`gap_seconds`/`max_segment`/`limit`/`messages_of` | `{segments[], count, message_count, gap_seconds, max_segment}` |
| `threads.outline` | `messages`；可选 `thread_id`/`topic_id`/`conclusions` | `{outline{opinion_chain[],disputes[],conclusions[],phases[]}, summary, message_count, thread_id, topic_id}` |
| `threads.weave` | `group_id`；可选 `messages`/`session_id`/`topic_id`/`now`/`save`/`limit` | `{thread_id, created, segment_count, message_count, outline, summary, saved, woven_at}` |
| `threads.link` | `thread_id`；可选 `messages`/`topic_id`/`session_id`/`now`/`detect_cross` | `{thread_id, edges[], edge_count, linked, cross[], cross_count}` |
| `cross.detect` | `text`；可选 `messages`/`thread_id`/`min_confidence`/`match` | `{references[], count, strong, weak, quotes, matched, matches[], strongest}` |
| `cross.parse` | `text`（或 `reference`）；可选 `group_id`/`thread_id`/`limit`/`match` | `{reference, query, keywords[], lines[], thread_id, group_id}` |
| `cross.match` | `query`（或 `text`）；可选 `group_id`/`limit`/`min_score`/`wake`/`threads` | `{matches[], count, best, query, keywords[], woken[]}` |

### 3.4 唤醒（wake）

| rpc | 入参 | 返回 |
| --- | --- | --- |
| `session.wake` | `session_id`；可选 `reason`/`ttl`/`snippets`/`messages`/`threads`/`now` | `{session_id, woken, reason, context, buffer, expires_at}` |
| `session.sleep` | `session_id`；可选 `peek`/`save`/`now` | `{session_id, slept, context, archive, saved, buffer}` |
| `wake.buffer.push` | `context`；可选 `session_id`/`reason`/`ttl`/`now` | `{pushed, session_id, size, expires_at, context}` |
| `wake.buffer.pop` | 可选 `session_id`/`peek`/`limit`/`now` | `{contexts[], count, context, peek, size, now}` |

`wake.buffer.pop` 的 `peek=True` 是生成器打包上下文用的「只看不删」；默认弹出即删（用完即弃）。

## 4. 算法与阈值

### 4.1 话题边界（`boundary`）

三路信号取优先级最高者作为 `reason`（`silence(3) > keyword_jump(2) > reply_target(1)`）：

* **静默**：相邻两条消息时间差 ≥ `DEFAULT_SILENCE_GAP = 300s`；
* **关键词突变**：把消息窗对半切，前后半段各取 top-12 关键词，`jaccard < DEFAULT_KEYWORD_JUMP = 0.25`；
* **回复对象变化**：最后两条带 `reply_to` 的消息，目标消息的发送者不同。

关键词来自 `messages.tokenize`：ASCII 词小写 + CJK 连续串的 **2 字滑窗**（首字是功能词的窗口丢弃，
如「的聊」），再按 `词频 + LENGTH_BONUS(0.2) × (词长-1)` 打分取前 N。
不用整串子串：那样「烤肉真香 / 烤肉配啤酒 / 烤肉再来一份」会共享太多长窗口，
同一话题的重叠度被抬高，反而掩盖真实切换；二元组下同话题实测 0.06~0.17、换话题实测 0.00。

### 4.2 话题候选（`candidate`）

`topic_phrase` 用「贪心不重叠的 2~4 字中文单元」拼接短语（`MIN_UNIT=2`/`MAX_UNIT=4`），
候选按 `min_count=2`（默认出现 2 次以上）过滤，`top=5` 截断；
`rank=False` 是默认——候选生成**不**回头调 `rpc:topic.detect`，避免排序器 ↔ 候选器的递归。
没有显式 `messages` 时，用 `rpc:topic.boundary.detect` 取到的那一段做候选（`source=boundary`）。

### 4.3 话题排序与归一（`ranker`）

`score = 0.45·cohesion + 0.30·support + 0.25·recency`：
cohesion 是候选关键词与消息窗关键词的 Jaccard，support 是 `min(1, message_count/12)` 饱和函数，
recency 以 `HALF_LIFE = 1800s` 衰减。`DEFAULT_THRESHOLD = 0.45` 以上才算「识别出话题」。
识别出新话题且与会话当前话题不同 → 发布 `kafka:grouppig.topic.changed` 并开/续会话。
`topic.resolve` 走相似度（`TopicEmbedder.best_match`）把口语说法并到已有话题；相似度不够则 `method=new` 新建
（`topic_id = topic-<group_id>-<sha1(phrase)[:12]>`，确定性可复现）。

#### 4.3.1 会话连续性（t17 修复：一条连贯群聊不再碎成多个会话）

**原缺陷**：`SessionLayer.on_message` 把**单条消息**交给 `ranker.detect`。候选短语只能从这一条里切词，
措辞一变（「周末一起去爬山吧」→「爬山好啊我也想去爬山」→「那就周六早上八点集合去爬山」）就是另一个短语、
另一个 `topic_id`，于是**每条消息都判 `changed=True` 并开新会话**：4 条消息 → 4 个会话 / 4 个话题
（`sessions_per_message = 1.0`），会话平均 1~2 条消息；会话结束、归档、反思、聊天线全部作用在碎片上。
旧调用方式的可复现证据留在 `test_single_message_detect_reproduces_the_original_fragmentation`。

修复分两处，**判据不是「永远复用上一个会话」**，而是「这条消息还属不属于当前话题」：

1. **窗口**（`session/runtime/di.py`）：`SessionLayer.window_of(group_id, message)` 维护每群的滚动窗口，
   `on_message` 把「本条消息 + 最近窗口」一起交给 `ranker.detect`。窗口两个边界：
   `window_messages = 12` 条封顶（0 = 关掉窗口、退回只看本条），相邻两条间隔 > `window_seconds = 300s`
   就重开（与 `boundary.DEFAULT_SILENCE_GAP` 一致 —— 静默这么久本来就该算新话题）。窗口按群隔离。
2. **归并**（`ranker.merge_with_current`）：`detect` 在判定 `changed` 之前，拿**新消息的关键词**与
   **当前会话话题的关键词**比共享实词，两个条件同时满足就沿用当前会话（`record.merged = True`、
   `topic_id` 不变、不发 `topic.changed`）：
   * 共享词数 ≥ `MERGE_MIN_SHARED = 1`；
   * 重叠系数 ≥ `MERGE_THRESHOLD = 0.1`，系数 = 共享词数 / 较短一侧的词数。

   用「系数」而不是 Jaccard，是因为两侧长度天然不对等：会话关键词会随对话累积到 10 个，
   单条群消息只有 4~8 个词，Jaccard 被长度差压得很低（实测同一话题只有 0.07~0.2），
   反而和「真的换话题」（0.0）分不开；只看「共享 ≥1 个词」又会被一个通用词（如「一起」）骗到。
   实测信号：爬山那段依次共享 2 / 2 / 1 个词（系数 0.4 / 0.2 / 0.125）→ 归并；
   换到打游戏时共享 0 个词（系数 0.0）→ 开新会话。

**换话题时的两个配套动作**（否则新话题照样会碎）：窗口里还压着上一话题的消息，候选短语与关键词都停在旧话题上
（实测换到打游戏后候选仍是「爬山、去爬山」），所以 `_open_session` 支持显式传入 `keywords` / `message_ids`：
由 `detect` 用**新话题的第一条消息**铺底，新会话不继承旧话题的词与消息。

**返回体新增字段**（`rpc:topic.detect`）：`merged: bool`、`merge_score: float`；
`SessionLayer.ingested` 的每条记录也带上 `merged` 与 `window`（本条消息看到的窗口长度）。
`SessionLayer.health()["window"]` 给出 `{messages, seconds, groups}`。

**观测**：`tools/smoke.py` 场景 1 的行内摘要直接打印
`sessions/messages=1/4 sessions_per_message=0.25(<=2)`，`SESSION_LIMIT = 2` 是硬上限，
超过就重新回单 `F1-session-churn`；`tests/test_smoke_t11.py` 的「话题链路可见」用例同时断言该字段存在。

**既有语义（未改）**：`rpc:session.update` 的 `message_count += len(items)` 是累加语义，
开新会话的 `rpc:session.open` 用候选话题的 `message_ids` 铺底（含触发开会的那条），`on_message` 随后又
`update` 同一条消息 —— 所以会话的第一条消息会让 `message_count` 多算 1（4 条消息的会话显示 5）。
这是既有行为（`test_state_machine_open_update_current_archive_cycle` 明确断言「累加」），
t17 没有改它；新会话的 `message_ids` 因此只认新话题那一条（可据此精确断言归属）。

### 4.4 向量与相似度（`embedder`）

`EmbeddingCache`：LRU + TTL（`DEFAULT_TTL = 3600s`、`DEFAULT_CAPACITY = 512`），键是 `sha1(prefix + 文本)`，
读写时惰性清过期条目；`topic.embed` 逐条查缓存，未命中的**合并成一次模型调用**，回来再逐条回填。
`topic.similarity` 是 `0.7·cosine + 0.3·jaccard`（`COSINE_WEIGHT = 0.7`）；拿不到向量时退化为纯 Jaccard。

### 4.5 热度（`heat`）

`heat = 0.45·rate + 0.35·participants + 0.20·recency`，窗口 `300s`：
rate 饱和值 `12 条/分钟`，participants 饱和值 8 人，recency 以 `HALF_LIFE = 120s` 衰减。
`≥ 0.6` 判 `hot`、`< 0.3` 判 `cold`（`cold` 即 `cooling=True`，状态机据此转 `cooling`）。
没给 `messages` 时调 `rpc:chat.window` 取窗口（设计依赖）；没有 caller 时 `HeatManager.available` 为 `False`，
调用方（状态机 / 归档触发器）降级处理而不报错。

### 4.6 归档条件（`archive-trigger`）

```
archive = cooldown_ok and (heat_low or drifted)
  cooldown_ok = (now - 上次活动时间) >= DEFAULT_COOLDOWN(900s)
  heat_low    = heat < DEFAULT_HEAT_THRESHOLD(0.3)
  drifted     = drift_score(session, messages) >= DEFAULT_DRIFT_THRESHOLD(0.4)
```

`drift_score` = `1 - jaccard(会话关键词, 窗口关键词)`；会话还没攒下关键词时，
窗口有内容算「没漂移」、空窗口才算漂移。`message_count < DEFAULT_MIN_MESSAGES(3)` 时额外记 `few-messages`
（只是标记，仍允许归档——没什么可留的）。

**冷却是按「上次活动时间」算的**（本轮修复）：`session.updated_at` 只在会话真正并入消息/事件时推进，
归档检查读的也是这个时间，避免「本次 update 刚把 updated_at 刷成 now → idle 恒为 0 → 永远不够冷却」的死循环。
热度取不到时（无 caller / 无窗口）降级为 `heat=None, heat_source="degraded"`，并把「冷却够久」当作冷场信号，
保证离线单测与无消息服务场景仍能归档。

## 5. 会话状态机

```
opening ──► active ──► cooling ──► archived
   │           │          │            ▲
   └───────────┴──────────┴────────────┘   （任意 live 状态可直接 archived）
archived 为终态：TRANSITIONS[archived] == ()
```

* `rpc:session.open` 建会话；同群已有 live 会话且换了话题时，旧会话转 `cooling`（返回 `cooled[]`）；
* `rpc:session.update` 并入消息 → 重算热度 → 状态转移 → 归档检查（`auto_archive=True` 时自动归档）；
* `rpc:session.current` 给 `session_id` 按 id 查（查不到返回 `{session: None, found: False}`），
  否则取该群 `updated_at` 最新的 live 会话；
* `rpc:session.archive` 转 `archived` → 组装档案 `build_archive_payload` → `rpc:archive.save` → 发完成事件。

**归档后非 `force` 更新抛 `InvalidTransition`**（本轮修复）：已归档会话不允许悄悄复活——
并入新消息会直接 `InvalidTransition`，其它更新（改标题、加聊天线等）同样拒绝；
确有必要时用 `force=True` 显式放行，或 `rpc:session.wake` 暂时唤醒后再 `rpc:session.sleep` 归还。
未知 `session_id` 抛 `SessionNotFound`；非法状态转移抛 `InvalidTransition`。

会话档案列对齐 `mysql:session_archives`：`session_id / group_id / title / summary / keywords / 
participants / thread_ids / topic_ids / message_count / started_at / ended_at / duration / review`。
`MAX_MESSAGE_IDS = 200`（会话内保留的消息 id 上限，防止长会话爆内存），`DEFAULT_MAX_ACTIVE = 3`。

## 6. 聊天线编织（threads.weaver）

* `segmenter`：按静默 `DEFAULT_GAP_SECONDS = 180s` 或 `DEFAULT_MAX_SEGMENT = 20` 条切段；
  段内识别指代（`ANAPHORA_MARKERS`，如「这个/那个/上面说的」）与引用；
* `linker`：`thread_id = thread-<sha1(group_id|session_id|topic_id)[:12]>`，
  把段与段连成聊天线（边类型 `reply` / `reference`），落 `rpc:thread.save`，状态 `open`；
* `outliner`：把聊天线整理成「观点链（`MAX_CHAIN=30`）/ 争议（关键词重叠 `< DISPUTE_OVERLAP=0.15` 视为对立）/
  结论（默认 3 条）/ 阶段（phases）」；
* `threads.link` 额外调 `rpc:cross.detect` 找跨会话引用（`detect_cross=True` 时）。

## 7. 跨会话引用与唤醒

```
消息文本
  │ rpc:cross.detect   （强标记「记得之前说过」/ 弱标记「上次、之前」/ 引号片段）
  │   强 STRONG_CONFIDENCE=0.80、引号 QUOTE_CONFIDENCE=0.60、弱 WEAK_CONFIDENCE=0.50，
  │   命中标记 + MARKER_BONUS=0.15；低于 DEFAULT_MIN_CONFIDENCE=0.5 直接丢弃
  ▼
rpc:cross.parse → {query, keywords, lines}
  ▼
rpc:cross.match → rpc:thread.find-cross（memory，跨会话检索）
  │   DEFAULT_MIN_SCORE=0.15 以上才算命中，DEFAULT_LIMIT=3，QUERY_KEYWORDS=12
  ▼
rpc:session.wake（wake=True 时）
  ├─ rpc:archive.load        取归档档案
  ├─ rpc:chat.query          取最近几条消息做片段（snippets=5）
  └─ rpc:wake.buffer.push    写入唤醒上下文（TTL 300s、容量 16）
  ▼
生成层 rpc:wake.buffer.pop（peek=True 读、默认读完即删）
  ▼
rpc:session.sleep → rpc:wake.buffer.pop + rpc:archive.save（把唤醒期间的引用信息补回档案）
```

唤醒**不改状态机状态**（归档会话仍是 `archived`），上下文只活在缓冲里，用完即弃；
缓冲按会话 id 去重、LRU + TTL（`DEFAULT_TTL=300s`、`DEFAULT_CAPACITY=16`、`DEFAULT_SNIPPETS=5`）。

## 8. 2 个 `kafka:` 主题的载荷

`kafka:grouppig.topic.changed`（`ranker` 在话题变更时发布）：

```json
{"group_id":100200300,"topic_id":"topic-100200300-9f2c1a7b0d34","phrase":"烤肉、啤酒",
 "keywords":["烤肉","啤酒"],"score":0.71,"previous_topic_id":"","session_id":"session-100200300-1700000000-a1b2c3",
 "message_count":4,"source":"grouppig.session.topic.detector.ranker","ts":1700000000.0}
```

`kafka:grouppig.session.completed`（`event-emitter` 在归档后发布）：

```json
{"event":"session.completed","event_id":"evt-…","session_id":"session-100200300-1700000000-a1b2c3",
 "group_id":100200300,"title":"烤肉、啤酒","summary":"…","keywords":["烤肉"],
 "thread_ids":["thread-…"],"topic_ids":["topic-…"],"message_count":12,"heat":0.42,
 "started_at":1700000000.0,"ended_at":1700003600.0,"duration":3600.0,
 "source":"grouppig.session.lifecycle.event-emitter","ts":1700003600.0}
```

事件投递是「发布 → 总线 →（无订阅者时）直投 `rpc:review.on-session-completed`」的双保险：
单进程里反思层还没接线时，归档也不会丢掉反思触发。

两个主题都**不进注册表**（发布语义，与 gateway 的 `kafka:` 同例），`contract.is_known_name()` 认它们。

## 9. 配置与可调参数

本域不新增配置项，阈值常量都在叶子模块里（测试可直接引用）：

| 位置 | 常量 | 默认 |
| --- | --- | --- |
| `boundary` | `DEFAULT_SILENCE_GAP` / `DEFAULT_KEYWORD_JUMP` | `300.0` / `0.25` |
| `candidate` | `DEFAULT_TOP` / `DEFAULT_UNITS` / `DEFAULT_MIN_COUNT` | `5` / `3` / `2` |
| `ranker` | `DEFAULT_THRESHOLD` / `WEIGHT_*` / `HALF_LIFE` / `SUPPORT_SATURATION` | `0.45` / `0.45·0.30·0.25` / `1800` / `12` |
| `embedder.cache` | `DEFAULT_TTL` / `DEFAULT_CAPACITY` | `3600` / `512` |
| `embedder.similarity` | `COSINE_WEIGHT` | `0.7` |
| `heat` | `DEFAULT_WINDOW_SECONDS` / `HALF_LIFE` / `HOT_THRESHOLD` / `COLD_THRESHOLD` | `300` / `120` / `0.6` / `0.3` |
| `archive_trigger` | `DEFAULT_COOLDOWN` / `DEFAULT_HEAT_THRESHOLD` / `DEFAULT_DRIFT_THRESHOLD` / `DEFAULT_MIN_MESSAGES` | `900` / `0.3` / `0.4` / `3` |
| `state_machine` | `MAX_MESSAGE_IDS` / `DEFAULT_MAX_ACTIVE` | `200` / `3` |
| `segmenter` | `DEFAULT_GAP_SECONDS` / `DEFAULT_MAX_SEGMENT` | `180` / `20` |
| `outliner` | `MAX_CHAIN` / `DEFAULT_CONCLUSIONS` / `DISPUTE_OVERLAP` | `30` / `3` / `0.15` |
| `reference_parser` | `DEFAULT_MIN_CONFIDENCE` / 三档置信度 / `MARKER_BONUS` | `0.5` / `0.8·0.6·0.5` / `0.15` |
| `matcher` | `DEFAULT_LIMIT` / `DEFAULT_MIN_SCORE` / `QUERY_KEYWORDS` | `3` / `0.15` / `12` |
| `wake.buffer` | `DEFAULT_TTL` / `DEFAULT_CAPACITY` / `DEFAULT_SNIPPETS` | `300` / `16` / `5` |

装配期可注入的开关：`attach_session(container, subscribe=True, layer=None, **options)`；
`SessionLayer(clock=..., heat=..., archive_trigger=..., emitter=..., states=..., cache=..., embedder=...,
boundary=..., candidate=..., ranker=..., segmenter=..., outliner=..., linker=..., matcher=...,
reference_parser=..., buffer=..., restorer=...)` 允许单测替换任意叶子实例；
`register_session_handlers(target, layer)` 只注册不订阅（隔离注册表场景）。
`layer.health()` 返回 `{handlers, topics, contract, states, cache, buffer, wake, emitter, ingested, subscribed}`。

## 10. 已知缺口与变通

**`rpc:model.embed` 不返回向量本体**（infra 现状：只回 `embedding_dim`）：

* `TopicEmbedder` 的取数顺序是「cache → `rpc:model.embed` 的 `vectors`/`embedding` 字段 → 注入的 `router.embed()`」；
* 三段都拿不到就抛 `DependencyMissing`，错误信息直接点名「请修 infra 的 rpc:model.embed 返回体，或给 TopicEmbedder 注入 router 兜底」；
* 单测/离线用 `tests/session_helpers.py` 的 `EmbeddingClient`（确定性哈希袋向量，维度 12）或直接注入假 router；
* `normalize_vectors()` 兼容三种返回形态：嵌套列表（每文本一个向量）、单层列表套一个向量、平铺数字列表（单条向量）。

其它遗留：

| 遗留 | 位置 | 说明 |
| --- | --- | --- |
| `grouppig.session.runtime.*` | `session/runtime/{messages,errors,di}.py` | 运行时补充模块，设计树暂无（与 `grouppig.memory.runtime.*` 同例），待队长登记 |
| 归档冷却的「上次活动」 | `lifecycle/state_machine.py` + `lifecycle/archive_trigger.py` | 目前按会话的 `updated_at` 算；跨进程重启后需要 `rpc:archive.load` 回填才能延续 |
| `session_archives` / `slang_entries` | memory 域 | 两张补充表待设计树补登（队长处理） |
| `message_count` 首条重复计 | `lifecycle/state_machine.py` | 开新会话时 `rpc:session.open` 已用候选话题的 `message_ids` 铺底，`on_message` 随后又 `update` 同一条 → 第一条消息让 `message_count` 多算 1（4 条显示 5）。既有「累加」语义（`test_state_machine_open_update_current_archive_cycle` 明确断言），t17 未改；需要精确条数时用 `message_ids` |
| 归并判据是词面 | `topic/detector/ranker.py` | `merge_with_current` 用关键词重叠系数（会话关键词累积到 10 个、单条 4~8 个词，Jaccard 分不开）；`rpc:model.embed` 若真返回向量，可升级为余弦相似度判据 |

## 11. 测试

```bash
# 本域两组测试：契约对齐（25 rpc + 2 kafka + 模块路径镜像）与话题/会话生命周期
uv run python -m pytest tests/test_session_contract.py tests/test_session_lifecycle.py -o addopts="" -q

# 全量验收（pyproject 的 addopts=-q 会与命令行 -q 叠加成 -qq 吞掉汇总行，务必清空 addopts）
uv run python -m pytest -o addopts="" -q

uv run ruff check src tests tools
uv run ruff format --check
uv run python tools/gen_skeleton.py --check

# 端到端冒烟（模拟 OneBot 服务端回放；场景 1 的行内摘要直接给出 sessions_per_message）
uv run python tools/smoke.py --scenario closed-loop
uv run python tools/smoke.py --summary-only
```

`tests/test_session_contract.py` 断言的是「契约名字 ↔ 注册表归属」：25 个 `rpc:` 全部注册、
每个处理器的 `module` 等于 `contract.owner(name)`、16 个设计叶子的源码路径存在、隔离注册表不污染 `default_registry`。
`tests/test_session_lifecycle.py` 覆盖边界三信号、候选短语确定性、排序打分、相似度与缓存命中、
热度冷热判定、归档触发三条件、状态机全生命周期（开/更新/冷却/归档/非法转移/force）、档案载荷与完成事件。

末尾 4 个用例是 t17 的会话连续性回归（全部不依赖墙钟，时间戳显式给出）：
test_on_message_keeps_one_topic_in_one_session（4 条同话题 → 1 个会话，sessions_per_message = 0.25 < 1）、
test_on_message_opens_new_session_when_topic_really_switches（爬山 → 打游戏恰好 2 个会话，且新会话不继承旧话题的消息）、
test_single_message_detect_reproduces_the_original_fragmentation（老调用方式仍能复现 4 个会话，说明缺陷判据没被绕过）、
test_window_restarts_after_silence_and_can_be_disabled（窗口静默重开 / 按群隔离 / window_messages=0 关闭）。
`tests/session_helpers.py` 提供 `MessageBuilder` / `make_messages` / `CallRecorder`（记录跨域 `rpc:` 调用，用来断言设计依赖确实走通）
与 `EmbeddingClient`；`tests/conftest.py` 暴露 `session_container`（内存库 + memory + session 全挂）与 `session_layer` 两个夹具。

## 12. 与其他域的接缝

| 方向 | 契约名字 | 说明 |
| --- | --- | --- |
| 入 | `kafka:grouppig.qq.message.received` | gateway 发消息即触发 `SessionLayer.on_message`（需 `attach_session` 时 `subscribe=True`） |
| 入 | `rpc:chat.query` / `rpc:chat.window` | 补消息窗（boundary / heat / restorer / state-machine 补料） |
| 入 | `rpc:archive.load` / `rpc:archive.save` | 唤醒读档、归档写档、休眠回写 |
| 入 | `rpc:thread.load` / `rpc:thread.save` / `rpc:thread.find-cross` | 聊天线存取与跨会话检索 |
| 入 | `rpc:model.classify`（可选） | 排序器的语义辅助；缺失时纯规则排序 |
| 出 | `kafka:grouppig.topic.changed` | 话题变更（感知/表达层可订阅） |
| 出 | `kafka:grouppig.session.completed` | 会话结束（反思层 t7 的主入口） |
| 出 | `rpc:review.on-session-completed` | 事件总线无订阅者时的直投兜底 |

所有跨域调用都通过 `container.call` 走注册表，叶子之间不互相 import；
本域 25 个名字全部注册后，`contract.check_registry(registry.names(), scope="grouppig.session")` 的 `missing` / `unknown` 均为空。

