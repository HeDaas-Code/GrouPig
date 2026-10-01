# social 社交层（t6 交付说明）

> 面向下游：reflection（t7）、expression（t8/t9）、integration（t10）、verify（t11）的对接说明。
> 契约以 `normify-grouppig/api-index.json` 为准，17 个 `rpc:` 名字、2 个 `kafka:` 主题逐字对齐设计。

## 1. 交付内容

| 设计叶子（normify id） | 源码 | 契约 API |
| --- | --- | --- |
| `grouppig.social.profile.extractor.fact-extractor` | `src/grouppig/social/profile/extractor/fact_extractor.py` | `rpc:profile.fact.extract` |
| `grouppig.social.profile.extractor.stance-extractor` | `src/grouppig/social/profile/extractor/stance_extractor.py` | `rpc:profile.stance.extract` |
| `grouppig.social.profile.extractor.conflict-resolver` | `src/grouppig/social/profile/extractor/conflict_resolver.py` | `rpc:profile.conflict` |
| `grouppig.social.profile.manager.lookup` | `src/grouppig/social/profile/manager/lookup.py` | `rpc:profile.get` |
| `grouppig.social.profile.manager.versioning` | `src/grouppig/social/profile/manager/versioning.py` | `rpc:profile.update` |
| `grouppig.social.profile.manager.events` | `src/grouppig/social/profile/manager/events.py` | 发布 `kafka:grouppig.profile.updated` |
| `grouppig.social.speech.profiler.lexicon` | `src/grouppig/social/speech/profiler/lexicon.py` | `rpc:speech.lexicon` |
| `grouppig.social.speech.profiler.temper` | `src/grouppig/social/speech/profiler/temper.py` | `rpc:speech.temper` |
| `grouppig.social.speech.profiler.style-metrics` | `src/grouppig/social/speech/profiler/style_metrics.py` | `rpc:speech.profile`、`rpc:speech.style` |
| `grouppig.social.speech.responder.validator` | `src/grouppig/social/speech/responder/validator.py` | `rpc:speech.validate` |
| `grouppig.social.speech.responder.adapter` | `src/grouppig/social/speech/responder/adapter.py` | `rpc:speech.advise`、`rpc:speech.tailor` |
| `grouppig.social.graph.manager.egonet` | `src/grouppig/social/graph/manager/egonet.py` | `rpc:graph.get-egonet` |
| `grouppig.social.graph.manager.tiering` | `src/grouppig/social/graph/manager/tiering.py` | `rpc:graph.tiering` |
| `grouppig.social.graph.manager.events` | `src/grouppig/social/graph/manager/events.py` | 发布 `kafka:grouppig.social.changed` |
| `grouppig.social.graph.relationship.rules` | `src/grouppig/social/graph/relationship/rules.py` | `rpc:relationship.get`、`rpc:relationship.adjust` |
| `grouppig.social.graph.relationship.decay` | `src/grouppig/social/graph/relationship/decay.py` | `rpc:relationship.decay` |

> 注意：设计里的连字符叶子在 Python 侧必须是**下划线文件名**（`fact-extractor` → `fact_extractor.py`），
> 但 `rpc:` 名字与 `contract.owner()` 仍按设计逐字对齐（测试 `test_social_contract.py` 断言）。

装配入口（集成层一行接入）：

```python
from grouppig.memory.runtime.di import attach_memory
from grouppig.social import install_social

await attach_memory(container)              # social 只通过 rpc: 名字读库，必须先挂 memory
layer = await install_social(container)     # 注册 17 个 rpc: + 订阅 2 个 kafka: 主题
await layer.health()                        # 契约缺口 / 各叶子计数
await layer.close()                         # 退订（总线由容器统一关闭）
```

`install_social(container, subscribe=False)` 只注册不订阅（单测用）；`await layer.start()` 显式启动。

## 2. 17 个 `rpc:` 的入参 / 返回

| rpc | 入参 | 返回 |
| --- | --- | --- |
| `profile.get` | `user_id`，或特征 `nickname`/`alias`/`tag`；可选 `with_facts`/`status`/`limit_facts`/`group_id`/`limit` | `{user_id, profile, profiles, facts, found, source, indexed}` |
| `profile.update` | `user_id`；可选 `patch`/`facts`/`expected_version`/`source`/`actor`/`reason`/`group_id`/`merge`/`force` + 任意档案字段（`nickname=` 等） | `{applied, conflict, profile, version, previous_version, changed, facts, superseded, event}` |
| `profile.fact.extract` | `user_id`；可选 `group_id`/`messages`/`text`/`since`/`limit`/`apply`/`min_confidence`/`use_llm` | `{user_id, group_id, facts[], count, message_count, applied, update}` |
| `profile.stance.extract` | `user_id`；可选 `group_id`/`thread_id`/`session_id`/`threads`/`messages`/`apply`/`min_evidence` | `{stances{topic:…}, changes[], count, thread_count, applied, update}` |
| `profile.conflict` | `user_id`；可选 `new_facts`/`existing`/`apply`/`now`/`margin`/`reason` | `{resolved[], kept, dropped, conflicts, updated, applied, update}` |
| `speech.lexicon` | `user_id`；可选 `group_id`/`messages`/`since`/`until`/`limit`/`min_count`/`top` | `{top_words, catchphrases, emoji, faces, fillers, lexicon, message_count}` |
| `speech.temper` | `user_id`；可选 `group_id`/`messages`/`since`/`limit` | `{warmth, aggressiveness, intimacy, politeness, label, counts, evidence, …}` |
| `speech.profile` | `user_id`；可选 `group_id`/`messages`/`since`/`limit`/`use_lexicon`/`use_temper` | `{portrait, metrics, tags, summary, sample_size, stored_style}` |
| `speech.style` | `user_id`；可选 `group_id`/`build`/`refresh`/`max_age`/`limit` | `{found, source, portrait, metrics, tags, summary}` |
| `speech.advise` | `user_id`；可选 `group_id`/`draft`/`portrait`/`refresh` | `{target, do[], dont[], suggested_openers, suggested_emoji, suggested_filler, summary, draft_hint}` |
| `speech.tailor` | `text`（或 `draft`）；可选 `user_id`/`group_id`/`portrait`/`max_rounds`/`strict` | `{text, original, changed, applied[], rounds, validation, ok, portrait_found}` |
| `speech.validate` | `text`（或 `draft`）；可选 `user_id`/`group_id`/`portrait`/`strict` | `{ok, score, issues[{code,severity,detail,suggestion}], metrics, target, portrait_used}` |
| `graph.get-egonet` | `group_id`；可选 `user_id`/`include_profiles`/`include_others`/`min_weight`/`limit` | `{self_id, nodes[], edges[], others[], clusters[][], counts}` |
| `graph.tiering` | 可选 `user_id`/`group_id`/`score`/`factors`/`event`/`reason`/`previous_tier`/`interaction`/`members`/`force`/`apply` | `{tiers{}, labels{}, changed[], events[], written, applied}` |
| `relationship.get` | 可选 `user_id`/`group_id`/`edges`/`limit` | 带 `user_id` → `{found, score, tier, tier_label, interactions, last_ts}`；否则整网统计 `{count, average, max, min}` |
| `relationship.adjust` | `user_id`；可选 `event`/`delta`/`group_id`/`factors`/`interaction`/`decay`/`decay_kind`/`apply`/`reason` | `{score_before, score, score_delta, tier, delta, decay, tiering, applied}` |
| `relationship.decay` | 可选 `user_id`/`group_id`/`now`/`kind`/`half_life_days`/`floor`/`edges`/`min_score`/`limit` | `{decays{user_id:…}, updates[], count, decayed, half_life_days, floor}` |

语义要点（下游对接注意）：

* **关系分的唯一真相来源是 affinity 边**：`src_id=0`（自己）、`dst_id=群友`、`edge_type="affinity"`、
  `attrs["score"]` = 绝对关系分（0-99），`weight` = 历次增量之和。设计只给了 `social-store.get-edges`/`put-edge`，
  没有「读关系分」的名字，因此 `relationship.get` / `graph.get-egonet` / `decay` 都从 affinity 边逆推
  （`relationship_scores` 表由 `put-edge` 的 `score_delta` 同步累计，供 memory 侧排行/统计）。
* **写社交边的唯一出口是 `graph.tiering`**（设计里只有它依赖 `put-edge`）；`relationship.adjust` 与
  `relationship.decay` 都**只算不写**，把结果交给分层器落盘，因此不会出现两处口径打架。
* `relationship.adjust` 的顺序固定为「先时间衰减 → 再叠加事件增量 → 再软上限收敛到 0-99 → 再分层写回」；
  `EVENT_DELTAS` 里 `being_replied/mentioned/replied/agreed` 为正、`conflict/insulted/spam/ignored` 为负，
  单次调整绝对值上限 `MAX_SINGLE_DELTA=15`。软上限 `f(x)=99·(x/100)/(1+x/100)` 保证连续加分不爆表。
* `graph.tiering` 的分层阈值 `close≥75 / friend≥45 / acquaintance≥20 / else stranger`，中文标签
  「核心/熟识/普通/陌生」，与 memory 的 `default_tier` 口径一致；`kafka:grouppig.social.changed`
  在**分层变化或本次带来增量**时发布（批量入参没有「旧分数」，用 delta 判断）。
* `profile.update` 是**乐观并发**：`expected_version` 不匹配时返回 `applied=False, conflict=True`
  而不写坏档案；`force=True` 可强写。版本历史保留在进程内（`ProfileVersioning.history/rollback`），
  `rollback(user_id, version)` 把旧快照作为**新版本**再写一次（不删历史，可审计）。
* `profile.get` 的**特征检索**（按昵称/别名/标签）只能覆盖进程内索引里见过的群友：memory 没有
  「按昵称列档案」的契约名，索引由 `profile.update`、`profile.fact.extract` 写入时喂入，
  集成层可用 `layer.profile_lookup.warm(stores.profiles.list_profiles())` 预热。未命中返回 `found=False`。
* `speech.profile` 结果只落**进程内缓存**（TTL 600s）：设计给 `style-metrics` 的依赖里没有
  `rpc:profile.update`，所以本域不自动写档案；需要持久化时把 `portrait["metrics"/"lexicon"/"temper"]`
  经 `rpc:profile.update` 塞进 `member_profiles.speaking_style`（`speech.style` 已能读回该字段）。
* `speech.tailor` 的改写动作是确定性的：`strip_boundary`（只删命中的那一小段，反复重试直到不再命中）
  → `soften` → `compact_repeats` → `trim` → `pad` → `add_emoji`/`add_filler`，每轮都过
  `rpc:speech.validate`；仍不通过时最多 `max_rounds` 轮。
* `speech.validate` 的**越界**判定（AI 自曝 / 设定泄露 / 敏感词 / @全体 / 空文本）会直接判 `ok=False`；
  **贴合画像**（长度、表情密度、语气词密度）只给 `low` 提示与扣分。项目铁律「始终否认是 AI」由
  `AI_DISCLOSURE_*` 模式保证。

## 3. 2 个 `kafka:` 主题的载荷

`kafka:grouppig.profile.updated`（`profile.update` 发布）：

```json
{"user_id":1001,"group_id":100200300,"version":3,"previous_version":2,
 "changed":["interests","nickname"],"nickname":"阿猪","facts":5,"superseded":1,
 "reason":"fact-extract","source":"grouppig.social.profile.manager.versioning","ts":1700000000.0}
```

`kafka:grouppig.social.changed`（`graph.tiering` 发布）：

```json
{"user_id":1001,"group_id":100200300,"tier":"friend","previous_tier":"acquaintance",
 "tier_label":"熟识","score":52.0,"delta":4.0,
 "reason":"being_replied","source":"grouppig.social.graph.manager.tiering","ts":1700000000.0}
```

订阅（反思/表达层）：

```python
from grouppig.social.profile.manager.events import TOPIC_PROFILE_UPDATED, subscribe
subscribe(container.bus, my_handler)     # 或 container.bus.subscribe(TOPIC_PROFILE_UPDATED, handler)
```

## 4. 配置

本域不新增配置项，全部走 build 期参数（`install_social(..., **options)` → `SocialLayer.extra`）：

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `extra["llm"]` | `None` | `rpc:profile.fact.extract` 的模型钩子（`async (prompt) -> str`，返回 JSON 数组）；不传则纯规则抽取（离线可测） |
| `now` | `time.time` | 时钟注入（单测用假时钟验证衰减/新鲜度） |
| `logger` | 容器 logger | 若容器没有 logger，`ctx.log` 为 no-op |

阈值常量都放在叶子模块里（可被测试直接引用）：`relation` 的 `EVENT_DELTAS`、`tiering` 的
`TIER_THRESHOLDS`/`TIER_LABELS`、`decay` 的 `DEFAULT_HALF_LIFE_DAYS=30`/`DEFAULT_FLOOR=5`/`GRACE_DAYS=2`、
`style-metrics` 的 `DEFAULT_TTL=600`、`validator` 的 `DEFAULT_MAX_LENGTH=220`/`SEVERITY_WEIGHT`。

## 5. 测试

```bash
# 本域五组测试（契约 / 档案 / 说话画像 / 社交网 / 验收）
uv run python -m pytest tests/test_social_contract.py tests/test_social_profile.py \
    tests/test_social_speech.py tests/test_social_graph.py tests/test_social_acceptance.py -q

uv run ruff check src/grouppig/social tests/test_social_*.py tests/social_helpers.py
uv run python tools/gen_skeleton.py --check
```

测试夹具在 `tests/social_helpers.py`：`social_env(config)` 一次搭好「内存库（memory 全表）+ 独立注册表 +
严格主题总线 + 假时钟 + 事件收集器」，`Clock` 可拨动（衰减断言可复现），`EventCollector` 收集 `kafka:` 主题。

## 6. 设计之外的补充（待队长登记进 normify 树）

| 补充点 | 位置 | 理由 |
| --- | --- | --- |
| 读档案旧值用 `rpc:profile.get` | `versioning`、`stance-extractor` | 设计里没有「版本控制器读当前值」「立场抽取读旧立场」的名字 |
| 读社交边用 `rpc:social-store.get-edges` | `rules`、`decay` | `rpc:relationship.get` / `relationship.decay` 要拿现有分数；设计里 `rules` 只有 `decay`/`tiering` 两个依赖 |
| 读消息正文用 `rpc:chat.query` | `stance-extractor` | 设计只给了 `rpc:thread.load`，而 `chat_threads` 行里没有消息正文；也支持调用方直接传 `messages=` 绕开 |
| `SCORE_MIN`/`SCORE_MAX`/`AFFINITY_EDGE`/`SELF_NODE_ID` 常量 | `egonet` | 关系分的存储约定（设计未规定 absolute score 落在哪个字段） |
| 特征索引（进程内） | `lookup` | 设计描述了「按 ID 与特征快速检索」，但没有对应的存储契约名 |
