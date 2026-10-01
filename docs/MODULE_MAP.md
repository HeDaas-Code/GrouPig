# GrouPig 模块地图（由 `tools/gen_skeleton.py` 生成，请勿手改）

契约来源：`normify-grouppig/api-index.json`（167 个名字：148 rpc / 9 kafka / 10 mysql）、`normify-grouppig/modules/**/*.md`（202 个模块：62 容器 / 140 叶子）。

路径映射规则：`grouppig.<domain>.<area>.<leaf>` → `src/grouppig/<domain>/<area>/<leaf>.py`，段名中的 `-` 在 Python 侧写作 `_`（`grouppig.infra.model-gateway.router` → `src/grouppig/infra/model_gateway/router.py`）；容器模块对应同名包的 `__init__.py`。

| 模块 id | 类型 | 状态 | 源码路径 | 契约 API | 负责人 |
| --- | --- | --- | --- | --- | --- |
| `grouppig` | 容器 | active | `src/grouppig/__init__.py` | - | unassigned |
| `grouppig.expression` | 容器 | active | `src/grouppig/expression/__init__.py` | - | unassigned |
| `grouppig.expression.generator` | 容器 | active | `src/grouppig/expression/generator/__init__.py` | - | expression-core-engineer |
| `grouppig.expression.generator.compressor` | 叶子 | active | `src/grouppig/expression/generator/compressor.py` | rpc:generator.compress | expression-core-engineer |
| `grouppig.expression.generator.context` | 叶子 | active | `src/grouppig/expression/generator/context.py` | rpc:generator.compose | expression-core-engineer |
| `grouppig.expression.generator.polisher` | 叶子 | active | `src/grouppig/expression/generator/polisher.py` | rpc:generator.humanize | expression-core-engineer |
| `grouppig.expression.generator.writer` | 叶子 | active | `src/grouppig/expression/generator/writer.py` | rpc:generator.write | expression-core-engineer |
| `grouppig.expression.identity` | 容器 | planned | `src/grouppig/expression/identity/__init__.py` | - | expression-flow-engineer |
| `grouppig.expression.identity.deflector` | 叶子 | active | `src/grouppig/expression/identity/deflector.py` | rpc:identity.deflect | expression-flow-engineer |
| `grouppig.expression.identity.denial` | 叶子 | active | `src/grouppig/expression/identity/denial.py` | rpc:identity.deny-ai | expression-flow-engineer |
| `grouppig.expression.orchestrator` | 容器 | planned | `src/grouppig/expression/orchestrator/__init__.py` | - | expression-flow-engineer |
| `grouppig.expression.orchestrator.flow` | 容器 | planned | `src/grouppig/expression/orchestrator/flow/__init__.py` | - | expression-flow-engineer |
| `grouppig.expression.orchestrator.flow.emitter` | 叶子 | active | `src/grouppig/expression/orchestrator/flow/emitter.py` | kafka:grouppig.reply.composed | expression-flow-engineer |
| `grouppig.expression.orchestrator.flow.state` | 叶子 | active | `src/grouppig/expression/orchestrator/flow/state.py` | rpc:flow.start, rpc:flow.next, rpc:flow.end | expression-flow-engineer |
| `grouppig.expression.orchestrator.flow.transition` | 叶子 | active | `src/grouppig/expression/orchestrator/flow/transition.py` | rpc:flow.transition | expression-flow-engineer |
| `grouppig.expression.orchestrator.planner` | 容器 | planned | `src/grouppig/expression/orchestrator/planner/__init__.py` | - | expression-flow-engineer |
| `grouppig.expression.orchestrator.planner.reviser` | 叶子 | active | `src/grouppig/expression/orchestrator/planner/reviser.py` | rpc:planner.revise | expression-flow-engineer |
| `grouppig.expression.orchestrator.planner.structure` | 叶子 | active | `src/grouppig/expression/orchestrator/planner/structure.py` | rpc:planner.plan | expression-flow-engineer |
| `grouppig.expression.orchestrator.selector` | 容器 | planned | `src/grouppig/expression/orchestrator/selector/__init__.py` | - | expression-flow-engineer |
| `grouppig.expression.orchestrator.selector.cost` | 叶子 | active | `src/grouppig/expression/orchestrator/selector/cost.py` | rpc:selector.estimate, rpc:selector.pick-preset | expression-flow-engineer |
| `grouppig.expression.orchestrator.selector.templates` | 叶子 | active | `src/grouppig/expression/orchestrator/selector/templates.py` | rpc:selector.pick-template | expression-flow-engineer |
| `grouppig.expression.persona` | 容器 | active | `src/grouppig/expression/persona/__init__.py` | - | expression-core-engineer |
| `grouppig.expression.persona.profile` | 叶子 | active | `src/grouppig/expression/persona/profile.py` | rpc:persona.get | expression-core-engineer |
| `grouppig.expression.persona.prompt-builder` | 叶子 | active | `src/grouppig/expression/persona/prompt_builder.py` | rpc:persona.style | expression-core-engineer |
| `grouppig.expression.runtime` | 叶子 | active | `src/grouppig/expression/runtime.py` | - | unassigned |
| `grouppig.expression.slang` | 容器 | planned | `src/grouppig/expression/slang/__init__.py` | - | expression-flow-engineer |
| `grouppig.expression.slang.injector` | 叶子 | active | `src/grouppig/expression/slang/injector.py` | rpc:slang.inject | expression-flow-engineer |
| `grouppig.expression.slang.learner` | 叶子 | active | `src/grouppig/expression/slang/learner.py` | rpc:slang.learn | expression-flow-engineer |
| `grouppig.expression.slang.recognizer` | 叶子 | active | `src/grouppig/expression/slang/recognizer.py` | rpc:slang.recognize | expression-flow-engineer |
| `grouppig.gateway` | 容器 | active | `src/grouppig/gateway/__init__.py` | - | gateway-engineer |
| `grouppig.gateway.adapter` | 容器 | active | `src/grouppig/gateway/adapter/__init__.py` | - | gateway-engineer |
| `grouppig.gateway.adapter.connector` | 叶子 | active | `src/grouppig/gateway/adapter/connector.py` | rpc:connector.connect, rpc:connector.heartbeat | gateway-engineer |
| `grouppig.gateway.adapter.event-codec` | 叶子 | active | `src/grouppig/gateway/adapter/event_codec.py` | rpc:codec.decode, rpc:codec.encode | gateway-engineer |
| `grouppig.gateway.adapter.onebot` | 叶子 | active | `src/grouppig/gateway/adapter/onebot.py` | rpc:onebot.start, rpc:onebot.send, kafka:grouppig.qq.message.received | gateway-engineer |
| `grouppig.gateway.router` | 容器 | active | `src/grouppig/gateway/router/__init__.py` | - | gateway-engineer |
| `grouppig.gateway.router.command` | 叶子 | active | `src/grouppig/gateway/router/command.py` | rpc:command.recognize, rpc:command.execute | gateway-engineer |
| `grouppig.gateway.router.demux` | 叶子 | active | `src/grouppig/gateway/router/demux.py` | rpc:demux.dispatch, kafka:grouppig.event.routed | gateway-engineer |
| `grouppig.gateway.router.priority` | 叶子 | active | `src/grouppig/gateway/router/priority.py` | rpc:priority.enqueue, rpc:priority.next | gateway-engineer |
| `grouppig.gateway.sender` | 容器 | active | `src/grouppig/gateway/sender/__init__.py` | - | gateway-engineer |
| `grouppig.gateway.sender.composer` | 叶子 | active | `src/grouppig/gateway/sender/composer.py` | rpc:sender.send_reply, rpc:composer.wrap | gateway-engineer |
| `grouppig.gateway.sender.rate-limiter` | 叶子 | active | `src/grouppig/gateway/sender/rate_limiter.py` | rpc:rate.check, rpc:rate.wait | gateway-engineer |
| `grouppig.gateway.sender.retract` | 叶子 | active | `src/grouppig/gateway/sender/retract.py` | rpc:retract.recall, rpc:retract.notify | gateway-engineer |
| `grouppig.infra` | 容器 | active | `src/grouppig/infra/__init__.py` | - | infra-engineer |
| `grouppig.infra.config` | 容器 | active | `src/grouppig/infra/config/__init__.py` | - | infra-engineer |
| `grouppig.infra.config.loader` | 叶子 | active | `src/grouppig/infra/config/loader.py` | rpc:config.get | infra-engineer |
| `grouppig.infra.config.reloader` | 叶子 | active | `src/grouppig/infra/config/reloader.py` | rpc:config.reload | infra-engineer |
| `grouppig.infra.config.validator` | 叶子 | active | `src/grouppig/infra/config/validator.py` | rpc:config.validate | infra-engineer |
| `grouppig.infra.logger` | 叶子 | active | `src/grouppig/infra/logger.py` | rpc:logger.log, rpc:logger.trace | infra-engineer |
| `grouppig.infra.model-gateway` | 容器 | active | `src/grouppig/infra/model_gateway/__init__.py` | - | infra-engineer |
| `grouppig.infra.model-gateway.codec` | 叶子 | active | `src/grouppig/infra/model_gateway/codec.py` | rpc:model.encode, rpc:model.decode | infra-engineer |
| `grouppig.infra.model-gateway.retry` | 叶子 | active | `src/grouppig/infra/model_gateway/retry.py` | rpc:model.retry | infra-engineer |
| `grouppig.infra.model-gateway.router` | 叶子 | active | `src/grouppig/infra/model_gateway/router.py` | rpc:model.chat, rpc:model.embed, rpc:model.classify, rpc:model.system1 | infra-engineer |
| `grouppig.infra.runtime` | 容器 | active | `src/grouppig/infra/runtime/__init__.py` | - | infra-engineer |
| `grouppig.infra.runtime.bus` | 叶子 | active | `src/grouppig/infra/runtime/bus.py` | - | infra-engineer |
| `grouppig.infra.runtime.contract` | 叶子 | active | `src/grouppig/infra/runtime/contract.py` | - | infra-engineer |
| `grouppig.infra.runtime.di` | 叶子 | active | `src/grouppig/infra/runtime/di.py` | - | infra-engineer |
| `grouppig.infra.runtime.errors` | 叶子 | active | `src/grouppig/infra/runtime/errors.py` | - | infra-engineer |
| `grouppig.infra.runtime.laya-system1` | 叶子 | active | `src/grouppig/infra/runtime/laya_system1.py` | - | infra-engineer |
| `grouppig.infra.runtime.local-embed` | 叶子 | active | `src/grouppig/infra/runtime/local_embed.py` | - | infra-engineer |
| `grouppig.infra.runtime.registry` | 叶子 | active | `src/grouppig/infra/runtime/registry.py` | - | infra-engineer |
| `grouppig.infra.runtime.transport` | 叶子 | active | `src/grouppig/infra/runtime/transport.py` | - | infra-engineer |
| `grouppig.infra.runtime.usage` | 叶子 | active | `src/grouppig/infra/runtime/usage.py` | - | infra-engineer |
| `grouppig.infra.token-budget` | 容器 | active | `src/grouppig/infra/token_budget/__init__.py` | - | infra-engineer |
| `grouppig.infra.token-budget.meter` | 叶子 | active | `src/grouppig/infra/token_budget/meter.py` | rpc:token.reserve, rpc:token.consume | infra-engineer |
| `grouppig.infra.token-budget.policy` | 叶子 | active | `src/grouppig/infra/token_budget/policy.py` | rpc:token.policy | infra-engineer |
| `grouppig.infra.token-budget.reporter` | 叶子 | active | `src/grouppig/infra/token_budget/reporter.py` | rpc:token.report | infra-engineer |
| `grouppig.memory` | 容器 | active | `src/grouppig/memory/__init__.py` | - | memory-engineer |
| `grouppig.memory.chat-store` | 容器 | active | `src/grouppig/memory/chat_store/__init__.py` | - | memory-engineer |
| `grouppig.memory.chat-store.dao` | 叶子 | active | `src/grouppig/memory/chat_store/dao.py` | rpc:chat.append, rpc:chat.query, rpc:chat.window | memory-engineer |
| `grouppig.memory.chat-store.schema` | 叶子 | active | `src/grouppig/memory/chat_store/schema.py` | mysql:chat_messages, mysql:chat_window_index | memory-engineer |
| `grouppig.memory.chat-store.window-index` | 叶子 | active | `src/grouppig/memory/chat_store/window_index.py` | rpc:chat.window.advance, rpc:chat.window.prune | memory-engineer |
| `grouppig.memory.profile-store` | 容器 | active | `src/grouppig/memory/profile_store/__init__.py` | - | memory-engineer |
| `grouppig.memory.profile-store.dao` | 叶子 | active | `src/grouppig/memory/profile_store/dao.py` | rpc:profile-store.get, rpc:profile-store.put | memory-engineer |
| `grouppig.memory.profile-store.schema` | 叶子 | active | `src/grouppig/memory/profile_store/schema.py` | mysql:member_profiles, mysql:profile_facts | memory-engineer |
| `grouppig.memory.runtime` | 容器 | active | `src/grouppig/memory/runtime/__init__.py` | - | memory-engineer |
| `grouppig.memory.runtime.columns` | 叶子 | active | `src/grouppig/memory/runtime/columns.py` | - | memory-engineer |
| `grouppig.memory.runtime.db` | 叶子 | active | `src/grouppig/memory/runtime/db.py` | - | memory-engineer |
| `grouppig.memory.runtime.di` | 叶子 | active | `src/grouppig/memory/runtime/di.py` | - | memory-engineer |
| `grouppig.memory.runtime.errors` | 叶子 | active | `src/grouppig/memory/runtime/errors.py` | - | memory-engineer |
| `grouppig.memory.runtime.meta` | 叶子 | active | `src/grouppig/memory/runtime/meta.py` | - | memory-engineer |
| `grouppig.memory.runtime.migrate` | 叶子 | active | `src/grouppig/memory/runtime/migrate.py` | - | memory-engineer |
| `grouppig.memory.runtime.rows` | 叶子 | active | `src/grouppig/memory/runtime/rows.py` | - | memory-engineer |
| `grouppig.memory.runtime.schema` | 叶子 | active | `src/grouppig/memory/runtime/schema.py` | - | memory-engineer |
| `grouppig.memory.runtime.similarity` | 叶子 | active | `src/grouppig/memory/runtime/similarity.py` | - | memory-engineer |
| `grouppig.memory.runtime.stores` | 叶子 | active | `src/grouppig/memory/runtime/stores.py` | - | memory-engineer |
| `grouppig.memory.session-archive` | 容器 | active | `src/grouppig/memory/session_archive/__init__.py` | - | memory-engineer |
| `grouppig.memory.session-archive.dao` | 叶子 | active | `src/grouppig/memory/session_archive/dao.py` | rpc:archive.save, rpc:archive.load, mysql:session_archives | memory-engineer |
| `grouppig.memory.session-archive.summary-index` | 叶子 | active | `src/grouppig/memory/session_archive/summary_index.py` | rpc:archive.summarize, rpc:archive.find | memory-engineer |
| `grouppig.memory.slang-kb` | 容器 | active | `src/grouppig/memory/slang_kb/__init__.py` | - | memory-engineer |
| `grouppig.memory.slang-kb.dictionary` | 叶子 | active | `src/grouppig/memory/slang_kb/dictionary.py` | rpc:slang.lookup, rpc:slang.upsert, mysql:slang_entries | memory-engineer |
| `grouppig.memory.slang-kb.freshness` | 叶子 | active | `src/grouppig/memory/slang_kb/freshness.py` | rpc:slang.decay, rpc:slang.refresh | memory-engineer |
| `grouppig.memory.social-store` | 容器 | active | `src/grouppig/memory/social_store/__init__.py` | - | memory-engineer |
| `grouppig.memory.social-store.dao` | 叶子 | active | `src/grouppig/memory/social_store/dao.py` | rpc:social-store.get-edges, rpc:social-store.put-edge | memory-engineer |
| `grouppig.memory.social-store.schema` | 叶子 | active | `src/grouppig/memory/social_store/schema.py` | mysql:social_edges, mysql:relationship_scores | memory-engineer |
| `grouppig.memory.thread-store` | 容器 | active | `src/grouppig/memory/thread_store/__init__.py` | - | memory-engineer |
| `grouppig.memory.thread-store.dao` | 叶子 | active | `src/grouppig/memory/thread_store/dao.py` | rpc:thread.save, rpc:thread.load, rpc:thread.find-cross | memory-engineer |
| `grouppig.memory.thread-store.schema` | 叶子 | active | `src/grouppig/memory/thread_store/schema.py` | mysql:chat_threads, mysql:chat_thread_edges | memory-engineer |
| `grouppig.panel` | 容器 | active | `src/grouppig/panel/__init__.py` | - | unassigned |
| `grouppig.panel.snapshot` | 叶子 | active | `src/grouppig/panel/snapshot.py` | - | unassigned |
| `grouppig.panel.tui` | 叶子 | active | `src/grouppig/panel/tui.py` | - | unassigned |
| `grouppig.panel.web` | 叶子 | active | `src/grouppig/panel/web.py` | - | unassigned |
| `grouppig.perception` | 容器 | active | `src/grouppig/perception/__init__.py` | - | perception-engineer |
| `grouppig.perception.behavior` | 容器 | planned | `src/grouppig/perception/behavior/__init__.py` | - | perception-engineer |
| `grouppig.perception.behavior.classifier` | 容器 | planned | `src/grouppig/perception/behavior/classifier/__init__.py` | - | perception-engineer |
| `grouppig.perception.behavior.classifier.aggregator` | 叶子 | active | `src/grouppig/perception/behavior/classifier/aggregator.py` | rpc:behavior.classify, kafka:grouppig.behavior.changed | perception-engineer |
| `grouppig.perception.behavior.classifier.features` | 叶子 | active | `src/grouppig/perception/behavior/classifier/features.py` | rpc:behavior.features.encode | perception-engineer |
| `grouppig.perception.behavior.classifier.llm-judge` | 叶子 | active | `src/grouppig/perception/behavior/classifier/llm_judge.py` | rpc:behavior.llm.judge | perception-engineer |
| `grouppig.perception.behavior.classifier.rule-engine` | 叶子 | active | `src/grouppig/perception/behavior/classifier/rule_engine.py` | rpc:behavior.rules.evaluate | perception-engineer |
| `grouppig.perception.behavior.flood` | 容器 | planned | `src/grouppig/perception/behavior/flood/__init__.py` | - | perception-engineer |
| `grouppig.perception.behavior.flood.repetition` | 叶子 | active | `src/grouppig/perception/behavior/flood/repetition.py` | rpc:flood.repetition | perception-engineer |
| `grouppig.perception.behavior.flood.velocity` | 叶子 | active | `src/grouppig/perception/behavior/flood/velocity.py` | rpc:flood.velocity | perception-engineer |
| `grouppig.perception.behavior.flood.verdict` | 叶子 | active | `src/grouppig/perception/behavior/flood/verdict.py` | rpc:flood.detect | perception-engineer |
| `grouppig.perception.behavior.rhythm` | 容器 | planned | `src/grouppig/perception/behavior/rhythm/__init__.py` | - | perception-engineer |
| `grouppig.perception.behavior.rhythm.meter` | 叶子 | active | `src/grouppig/perception/behavior/rhythm/meter.py` | rpc:rhythm.measure | perception-engineer |
| `grouppig.perception.behavior.rhythm.trend` | 叶子 | active | `src/grouppig/perception/behavior/rhythm/trend.py` | rpc:rhythm.trend | perception-engineer |
| `grouppig.perception.interrupt` | 容器 | planned | `src/grouppig/perception/interrupt/__init__.py` | - | perception-engineer |
| `grouppig.perception.interrupt.cooldown` | 叶子 | active | `src/grouppig/perception/interrupt/cooldown.py` | rpc:interrupt.cooldown | perception-engineer |
| `grouppig.perception.interrupt.decision` | 叶子 | active | `src/grouppig/perception/interrupt/decision.py` | rpc:interrupt.decide, kafka:grouppig.interrupt.triggered | perception-engineer |
| `grouppig.perception.interrupt.scorer` | 叶子 | active | `src/grouppig/perception/interrupt/scorer.py` | rpc:interrupt.score | perception-engineer |
| `grouppig.perception.normalizer` | 容器 | planned | `src/grouppig/perception/normalizer/__init__.py` | - | perception-engineer |
| `grouppig.perception.normalizer.cleaner` | 叶子 | active | `src/grouppig/perception/normalizer/cleaner.py` | rpc:normalizer.clean, rpc:normalizer.strip | perception-engineer |
| `grouppig.perception.normalizer.dedup` | 叶子 | active | `src/grouppig/perception/normalizer/dedup.py` | rpc:normalizer.dedup | perception-engineer |
| `grouppig.perception.normalizer.featurizer` | 叶子 | active | `src/grouppig/perception/normalizer/featurizer.py` | rpc:normalizer.features, rpc:normalizer.batch | perception-engineer |
| `grouppig.perception.observer` | 容器 | planned | `src/grouppig/perception/observer/__init__.py` | - | perception-engineer |
| `grouppig.perception.observer.buffer` | 叶子 | active | `src/grouppig/perception/observer/buffer.py` | rpc:observer.ingest, rpc:observer.buffer.drain | perception-engineer |
| `grouppig.perception.observer.window` | 叶子 | active | `src/grouppig/perception/observer/window.py` | rpc:observer.window.slide, rpc:observer.window.slice | perception-engineer |
| `grouppig.perception.runtime` | 容器 | planned | `src/grouppig/perception/runtime/__init__.py` | - | perception-engineer |
| `grouppig.perception.runtime.decision-cache` | 叶子 | active | `src/grouppig/perception/runtime/decision_cache.py` | - | perception-engineer |
| `grouppig.perception.runtime.decision-packet` | 叶子 | active | `src/grouppig/perception/runtime/decision_packet.py` | - | perception-engineer |
| `grouppig.perception.runtime.persona` | 叶子 | active | `src/grouppig/perception/runtime/persona.py` | - | perception-engineer |
| `grouppig.reflection` | 容器 | active | `src/grouppig/reflection/__init__.py` | - | social-reflect-engineer |
| `grouppig.reflection.evaluator` | 容器 | active | `src/grouppig/reflection/evaluator/__init__.py` | - | social-reflect-engineer |
| `grouppig.reflection.evaluator.ab-test` | 叶子 | active | `src/grouppig/reflection/evaluator/ab_test.py` | rpc:strategy.evaluate | social-reflect-engineer |
| `grouppig.reflection.evaluator.scoring` | 叶子 | active | `src/grouppig/reflection/evaluator/scoring.py` | rpc:strategy.score | social-reflect-engineer |
| `grouppig.reflection.presets` | 容器 | active | `src/grouppig/reflection/presets/__init__.py` | - | social-reflect-engineer |
| `grouppig.reflection.presets.matcher` | 叶子 | active | `src/grouppig/reflection/presets/matcher.py` | rpc:presets.match | social-reflect-engineer |
| `grouppig.reflection.presets.registry` | 叶子 | active | `src/grouppig/reflection/presets/registry.py` | rpc:presets.load, rpc:presets.register | social-reflect-engineer |
| `grouppig.reflection.runtime` | 叶子 | active | `src/grouppig/reflection/runtime.py` | - | social-reflect-engineer |
| `grouppig.reflection.session-review` | 容器 | active | `src/grouppig/reflection/session_review/__init__.py` | - | social-reflect-engineer |
| `grouppig.reflection.session-review.insights` | 叶子 | active | `src/grouppig/reflection/session_review/insights.py` | rpc:review.analyze | social-reflect-engineer |
| `grouppig.reflection.session-review.metrics` | 叶子 | active | `src/grouppig/reflection/session_review/metrics.py` | rpc:review.metrics | social-reflect-engineer |
| `grouppig.reflection.session-review.timeline` | 叶子 | active | `src/grouppig/reflection/session_review/timeline.py` | rpc:review.on-session-completed, rpc:review.timeline | social-reflect-engineer |
| `grouppig.reflection.strategy` | 容器 | active | `src/grouppig/reflection/strategy/__init__.py` | - | social-reflect-engineer |
| `grouppig.reflection.strategy.rollback` | 叶子 | active | `src/grouppig/reflection/strategy/rollback.py` | rpc:strategy.rollback | social-reflect-engineer |
| `grouppig.reflection.strategy.synthesizer` | 叶子 | active | `src/grouppig/reflection/strategy/synthesizer.py` | rpc:strategy.generate | social-reflect-engineer |
| `grouppig.reflection.strategy.validator` | 叶子 | active | `src/grouppig/reflection/strategy/validator.py` | rpc:strategy.validate | social-reflect-engineer |
| `grouppig.session` | 容器 | active | `src/grouppig/session/__init__.py` | - | session-engineer |
| `grouppig.session.lifecycle` | 容器 | active | `src/grouppig/session/lifecycle/__init__.py` | - | session-engineer |
| `grouppig.session.lifecycle.archive-trigger` | 叶子 | active | `src/grouppig/session/lifecycle/archive_trigger.py` | rpc:session.archive.check | session-engineer |
| `grouppig.session.lifecycle.event-emitter` | 叶子 | active | `src/grouppig/session/lifecycle/event_emitter.py` | kafka:grouppig.session.completed | session-engineer |
| `grouppig.session.lifecycle.heat` | 叶子 | active | `src/grouppig/session/lifecycle/heat.py` | rpc:session.heat | session-engineer |
| `grouppig.session.lifecycle.state-machine` | 叶子 | active | `src/grouppig/session/lifecycle/state_machine.py` | rpc:session.open, rpc:session.update, rpc:session.current, rpc:session.archive | session-engineer |
| `grouppig.session.runtime` | 容器 | active | `src/grouppig/session/runtime/__init__.py` | - | session-engineer |
| `grouppig.session.runtime.di` | 叶子 | active | `src/grouppig/session/runtime/di.py` | - | session-engineer |
| `grouppig.session.runtime.errors` | 叶子 | active | `src/grouppig/session/runtime/errors.py` | - | session-engineer |
| `grouppig.session.runtime.messages` | 叶子 | active | `src/grouppig/session/runtime/messages.py` | - | session-engineer |
| `grouppig.session.threads` | 容器 | active | `src/grouppig/session/threads/__init__.py` | - | session-engineer |
| `grouppig.session.threads.cross` | 容器 | active | `src/grouppig/session/threads/cross/__init__.py` | - | session-engineer |
| `grouppig.session.threads.cross.matcher` | 叶子 | active | `src/grouppig/session/threads/cross/matcher.py` | rpc:cross.match | session-engineer |
| `grouppig.session.threads.cross.reference-parser` | 叶子 | active | `src/grouppig/session/threads/cross/reference_parser.py` | rpc:cross.detect, rpc:cross.parse | session-engineer |
| `grouppig.session.threads.weaver` | 容器 | active | `src/grouppig/session/threads/weaver/__init__.py` | - | session-engineer |
| `grouppig.session.threads.weaver.linker` | 叶子 | active | `src/grouppig/session/threads/weaver/linker.py` | rpc:threads.weave, rpc:threads.link | session-engineer |
| `grouppig.session.threads.weaver.outliner` | 叶子 | active | `src/grouppig/session/threads/weaver/outliner.py` | rpc:threads.outline | session-engineer |
| `grouppig.session.threads.weaver.segmenter` | 叶子 | active | `src/grouppig/session/threads/weaver/segmenter.py` | rpc:threads.segment | session-engineer |
| `grouppig.session.topic` | 容器 | active | `src/grouppig/session/topic/__init__.py` | - | session-engineer |
| `grouppig.session.topic.detector` | 容器 | active | `src/grouppig/session/topic/detector/__init__.py` | - | session-engineer |
| `grouppig.session.topic.detector.boundary` | 叶子 | active | `src/grouppig/session/topic/detector/boundary.py` | rpc:topic.boundary.detect | session-engineer |
| `grouppig.session.topic.detector.candidate` | 叶子 | active | `src/grouppig/session/topic/detector/candidate.py` | rpc:topic.candidate.generate | session-engineer |
| `grouppig.session.topic.detector.ranker` | 叶子 | active | `src/grouppig/session/topic/detector/ranker.py` | rpc:topic.detect, rpc:topic.resolve, kafka:grouppig.topic.changed | session-engineer |
| `grouppig.session.topic.embedder` | 容器 | active | `src/grouppig/session/topic/embedder/__init__.py` | - | session-engineer |
| `grouppig.session.topic.embedder.cache` | 叶子 | active | `src/grouppig/session/topic/embedder/cache.py` | rpc:topic.embed.cache.get, rpc:topic.embed.cache.set | session-engineer |
| `grouppig.session.topic.embedder.similarity` | 叶子 | active | `src/grouppig/session/topic/embedder/similarity.py` | rpc:topic.embed, rpc:topic.similarity | session-engineer |
| `grouppig.session.wake` | 容器 | active | `src/grouppig/session/wake/__init__.py` | - | session-engineer |
| `grouppig.session.wake.buffer` | 叶子 | active | `src/grouppig/session/wake/buffer.py` | rpc:wake.buffer.push, rpc:wake.buffer.pop | session-engineer |
| `grouppig.session.wake.restorer` | 叶子 | active | `src/grouppig/session/wake/restorer.py` | rpc:session.wake, rpc:session.sleep | session-engineer |
| `grouppig.social` | 容器 | active | `src/grouppig/social/__init__.py` | - | social-reflect-engineer |
| `grouppig.social.graph` | 容器 | active | `src/grouppig/social/graph/__init__.py` | - | social-reflect-engineer |
| `grouppig.social.graph.manager` | 容器 | active | `src/grouppig/social/graph/manager/__init__.py` | - | social-reflect-engineer |
| `grouppig.social.graph.manager.egonet` | 叶子 | active | `src/grouppig/social/graph/manager/egonet.py` | rpc:graph.get-egonet | social-reflect-engineer |
| `grouppig.social.graph.manager.events` | 叶子 | active | `src/grouppig/social/graph/manager/events.py` | kafka:grouppig.social.changed | social-reflect-engineer |
| `grouppig.social.graph.manager.tiering` | 叶子 | active | `src/grouppig/social/graph/manager/tiering.py` | rpc:graph.tiering | social-reflect-engineer |
| `grouppig.social.graph.relationship` | 容器 | active | `src/grouppig/social/graph/relationship/__init__.py` | - | social-reflect-engineer |
| `grouppig.social.graph.relationship.decay` | 叶子 | active | `src/grouppig/social/graph/relationship/decay.py` | rpc:relationship.decay | social-reflect-engineer |
| `grouppig.social.graph.relationship.rules` | 叶子 | active | `src/grouppig/social/graph/relationship/rules.py` | rpc:relationship.get, rpc:relationship.adjust | social-reflect-engineer |
| `grouppig.social.profile` | 容器 | active | `src/grouppig/social/profile/__init__.py` | - | social-reflect-engineer |
| `grouppig.social.profile.extractor` | 容器 | active | `src/grouppig/social/profile/extractor/__init__.py` | - | social-reflect-engineer |
| `grouppig.social.profile.extractor.conflict-resolver` | 叶子 | active | `src/grouppig/social/profile/extractor/conflict_resolver.py` | rpc:profile.conflict | social-reflect-engineer |
| `grouppig.social.profile.extractor.fact-extractor` | 叶子 | active | `src/grouppig/social/profile/extractor/fact_extractor.py` | rpc:profile.fact.extract | social-reflect-engineer |
| `grouppig.social.profile.extractor.stance-extractor` | 叶子 | active | `src/grouppig/social/profile/extractor/stance_extractor.py` | rpc:profile.stance.extract | social-reflect-engineer |
| `grouppig.social.profile.manager` | 容器 | active | `src/grouppig/social/profile/manager/__init__.py` | - | social-reflect-engineer |
| `grouppig.social.profile.manager.events` | 叶子 | active | `src/grouppig/social/profile/manager/events.py` | kafka:grouppig.profile.updated | social-reflect-engineer |
| `grouppig.social.profile.manager.lookup` | 叶子 | active | `src/grouppig/social/profile/manager/lookup.py` | rpc:profile.get | social-reflect-engineer |
| `grouppig.social.profile.manager.versioning` | 叶子 | active | `src/grouppig/social/profile/manager/versioning.py` | rpc:profile.update | social-reflect-engineer |
| `grouppig.social.runtime` | 叶子 | active | `src/grouppig/social/runtime.py` | - | social-reflect-engineer |
| `grouppig.social.speech` | 容器 | active | `src/grouppig/social/speech/__init__.py` | - | social-reflect-engineer |
| `grouppig.social.speech.profiler` | 容器 | active | `src/grouppig/social/speech/profiler/__init__.py` | - | social-reflect-engineer |
| `grouppig.social.speech.profiler.lexicon` | 叶子 | active | `src/grouppig/social/speech/profiler/lexicon.py` | rpc:speech.lexicon | social-reflect-engineer |
| `grouppig.social.speech.profiler.style-metrics` | 叶子 | active | `src/grouppig/social/speech/profiler/style_metrics.py` | rpc:speech.profile, rpc:speech.style | social-reflect-engineer |
| `grouppig.social.speech.profiler.temper` | 叶子 | active | `src/grouppig/social/speech/profiler/temper.py` | rpc:speech.temper | social-reflect-engineer |
| `grouppig.social.speech.responder` | 容器 | active | `src/grouppig/social/speech/responder/__init__.py` | - | social-reflect-engineer |
| `grouppig.social.speech.responder.adapter` | 叶子 | active | `src/grouppig/social/speech/responder/adapter.py` | rpc:speech.advise, rpc:speech.tailor | social-reflect-engineer |
| `grouppig.social.speech.responder.validator` | 叶子 | active | `src/grouppig/social/speech/responder/validator.py` | rpc:speech.validate | social-reflect-engineer |
