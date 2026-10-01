---
uid: 7e960c30
id: grouppig.reflection.evaluator.scoring
parent: grouppig.reflection.evaluator
name: {zh: "策略评分器", en: "Strategy Scorer"}
description:
  zh: >
      给策略打分并输出保留/回滚建议。
      
  en: >
      Scores strategies and outputs keep-or-rollback suggestions.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.971Z"
fingerprint: 405435636c516bdb04bc0c71653671b6fda3b21aaafab1c4a2f25f6d4ce3a30c
source:
  - path: "src/grouppig/reflection/evaluator/scoring.py"
apis:
  - protocol: rpc
    path: "strategy.score"
    description:
      zh: >
          给策略打分
          
      en: >
          Score a strategy
          
deps:
  - kind: call
    to: grouppig.memory.chat-store.dao
    from_api: "rpc:strategy.score"
    to_api: "rpc:chat.query"
    label: {zh: "读后续消息", en: "Read later messages"}
  - kind: call
    to: grouppig.reflection.presets.matcher
    from_api: "rpc:strategy.score"
    to_api: "rpc:presets.match"
    label: {zh: "对照预设", en: "Match presets"}
---
