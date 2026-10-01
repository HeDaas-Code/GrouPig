---
uid: 38d2e89e
id: grouppig.reflection.session-review.insights
parent: grouppig.reflection.session-review
name: {zh: "反思结论生成器", en: "Insight Generator"}
description:
  zh: >
      根据指标生成反思结论：哪里插话过早、哪里该接没接。
      
  en: >
      Generates review insights from metrics: where interrupts were early, where replies were missed.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.972Z"
fingerprint: 50f26145d451008c8e5a2165fdadd57eb4483a63d6914751bf33ace332ef3286
source:
  - path: "src/grouppig/reflection/session_review/insights.py"
apis:
  - protocol: rpc
    path: "review.analyze"
    description:
      zh: >
          分析本段会话行为
          
      en: >
          Analyze this session's behavior
          
deps:
  - kind: call
    to: grouppig.reflection.session-review.metrics
    from_api: "rpc:review.analyze"
    to_api: "rpc:review.metrics"
    label: {zh: "读指标", en: "Read metrics"}
  - kind: call
    to: grouppig.reflection.strategy.synthesizer
    from_api: "rpc:review.analyze"
    to_api: "rpc:strategy.generate"
    label: {zh: "生成策略", en: "Generate strategy"}
---
