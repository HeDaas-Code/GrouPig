---
uid: 65ff9345
id: grouppig.reflection.evaluator.ab-test
parent: grouppig.reflection.evaluator
name: {zh: "A/B 对比评估器", en: "A/B Comparison Evaluator"}
description:
  zh: >
      对比启用与未启用策略的会话指标。
      
  en: >
      Compares session metrics with and without the strategy enabled.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.971Z"
fingerprint: 56465497f5536ba73eeb0ec6b7b0394d1a46bfa37e33863ced50d7cfd2f12238
source:
  - path: "src/grouppig/reflection/evaluator/ab_test.py"
apis:
  - protocol: rpc
    path: "strategy.evaluate"
    description:
      zh: >
          评估策略效果
          
      en: >
          Evaluate strategy effectiveness
          
deps:
  - kind: call
    to: grouppig.reflection.evaluator.scoring
    from_api: "rpc:strategy.evaluate"
    to_api: "rpc:strategy.score"
    label: {zh: "打分", en: "Score"}
---
