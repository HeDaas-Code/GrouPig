---
uid: 6559f672
id: grouppig.perception.interrupt.scorer
parent: grouppig.perception.interrupt
name: {zh: "插话价值评分器", en: "Interrupt Value Scorer"}
description:
  zh: >
      插话价值评分器：自动编码窗口特征，按点名场景动态归一四项权重，融合冷却、节奏、刷屏与自学习话题词表后交给决策器。无点名时默认把 mentioned 权重归一给有效分量，被点名时保留原始权重和 mention_floor 兜底。
      
  en: >
      Interrupt value scorer: automatically encodes window features, dynamically renormalizes the four component weights by mention presence, applies cooldown, rhythm, flood and learned topic terms, then delegates to the decision gate. With no mention it redistributes the mentioned weight across usable components; with a mention it preserves the original weights and mention-floor override.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-29T19:07:38.995Z"
fingerprint: df91a9ddaefed0a3c9ba8e6f7019ccfc335ca1234c808ffb76521bb2af8e7f07
source:
  - path: "src/grouppig/perception/interrupt/scorer.py"
apis:
  - protocol: rpc
    path: "interrupt.score"
    description:
      zh: >
          计算插话价值
          
      en: >
          Score interrupt value
          
deps:
  - kind: call
    to: grouppig.perception.interrupt.cooldown
    from_api: "rpc:interrupt.score"
    to_api: "rpc:interrupt.cooldown"
    label: {zh: "冷却检查", en: "Cooldown check"}
  - kind: call
    to: grouppig.perception.interrupt.decision
    from_api: "rpc:interrupt.score"
    to_api: "rpc:interrupt.decide"
    label: {zh: "给出决策", en: "Produce decision"}
---
