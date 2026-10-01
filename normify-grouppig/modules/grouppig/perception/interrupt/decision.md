---
uid: 89fdb036
id: grouppig.perception.interrupt.decision
parent: grouppig.perception.interrupt
name: {zh: "插话决策器", en: "Interrupt Decision Maker"}
description:
  zh: >
      根据评分与阈值决定插话或克制，并触发表达层。被点名（mentioned>=0.9）可越过冷却与退避，但越不过每小时硬上限；只有判断性克制（low_score/near_threshold）才计入退避，被闸门自己拦下的 hold 不记（否则「因为被罚所以再罚」会自锁）。
      
  en: >
      Decides to interject or hold back based on score and thresholds, then triggers the expression layer. Being mentioned (mentioned>=0.9) overrides cooldown and backoff but not the hourly hard cap; only deliberate restraint (low_score/near_threshold) counts toward backoff — gate-forced holds do not, or the gate would penalize itself.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-29T19:07:38.995Z"
fingerprint: 91868131ca2b5ea02f31ea54c1cbb1bc71b83a9a07077b229fa40a9392602b32
source:
  - path: "src/grouppig/perception/interrupt/decision.py"
apis:
  - protocol: rpc
    path: "interrupt.decide"
    description:
      zh: >
          决定是否插话
          
      en: >
          Decide whether to interject
          
  - protocol: kafka
    path: "grouppig.interrupt.triggered"
    description:
      zh: >
          插话时机触发事件
          
      en: >
          Interrupt timing triggered event
          
deps:
  - kind: call
    to: grouppig.expression.orchestrator.flow.state
    from_api: "rpc:interrupt.decide"
    to_api: "rpc:flow.start"
    label: {zh: "触发编排", en: "Trigger flow"}
---
