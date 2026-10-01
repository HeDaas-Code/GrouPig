---
uid: bbba13a9
id: grouppig.expression.orchestrator.flow.transition
parent: grouppig.expression.orchestrator.flow
name: {zh: "状态转移表", en: "Flow Transition Table"}
description:
  zh: >
      维护心流状态转移表：承接、展开、收束、打断。
      
  en: >
      Maintains the flow transition table: acknowledge, expand, close, interrupt.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.969Z"
fingerprint: f7415d4b2f0f7503679ab7675a2c84f0552d00ef20fd63904a8d8fc6da9c3fd1
source:
  - path: "src/grouppig/expression/orchestrator/flow/transition.py"
apis:
  - protocol: rpc
    path: "flow.transition"
    description:
      zh: >
          执行状态转移
          
      en: >
          Execute a state transition
          
---
