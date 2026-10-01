---
uid: 716a915b
id: grouppig.expression.orchestrator.flow.emitter
parent: grouppig.expression.orchestrator.flow
name: {zh: "编排事件发布器", en: "Flow Event Emitter"}
description:
  zh: >
      发布回复编排完成事件。
      
  en: >
      Publishes reply composed events.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.969Z"
fingerprint: 2cc9a381ed78e3acd95833fe18364f30d8d9b4a9545e3c2aad08e49e408c7c6f
source:
  - path: "src/grouppig/expression/orchestrator/flow/emitter.py"
apis:
  - protocol: kafka
    path: "grouppig.reply.composed"
    description:
      zh: >
          回复编排完成事件
          
      en: >
          Reply composed event
          
---
