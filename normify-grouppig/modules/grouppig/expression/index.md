---
uid: c8985a92
id: grouppig.expression
parent: grouppig
name: {zh: "表达层", en: "Expression Layer"}
description:
  zh: >
      以心流多轮结构化编排生成回复：人设、否认 AI、个性化风格、黑话学习与省 token 生成。
      
  en: >
      Generates replies through flow-based multi-turn structured orchestration: persona, AI denial, personalized style, slang learning and token-efficient generation.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:42:32.938Z"
fingerprint: a8fe010c0467090ea875eb999236baf5416652b056a04b5241b69c1fca7139d6
source:
  - path: "src/grouppig/expression/__init__.py"
deps:
  - kind: call
    to: grouppig.social
    label: {zh: "个性化", en: "Personalization"}
  - kind: call
    to: grouppig.reflection
    label: {zh: "预设策略", en: "Presets & strategy"}
  - kind: call
    to: grouppig.memory
    label: {zh: "黑话记忆", en: "Slang memory"}
  - kind: call
    to: grouppig.infra
    label: {zh: "模型与预算", en: "Model & budget"}
---
