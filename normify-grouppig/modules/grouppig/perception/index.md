---
uid: 51ee1e19
id: grouppig.perception
parent: grouppig
name: {zh: "感知层", en: "Perception Layer"}
description:
  zh: >
      观察群聊流：清洗、去重、特征提取、群体行为分类、刷屏检测、节奏感知与插话决策。
      
  en: >
      Observes the chat stream: cleaning, dedup, feature extraction, behavior classification, flood detection, rhythm sensing and interrupt decisions.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.753Z"
fingerprint: 7b8f3603c6ac94b1bb79056ca3284c1f1572c0a39978179dc98a52f9c3e20552
source:
  - path: "src/grouppig/perception/__init__.py"
deps:
  - kind: call
    to: grouppig.session
    label: {zh: "话题会话", en: "Topic session"}
  - kind: call
    to: grouppig.expression
    label: {zh: "触发表达", en: "Trigger expression"}
  - kind: call
    to: grouppig.infra
    label: {zh: "模型能力", en: "Model capability"}
---
