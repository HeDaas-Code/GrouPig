---
uid: f6149eb3
id: grouppig.session.topic.detector.ranker
parent: grouppig.session.topic.detector
name: {zh: "话题排序器", en: "Topic Ranker"}
description:
  zh: >
      对候选话题排序并归一，输出当前话题与置信度。
      
  en: >
      Ranks and normalizes candidate topics, outputting the current topic and confidence.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.973Z"
fingerprint: 4d0b3e277e77085e41db868d6f92dbd379fe5dab5d62b990611ef7143c4f0571
source:
  - path: "src/grouppig/session/topic/detector/ranker.py"
apis:
  - protocol: rpc
    path: "topic.detect"
    description:
      zh: >
          识别当前话题
          
      en: >
          Detect the current topic
          
  - protocol: rpc
    path: "topic.resolve"
    description:
      zh: >
          把模糊话题归一
          
      en: >
          Resolve a fuzzy topic
          
  - protocol: kafka
    path: "grouppig.topic.changed"
    description:
      zh: >
          话题切换事件
          
      en: >
          Topic changed event
          
deps:
  - kind: call
    to: grouppig.session.topic.embedder.similarity
    from_api: "rpc:topic.resolve"
    to_api: "rpc:topic.similarity"
    label: {zh: "相似归一", en: "Similarity merge"}
  - kind: call
    to: grouppig.session.lifecycle.state-machine
    from_api: "rpc:topic.detect"
    to_api: "rpc:session.open"
    label: {zh: "开启会话", en: "Open session"}
  - kind: call
    to: grouppig.session.lifecycle.state-machine
    from_api: "rpc:topic.resolve"
    to_api: "rpc:session.update"
    label: {zh: "更新会话", en: "Update session"}
  - kind: call
    to: grouppig.infra.model-gateway.router
    from_api: "rpc:topic.detect"
    to_api: "rpc:model.classify"
    label: {zh: "模型分类", en: "Model classify"}
---
