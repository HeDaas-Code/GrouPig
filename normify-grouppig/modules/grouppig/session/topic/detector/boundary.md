---
uid: c81e8ceb
id: grouppig.session.topic.detector.boundary
parent: grouppig.session.topic.detector
name: {zh: "话题边界检测器", en: "Topic Boundary Detector"}
description:
  zh: >
      检测话题切换点：静默、关键词突变、回复对象变化。
      
  en: >
      Detects topic switch points: silence, keyword jumps, changes in reply targets.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.973Z"
fingerprint: 14acc6b246cde19f5f6d4c226b81782da6ee3669de85a24a2b8af1f6375ad2a7
source:
  - path: "src/grouppig/session/topic/detector/boundary.py"
apis:
  - protocol: rpc
    path: "topic.boundary.detect"
    description:
      zh: >
          检测话题边界
          
      en: >
          Detect topic boundary
          
deps:
  - kind: call
    to: grouppig.memory.chat-store.dao
    from_api: "rpc:topic.boundary.detect"
    to_api: "rpc:chat.query"
    label: {zh: "查历史消息", en: "Query history"}
---
