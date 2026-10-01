---
uid: bf35a7eb
id: grouppig.session
parent: grouppig
name: {zh: "话题会话层", en: "Topic Session Layer"}
description:
  zh: >
      识别当前话题并管理主题会话生命周期，把消息编织成聊天线，支持跨会话引用暂时唤醒归档会话。
      
  en: >
      Identifies the current topic and manages themed session lifecycle, weaves messages into chat threads, and temporarily wakes archived sessions for cross-session references.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:42:32.941Z"
fingerprint: 0d9895739938dd65a962ea604bc1532d5a61aef84e46560c41535faef94a693c
source:
  - path: "src/grouppig/session/__init__.py"
deps:
  - kind: event
    to: grouppig.reflection
    label: {zh: "会话反思", en: "Session review"}
  - kind: call
    to: grouppig.memory
    label: {zh: "读写记忆", en: "Memory access"}
  - kind: call
    to: grouppig.infra
    label: {zh: "模型能力", en: "Model capability"}
---
