---
uid: 3f76449e
id: grouppig.reflection
parent: grouppig
name: {zh: "反思策略层", en: "Reflection & Strategy Layer"}
description:
  zh: >
      维护行为预设库，在会话结束后反思行为模式，生成并评估新的行为策略。
      
  en: >
      Maintains behavior presets, reflects on behavior patterns after a session ends, and generates/evaluates new behavior strategies.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:42:32.941Z"
fingerprint: 7db814658240fa61699f0520706ed2963e6cb18fc4eb29829db85772b676319e
source:
  - path: "src/grouppig/reflection/__init__.py"
deps:
  - kind: call
    to: grouppig.memory
    label: {zh: "读取记忆", en: "Memory access"}
---
