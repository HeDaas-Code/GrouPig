---
uid: 5cfcaf6a
id: grouppig.social.speech.profiler.temper
parent: grouppig.social.speech.profiler
name: {zh: "语气温度分析器", en: "Temper Analyzer"}
description:
  zh: >
      分析群友语气的冷热、攻击性与亲密感。
      
  en: >
      Analyzes members' tone warmth, aggressiveness and intimacy.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.974Z"
fingerprint: b4865360a09083ca1a4e82964d34d3b2aea31bd623181c0b6a998020586d98ca
source:
  - path: "src/grouppig/social/speech/profiler/temper.py"
apis:
  - protocol: rpc
    path: "speech.temper"
    description:
      zh: >
          分析语气温度
          
      en: >
          Analyze tone temper
          
deps:
  - kind: call
    to: grouppig.memory.chat-store.dao
    from_api: "rpc:speech.temper"
    to_api: "rpc:chat.query"
    label: {zh: "读历史消息", en: "Read history"}
---
