---
uid: 6a4b872a
id: grouppig.social.speech.profiler.lexicon
parent: grouppig.social.speech.profiler
name: {zh: "口头禅统计器", en: "Lexicon Counter"}
description:
  zh: >
      统计群友高频词、口头禅与表情偏好。
      
  en: >
      Counts members' high-frequency words, catchphrases and emoji preferences.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.974Z"
fingerprint: 39e8908d57b152dd2ad1fff6bc576e9ca4d6386de3ba463bc6efe1818fbdb115
source:
  - path: "src/grouppig/social/speech/profiler/lexicon.py"
apis:
  - protocol: rpc
    path: "speech.lexicon"
    description:
      zh: >
          统计口头禅用词
          
      en: >
          Count catchphrases
          
deps:
  - kind: call
    to: grouppig.memory.chat-store.dao
    from_api: "rpc:speech.lexicon"
    to_api: "rpc:chat.query"
    label: {zh: "读历史消息", en: "Read history"}
---
