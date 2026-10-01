---
uid: 613273a1
id: grouppig.session.threads.cross.matcher
parent: grouppig.session.threads.cross
name: {zh: "历史聊天线匹配器", en: "Historical Thread Matcher"}
description:
  zh: >
      在聊天线存储中检索最相关的历史线程。
      
  en: >
      Retrieves the most relevant historical threads from thread storage.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.972Z"
fingerprint: a3a8aa0225ef2171364bc57c63031a668569d6117fe8a33be2f1c492a0450148
source:
  - path: "src/grouppig/session/threads/cross/matcher.py"
apis:
  - protocol: rpc
    path: "cross.match"
    description:
      zh: >
          匹配历史聊天线
          
      en: >
          Match historical threads
          
deps:
  - kind: call
    to: grouppig.memory.thread-store.dao
    from_api: "rpc:cross.match"
    to_api: "rpc:thread.find-cross"
    label: {zh: "查找旧线", en: "Find old threads"}
  - kind: call
    to: grouppig.session.wake.restorer
    from_api: "rpc:cross.match"
    to_api: "rpc:session.wake"
    label: {zh: "唤醒旧会话", en: "Wake archived session"}
---
