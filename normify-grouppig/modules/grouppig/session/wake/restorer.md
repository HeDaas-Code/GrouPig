---
uid: 44105d4f
id: grouppig.session.wake.restorer
parent: grouppig.session.wake
name: {zh: "会话上下文恢复器", en: "Session Context Restorer"}
description:
  zh: >
      加载归档会话并恢复上下文，供当前回复引用。
      
  en: >
      Loads archived sessions and restores their context for current reply reference.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.973Z"
fingerprint: f33679ec7732a2d4bd2e8423466c3d212cf55c1a51fa5e4bbd9141137f517e01
source:
  - path: "src/grouppig/session/wake/restorer.py"
apis:
  - protocol: rpc
    path: "session.wake"
    description:
      zh: >
          暂时唤醒归档会话
          
      en: >
          Temporarily wake an archived session
          
  - protocol: rpc
    path: "session.sleep"
    description:
      zh: >
          把唤醒会话重新归档
          
      en: >
          Re-archive a woken session
          
deps:
  - kind: call
    to: grouppig.memory.session-archive.dao
    from_api: "rpc:session.wake"
    to_api: "rpc:archive.load"
    label: {zh: "加载旧档", en: "Load archive"}
  - kind: call
    to: grouppig.session.wake.buffer
    from_api: "rpc:session.wake"
    to_api: "rpc:wake.buffer.push"
    label: {zh: "写入缓冲", en: "Push buffer"}
---
