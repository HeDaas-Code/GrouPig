---
uid: 7ddda2c5
id: grouppig.reflection.session-review.timeline
parent: grouppig.reflection.session-review
name: {zh: "会话时间线重建器", en: "Session Timeline Rebuilder"}
description:
  zh: >
      从聊天流水与会话档案重建会话时间线。
      
  en: >
      Rebuilds the session timeline from chat streams and session archives.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.972Z"
fingerprint: be764fb163cef32ea9e51b9df74afe4fcfe483bfa68018d5d45e7f0dbc58917f
source:
  - path: "src/grouppig/reflection/session_review/timeline.py"
apis:
  - protocol: rpc
    path: "review.on-session-completed"
    description:
      zh: >
          接收会话完成事件
          
      en: >
          Receive session completed event
          
  - protocol: rpc
    path: "review.timeline"
    description:
      zh: >
          重建会话时间线
          
      en: >
          Rebuild session timeline
          
deps:
  - kind: call
    to: grouppig.memory.session-archive.dao
    from_api: "rpc:review.timeline"
    to_api: "rpc:archive.load"
    label: {zh: "读会话档", en: "Read session archive"}
  - kind: call
    to: grouppig.memory.chat-store.dao
    from_api: "rpc:review.timeline"
    to_api: "rpc:chat.query"
    label: {zh: "读聊天流水", en: "Read chat stream"}
---
