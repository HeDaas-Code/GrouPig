---
uid: 85f633ee
id: grouppig.session.lifecycle.state-machine
parent: grouppig.session.lifecycle
name: {zh: "会话状态机", en: "Session State Machine"}
description:
  zh: >
      维护会话状态转移表，提供开、更、查、归档接口。
      
  en: >
      Maintains the session state transition table and provides open/update/get/archive interfaces.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.972Z"
fingerprint: 10f883a7d867d01577c812164f7a749019f28a6494f62df2f651834c664b8cff
source:
  - path: "src/grouppig/session/lifecycle/state_machine.py"
apis:
  - protocol: rpc
    path: "session.open"
    description:
      zh: >
          开启新主题会话
          
      en: >
          Open a new topic session
          
  - protocol: rpc
    path: "session.update"
    description:
      zh: >
          更新会话状态
          
      en: >
          Update session state
          
  - protocol: rpc
    path: "session.current"
    description:
      zh: >
          查询当前活跃会话
          
      en: >
          Get the current active session
          
  - protocol: rpc
    path: "session.archive"
    description:
      zh: >
          归档会话
          
      en: >
          Archive the session
          
deps:
  - kind: call
    to: grouppig.session.lifecycle.heat
    from_api: "rpc:session.update"
    to_api: "rpc:session.heat"
    label: {zh: "热度更新", en: "Heat update"}
  - kind: call
    to: grouppig.session.lifecycle.archive-trigger
    from_api: "rpc:session.update"
    to_api: "rpc:session.archive.check"
    label: {zh: "归档检查", en: "Archive check"}
  - kind: event
    to: grouppig.session.lifecycle.event-emitter
    from_api: "rpc:session.archive"
    to_api: "kafka:grouppig.session.completed"
    label: {zh: "发布完成事件", en: "Publish completion"}
  - kind: call
    to: grouppig.memory.session-archive.dao
    from_api: "rpc:session.archive"
    to_api: "rpc:archive.save"
    label: {zh: "归档落盘", en: "Persist archive"}
---
