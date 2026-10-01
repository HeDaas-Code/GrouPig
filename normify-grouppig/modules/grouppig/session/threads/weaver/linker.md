---
uid: efbdffa1
id: grouppig.session.threads.weaver.linker
parent: grouppig.session.threads.weaver
name: {zh: "引用链接器", en: "Thread Linker"}
description:
  zh: >
      把片段链接进当前会话聊天线，续接已有线程，并保存到聊天线存储。
      
  en: >
      Links segments into the current session thread, appends to existing threads, and saves to thread storage.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.973Z"
fingerprint: 28872edf8e018210933beb3e0ff25611b596974b0335518ca5bf045459560a7d
source:
  - path: "src/grouppig/session/threads/weaver/linker.py"
apis:
  - protocol: rpc
    path: "threads.weave"
    description:
      zh: >
          把消息编入聊天线
          
      en: >
          Weave a message into a thread
          
  - protocol: rpc
    path: "threads.link"
    description:
      zh: >
          链接两个片段
          
      en: >
          Link two segments
          
deps:
  - kind: call
    to: grouppig.session.lifecycle.state-machine
    from_api: "rpc:threads.weave"
    to_api: "rpc:session.current"
    label: {zh: "当前会话", en: "Current session"}
  - kind: call
    to: grouppig.memory.thread-store.dao
    from_api: "rpc:threads.weave"
    to_api: "rpc:thread.save"
    label: {zh: "保存聊天线", en: "Save thread"}
  - kind: call
    to: grouppig.session.threads.weaver.outliner
    from_api: "rpc:threads.link"
    to_api: "rpc:threads.outline"
    label: {zh: "更新大纲", en: "Update outline"}
  - kind: call
    to: grouppig.session.threads.cross.reference-parser
    from_api: "rpc:threads.link"
    to_api: "rpc:cross.detect"
    label: {zh: "查跨会话引用", en: "Detect cross ref"}
---
