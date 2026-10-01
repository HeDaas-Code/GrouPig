---
uid: 3e74d761
id: grouppig.session.threads.cross.reference-parser
parent: grouppig.session.threads.cross
name: {zh: "引用解析器", en: "Reference Parser"}
description:
  zh: >
      解析消息中的指代、引用与“上次说的”类表达。
      
  en: >
      Parses references, anaphora and "what we said last time" expressions in messages.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.972Z"
fingerprint: e1e4d66d4dec85eef4771088b6ff3b2d12cf2523bcd76a3a049da3dc49192708
source:
  - path: "src/grouppig/session/threads/cross/reference_parser.py"
apis:
  - protocol: rpc
    path: "cross.detect"
    description:
      zh: >
          检测跨会话引用
          
      en: >
          Detect cross-session references
          
  - protocol: rpc
    path: "cross.parse"
    description:
      zh: >
          解析引用内容
          
      en: >
          Parse reference content
          
deps:
  - kind: call
    to: grouppig.session.threads.cross.matcher
    from_api: "rpc:cross.detect"
    to_api: "rpc:cross.match"
    label: {zh: "匹配旧线", en: "Match old threads"}
---
