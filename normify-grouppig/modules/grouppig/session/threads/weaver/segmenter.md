---
uid: 030c0239
id: grouppig.session.threads.weaver.segmenter
parent: grouppig.session.threads.weaver
name: {zh: "消息分段器", en: "Message Segmenter"}
description:
  zh: >
      把消息流切分为可编织的片段：说者、对象、内容、引用。
      
  en: >
      Segments the message stream into weavable pieces: speaker, target, content and references.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.973Z"
fingerprint: 989fbafc63395043f0a000875eea19816d01abdf24e35bf0fd0fe34d8bbb7cf6
source:
  - path: "src/grouppig/session/threads/weaver/segmenter.py"
apis:
  - protocol: rpc
    path: "threads.segment"
    description:
      zh: >
          把消息分段
          
      en: >
          Segment a message
          
---
