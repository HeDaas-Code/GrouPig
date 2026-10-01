---
uid: 1ba224c5
id: grouppig.memory.chat-store.window-index
parent: grouppig.memory.chat-store
name: {zh: "时间窗索引器", en: "Window Index"}
description:
  zh: >
      维护滚动时间窗索引，供刷屏与节奏检测快速切片。
      
  en: >
      Maintains the rolling window index for fast slicing by flood and rhythm detection.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.751Z"
fingerprint: ff96a719ee9eaef33e037a4608c3da4eac536d41965a1dbd55a3ae2a052795b1
source:
  - path: "src/grouppig/memory/chat_store/window_index.py"
apis:
  - protocol: rpc
    path: "chat.window.advance"
    description:
      zh: >
          推进时间窗索引
          
      en: >
          Advance the window index
          
  - protocol: rpc
    path: "chat.window.prune"
    description:
      zh: >
          清理过期窗口
          
      en: >
          Prune expired windows
          
deps:
  - kind: dataflow
    to: grouppig.memory.chat-store.schema
    from_api: "rpc:chat.window.advance"
    to_api: "mysql:chat_window_index"
    label: {zh: "写索引表", en: "Write index"}
---
