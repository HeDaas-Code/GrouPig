---
uid: 9dc7cdb5
id: grouppig.perception.observer.window
parent: grouppig.perception.observer
name: {zh: "滚动时间窗", en: "Rolling Window"}
description:
  zh: >
      维护最近 N 分钟消息窗口，为刷屏检测与节奏感知提供切片。
      
  en: >
      Maintains the last-N-minutes message window, providing slices for flood and rhythm detection.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.754Z"
fingerprint: 7016af6060eac1663cadf062fb7d966b43ba92b412dac8200816f1546e8b6405
source:
  - path: "src/grouppig/perception/observer/window.py"
apis:
  - protocol: rpc
    path: "observer.window.slide"
    description:
      zh: >
          滑动时间窗
          
      en: >
          Slide the window
          
  - protocol: rpc
    path: "observer.window.slice"
    description:
      zh: >
          取窗口切片
          
      en: >
          Slice the window
          
---
