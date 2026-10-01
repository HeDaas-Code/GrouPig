---
uid: 7a3f9c21
id: grouppig.infra.runtime.laya-system1
parent: grouppig.infra.runtime
name: {zh: "LAY A System-1 传输", en: "LAY A System-1 Transport"}
description:
  zh: >
      非自回归 System-1 决策模型的传输层：把「一段状态 + 一组类型化问题」做一次前向，直接返回 choice / score / noul 三种原语的结构化答案与校准概率；不生成文本，实测单次 110–270ms。
      
  en: >
      Transport for the non-autoregressive System-1 decision model: one forward pass over a state plus typed questions returns structured answers and calibrated probabilities for the choice / score / noul primitives; no text generation, measured at 110-270ms per call.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-29T19:07:38.994Z"
fingerprint: bd27679963aa614c6b05f0aa8256af5163bccf8c964831e1fe854de29897853c
source:
  - path: "src/grouppig/infra/runtime/laya_system1.py"
apis: []
---
