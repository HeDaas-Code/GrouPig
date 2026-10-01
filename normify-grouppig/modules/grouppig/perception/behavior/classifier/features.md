---
uid: c81407df
id: grouppig.perception.behavior.classifier.features
parent: grouppig.perception.behavior.classifier
name: {zh: "行为特征编码器", en: "Behavior Feature Encoder"}
description:
  zh: >
      从消息窗口编码行为特征，提供 topic_focus、关键词、节奏与互动统计；显式 seconds 优先，空窗口回落到配置窗口秒数，供插话评分自动补全特征。
      
  en: >
      Encodes behavior features from message windows, including topic focus, keywords, rhythm and interaction statistics; explicit seconds wins and empty windows fall back to the configured window duration so interrupt scoring can fill missing features.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-26T17:09:57.380Z"
fingerprint: 7d570eef019c1b882888a38201bfdce518ff74bf323e48ba5990b181ec4f0986
source:
  - path: "src/grouppig/perception/behavior/classifier/features.py"
apis:
  - protocol: rpc
    path: "behavior.features.encode"
    description:
      zh: >
          编码行为特征
          
      en: >
          Encode behavior features
          
---
