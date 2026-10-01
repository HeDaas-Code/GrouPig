---
uid: 0a162195
id: grouppig.social.graph.manager.tiering
parent: grouppig.social.graph.manager
name: {zh: "亲疏分层器", en: "Intimacy Tiering"}
description:
  zh: >
      按关系分把群友分为核心/熟识/普通/陌生四层，并更新社交边。
      
  en: >
      Tiers members into core/known/normal/stranger by relationship score and updates social edges.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.973Z"
fingerprint: d208de719f076313b00f48097aea963ed4cd36761850a4b4c1442c163a24bc60
source:
  - path: "src/grouppig/social/graph/manager/tiering.py"
apis:
  - protocol: rpc
    path: "graph.tiering"
    description:
      zh: >
          计算亲疏分层
          
      en: >
          Compute intimacy tiers
          
deps:
  - kind: call
    to: grouppig.social.graph.manager.egonet
    from_api: "rpc:graph.tiering"
    to_api: "rpc:graph.get-egonet"
    label: {zh: "读当前网络", en: "Read current graph"}
  - kind: call
    to: grouppig.memory.social-store.dao
    from_api: "rpc:graph.tiering"
    to_api: "rpc:social-store.put-edge"
    label: {zh: "写社交边", en: "Write edge"}
  - kind: event
    to: grouppig.social.graph.manager.events
    from_api: "rpc:graph.tiering"
    to_api: "kafka:grouppig.social.changed"
    label: {zh: "发布变化", en: "Publish change"}
---
