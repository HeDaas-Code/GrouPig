---
uid: d24e2ae6
id: grouppig.social.graph.manager.egonet
parent: grouppig.social.graph.manager
name: {zh: "自我中心网络构建器", en: "Ego-Net Builder"}
description:
  zh: >
      构建并读取以自己为中心的社交网。
      
  en: >
      Builds and reads the self-centered social graph.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.973Z"
fingerprint: eb263d95b2a7d23ffd145f2e8e89b8383beb1b51226a5dd9686b9a1203b7b76e
source:
  - path: "src/grouppig/social/graph/manager/egonet.py"
apis:
  - protocol: rpc
    path: "graph.get-egonet"
    description:
      zh: >
          读取自我中心社交网
          
      en: >
          Get the self-centered social graph
          
deps:
  - kind: call
    to: grouppig.social.profile.manager.lookup
    from_api: "rpc:graph.get-egonet"
    to_api: "rpc:profile.get"
    label: {zh: "读档案", en: "Read profile"}
  - kind: call
    to: grouppig.memory.social-store.dao
    from_api: "rpc:graph.get-egonet"
    to_api: "rpc:social-store.get-edges"
    label: {zh: "读社交边", en: "Read edges"}
---
