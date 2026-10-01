---
uid: 0c92d083
id: grouppig.social.profile.extractor.fact-extractor
parent: grouppig.social.profile.extractor
name: {zh: "事实抽取器", en: "Fact Extractor"}
description:
  zh: >
      抽取群友属性与事实：称呼、年龄、城市、职业、兴趣。
      
  en: >
      Extracts member attributes and facts: name, age, city, job, interests.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.974Z"
fingerprint: 3dbe755ddf209f2cd10157706838f91cd65347a4719b2f14176d04afb3e2a755
source:
  - path: "src/grouppig/social/profile/extractor/fact_extractor.py"
apis:
  - protocol: rpc
    path: "profile.fact.extract"
    description:
      zh: >
          抽取档案事实
          
      en: >
          Extract profile facts
          
deps:
  - kind: call
    to: grouppig.memory.chat-store.dao
    from_api: "rpc:profile.fact.extract"
    to_api: "rpc:chat.query"
    label: {zh: "读消息", en: "Read messages"}
  - kind: call
    to: grouppig.social.profile.manager.versioning
    from_api: "rpc:profile.fact.extract"
    to_api: "rpc:profile.update"
    label: {zh: "更新档案", en: "Update profile"}
---
