---
uid: a1f525c8
id: grouppig.social.profile.manager.lookup
parent: grouppig.social.profile.manager
name: {zh: "档案索引器", en: "Profile Index"}
description:
  zh: >
      按群友 ID 与特征快速检索档案。
      
  en: >
      Retrieves profiles quickly by member ID and features.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.974Z"
fingerprint: c1817919b387e42c9f6914672216f49967a29db7d333a2d74f659da175704149
source:
  - path: "src/grouppig/social/profile/manager/lookup.py"
apis:
  - protocol: rpc
    path: "profile.get"
    description:
      zh: >
          读取群友档案
          
      en: >
          Get a member profile
          
deps:
  - kind: call
    to: grouppig.memory.profile-store.dao
    from_api: "rpc:profile.get"
    to_api: "rpc:profile-store.get"
    label: {zh: "读档案", en: "Read profile store"}
---
