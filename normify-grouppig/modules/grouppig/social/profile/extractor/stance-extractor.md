---
uid: 846478f2
id: grouppig.social.profile.extractor.stance-extractor
parent: grouppig.social.profile.extractor
name: {zh: "立场变化抽取器", en: "Stance Extractor"}
description:
  zh: >
      抽取群友在话题上的立场与变化，记录到档案事实。
      
  en: >
      Extracts members' stances on topics and their changes, recording them as profile facts.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.974Z"
fingerprint: 2c5b28e1f12ab23d29705ca32508fe426e2cf6ce99842f3d59d8d78548e65e32
source:
  - path: "src/grouppig/social/profile/extractor/stance_extractor.py"
apis:
  - protocol: rpc
    path: "profile.stance.extract"
    description:
      zh: >
          抽取立场变化
          
      en: >
          Extract stance changes
          
deps:
  - kind: call
    to: grouppig.memory.thread-store.dao
    from_api: "rpc:profile.stance.extract"
    to_api: "rpc:thread.load"
    label: {zh: "读聊天线", en: "Read threads"}
  - kind: call
    to: grouppig.social.profile.manager.versioning
    from_api: "rpc:profile.stance.extract"
    to_api: "rpc:profile.update"
    label: {zh: "更新档案", en: "Update profile"}
---
