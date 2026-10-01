---
uid: 4102d78a
id: grouppig.social.profile.extractor.conflict-resolver
parent: grouppig.social.profile.extractor
name: {zh: "档案冲突消解器", en: "Profile Conflict Resolver"}
description:
  zh: >
      当新旧事实冲突时，按来源可信度与时间衰减消解。
      
  en: >
      Resolves conflicts between new and old facts by source credibility and time decay.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.974Z"
fingerprint: e01174c366f96d21140c138cad35df2b941d32508c908cff4ca3547cd07ead4b
source:
  - path: "src/grouppig/social/profile/extractor/conflict_resolver.py"
apis:
  - protocol: rpc
    path: "profile.conflict"
    description:
      zh: >
          处理档案信息冲突
          
      en: >
          Resolve profile conflicts
          
deps:
  - kind: call
    to: grouppig.social.profile.manager.versioning
    from_api: "rpc:profile.conflict"
    to_api: "rpc:profile.update"
    label: {zh: "写回消解", en: "Write resolution"}
---
