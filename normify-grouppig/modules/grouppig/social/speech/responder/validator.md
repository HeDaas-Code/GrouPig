---
uid: 7f88e26b
id: grouppig.social.speech.responder.validator
parent: grouppig.social.speech.responder
name: {zh: "风格一致性校验器", en: "Style Validator"}
description:
  zh: >
      校验改写后的回复是否贴合画像且不越界。
      
  en: >
      Validates that rewritten replies fit the portrait without crossing boundaries.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.974Z"
fingerprint: 6a00b2c6b000f9f46a1a215066debd578fd9ab27a01f41a2890fa7eeb37b6b6c
source:
  - path: "src/grouppig/social/speech/responder/validator.py"
apis:
  - protocol: rpc
    path: "speech.validate"
    description:
      zh: >
          校验风格一致性
          
      en: >
          Validate style consistency
          
---
