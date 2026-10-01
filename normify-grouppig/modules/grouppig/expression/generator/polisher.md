---
uid: d275ea97
id: grouppig.expression.generator.polisher
parent: grouppig.expression.generator
name: {zh: "人性化润色器", en: "Humanizing Polisher"}
description:
  zh: >
      把回复润色得更像人：口语化、接梗、按画像改写。
      
  en: >
      Polishes replies to sound human: colloquial, meme-aware and portrait-tailored.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.968Z"
fingerprint: a136a2c4986a935b3185fc305d019f4e7eff9fdc687b3ab426bf212af82bec29
source:
  - path: "src/grouppig/expression/generator/polisher.py"
apis:
  - protocol: rpc
    path: "generator.humanize"
    description:
      zh: >
          把回复润色得更像人
          
      en: >
          Humanize the reply text
          
deps:
  - kind: call
    to: grouppig.social.speech.responder.adapter
    from_api: "rpc:generator.humanize"
    to_api: "rpc:speech.tailor"
    label: {zh: "按画像改写", en: "Tailor by portrait"}
---
