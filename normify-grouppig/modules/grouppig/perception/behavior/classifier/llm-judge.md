---
uid: 0db4fd77
id: grouppig.perception.behavior.classifier.llm-judge
parent: grouppig.perception.behavior.classifier
name: {zh: "轻量模型判别器", en: "Lightweight Model Judge"}
description:
  zh: >
      对模糊窗口调用轻量分类模型，输出行为类别候选。实测边界（2026-09-24，18 例 6 类，真实端点对照）：散文 state + 单问 choice 准确率 33%（随机 16.7%），改喂数值特征字典降到 6%，加统计行降到 25%，noul/score 原语（冷场/复读/刷屏强度）无可用判别力。故本模块只作规则引擎的模糊窗口补充：不得把行为主判定迁到模型，也不要用结构化数值向量替换散文 state。
      
  en: >
      Calls a lightweight classification model for ambiguous windows. Measured boundary (2026-09-24, 18 windows / 6 classes, live endpoint): prose state plus one choice question scores 33% (random 16.7%), a numeric feature dict drops to 6%, adding a statistics line to 25%, and the noul/score primitives carry no usable signal. Keep this module as a supplement to the rule engine only: do not move the primary behavior decision to the model, and never replace the prose state with a numeric vector.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.753Z"
fingerprint: f763c3c40f33d6373e1bdfac8aaf457bfcf0391f48b5bffed095a1ebc1acb03f
source:
  - path: "src/grouppig/perception/behavior/classifier/llm_judge.py"
apis:
  - protocol: rpc
    path: "behavior.llm.judge"
    description:
      zh: >
          模型判别行为
          
      en: >
          Judge behavior by model
          
deps:
  - kind: call
    to: grouppig.infra.model-gateway.router
    from_api: "rpc:behavior.llm.judge"
    to_api: "rpc:model.classify"
    label: {zh: "调用分类模型", en: "Call classifier"}
---
