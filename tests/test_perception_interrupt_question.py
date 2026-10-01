"""提问密度折让的回归测试：有人真的在问问题时，门槛该低一点。

背景（``src/grouppig/perception/interrupt/decision.py`` 的
``DEFAULT_QUESTION_RATIO_MIN`` 说明）：``question_ratio`` 一直由
``rpc:behavior.features.encode`` 算出来（逐条 ``is_question`` 判定），却**没有任何消费方**
—— 一个满屏「这怎么弄？」的群和一个满屏闲聊的群拿到同一个阈值。

修法刻意**不动权重模型**，而是复用「被点名」那套机制（降门槛而不是加分）：
所以非提问窗口的分数与阈值**逐位不变**，回归面最小。本文件的用例都是先红后绿。
"""

from __future__ import annotations

import pytest

from grouppig.perception.interrupt.decision import (
    ACTION_HOLD,
    ACTION_SPEAK,
    ACTION_WAIT,
    DEFAULT_QUESTION_DISCOUNT,
    DEFAULT_QUESTION_RATIO_MIN,
    QUESTION_FLOOR,
    REASON_MENTIONED,
    REASON_QUESTION,
    InterruptDecision,
)

GROUP = 100
T0 = 1_700_000_000.0


def build(**kwargs: object) -> InterruptDecision:
    """一个不发事件、不触发心流、无冷却的决策器（纯判据测试）。"""

    kwargs.setdefault("config", None)
    kwargs.setdefault("trigger_flow", False)
    kwargs.setdefault("publish", False)
    return InterruptDecision(**kwargs)  # type: ignore[arg-type]


def evaluate(decision: InterruptDecision, score: float, *, ratio: float | None = None, **kwargs: object):
    """按给定提问占比评一次分。``ratio=None`` 表示调用方根本没给 features。"""

    features = {} if ratio is None else {"question_ratio": ratio}
    return decision.evaluate(
        score=score,
        band="",
        components=kwargs.pop("components", {}),
        behavior=kwargs.pop("behavior", "smalltalk"),
        cooldown={"allowed": True},
        features=features,
        **kwargs,
    )


# --------------------------------------------------------------------------
# 红：修复前 question_ratio 完全不影响判定
# --------------------------------------------------------------------------


def test_question_ratio_lowers_the_threshold():
    """提问占比够高时，阈值必须真的降下来。"""

    decision = build(threshold=0.55)
    plain = evaluate(decision, 0.30, ratio=0.0)
    asking = evaluate(decision, 0.30, ratio=0.8)
    assert plain["threshold"] == pytest.approx(0.55)
    assert asking["threshold"] == pytest.approx(0.55 - DEFAULT_QUESTION_DISCOUNT)
    assert asking["question"]["applied"] is True
    assert asking["question"]["ratio"] == pytest.approx(0.8)


def test_question_discount_turns_a_marginal_score_into_speaking():
    """这条是缺陷的用户可见后果：差一点点的那次插话，因为「有人在问」而说出口。"""

    decision = build(threshold=0.55)
    score = 0.50  # 卡在 0.55 与 0.55-0.08=0.47 之间

    plain = evaluate(decision, score, ratio=0.0)
    assert plain["action"] == ACTION_WAIT, "没有提问时只到 wait"
    assert plain["reason"] == "near_threshold"

    asking = evaluate(decision, score, ratio=0.5)
    assert asking["action"] == ACTION_SPEAK
    assert asking["reason"] == REASON_QUESTION, "理由要能区分「因为有人问」与「分够高」"


def test_non_question_window_is_bit_for_bit_unchanged():
    """非提问窗口必须逐位不变 —— 这是「不动权重模型」这个选择的全部意义。"""

    decision = build(threshold=0.55)
    for ratio in (0.0, 0.1, 0.2, 0.33):
        out = evaluate(decision, 0.50, ratio=ratio)
        assert out["threshold"] == pytest.approx(0.55), f"ratio={ratio} 不该触发折让"
        assert out["question"]["applied"] is False
        assert out["action"] == ACTION_WAIT


def test_missing_features_do_not_change_anything():
    """调用方没给 features（旧调用方、smoke 的手工喂分）时必须退回原语义。"""

    decision = build(threshold=0.55)
    out = evaluate(decision, 0.50, ratio=None)
    assert out["threshold"] == pytest.approx(0.55)
    assert out["question"]["applied"] is False
    assert out["action"] == ACTION_WAIT


def test_ratio_min_is_a_real_gate():
    """门槛两侧必须给出相反结论（0.33 vs 0.35）。"""

    decision = build(threshold=0.55)
    below = evaluate(decision, 0.50, ratio=DEFAULT_QUESTION_RATIO_MIN - 0.01)
    at = evaluate(decision, 0.50, ratio=DEFAULT_QUESTION_RATIO_MIN + 0.01)
    assert below["question"]["applied"] is False
    assert at["question"]["applied"] is True


# --------------------------------------------------------------------------
# 边界与优先级
# --------------------------------------------------------------------------


def test_mention_takes_precedence_over_questions():
    """被点名信号更强，两者**互斥**：叠加会把门槛压穿。"""

    decision = build(threshold=0.55)
    out = evaluate(decision, 0.90, ratio=1.0, components={"mentioned": 1.0})
    assert out["action"] == ACTION_SPEAK
    assert out["reason"] == REASON_MENTIONED
    assert out["question"]["applied"] is False, "被点名时不叠加提问折让"
    # 只吃了点名折让（0.15），没有多吃提问折让（0.08）
    assert out["threshold"] == pytest.approx(0.40)


def test_discount_never_pushes_the_threshold_below_the_floor():
    """折让要有地板，否则低阈值部署会变成「几乎必说」。"""

    decision = build(threshold=0.16)
    out = evaluate(decision, 0.0, ratio=1.0)
    assert out["threshold"] == pytest.approx(QUESTION_FLOOR)
    assert out["threshold"] >= QUESTION_FLOOR


def test_discount_can_be_switched_off():
    """``question_discount = 0`` 是给「不想改行为」的部署留的逃生口。"""

    decision = build(threshold=0.55, question_discount=0.0)
    out = evaluate(decision, 0.50, ratio=1.0)
    assert out["threshold"] == pytest.approx(0.55)
    assert out["question"]["applied"] is False
    assert out["action"] == ACTION_WAIT


def test_ratio_min_can_be_switched_off():
    """``question_ratio_min = 0`` 同样表示「不要这个折让」（而不是「永远折让」）。"""

    decision = build(threshold=0.55, question_ratio_min=0.0)
    out = evaluate(decision, 0.50, ratio=1.0)
    assert out["question"]["applied"] is False
    assert out["threshold"] == pytest.approx(0.55)


def test_ratio_is_read_from_config():
    """配置要真的被读到（容错读取会把「配错了」和「没配」变成同一件事）。"""

    decision = build(config={"perception.interrupt.question_ratio_min": 0.9})
    assert decision.question_ratio_min == pytest.approx(0.9)


def test_ratio_is_clamped_and_junk_is_ignored():
    """负占比夹到 0；非数值不抛异常（线上配置脏不该把决策打崩）。"""

    decision = build(threshold=0.55)
    assert evaluate(decision, 0.5, ratio=-1.0)["question"]["applied"] is False
    out = decision.evaluate(
        score=0.5,
        band="",
        components={},
        behavior="smalltalk",
        cooldown={"allowed": True},
        features={"question_ratio": "not-a-number"},
    )
    assert out["question"]["applied"] is False


def test_high_ratio_can_still_be_held_by_flooding():
    """提问折让不能越过安全闸门：刷屏时照样 hold。"""

    decision = build(threshold=0.55)
    out = evaluate(decision, 0.99, ratio=1.0, behavior="flooding")
    assert out["action"] == ACTION_HOLD
    assert out["reason"] == "flooding"


# --------------------------------------------------------------------------
# 端到端：走 rpc:interrupt.score → rpc:interrupt.decide
# --------------------------------------------------------------------------


async def test_score_pipeline_lets_questions_through():
    """真实链路：``rpc:interrupt.score`` 把编码器的 question_ratio 一路带到决策。"""

    from grouppig.infra.runtime.registry import Registry
    from grouppig.perception.interrupt.decision import register as register_decision
    from grouppig.perception.interrupt.scorer import build_scorer

    registry = Registry()
    scorer = build_scorer(config=None, window=None)
    scorer.register(registry)
    register_decision(registry, InterruptDecision(config=None, threshold=0.55, trigger_flow=False, publish=False))

    features = {"question_ratio": 0.8, "topic_focus": 0.0, "keywords": []}
    result = await registry.acall(
        "rpc:interrupt.score",
        GROUP,
        features=features,
        messages=[{"text": "这怎么弄？", "ts": T0, "sender_id": 2001}],
        # 钉住节奏分量，让总分落在「折让窗口」里：0.2857*0.5 + 0.3571*1.0 ≈ 0.50，
        # 高于 0.55-0.08=0.47、低于 0.55。不钉的话 silence 由窗口自己算，结论不稳。
        rhythm={"label": "silent"},
        behavior="smalltalk",
        now=T0,
        decide=True,
        self_id=999,
    )
    decision = result.get("decision") or {}
    assert decision, "决策必须被执行"
    assert decision["question"]["applied"] is True, "question_ratio 必须一路传到决策器"
    assert decision["threshold"] < 0.55
    assert decision["reason"] == REASON_QUESTION
    assert decision["action"] == ACTION_SPEAK


def test_defaults_are_sane():
    """默认值的合理性：折让小于点名折让（提问比被点名弱），且地板高于 0。"""

    assert 0 < DEFAULT_QUESTION_RATIO_MIN < 1
    assert 0 < DEFAULT_QUESTION_DISCOUNT < 0.15
    assert QUESTION_FLOOR > 0
