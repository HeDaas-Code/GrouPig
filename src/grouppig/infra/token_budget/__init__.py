"""grouppig.infra.token-budget —— Token 预算管理（策略 / 计量 / 报告）。

normify id: ``grouppig.infra.token-budget``（容器模块）。

叶子：

* :mod:`grouppig.infra.token_budget.policy` —— ``rpc:token.policy``
* :mod:`grouppig.infra.token_budget.meter` —— ``rpc:token.reserve`` / ``rpc:token.consume``
* :mod:`grouppig.infra.token_budget.reporter` —— ``rpc:token.report``
"""

from __future__ import annotations

from grouppig.infra.token_budget.meter import BudgetSnapshot, Reservation, TokenMeter, get_meter, set_meter
from grouppig.infra.token_budget.policy import BudgetPolicy, estimate_messages_tokens, estimate_tokens, get_policy
from grouppig.infra.token_budget.reporter import TokenReporter, UsageEntry, get_reporter, set_reporter

__all__ = [
    "BudgetPolicy",
    "BudgetSnapshot",
    "Reservation",
    "TokenMeter",
    "TokenReporter",
    "UsageEntry",
    "estimate_messages_tokens",
    "estimate_tokens",
    "get_meter",
    "get_policy",
    "get_reporter",
    "set_meter",
    "set_reporter",
]
