"""grouppig.panel —— 管理面板域。

三层结构：

* :mod:`grouppig.panel.snapshot` —— 把运行时状态收敛成一份只读 JSON 快照；
* :mod:`grouppig.panel.web` —— 标准库 ``http.server`` 的只读 Web 面板；
* :mod:`grouppig.panel.tui` —— ``curses`` 终端面板。

两个前端都只依赖快照，互不引用；写操作与鉴权不在本域范围内。
"""

from __future__ import annotations

from grouppig.panel.snapshot import SnapshotOptions, build_snapshot

__all__ = ["SnapshotOptions", "build_snapshot"]
