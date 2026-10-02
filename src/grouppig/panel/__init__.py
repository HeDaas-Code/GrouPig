"""GrouPig operational command consoles shared by Web and TUI."""

from grouppig.panel.snapshot import SnapshotOptions, build_snapshot
from grouppig.panel.viewmodel import build_dashboard_snapshot

__all__ = ["SnapshotOptions", "build_snapshot", "build_dashboard_snapshot"]
