"""`python -m grouppig.runtime` 入口。"""

from __future__ import annotations

import sys

from grouppig.runtime.app import main

if __name__ == "__main__":
    sys.exit(main())
