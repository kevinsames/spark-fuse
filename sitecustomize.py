"""
Ensure the project `src` directory is available on ``sys.path``.

Python automatically imports ``sitecustomize`` when present on the import path.
This allows running the repository in-place (for tests or notebooks) without
installing the package first.
"""

from __future__ import annotations

import os
import sys


_SRC_PATH = os.path.join(os.path.dirname(__file__), "src")

if os.path.isdir(_SRC_PATH) and _SRC_PATH not in sys.path:
    sys.path.insert(0, _SRC_PATH)
