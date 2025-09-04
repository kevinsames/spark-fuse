from __future__ import annotations

from typing import Optional

from rich.console import Console
from rich.theme import Theme


_console: Optional[Console] = None


def console() -> Console:
    global _console
    if _console is None:
        _console = Console(theme=Theme({"info": "cyan", "warn": "yellow", "error": "bold red"}))
    return _console

