from __future__ import annotations

from typing import Any, Optional, Sequence

__all__ = ["as_seq", "as_bool"]


def as_seq(val: Any) -> Optional[Sequence[str]]:
    """Parse a value into a sequence of strings.

    - None -> None
    - "a,b,c" -> ["a","b","c"] (trimmed, empty removed)
    - Iterable -> list(val)
    - Other -> [str(val)]
    """
    if val is None:
        return None
    if isinstance(val, str):
        return [s.strip() for s in val.split(",") if s.strip()]
    try:
        return list(val)  # type: ignore[arg-type]
    except Exception:
        return [str(val)]


def as_bool(val: Any, default: bool) -> bool:
    """Parse a value into a boolean with a default for None.

    Accepts common string forms like yes/no, true/false, 1/0, on/off.
    """
    if val is None:
        return default
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        v = val.strip().lower()
        if v in {"1", "true", "yes", "y", "on"}:
            return True
        if v in {"0", "false", "no", "n", "off"}:
            return False
    return bool(val)
