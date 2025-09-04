from __future__ import annotations

from typing import Dict, Iterable, Optional, Type

from .base import Connector


_REGISTRY: Dict[str, Type[Connector]] = {}


def register_connector(connector_cls: Type[Connector]) -> Type[Connector]:
    """Class decorator to register a connector by its `name` attribute."""
    name = getattr(connector_cls, "name", None)
    if not name:
        raise ValueError("Connector class must define a non-empty 'name' attribute")
    _REGISTRY[name] = connector_cls
    return connector_cls


def get_connector(name: str) -> Optional[Type[Connector]]:
    """Return a registered connector class by name, if present."""
    return _REGISTRY.get(name)


def list_connectors() -> Iterable[str]:
    """List available connector names in sorted order."""
    return sorted(_REGISTRY.keys())


def connector_for_path(path: str) -> Optional[Connector]:
    """Return the first connector instance that `validate_path` for the given path.

    Iterates through registered connectors and instantiates each to check path support.
    Returns `None` if no connector validates the path.
    """
    for cls in _REGISTRY.values():
        inst = cls()
        try:
            if inst.validate_path(path):
                return inst
        except Exception:
            continue
    return None
