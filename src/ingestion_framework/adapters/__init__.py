"""Source adapter registry. Importing this package registers the built-in adapters."""

from __future__ import annotations

from ingestion_framework.adapters.base import Dataset, RuntimeOptions, SourceAdapter
from ingestion_framework.config import ConfigError

_REGISTRY: dict[str, SourceAdapter] = {}


def register_adapter(adapter: SourceAdapter) -> SourceAdapter:
    if adapter.type in _REGISTRY:
        raise ValueError(f"adapter type '{adapter.type}' registered twice")
    _REGISTRY[adapter.type] = adapter
    return adapter


def unregister_adapter(type_: str) -> None:
    _REGISTRY.pop(type_, None)


def get_adapter(type_: str) -> SourceAdapter:
    try:
        return _REGISTRY[type_]
    except KeyError as exc:
        raise ConfigError(f"unknown source type '{type_}' (known: {', '.join(sorted(_REGISTRY))})") from exc


def known_types() -> list[str]:
    return sorted(_REGISTRY)


from ingestion_framework.adapters import qlik_cdc_files as _qlik  # noqa: E402  (registers itself)

__all__ = [
    "Dataset",
    "RuntimeOptions",
    "SourceAdapter",
    "get_adapter",
    "known_types",
    "register_adapter",
    "unregister_adapter",
]
del _qlik
