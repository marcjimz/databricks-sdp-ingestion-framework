"""The source-adapter seam.

A source ``type`` (in ``sources/<source>/source.yml``) selects an adapter. An adapter knows only
what is specific to that kind of source:

* ``datasets``       which UC tables a group produces (feeds the registry, tags, audits)
* ``pipeline_body``  the adapter-specific part of the pipeline resource (libraries/configuration
                     for framework code, or ``ingestion_definition`` for a managed connector)
* ``trigger_url``    what a file-arrival cadence watches, if applicable
* ``define``         the SDP graph, for adapters that run framework code inside the pipeline

Everything cross-cutting — naming, tags, event log, notifications, cadence jobs, schemas, DQM,
alerts, permissions, prevent_destroy — is applied by :mod:`ingestion_framework.bundle.generate`
around the adapter's output, so every new source type inherits it automatically.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, ClassVar

from ingestion_framework.config import GroupConfig, ResolvedConfig, TableConfig

WORKSPACE_FILE_PATH = "${workspace.file_path}"
ENTRYPOINT = "src/pipelines/entrypoint.py"


@dataclass(frozen=True)
class Dataset:
    layer: str  # bronze | silver
    fqn: str
    table: TableConfig


@dataclass(frozen=True)
class RuntimeOptions:
    """Values only known when the pipeline graph is built."""

    ct_lookback_days: int | None = None
    now: datetime | None = None
    reload_cutoffs: Mapping[str, str] = field(default_factory=dict)  # silver fqn -> min change seq


class SourceAdapter(ABC):
    type: ClassVar[str]
    #: True when the pipeline runs this framework's generic entrypoint (vs. a managed connector).
    runs_framework_code: ClassVar[bool] = True

    @abstractmethod
    def datasets(self, cfg: ResolvedConfig, group: GroupConfig) -> list[Dataset]: ...

    @abstractmethod
    def pipeline_body(self, cfg: ResolvedConfig, group: GroupConfig) -> dict[str, Any]: ...

    def trigger_url(self, cfg: ResolvedConfig, group: GroupConfig) -> str | None:
        return None

    def code_reference(self) -> str:
        """Value of the ingest_code tag: where an operator finds the logic for this pipeline."""
        module = type(self).__module__.replace(".", "/")
        return f"{ENTRYPOINT} -> src/{module}"

    def define(self, dp: Any, spark: Any, cfg: ResolvedConfig, group: GroupConfig, options: RuntimeOptions) -> None:
        raise NotImplementedError(f"adapter '{self.type}' does not run framework code inside the pipeline")
