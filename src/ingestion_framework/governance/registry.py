"""The table registry: one row per declared table, published to <bronze>.ingest_ops.table_registry.

It is the join point for operations: alerts compare UC against it (undeclared objects, tag
drift, pipeline health), and `make trace` reads it.
"""

from __future__ import annotations

from typing import Any

from ingestion_framework.adapters import get_adapter
from ingestion_framework.config import ResolvedConfig

REGISTRY_TABLE = "table_registry"

REGISTRY_COLUMNS = (
    "table_fqn",
    "catalog_name",
    "schema_name",
    "table_name",
    "layer",
    "source",
    "group_name",
    "source_table",
    "pipeline_name",
    "cadence",
    "cadence_mode",
    "max_gap_minutes",
    "config_path",
    "contract_path",
    "contract_id",
    "contract_version",
    "adapter",
    "code_ref",
    "classification",
    "scd_type",
    "git_commit",
)


def registry_rows(cfg: ResolvedConfig, *, git_commit: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for group in cfg.groups_in_scope():
        src = cfg.source(group.source)
        adapter = get_adapter(src.type)
        cadence = cfg.cadence_for(group.source, group.group)
        for ds in adapter.datasets(cfg, group):
            catalog, schema, table = ds.fqn.split(".")
            t = ds.table
            rows.append(
                {
                    "table_fqn": ds.fqn,
                    "catalog_name": catalog,
                    "schema_name": schema,
                    "table_name": table,
                    "layer": ds.layer,
                    "source": t.source,
                    "group_name": t.group,
                    "source_table": t.table,
                    "pipeline_name": cfg.pipeline_name(t.source, t.group),
                    "cadence": cadence.name,
                    "cadence_mode": cadence.mode,
                    "max_gap_minutes": cadence.max_gap_minutes,
                    "config_path": cfg.rel(t.config_path),
                    "contract_path": cfg.rel(t.contract_path),
                    "contract_id": t.contract.id,
                    "contract_version": t.contract.version,
                    "adapter": src.type,
                    "code_ref": adapter.code_reference(),
                    "classification": t.classification,
                    "scd_type": t.scd_type if ds.layer == "silver" else None,
                    "git_commit": git_commit,
                }
            )
    return rows


def managed_schemas(cfg: ResolvedConfig) -> list[tuple[str, str]]:
    """(catalog, schema) pairs whose contents the framework owns exclusively."""
    pairs = []
    for src in cfg.sources_in_scope():
        for layer in ("bronze", "silver"):
            pairs.append((cfg.catalog(layer), cfg.schema(src.source)))
    return pairs
