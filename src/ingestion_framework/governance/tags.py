"""Traceability tags: table -> pipeline (table tags), pipeline -> config + code (pipeline tags)."""

from __future__ import annotations

from ingestion_framework.adapters import Dataset, SourceAdapter, get_adapter
from ingestion_framework.config import GroupConfig, ResolvedConfig


def table_tags(cfg: ResolvedConfig, dataset: Dataset) -> dict[str, str]:
    t = dataset.table
    return {
        cfg.tag_key("ingest_pipeline"): cfg.pipeline_name(t.source, t.group),
        cfg.tag_key("ingest_source"): t.source,
        cfg.tag_key("ingest_layer"): dataset.layer,
    }


def pipeline_tags(
    cfg: ResolvedConfig, group: GroupConfig, adapter: SourceAdapter, *, git_commit: str
) -> dict[str, str]:
    return {
        cfg.tag_key("ingest_pipeline"): cfg.pipeline_name(group.source, group.group),
        cfg.tag_key("ingest_source"): group.source,
        cfg.tag_key("ingest_config"): cfg.rel(group.path) + "/",
        cfg.tag_key("ingest_code"): adapter.code_reference(),
        cfg.tag_key("ingest_git_commit"): git_commit,
    }


def governed_tag_values(cfg: ResolvedConfig) -> dict[str, list[str]]:
    """Allowed values each governed tag policy needs for this environment (for `make prereqs`)."""
    governed = set(cfg.framework.tags.get("governed") or [])
    values: dict[str, set[str]] = {cfg.tag_key(k): set() for k in governed}
    for group in cfg.groups_in_scope():
        adapter = get_adapter(cfg.source(group.source).type)
        for ds in adapter.datasets(cfg, group):
            for key, value in table_tags(cfg, ds).items():
                if key in values:
                    values[key].add(value)
    return {k: sorted(v) for k, v in sorted(values.items())}
