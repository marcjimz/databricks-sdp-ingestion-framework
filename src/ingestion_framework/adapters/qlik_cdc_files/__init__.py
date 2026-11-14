"""Adapter for Qlik Replicate full-load + change (``__ct``) files in cloud storage."""

from __future__ import annotations

from typing import Any

from ingestion_framework.adapters.base import (
    ENTRYPOINT,
    WORKSPACE_FILE_PATH,
    Dataset,
    RuntimeOptions,
    SourceAdapter,
)
from ingestion_framework.config import GroupConfig, ResolvedConfig


class QlikCdcFilesAdapter(SourceAdapter):
    type = "qlik_cdc_files"

    def datasets(self, cfg: ResolvedConfig, group: GroupConfig) -> list[Dataset]:
        out: list[Dataset] = []
        for t in group.tables:
            out.append(Dataset("bronze", cfg.table_fqn("bronze", t.source, t.name), t))
            if t.silver:
                out.append(Dataset("silver", cfg.table_fqn("silver", t.source, t.name), t))
        return out

    def pipeline_body(self, cfg: ResolvedConfig, group: GroupConfig) -> dict[str, Any]:
        lookback = cfg.cadence_for(group.source, group.group).ct_lookback_days
        body: dict[str, Any] = {
            # Default catalog/schema for anything unqualified; every table is fully qualified.
            "catalog": cfg.catalog("bronze"),
            "schema": cfg.ops_schema,
            "root_path": f"{WORKSPACE_FILE_PATH}/src",
            "libraries": [{"file": {"path": f"{WORKSPACE_FILE_PATH}/{ENTRYPOINT}"}}],
            "configuration": {
                "framework.root": WORKSPACE_FILE_PATH,
                "framework.env": cfg.env.name,
                "framework.source": group.source,
                "framework.group": group.group,
                "framework.ct_lookback_days": str(lookback or ""),
                # Reload cutoffs and Qlik change sequences are UTC; date_format() follows the session zone.
                "spark.sql.session.timeZone": "UTC",
            },
        }
        if cfg.env.pipelines.get("serverless", True):
            body["environment"] = {"dependencies": ["pyyaml>=6.0"]}
        return body

    def trigger_url(self, cfg: ResolvedConfig, group: GroupConfig) -> str | None:
        return cfg.group_landing(group) + "/"

    def define(self, dp: Any, spark: Any, cfg: ResolvedConfig, group: GroupConfig, options: RuntimeOptions) -> None:
        from ingestion_framework.adapters.qlik_cdc_files.runtime import define_group

        define_group(dp, spark, cfg, group, options)


ADAPTER = QlikCdcFilesAdapter()


def _register() -> None:
    from ingestion_framework.adapters import register_adapter

    register_adapter(ADAPTER)


_register()
