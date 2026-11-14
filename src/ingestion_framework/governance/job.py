"""The governance job (``ingest-governance-<env>``): runs after every deploy and on a schedule.

1. publishes ``<ops>.table_registry`` from configuration (what SHOULD exist and where it comes from)
2. (re)creates ``<ops>.v_event_log``, the union of every pipeline's event log, used by alerts
3. applies the traceability tags to every declared table that exists (SDP cannot tag its tables)
4. records undeclared tables in framework-owned schemas to ``<ops>.undeclared_objects``

Planning is pure (unit-tested); :func:`run` is the thin Spark side.
"""

from __future__ import annotations

from typing import Any

from ingestion_framework.adapters import get_adapter
from ingestion_framework.bundle.alerts import EVENT_LOG_VIEW
from ingestion_framework.config import ResolvedConfig
from ingestion_framework.governance.registry import (
    REGISTRY_COLUMNS,
    REGISTRY_TABLE,
    managed_schemas,
    registry_rows,
)
from ingestion_framework.governance.tags import table_tags

EVENT_COLUMNS = "id, origin, timestamp, level, event_type, message, details"
RELOAD_CUTOFFS = "reload_cutoffs"
UNDECLARED = "undeclared_objects"


def _quote_fqn(fqn: str) -> str:
    return ".".join(f"`{p}`" for p in fqn.split("."))


def _lit(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def desired_table_tags(cfg: ResolvedConfig) -> dict[str, dict[str, str]]:
    desired: dict[str, dict[str, str]] = {}
    for group in cfg.groups_in_scope():
        for ds in get_adapter(cfg.source(group.source).type).datasets(cfg, group):
            desired[ds.fqn] = table_tags(cfg, ds)
    return desired


def tag_statements(
    desired: dict[str, dict[str, str]], current: dict[str, dict[str, str]], *, existing: set[str]
) -> list[str]:
    out = []
    for fqn in sorted(desired):
        if fqn not in existing:
            continue  # pipeline has not created it yet; the next scheduled run tags it
        want = desired[fqn]
        have = current.get(fqn, {})
        if any(have.get(k) != v for k, v in want.items()):
            pairs = ", ".join(f"{_lit(k)} = {_lit(v)}" for k, v in sorted(want.items()))
            out.append(f"ALTER TABLE {_quote_fqn(fqn)} SET TAGS ({pairs})")
    return out


def undeclared_tables(existing: set[str], declared: set[str]) -> list[str]:
    return sorted(t for t in existing - declared if not t.rsplit(".", 1)[-1].startswith("__"))


def event_log_view_sql(view_fqn: str, event_logs: list[str]) -> str:
    if not event_logs:
        return (
            f"CREATE OR REPLACE VIEW {view_fqn} AS SELECT CAST(NULL AS STRING) AS id, "
            "named_struct('pipeline_id', CAST(NULL AS STRING), 'pipeline_name', CAST(NULL AS STRING)) AS origin, "
            "CAST(NULL AS TIMESTAMP) AS timestamp, CAST(NULL AS STRING) AS level, "
            "CAST(NULL AS STRING) AS event_type, CAST(NULL AS STRING) AS message, "
            "CAST(NULL AS STRING) AS details WHERE false"
        )
    selects = "\nUNION ALL\n".join(f"SELECT {EVENT_COLUMNS} FROM {t}" for t in event_logs)
    return f"CREATE OR REPLACE VIEW {view_fqn} AS\n{selects}"


# ---- Spark side -----------------------------------------------------------------------------
def _existing_tables(spark: Any, cfg: ResolvedConfig) -> set[str]:
    found: set[str] = set()
    for catalog, schema in managed_schemas(cfg):
        rows = spark.sql(
            f"SELECT table_name FROM `{catalog}`.information_schema.tables WHERE table_schema = {_lit(schema)}"
        ).collect()
        found |= {f"{catalog}.{schema}.{r['table_name']}" for r in rows}
    return found


def _current_tags(spark: Any, cfg: ResolvedConfig) -> dict[str, dict[str, str]]:
    current: dict[str, dict[str, str]] = {}
    for catalog, schema in managed_schemas(cfg):
        rows = spark.sql(
            f"SELECT table_name, tag_name, tag_value FROM `{catalog}`.information_schema.table_tags "
            f"WHERE schema_name = {_lit(schema)}"
        ).collect()
        for r in rows:
            current.setdefault(f"{catalog}.{schema}.{r['table_name']}", {})[r["tag_name"]] = r["tag_value"]
    return current


def run(spark: Any, cfg: ResolvedConfig, *, git_commit: str) -> dict[str, Any]:
    from pyspark.sql import functions as F

    ops = cfg.ops_schema_fqn
    rows = registry_rows(cfg, git_commit=git_commit)
    existing = _existing_tables(spark, cfg)

    schema = (
        "table_fqn string, catalog_name string, schema_name string, table_name string, layer string, "
        "source string, group_name string, source_table string, pipeline_name string, cadence string, "
        "cadence_mode string, max_gap_minutes int, config_path string, contract_path string, "
        "contract_id string, contract_version string, adapter string, code_ref string, "
        "classification string, scd_type int, git_commit string, table_exists boolean"
    )
    data = [tuple([*(r[c] for c in REGISTRY_COLUMNS), r["table_fqn"] in existing]) for r in rows]
    (
        spark.createDataFrame(data, schema=schema)
        .write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{ops}.{REGISTRY_TABLE}")
    )

    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {ops}.{RELOAD_CUTOFFS} (table_fqn STRING, min_change_seq STRING, "
        "reason STRING, set_by STRING, set_at TIMESTAMP) COMMENT 'Qlik reload cutoffs (make reload)'"
    )

    logs = [cfg.event_log_fqn(g.source, g.group) for g in cfg.groups_in_scope()]
    present = [t for t in logs if spark.catalog.tableExists(t)]
    spark.sql(event_log_view_sql(f"{ops}.{EVENT_LOG_VIEW}", present))

    failures: list[str] = []
    applied = 0
    for stmt in tag_statements(desired_table_tags(cfg), _current_tags(spark, cfg), existing=existing):
        try:
            spark.sql(stmt)
            applied += 1
        except Exception as exc:  # report every table, fail the job at the end
            failures.append(f"{stmt}: {exc}")

    declared = {r["table_fqn"] for r in rows}
    stray = undeclared_tables(existing, declared)
    (
        spark.createDataFrame([(t,) for t in stray], schema="table_fqn string")
        .withColumn("detected_at", F.current_timestamp())
        .write.mode("overwrite")
        .saveAsTable(f"{ops}.{UNDECLARED}")
    )

    summary = {
        "declared_tables": len(declared),
        "existing_declared_tables": len(declared & existing),
        "event_logs": len(present),
        "tags_applied": applied,
        "tag_failures": failures,
        "undeclared_tables": stray,
    }
    return summary
