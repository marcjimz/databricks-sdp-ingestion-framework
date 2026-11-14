"""SQL for the declared health alerts (Databricks SQL alerts v2). Each query returns one number;
the alert fires when it is greater than zero."""

from __future__ import annotations

from dataclasses import dataclass

from ingestion_framework.config import ResolvedConfig
from ingestion_framework.governance.registry import REGISTRY_TABLE, managed_schemas

EVENT_LOG_VIEW = "v_event_log"


@dataclass(frozen=True)
class AlertSpec:
    name: str
    title: str
    column: str
    query: str
    description: str


def _in_list(values: list[str]) -> str:
    return ", ".join(f"'{v}'" for v in sorted(set(values)))


def build_alerts(cfg: ResolvedConfig) -> list[AlertSpec]:
    ops = cfg.ops_schema_fqn
    registry = f"{ops}.{REGISTRY_TABLE}"
    events = f"{ops}.{EVENT_LOG_VIEW}"
    lookback = int(cfg.env.alerts.get("lookback_minutes", 60))
    schemas = managed_schemas(cfg)
    catalogs = _in_list([c for c, _ in schemas])
    schema_names = _in_list([s for _, s in schemas])
    pipeline_tag = cfg.tag_key("ingest_pipeline")

    specs = [
        AlertSpec(
            name="expectation_failures",
            title="Data contract expectations failing",
            column="failed_records",
            description="Rows that violated a contract expectation in the lookback window (event log).",
            query=f"""
SELECT COALESCE(SUM(e.failed_records), 0) AS failed_records
FROM (
  SELECT explode(from_json(
           details:flow_progress:data_quality:expectations,
           'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) AS e
  FROM {events}
  WHERE event_type = 'flow_progress'
    AND timestamp >= current_timestamp() - INTERVAL {lookback} MINUTES
)""".strip(),
        ),
        AlertSpec(
            name="pipeline_health",
            title="Ingestion pipelines failed, stopped or overdue",
            column="unhealthy_pipelines",
            description=(
                "Pipelines whose latest update failed, continuous pipelines that are not running, and "
                "triggered pipelines with no completed update within their cadence's max gap."
            ),
            query=f"""
WITH expected AS (
  SELECT DISTINCT pipeline_name, cadence_mode, max_gap_minutes FROM {registry}
),
updates AS (
  SELECT origin.pipeline_name AS pipeline_name,
         details:update_progress:state::string AS state,
         timestamp
  FROM {events}
  WHERE event_type = 'update_progress'
),
latest AS (
  SELECT pipeline_name, state FROM updates
  QUALIFY row_number() OVER (PARTITION BY pipeline_name ORDER BY timestamp DESC) = 1
),
last_completed AS (
  SELECT pipeline_name, max(timestamp) AS completed_at FROM updates
  WHERE state = 'COMPLETED' GROUP BY pipeline_name
)
SELECT count(*) AS unhealthy_pipelines
FROM expected e
LEFT JOIN latest l USING (pipeline_name)
LEFT JOIN last_completed c USING (pipeline_name)
WHERE l.state IN ('FAILED', 'CANCELED')
   OR (e.cadence_mode = 'continuous' AND coalesce(l.state, '') <> 'RUNNING')
   OR (e.cadence_mode <> 'continuous'
       AND (c.completed_at IS NULL
            OR c.completed_at < current_timestamp() - make_dt_interval(0, 0, e.max_gap_minutes, 0)))""".strip(),
        ),
        AlertSpec(
            name="undeclared_objects",
            title="Undeclared tables in framework-owned schemas",
            column="undeclared_tables",
            description="Tables in bronze/silver source schemas that no configuration declares.",
            # Skips SDP-internal backing tables (__materialization_*). Uses left(...) on purpose: a
            # `NOT startswith(...)` predicate pushed down into information_schema silently matches nothing.
            query=f"""
SELECT count(*) AS undeclared_tables
FROM system.information_schema.tables t
WHERE t.table_catalog IN ({catalogs})
  AND t.table_schema IN ({schema_names})
  AND left(t.table_name, 2) <> '__'
  AND NOT EXISTS (
    SELECT 1 FROM {registry} r
    WHERE r.table_fqn = concat_ws('.', t.table_catalog, t.table_schema, t.table_name)
  )""".strip(),
        ),
        AlertSpec(
            name="tag_drift",
            title="Declared tables missing their ingest_pipeline tag",
            column="untagged_tables",
            description="Existing declared tables whose pipeline tag is missing or points elsewhere.",
            query=f"""
SELECT count(*) AS untagged_tables
FROM {registry} r
JOIN system.information_schema.tables t
  ON r.table_fqn = concat_ws('.', t.table_catalog, t.table_schema, t.table_name)
WHERE NOT EXISTS (
  SELECT 1 FROM system.information_schema.table_tags tt
  WHERE concat_ws('.', tt.catalog_name, tt.schema_name, tt.table_name) = r.table_fqn
    AND tt.tag_name = '{pipeline_tag}'
    AND tt.tag_value = r.pipeline_name
)""".strip(),
        ),
    ]
    if cfg.env.alerts.get("dqm_system_table_alert", False):
        specs.append(
            AlertSpec(
                name="dqm_unhealthy_tables",
                title="Data quality monitoring: unhealthy tables",
                column="unhealthy_tables",
                description="Latest anomaly-detection result per table is unhealthy (freshness/completeness).",
                query=f"""
WITH latest AS (
  SELECT * FROM system.data_quality_monitoring.table_results
  WHERE catalog_name IN ({catalogs}) AND schema_name IN ({schema_names})
  QUALIFY row_number() OVER (PARTITION BY table_id ORDER BY event_time DESC) = 1
)
SELECT count(*) AS unhealthy_tables FROM latest
WHERE freshness.status = 'Unhealthy' OR completeness.status = 'Unhealthy'""".strip(),
            )
        )
    return specs
