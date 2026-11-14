# Observability and alerts

Everything below is **declared in the bundle** for every environment. Enforcement tests fail if a change removes
any of it.

## Data quality monitoring (DQM)

`data_quality_monitors` with `object_type: schema` on every source schema, bronze and silver. This is Unity
Catalog **anomaly detection**:

- **Freshness**: is the table being updated on its learned cadence?
- **Completeness**: is the daily row volume within the learned bounds?

Unhealthy tables notify `dqm.notify` (emails / destination ids) natively. This needs CLI ≥ 1.24 and the bundle
`engine: direct`. An optional extra SQL alert over `system.data_quality_monitoring.table_results` is enabled with
`alerts.dqm_system_table_alert: true`; it needs an account-admin grant.

New tables report `Training` (`RECENTLY_CREATED`) until DQM has learned their commit history. That is expected
for the first days after onboarding. How DQM, contract expectations and DQX fit together:
[Data quality](../concepts/data-quality.md).

## In-pipeline data quality

Contract rules become SDP expectations on the silver source view:

- `warn` records the violation and keeps the row;
- `fail` stops the update.

Pass/fail counts per expectation are in each pipeline's event log (`ingest_ops.event_log_<source>_<group>`).

## Pipeline notifications

Every pipeline emails `alerts.notify.emails` on `on-update-failure`, `on-update-fatal-failure` and
`on-flow-failure`. Triggered groups' jobs also notify on failure and when a run exceeds the profile's
`max_run_duration_seconds` (job health rule `RUN_DURATION_SECONDS`).

## Health alerts (Databricks SQL alerts)

| Alert | Fires when | Source |
|---|---|---|
| `expectation_failures` | any contract expectation failed rows in the lookback window | `ingest_ops.v_event_log` |
| `pipeline_health` | latest update FAILED/CANCELED; a continuous pipeline not RUNNING; a triggered pipeline with no completed update within its cadence's `max_gap_minutes` | event logs + registry |
| `undeclared_objects` | a table exists in a framework-owned schema that no config declares | `information_schema` vs registry |
| `tag_drift` | a declared table is missing its `ingest_pipeline` tag, or the tag points elsewhere | `information_schema.table_tags` vs registry |
| `dqm_unhealthy_tables` (optional) | latest anomaly-detection result unhealthy | `system.data_quality_monitoring` |

The alerts run on `alerts.warehouse_name` with `alerts.schedule`, notify `alerts.notify`, and also notify when
the condition clears.

## Where to look

| Question | Look at |
|---|---|
| What is declared, and where does it come from? | `ingest_ops.table_registry`, `make trace` |
| Is a pipeline healthy? | pipeline UI, `ingest_ops.v_event_log` (`event_type = 'update_progress'`) |
| Which rows broke a contract? | `v_event_log`, `details:flow_progress:data_quality` |
| What appeared that nobody declared? | `ingest_ops.undeclared_objects` |
| Did the source add a column? | non-null `_rescued_data` in bronze |
