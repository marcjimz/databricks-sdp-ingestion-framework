# Data quality: expectations, DQM and DQX

Three Databricks capabilities check data quality. They answer different questions, run in different places, and
complement each other. The framework uses the first two. The third is an option for checks the first two cannot
express.

| | Contract expectations (SDP) | DQM anomaly detection | DQX (Databricks Labs) |
|---|---|---|---|
| Question it answers | "Is this **row** valid?" | "Did this **table** get the data it usually gets, on time?" | "Is this **dataset** valid?" (row rules + uniqueness, FKs, aggregates) |
| Runs | inside the pipeline, on every row | serverless, outside the pipeline, on Unity Catalog tables | a Python library, in a pipeline or as a separate job |
| Rules come from | the table's `contract.odcs.yaml` `quality[]` | learned per table from its commit history; no rules to write | code, YAML/JSON, a profiler, or generated from an ODCS contract |
| Enabled by | lint + the generic entrypoint (always on) | `dqm.enabled` → bundle `data_quality_monitors`, one per source schema | not used today |
| Results | pipeline event log → expectation-failures alert | `system.data_quality_monitoring.table_results`, Catalog Explorer health | `_errors` / `_warnings` columns, quarantine + metrics tables |
| Support | product (GA) | product | Labs project: no SLA, issues on GitHub |

## 1. Contract expectations: the in-pipeline gate

Each `quality[]` rule in a contract becomes an SDP expectation on the silver source view (see
[Data contracts](data-contracts.md)).

- **What happens to a bad row.** `warn` keeps it and records the failure. `fail` stops the update.
- **No `drop`.** Silver is maintained by AUTO CDC. Dropping a delete (`D`) row would leave a zombie row in silver, so
  lint rejects `drop`.
- **Where to look.** Metrics land in the pipeline event log (`ingest_ops.event_log_<source>_<group>`), and the
  *expectation failures* alert reads them from there.
- **What it cannot see.** Expectations evaluate rows that arrive. If Qlik stops delivering, or delivers half a
  load, there is no row to fail.

## 2. DQM anomaly detection: is the table healthy?

Data Quality Monitoring (in Unity Catalog) learns each table's normal behaviour and flags deviations. It
runs two checks:

- **Freshness.** It predicts when the next commit is due and marks the table stale when it is late.
- **Completeness.** It compares the last 24 hours of rows against a predicted range.

A null-percentage check and completeness slicing are in Beta.

**Enabling it**

- Anomaly detection is enabled per schema. From the UI you can also enable it from a catalog, for up to 50
  schemas at once, but schemas added later are **not** enabled automatically.
- The framework therefore declares one monitor per framework-owned source schema in the bundle. A new source gets
  its monitor on the next deploy.
- It needs MANAGE on the schema, the bundle resource `data_quality_monitors` (CLI ≥ 1.24, direct engine), and
  serverless compute. It is billed as serverless DBUs.

**What to expect**

- **A training period.** New tables report `Training` (error code `RECENTLY_CREATED`) until they have enough
  commit history to predict. Seen live in the sandbox on day one: every bronze and silver table was being scanned
  and every table was still `Training`.
- **Uneven scan frequency.** DQM scans high-impact tables more often, and can scan rarely updated tables up to two
  weeks apart. The weekly dev/tst cadence will therefore learn slowly. DQM matters most in prod, where tables update
  continuously.

**Where results go**

- `system.data_quality_monitoring.table_results` holds `status`, freshness, completeness, downstream impact and
  upstream-job root cause. By default only account admins can read it, so grant SELECT to the ops group.
- The Catalog Explorer health indicators and each table's *Quality* tab show the same results.
- Notifications go to `dqm.notify` (workspace users, or notification destinations such as Slack, Teams or
  webhooks), one per unhealthy table. Every environment in this repo keeps the lists empty, so DQM sends nothing
  until you add a destination.

**Data profiling** (formerly Lakehouse Monitoring) is a separate, per-table DQM feature: column profiles, drift
and custom metrics. It is not enabled here. Add it per table if a consumer needs distribution or drift tracking.

## 3. DQX: dataset-level checks, if you need them

[DQX](https://databrickslabs.github.io/dqx/) is a Databricks Labs PySpark library.

**What it adds over expectations**

- **Dataset-level checks**, such as `is_unique`, `foreign_key` and aggregate bounds.
- **A larger library of row checks.**
- **A profiler** that proposes rules from data, plus LLM-assisted rule generation.
- **A quarantine split.** `apply_checks_and_split` returns valid and invalid DataFrames.
- **ODCS input.** `DQGenerator.generate_rules_from_contract` builds checks from a contract.

**Fit with this framework**

- **Not on the CDC stream.** Quarantining a delete or update before AUTO CDC leaves silver wrong, which is the same
  problem as `drop`. If DQX runs in a pipeline, use `warn` criticality only.
- **Better as a post-hoc job on silver.** For example, a scheduled job checks that every claim's `MEME_CK` exists in
  members, and writes results to `ingest_ops`. Dataset-level checks are a natural fit there, and nothing in the
  ingestion path depends on a Labs library.
- **Check the ODCS version first.** DQX documents ODCS v3.0.x. Our contracts are v3.2.0, so test-parse one before
  relying on contract-generated rules.

## Summary

- **Contract expectations**: is each row correct?
- **DQM**: did the data arrive on time and in the usual volume?
- **DQX**: optional, for cross-row and cross-table rules, run beside the pipeline rather than inside it.
