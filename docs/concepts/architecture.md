# Architecture

## Goals

1. **Declared, not built by hand.** Every object in the framework-owned bronze and silver schemas comes from
   configuration in this repo. Undeclared objects are detected and alerted on.
2. **Configuration-driven at scale.** The first source has ~2,700 tables that all follow one replication pattern.
   Onboarding a table is a scaffolded config change, never new pipeline code.
3. **Operational layers only.** Bronze holds the history of record; silver holds the current or historical
   operational state. Business-facing gold is out of scope.
4. **Event-driven ingestion.** Files are discovered through managed file events, never directory listing.
5. **Multi-source.** New source types plug in as adapters and inherit all governance.

## Building blocks

| Concept | Where | Becomes |
|---|---|---|
| Framework defaults | `conf/framework.yml` | naming, cadence profiles, limits, tag keys, expectation policy |
| Environment | `conf/environments/<env>.yml` (+ gitignored `<env>.local.yml`) | catalogs, landing roots, cadence, scope, recipients, protections |
| Prerequisites | `conf/prerequisites.yml` | the preflight gate in front of every deploy |
| Source | `sources/<source>/source.yml` | an adapter (`type`) + source conventions |
| Group | `sources/<source>/groups/<group>/group.yml` | **one pipeline** + its cadence job, event log, tags |
| Table | `<group>/tables/<table>/table.yml` + `contract.odcs.yaml` | bronze + silver tables, expectations, registry row, tags |

Resource generation is pure Python ([`bundle/generate.py`](../../src/ingestion_framework/bundle/generate.py)),
so the whole deployment is unit-tested. [`resources/__init__.py`](../../resources/__init__.py) hands the result
to the bundle at deploy time.

## Per environment

```
catalog bronze_<env>
  schema <source>           bronze tables (framework-owned; DQM anomaly detection)
  schema ingest_ops         event_log_<source>_<group>, table_registry, v_event_log,
                            undeclared_objects, reload_cutoffs (and seed_* in sandbox)
catalog silver_<env>
  schema <source>           silver tables (framework-owned; DQM anomaly detection)

pipelines   ingest-<source>-<group>-<env>          one per group (<= 150 source tables)
jobs        ingest-<source>-<group>-<env>-trigger  per triggered group (file arrival / cron / periodic)
            ingest-governance-<env>                registry + tags + undeclared audit (hourly + every deploy)
            ingest-seed-<env>                      sandbox only
alerts      [ingest][<env>] ...                    expectation failures, pipeline health, undeclared, tag drift
monitors    data_quality_monitors on every source schema (freshness + completeness)
```

## The Qlik adapter (`qlik_cdc_files`)

**Bronze**: one streaming table per source table, fed by two append flows.

| Flow | Reads | Adds |
|---|---|---|
| `<t>__full_load` | `<group>/<TABLE>/` | typed-null `header__*`, op `L` |
| `<t>__changes` | `<group>/<TABLE>__ct/` | Qlik headers as delivered |

Both flows add `_load_type`, `_cdc_seq`, `_ingest_file`, `_file_modification_time`, `_ingest_ts` and
`_rescued_data`.

The reader schema is **the contract's columns** (plus the Qlik headers for change files):

- bronze is declared by the contract, with no schema inference;
- unexpected new source columns land in `_rescued_data` instead of evolving the table silently. Update the
  contract (re-scaffold) to promote them.

**Ordering**: `_cdc_seq` is a sortable string.

- Full-load rows are rank `0` plus the file time, so a later Qlik reload outranks an earlier snapshot.
- Change rows are rank `1` plus `header__change_seq`.

So the initial snapshot plus Qlik's cached changes apply correctly, and replays are idempotent.

**Silver**: built with `create_auto_cdc_flow` from a view over bronze.

- The view drops `B` before-images.
- Columns are cast to contract types.
- The view carries the contract's row-level expectations.
- Keys come from the contract's primary key, deletes are `op = 'D'`, and the table is SCD1 by default or SCD2
  per table.
- Expectations are `warn` or `fail` only. `drop` is rejected by lint, because dropping a CDC delete leaves a
  zombie row.

**Reload**: `make reload` records a cutoff and full-refreshes **only silver**. The silver view then ignores
snapshots and changes older than the reload. Bronze, the system of record, is never reset, and prod blocks
bronze resets with `pipelines.reset.allowed=false`.

- **Same-name snapshots.** The full-load flow sets `cloudFiles.allowOverwrites`, so a snapshot rewritten under the
  same name is read again.
- **Visible cutoff.** The silver table records the cutoff it was built from in the property
  `ingest.reload_cutoff`.
- **Loud failure.** If the cutoff table exists but cannot be read, the update fails rather than building silver
  without it.
- **UTC.** Pipelines run in UTC, so cutoffs compare with file times and Qlik sequences in one time zone.

## Scaling to thousands of tables

- SDP allows 1,000 tables per pipeline. Each source table is two (bronze + silver), so groups are capped at 150
  source tables. Lint enforces the cap.
- One group should equal one Qlik task writing to its own landing folder. File-arrival triggers then fire only the
  affected pipeline.
- Group membership is effectively permanent. Moving a table to another group recreates it, so treat it like a
  migration.
- Environment `scope` limits what dev/tst onboard, e.g. two groups instead of all of them.

## Source adapters

An adapter declares what is specific to a kind of source. See
[`adapters/base.py`](../../src/ingestion_framework/adapters/base.py) and
[Adding a source](../guides/adding-a-source.md).

| Method | Purpose |
|---|---|
| `datasets` | tables a group produces (registry, tags, audits) |
| `pipeline_body` | libraries/configuration, or `ingestion_definition` for managed connectors |
| `trigger_url` | what a file-arrival cadence watches |
| `define` | the SDP graph, for adapters that run framework code |

Naming, tags, event log, notifications, cadence jobs, schemas, DQM, alerts, permissions and protections are
applied *around* the adapter. A test with a dummy adapter proves that a new source type gets all of it.
