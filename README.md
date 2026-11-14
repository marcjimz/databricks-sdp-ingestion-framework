# SDP Ingestion Framework

![./static/img/dbx-banner.jpeg](./static/img/dbx-banner.jpeg)

A **configuration-driven, multi-source ingestion framework** for operational bronze and silver layers on
**Lakeflow Spark Declarative Pipelines (SDP)**, deployed as a **Declarative Automation Bundle** by a service
principal with one command.

The first source is a core healthcare administration platform (Facets), replicated by **Qlik Replicate** into
cloud storage. The same machinery is designed to take further sources, such as SaaS applications through
**Lakeflow Connect**.

| You declare | The framework generates and governs |
|---|---|
| a **table**: a folder with `table.yml` (a few lines) + `contract.odcs.yaml` ([ODCS v3.2.0](https://bitol-io.github.io/open-data-contract-standard/) data contract) | bronze history table, silver table (AUTO CDC, SCD1/SCD2), contract expectations |
| a **group** of tables (`group.yml`) | one pipeline, its event log, failure notifications, cadence job, traceability tags |
| an **environment** (`conf/environments/<env>.yml`) | catalogs/schemas, cadence (prod always-on, dev/tst weekly sample), data quality monitors (DQM), health alerts |

Everything in the bronze and silver schemas is declared here. Anything else that appears there is reported as an
**undeclared object**.

---

## Contents

- [How it works](#how-it-works)
- [Prerequisites (required first step)](#prerequisites-required-first-step)
- [Quick start (sandbox)](#quick-start-sandbox)
- [Your own dev / tst / prod](#your-own-dev--tst--prod)
- [Day-to-day](#day-to-day)
- [Repository layout](#repository-layout)
- [Documentation](#documentation)

## How it works

```
Qlik Replicate ──► cloud storage (file events on)                     Unity Catalog
  <group>/<TABLE>/       full load  ─┐
  <group>/<TABLE>__ct/   changes    ─┤  Auto Loader (managed file events, contract schema)
                                     ▼
                     bronze_<env>.facets.<table>   append-only history, every Qlik header kept
                                     │  drop before-images, contract expectations (warn | fail)
                                     ▼  AUTO CDC  keys = contract primary key, sequence = Qlik change seq
                     silver_<env>.facets.<table>   current state (SCD1) or history (SCD2)
```

- **One generic pipeline entrypoint.** [`src/pipelines/entrypoint.py`](src/pipelines/entrypoint.py) builds any
  group from configuration. Adding a table is a config change, never new pipeline code.
- **Event-driven only.** Auto Loader runs in managed file-events mode, never in directory listing mode, and
  file-arrival triggers fire on new Qlik files. Tests and preflight both enforce this.
- **Environments differ only in config.** Prod is `continuous`. dev/tst default to `weekly_sample`: one run a week
  that reads only the last day of change files. The baseline is read once.
- **Traceable in two hops.** Every table carries the tag `ingest_pipeline=<pipeline>`. Every pipeline carries
  `ingest_config` (its config folder), `ingest_code` (the code that builds it) and `ingest_git_commit`.
  `make trace` walks that path for you.
- **Governed by default.** The bundle declares:
  - Unity Catalog data quality monitoring (DQM: freshness and volume anomaly detection on every source schema);
  - pipeline event logs and failure notifications;
  - four health alerts: expectation failures, pipeline health, undeclared objects and tag drift.

See [docs/concepts/architecture.md](docs/concepts/architecture.md) for the full design.

## Prerequisites (required first step)

Prerequisites are **declared** in [`conf/prerequisites.yml`](conf/prerequisites.yml). Each one has an owner, a
severity and a fix, and they are **checked by the Makefile on every workspace command**. `make deploy` refuses to
continue while a *required* one is missing; `validate` and `start` print the same report as a warning.

| Prerequisite | Who | Automated by |
|---|---|---|
| Databricks CLI ≥ 1.24, Python ≥ 3.11, [uv](https://docs.astral.sh/uv/) | you | — |
| Bronze + silver catalogs | platform team | `make prereqs` (sandbox) |
| External location over the landing path, **file events enabled** | platform team | `make prereqs` (sandbox) |
| Deployer service principal + CLI profile | platform team | — |
| Deployer: `USE CATALOG`, `CREATE SCHEMA`, `READ FILES`, warehouse `CAN USE` | resource owners | `make prereqs` |
| Governed tag policies (`ingest_pipeline`, `ingest_source`, `ingest_layer`) | account admin | `make prereqs` |
| Alert/DQM recipients, notification destinations | you / workspace admin | — |

```bash
make prereqs   <env> PROFILE=<admin profile> DRY_RUN=1   # show what would be applied
make prereqs   <env> PROFILE=<admin profile>             # apply the automatable ones
make preflight <env> PROFILE=<deployer profile>          # verify everything, as the deployer
```

Details and manual steps: [docs/operations/prerequisites.md](docs/operations/prerequisites.md).

## Quick start (sandbox)

The `sandbox` environment runs the whole framework end to end on **synthetic Qlik Replicate output**: the same
folders, header columns and `.dfm` files, generated from the table contracts. No Qlik installation or real data
is needed, and `infra/` builds a dedicated Azure workspace and storage for it.

```bash
make install && make check                                   # local: lint (every table has a contract), types, tests
make infra-sandbox                                           # Terraform (after filling terraform.tfvars): workspace + ADLS landing
make prereqs   sandbox PROFILE=azure-cli                     # as the workspace admin (your az login)
make deploy    sandbox PROFILE=<deployer profile>            # preflight -> validate -> deploy -> governance -> start
make seed      sandbox PROFILE=<deployer profile>            # synthetic Qlik full load (then BATCH=changes)
make verify    sandbox PROFILE=<deployer profile>            # silver == expected state, row for row
```

The [getting-started guide](docs/guides/getting-started.md) covers each step. That includes what you need
first, creating the deployer service principal and its CLI profile, and the other scenarios proven live: a Qlik
table reload, tables with no files yet, an always-on group, and onboarding a table. Infrastructure details are in
[infra/README.md](infra/README.md).

## Your own dev / tst / prod

These environments use workspaces and storage the platform team already owns, so nothing in `infra/` applies.

1. Fill the `<...>` placeholders in `conf/environments/<env>.yml`, or in a gitignored `<env>.local.yml` next to it:
   workspace host, deployer SP, landing path, warehouse.
2. `make prereqs <env> PROFILE=<admin> DRY_RUN=1`, then without `DRY_RUN`, then do the manual steps it prints.
   That includes attesting that non-prod data is masked (`governance.nonprod_data_attested`).
3. `make deploy <env> PROFILE=<env deployer>`. Preflight refuses while any placeholder or required prerequisite is
   missing, and tells you who owns the fix.

## Day-to-day

```bash
# onboard tables: scaffold config + contract pairs from a data dictionary or Qlik .dfm files
make scaffold SOURCE=facets GROUP=claims FROM=csv INPUT=examples/scaffold/claims_dictionary.csv OWNER=claims-data

# where does this table come from?  table -> pipeline -> config -> code
make trace <env> TABLE=cmc_clcl_claim PROFILE=<p>

# after a Qlik table reload: rebuild that table's silver from bronze (bronze is never reset)
make reload <env> TABLE=cmc_clcl_claim SINCE=20261113100000 PROFILE=<p>
```

| Command | What it does |
|---|---|
| `make deploy <env>` | tools check → **preflight (strict)** → validate → deploy → governance job → start |
| `make validate <env>` | preflight (warn) → `bundle validate` |
| `make start` / `stop <env>` | start continuous pipelines + drain triggered groups / stop all |
| `make governance <env>` | publish registry, apply table tags, audit undeclared objects now |
| `make destroy <env> CONFIRM=<env>` | tear down (refused for `prod`) |

## Repository layout

```
conf/                    framework defaults, one file per environment, declared prerequisites
sources/<source>/        what is ingested
  source.yml             source type (adapter) + conventions, e.g. Qlik header columns
  groups/<group>/
    group.yml            one group = one pipeline (and one Qlik task / landing folder)
    tables/<table>/
      table.yml          ops config: usually just `table:` and `enabled:`
      contract.odcs.yaml the data contract: columns, keys, quality rules, SLA, owner
src/ingestion_framework/ the framework (config, contracts, adapters, bundle generation, governance, prereqs)
src/pipelines/           the one generic SDP entrypoint
src/jobs/                governance job, sandbox seed job
resources/               bundle resource loader (everything generated from config)
contracts/               vendored ODCS v3.2.0 JSON schema (offline lint)
examples/scaffold/       sample data dictionary + Qlik .dfm used to generate the sample tables
infra/                   Terraform for a dedicated sandbox (workspace, ADLS landing, identity)
tests/                   unit tests + enforcement tests (framework guarantees)
docs/                    concepts, guides, operations, deck
```

## Documentation

| | |
|---|---|
| **Concepts** | [Architecture](docs/concepts/architecture.md) · [Data contracts](docs/concepts/data-contracts.md) · [Data quality: expectations, DQM, DQX](docs/concepts/data-quality.md) · [Traceability & tags](docs/concepts/traceability-and-tags.md) |
| **Guides** | [Getting started (sandbox)](docs/guides/getting-started.md) · [Onboarding tables](docs/guides/onboarding-tables.md) · [Adding a source (Lakeflow Connect)](docs/guides/adding-a-source.md) |
| **Operations** | [Prerequisites](docs/operations/prerequisites.md) · [Environments & cadence](docs/operations/environments-and-cadence.md) · [Observability & alerts](docs/operations/observability-and-alerts.md) · [Runbook](docs/operations/runbook.md) · [Silver migration & cutover](docs/operations/migration-cutover.md) · [CI example](docs/operations/ci-example.md) |
| **Deck** | [Overview slides source](docs/deck/slides.json) · `make deck` renders `docs/deck/ingestion-framework-overview.pptx` |

Inspired by the patterns of [databricks-solutions/lakeflow_framework](https://github.com/databricks-solutions/lakeflow_framework):
group filters, defaults with per-environment overrides, and templated specs.
