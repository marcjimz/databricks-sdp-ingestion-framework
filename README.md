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
| a **table**: `<table>.yml` (a few lines) + `<table>.odcs.yaml` ([ODCS v3.2.0](https://bitol-io.github.io/open-data-contract-standard/) data contract) | bronze history table, silver table (AUTO CDC, SCD1/SCD2), contract expectations |
| a **group** of tables (`_group.yml`) | one pipeline, its event log, failure notifications, cadence job, traceability tags |
| an **environment** (`conf/environments/<env>.yml`) | catalogs/schemas, cadence (prod always-on, dev/tst weekly sample), DQM monitors, health alerts |

Everything in the bronze and silver schemas is declared here. Anything else that appears there is reported as an
**undeclared object**.

---

## Contents

- [How it works](#how-it-works)
- [Prerequisites (required first step)](#prerequisites-required-first-step)
- [Quick start (your own workspace)](#quick-start-your-own-workspace)
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
- **Governed by default.** Bundle-declared data quality monitoring (anomaly detection on every source schema),
  pipeline event logs, failure notifications, and four declared health alerts: expectation failures, pipeline
  health, undeclared objects and tag drift.

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

## Quick start (your own workspace)

The repo is self-contained: nothing outside it is assumed beyond the prerequisites above. The `sandbox`
environment runs the whole framework end to end on **synthetic Qlik Replicate output**: the same folders, header
columns and `.dfm` files, generated from the table contracts. No Qlik installation or real data is needed, and
`infra/` builds a dedicated workspace and storage for it.

```bash
# 1. local tooling
make install                    # uv venv + dev dependencies
make check                      # lint (every table has a valid contract) + typecheck + tests

# 2. a dedicated sandbox: workspace + ADLS landing + identity (Terraform; writes sandbox.local.yml)
cp infra/azure-sandbox/terraform.tfvars.example infra/azure-sandbox/terraform.tfvars   # fill in
make infra-sandbox

# 3. prerequisites: apply as the workspace admin (your az login), verify as the deployer
make prereqs   sandbox PROFILE=azure-cli
make preflight sandbox PROFILE=<deployer profile>

# 4. one command: preflight -> validate -> deploy -> governance -> start
make deploy sandbox PROFILE=<deployer profile>

# 5. feed it synthetic Qlik files and check silver against the expected state
make seed   sandbox PROFILE=<deployer profile>                 # full load
make seed   sandbox PROFILE=<deployer profile> BATCH=changes   # I/U/D + before images (repeat freely)
make verify sandbox PROFILE=<deployer profile>                 # silver == expected, row for row
```

Walkthrough, including AWS and shared-workspace notes: [docs/guides/getting-started.md](docs/guides/getting-started.md).
Infrastructure details: [infra/README.md](infra/README.md).

## Day-to-day

```bash
# onboard tables: scaffold config + contract pairs from a data dictionary or Qlik .dfm files
make scaffold SOURCE=facets GROUP=claims FROM=csv INPUT=exports/claims_dictionary.csv OWNER=claims-data

# where does this table come from?  table -> pipeline -> config -> code
make trace prod TABLE=cmc_clcl_claim PROFILE=<p>

# after a Qlik table reload: rebuild that table's silver from bronze (bronze is never reset)
make reload prod TABLE=cmc_clcl_claim SINCE=20261113100000 PROFILE=<p>
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
  groups/<group>/        one pipeline: _group.yml + <table>.yml / <table>.odcs.yaml pairs
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
| **Concepts** | [Architecture](docs/concepts/architecture.md) · [Data contracts](docs/concepts/data-contracts.md) · [Traceability & tags](docs/concepts/traceability-and-tags.md) |
| **Guides** | [Getting started (sandbox)](docs/guides/getting-started.md) · [Onboarding tables](docs/guides/onboarding-tables.md) · [Adding a source (Lakeflow Connect)](docs/guides/adding-a-source.md) |
| **Operations** | [Prerequisites](docs/operations/prerequisites.md) · [Environments & cadence](docs/operations/environments-and-cadence.md) · [Observability & alerts](docs/operations/observability-and-alerts.md) · [Runbook](docs/operations/runbook.md) · [Silver migration & cutover](docs/operations/migration-cutover.md) · [CI example](docs/operations/ci-example.md) |
| **Deck** | [Overview slides source](docs/deck/slides.json) · `make deck` renders `docs/deck/ingestion-framework-overview.pptx` |

Inspired by the patterns of [databricks-solutions/lakeflow_framework](https://github.com/databricks-solutions/lakeflow_framework):
group filters, defaults with per-environment overrides, and templated specs.
