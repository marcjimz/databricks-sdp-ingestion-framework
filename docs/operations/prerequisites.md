# Prerequisites

Prerequisites are a **required incoming step**. They are declared once in
[`conf/prerequisites.yml`](../../conf/prerequisites.yml) with an owner, a severity and a fix. Values such as
catalog names, landing paths and the warehouse always come from `conf/environments/<env>.yml`.

| Command | Profile | Effect |
|---|---|---|
| `make prereqs <env> [DRY_RUN=1]` | an **admin** (catalog / external location owner, tag admin) | applies the automatable prerequisites, idempotently, then prints the manual ones |
| `make preflight <env>` | the **deployer** | verifies every prerequisite, read-only; exits 1 on a required failure |
| `make deploy <env>` | the deployer | runs preflight in strict mode first; **any failing required check stops the deploy** |
| `make validate`, `start`, `governance` | the deployer | print the same report in warn mode and continue |

A test (`tests/enforcement/test_repo_rules.py`) fails if a declared prerequisite has no implementation, or an
implementation is not declared.

## The checks

| Check | Required? | Automated by `make prereqs` |
|---|---|---|
| `local_tools`: CLI ≥ 1.24, Python ≥ 3.11, uv | required | — |
| `env_placeholders`: no `<…>` left in the env config | required | — |
| `workspace_host`: PROFILE points at `workspace_host` | required (when pinned) | — |
| `deployer_identity`: deploying identity == `deployer.service_principal` | required (sandbox: recommended) | — |
| `catalogs`: bronze + silver exist | required | sandbox (`storage.create`) |
| `catalog_privileges`: `USE CATALOG` + `CREATE SCHEMA` | required | yes |
| `landing_external_location`: landing roots covered | required | sandbox |
| `file_events`: file events **on** for that location | required | sandbox (new locations) |
| `landing_privileges`: `READ FILES` (+ `WRITE FILES` if seeding) | required | yes |
| `warehouse`: alerts warehouse exists, `CAN USE` | required | yes (permission) |
| `tag_policies`: governed tag policies include every value | required (sandbox: recommended) | yes |
| `alert_recipients` | recommended | — |
| `notification_destinations`: configured ids exist | required (if any) | — |
| `system_tables`: read `system.data_quality_monitoring` | required (if that alert is on) | — |
| `connections`: UC connections for Lakeflow Connect sources | required (if any) | — |
| `nonprod_data_attestation`: data handling approved for non-prod | recommended (not prod) | — |

## Manual steps `make prereqs` cannot do

- **Deployer service principal**: create it, generate an OAuth secret, and add a CLI profile, e.g.
  `[sp-ingest-dev]` with `host`, `client_id` and `client_secret`.
- **Tag ASSIGN permission** for the deployer on the governed tag policies.
- **File events on an existing external location**: Catalog > External locations > Edit > Advanced > Enable file
  events (Automatic), then Test connection. The cloud identity needs queue permissions: Event Grid / Storage
  Queues on Azure, SNS/SQS on AWS.
- **Qlik Replicate endpoint settings**: Parquet output, *Store Changes* on, metadata (`.dfm`) files on, and one
  task per group folder.
- **Data handling for non-prod**: if dev/tst read real source data, confirm it is masked or approved, then set
  `governance.nonprod_data_attested: true`.
