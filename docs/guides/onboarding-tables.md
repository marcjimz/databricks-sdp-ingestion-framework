# Onboarding tables

A table is two files in its group folder. Both are generated; you only review and enrich.

```
sources/facets/groups/claims/
  _group.yml                     the pipeline (one Qlik task / landing folder)
  cmc_clcl_claim.yml             ops config — usually just `table:` and `enabled:`
  cmc_clcl_claim.odcs.yaml       data contract — columns, types, keys, quality, SLA, owner
```

## 1. Pick or create the group

Which groups deploy is set per environment by `scope.groups` in `conf/environments/<env>.yml`. The repo starts
small with two domains (`facets/membership`, `facets/claims`); `facets/provider` is declared and lint-checked but
commented out of scope. Uncomment it to deploy it.

Groups map 1:1 to pipelines and should map 1:1 to Qlik tasks, so each has its own landing folder. Keep groups at
**≤ 150 tables** (lint enforces the cap). To add one, create `sources/<source>/groups/<group>/_group.yml` with
`group`, `description`, `domain` and `landing_folder`.

A new group adds a new value to the governed `ingest_pipeline` tag, so run `make prereqs <env>` before deploying.
Preflight will remind you.

## 2. Scaffold

From a data-dictionary export: one row per column, with the columns `table, column, position, data_type,
nullable, primary_key_position, description`.

```bash
make scaffold SOURCE=facets GROUP=claims FROM=csv INPUT=exports/claims.csv OWNER=claims-data STATUS=draft
```

From Qlik metadata files (`.dfm`, written next to every data file when the endpoint's metadata option is on):

```bash
make scaffold SOURCE=facets GROUP=provider FROM=dfm INPUT=path/to/dfm-folder
```

Scaffold never overwrites existing files. Source types are mapped to Spark types (SQL Server, Sybase, Oracle and
Qlik types), and an unknown type stops with a clear error.

## 3. Enrich the contract

Add quality rules, set the SLA and classification, and move `status` to `active`. See
[Data contracts](../concepts/data-contracts.md). Optional per-table overrides in `<table>.yml`:

| Key | Effect |
|---|---|
| `enabled: false` | declared but not deployed |
| `scd_type: 2` | keep history in silver (`__START_AT` / `__END_AT`) |
| `append_only: true` | no CDC (e.g. log tables); silver is an append flow |
| `silver: false` | bronze only |
| `name: <uc_name>` | target table name, if not the lower-cased source name |

## 4. Check and ship

```bash
make lint                       # every table has a valid, enforceable contract
make test
make deploy dev PROFILE=<dev deployer>
```

## Removing or moving a table

- Set `enabled: false`, or delete both files. The table's pipeline stops maintaining it. Prod pipelines and
  schemas are `prevent_destroy`, so drop data deliberately, out of band.
- Moving a table to another group **recreates it under a new pipeline**. Treat it as a migration.
