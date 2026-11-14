# Onboarding tables

A table is one folder holding two files. Both are generated; you only review and enrich.

```
sources/facets/groups/claims/
  group.yml                      the pipeline (one Qlik task / landing folder)
  tables/
    cmc_clcl_claim/
      table.yml                  ops config: usually just `table:` and `enabled:`
      contract.odcs.yaml         data contract: columns, types, keys, quality, SLA, owner
```

Lint keeps it that way: nothing but `group.yml` and `tables/` in a group folder, nothing but those two files in a
table folder, and the folder named after the table.

## 1. Pick or create the group

Which groups deploy is set per environment by `scope.groups` in `conf/environments/<env>.yml`. The repo starts
small with two domains (`facets/membership`, `facets/claims`); `facets/provider` is declared and lint-checked but
commented out of scope. Uncomment it to deploy it.

Groups map 1:1 to pipelines and should map 1:1 to Qlik tasks, so each has its own landing folder. Keep groups at
**≤ 150 tables** (lint enforces the cap). To add one, create `sources/<source>/groups/<group>/group.yml` with
`group`, `description`, `domain` and `landing_folder`.

A new group means a new pipeline. Every table carries the pipeline's name in the `ingest_pipeline` tag, and that
tag is *governed*, which means only declared values are allowed
([Traceability & tags](../concepts/traceability-and-tags.md)). So run `make prereqs <env> PROFILE=<admin>`
before deploying to add the new value. Preflight will remind you.

## 2. Scaffold

From a data-dictionary export: one row per column, with the columns `table, column, position, data_type,
nullable, primary_key_position, description`.

```bash
make scaffold SOURCE=facets GROUP=claims FROM=csv INPUT=examples/scaffold/claims_dictionary.csv OWNER=claims-data STATUS=draft
```

From Qlik metadata files (`.dfm`, written next to every data file when the endpoint's metadata option is on):

```bash
make scaffold SOURCE=facets GROUP=provider FROM=dfm INPUT=examples/scaffold/dfm   # or any folder of .dfm files
```

Scaffold never overwrites existing files: re-running the samples above reports their tables as already
declared. Source types are mapped to Spark types (SQL Server, Sybase, Oracle and
Qlik types), and an unknown type stops with a clear error.

## 3. Enrich the contract

Add quality rules, set the SLA and classification, and move `status` to `active`. See
[Data contracts](../concepts/data-contracts.md). Optional per-table overrides in `table.yml`:

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

The new tables get their tags at the next hourly governance run, or immediately with `make governance <env>`.

## Removing or moving a table

- Set `enabled: false`, or delete the table folder. The table's pipeline stops maintaining it. Prod pipelines and
  schemas are `prevent_destroy`, so drop data deliberately, out of band.
- Moving a table to another group **recreates it under a new pipeline**. Treat it as a migration.
