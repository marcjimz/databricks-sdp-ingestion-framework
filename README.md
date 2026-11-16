# Healthcare Adaptation on the Lakeflow Framework

![./static/img/dbx-banner.jpeg](./static/img/dbx-banner.jpeg)

A healthcare adaptation of the [Lakeflow Framework](https://github.com/databricks-solutions/lakeflow_framework)
(Databricks Solutions; pinned here as the `lakeflow_framework/` submodule at **v0.28.0**). It is a **pipeline
bundle** that lands a health plan's source systems in a medallion lakehouse on Databricks.

The repo is metadata only. There is no pipeline code here: the framework builds every pipeline from the data
flow specs in `src/dataflows`.

## Sources

| Source | Arrives as | Status |
|---|---|---|
| Facets (core administration: members, claims, providers) | **Qlik Replicate** CDC files on ADLS, read by **Auto Loader** | One table, `CMC_MEME_MEMBER`, end to end |

Each new source or table follows the same pattern: data flow specs, schemas, and per-environment substitutions.

The Facets example: Qlik's full load and change files land in bronze, then AUTO CDC keeps silver current.

```
Qlik Replicate -> ADLS                         Unity Catalog
  CMC_MEME_MEMBER/      full load   --f_full_load--+
  CMC_MEME_MEMBER__ct/  changes     --f_changes----+-> <bronze>.facets.cmc_meme_member   every row as delivered
                                                          | skip before-images (B), sequence by header__change_seq
                                                          v AUTO CDC, SCD1, deletes on D
                                                     <silver>.facets.cmc_meme_member   current state
```

## Layout

```
databricks.yml                 bundle, variables, targets (sandbox | dev | prod)
resources/                     facets_bronze and facets_silver pipelines, facets_ingestion job (bronze, then silver)
src/dataflows/
  bronze/dataflowspec/         cmc_meme_member_main.json: two Auto Loader sources into one bronze table
  bronze/schemas/              column schemas of the Qlik files (full load, and changes with header__ columns)
  silver/dataflowspec/         cmc_meme_member_main.json: bronze -> silver, cdc_settings (keys, sequence, deletes)
src/pipeline_configs/          <target>_substitutions.json: landing path and catalog.schema per target
tools/synthetic_qlik.py        sandbox only: writes Qlik-shaped Parquet files from the bronze schemas
lakeflow_framework/            the framework (git submodule, pinned)
infra/azure-sandbox/           Terraform + Unity Catalog script for a dedicated Azure sandbox
```

How a spec works, what each key means, and which features exist (templates, expectations, quarantine,
substitutions, SCD2, snapshots...) is all in the
[framework documentation](https://databricks-solutions.github.io/lakeflow_framework/current/). This repo uses only
what it needs today; more follows as tables are added.

## Prerequisites

- Databricks CLI (recent), [uv](https://docs.astral.sh/uv/), Python 3.11+, and `git clone --recurse-submodules`
  (or `git submodule update --init` after cloning).
- A workspace with Unity Catalog and serverless pipelines.
- For the sandbox: Terraform, `az login`, and the rights listed in [infra/README.md](infra/README.md).

## Run it in the sandbox

```bash
make check                      # specs against the framework's JSON schemas + the customer-name guard (offline)
make infra                      # Azure workspace + ADLS (Terraform), then the Unity Catalog objects
make framework                  # deploy the pinned Lakeflow Framework into the workspace
make deploy                     # this bundle: two pipelines, the job, and the sandbox-only file generator
make seed BATCH=full            # Qlik full load: 100 members
make run                        # bronze, then silver
make seed BATCH=changes         # 10 updates (each with a before-image), 5 deletes, 5 inserts
make run
```

The sandbox target has no fixed workspace: the Makefile reads the URL from Terraform and signs in with your
`az login`.

## Customer environments (dev, prod)

1. In `databricks.yml`, set the target's `bronze_catalog` and `silver_catalog`.
2. In `src/pipeline_configs/<target>_substitutions.json`, set `landing_root` (where Qlik Replicate writes) and the
   two `catalog.schema` tokens.
3. Deploy with a CLI profile for that workspace, preferably a service principal:
   `make framework PROFILE=<p>` once, then `make deploy run TARGET=dev PROFILE=<p>`.

The synthetic file generator is defined only in the sandbox target, so customer targets never get it.
Auto Loader reads with managed file events, so the landing path must sit under an external location that has
file events enabled.

## Add a table

Copy the two spec files and the schemas, and rename them for the new table. Then set the key (`cdc_settings.keys`),
the paths, and the columns. Run `make check`, then `make deploy`.

For many tables, the framework's [templates](https://databricks-solutions.github.io/lakeflow_framework/current/features/metadata/templates.html)
generate one spec per table from a single definition.
