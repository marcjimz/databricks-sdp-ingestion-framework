# Traceability and tags

Operations question: *"this table looks wrong — what loads it, from which config, with which code?"*
The framework answers it in two hops, in the Unity Catalog UI or with `make trace`.

```
table  silver_prod.facets.cmc_clcl_claim
  └─ tag ingest_pipeline = ingest-facets-claims-prod          (hop 1: table → pipeline)
pipeline ingest-facets-claims-prod
  ├─ tag ingest_config     = sources/facets/groups/claims/     (hop 2: pipeline → config)
  ├─ tag ingest_code       = src/pipelines/entrypoint.py -> src/ingestion_framework/adapters/qlik_cdc_files
  └─ tag ingest_git_commit = <sha>                             (exact version deployed)
```

| On | Tag | Governed tag policy |
|---|---|---|
| every table | `ingest_pipeline`, `ingest_source`, `ingest_layer` | yes. Allowed values are generated from config and applied by `make prereqs` |
| every pipeline and cadence job | `ingest_pipeline`, `ingest_source`, `ingest_config`, `ingest_code`, `ingest_git_commit` | n/a (bundle tags) |

Tables also carry the table properties `ingest.pipeline` and `ingest.contract` (`<contract id>@<version>`). These
are set by the pipeline itself at creation time.

## Why a governance job applies table tags

SDP can set table *properties* but not Unity Catalog *tags* on the tables it creates. The
`ingest-governance-<env>` job runs after every deploy and hourly. It:

1. publishes `ingest_ops.table_registry`: one row per declared table, holding the pipeline, config path,
   contract, adapter, cadence and git commit;
2. applies the table tags to every declared table that exists, changing only what drifted;
3. writes `ingest_ops.undeclared_objects`: tables in framework-owned schemas that no config declares.

The `tag_drift` and `undeclared_objects` alerts fire when either check finds something.

## Governed tags and prefixes

Governed tag policies are account-wide. Their allowed values (pipeline names, sources, layers) are generated from
configuration, so **adding a group means updating the policy first**. `make prereqs` does that, and preflight
blocks until it is done.

Shared accounts may already use generic keys. Set `governance.tag_prefix` per environment (the sandbox uses
`sdpif_`) and every key is prefixed consistently. If you cannot create tag policies, set
`governance.governed_tags: false`: the tags are still applied, ungoverned, and preflight warns.

## `make trace`

```bash
make trace prod TABLE=cmc_clcl_claim                     # repo only (no PROFILE)
make trace prod TABLE=cmc_clcl_claim PROFILE=<p>         # + live tag and pipeline state, flags drift
make trace prod PIPELINE=ingest-facets-claims-prod       # reverse: every table a pipeline owns
```
