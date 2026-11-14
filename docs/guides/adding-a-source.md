# Adding a source (example: Salesforce via Lakeflow Connect)

> Status: **designed, not built.** The adapter seam exists and is tested with a dummy managed adapter
> (`tests/enforcement/test_bundle_rules.py::test_a_new_source_type_inherits_all_governance`). This page is the
> implementation plan for the first Lakeflow Connect source.

## What a source type is

`sources/<source>/source.yml` names an adapter with `type:`. Today there is one: `qlik_cdc_files`. A Lakeflow
Connect source does not run framework code. Its pipeline is a **managed ingestion pipeline** (an
`ingestion_definition` on the pipeline resource), so its adapter only describes the pipeline and its tables.

```yaml
# sources/salesforce/source.yml
source: salesforce
type: lakeflow_connect
description: Salesforce CRM via Lakeflow Connect.
connection_name: salesforce_prod      # Unity Catalog connection (a prerequisite)
connector:
  source_schema: objects
defaults:
  scd_type: 1
  silver: false                       # the connector already applies changes; see below
  classification: confidential
```

```yaml
# sources/salesforce/groups/sales/account.yml
table: Account
enabled: true
scd_type: 2
```

Every object still gets an `account.odcs.yaml` contract. Lint and the "every table has a contract" rule are
source-agnostic.

## The adapter (about 60 lines)

```python
class LakeflowConnectAdapter(SourceAdapter):
    type = "lakeflow_connect"
    runs_framework_code = False

    def datasets(self, cfg, group):          # one bronze table per object
        return [Dataset("bronze", cfg.table_fqn("bronze", t.source, t.name), t) for t in group.tables]

    def pipeline_body(self, cfg, group):     # managed ingestion pipeline
        src = cfg.source(group.source)
        return {
            "catalog": cfg.catalog("bronze"),
            "schema": cfg.schema(group.source),
            "ingestion_definition": {
                "connection_name": src.settings["connection_name"],
                "objects": [{"table": {
                    "source_schema": src.settings["connector"]["source_schema"],
                    "source_table": t.table,
                    "destination_catalog": cfg.catalog("bronze"),
                    "destination_schema": cfg.schema(group.source),
                    "destination_table": t.name,
                    "table_configuration": {"scd_type": f"SCD_TYPE_{t.scd_type}",
                                            "primary_keys": list(t.contract.keys) or None},
                }} for t in group.tables],
            },
        }
```

Register it in `adapters/__init__.py`, the same way as the Qlik adapter.

## What it inherits automatically

The framework applies all of the following without changes:

- naming: `ingest-salesforce-<group>-<env>`;
- pipeline tags (`ingest_config`, `ingest_code`, git commit) and, via the governance job, table tags;
- event log in `ingest_ops`, failure notifications;
- a cadence job, which must be periodic or cron: managed connectors have no landing folder, and lint rejects
  `on_arrival`;
- `bronze_<env>.salesforce` / `silver_<env>.salesforce` schemas with DQM anomaly detection;
- registry rows, the undeclared-object and tag-drift alerts, and `make trace`;
- preflight: the `connections` check verifies the UC connection exists before deploy.

## Silver for connector sources

Lakeflow Connect already maintains SCD1/SCD2 tables. Two options:

1. Treat the connector table as silver directly (`destination_catalog: silver_<env>`). This is simplest.
2. Keep connector output in bronze and add a second group with a small `delta_cdf_scd` adapter. That adapter
   streams the table's change feed into a contract-governed silver table, the same shape as the Qlik silver.

Choose per source. The contract and governance are identical either way.
