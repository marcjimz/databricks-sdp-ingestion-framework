# Getting started: run it end to end in a sandbox

The `sandbox` environment proves the whole framework on any workspace you control, using **synthetic Qlik
Replicate files** generated from the table contracts. No real source data is needed.

## 0. What you need

- An Azure subscription where you can create resource groups **and role assignments**, in the same Entra
  tenant as a Databricks account with a regional Unity Catalog metastore.
- Azure CLI (`az login`), Terraform ≥ 1.5, Databricks CLI ≥ 1.24, uv.

Nothing about Qlik is required. The sandbox **simulates Qlik Replicate's output**: the seed job writes the same
folder layout (`<TABLE>/`, `<TABLE>__ct/`), header columns (`header__change_seq`, `header__change_oper`
I/U/D/B) and `.dfm` metadata files from the table contracts. The pipelines cannot tell the difference.

## 1. Build the sandbox (`infra/`)

```bash
cp infra/azure-sandbox/terraform.tfvars.example infra/azure-sandbox/terraform.tfvars   # subscription, tenant, account, metastore
make infra-plan
make infra-sandbox
```

This creates a dedicated resource group, a Premium workspace assigned to the metastore, an access connector,
and an ADLS Gen2 account with `landing/` and `catalogs/` containers plus the file-events role assignments. It
writes the results (`workspace_host`, `landing.facets`, `storage.*`) into `conf/environments/sandbox.local.yml`,
which is gitignored. Add your own values there:

| Key | Value |
|---|---|
| `deployer.service_principal` | the deployer SP's application id (or omit it to deploy as yourself) |
| `alerts.notify.emails`, `dqm.notify.emails` | you |
| `alerts.warehouse_name` | a SQL warehouse in the new workspace |

## 2. Prerequisites

```bash
make prereqs   sandbox PROFILE=azure-cli DRY_RUN=1   # review the exact CLI calls (as you, via az login)
make prereqs   sandbox PROFILE=azure-cli             # storage credential, external locations (file events on),
                                                     # catalogs, grants to the SP, tag policies
make preflight sandbox PROFILE=<deployer profile>          # must end "all required prerequisites met"
```

## 3. Deploy, seed, verify

```bash
make deploy sandbox PROFILE=<deployer>                     # preflight -> validate -> deploy -> governance -> start
make seed   sandbox PROFILE=<deployer>                     # full load for every table (+ .dfm files)
make seed   sandbox PROFILE=<deployer> BATCH=changes       # inserts, updates (+ before images), deletes
make verify sandbox PROFILE=<deployer>                     # silver == expected state, or the job fails
make trace  sandbox TABLE=cmc_meme_member PROFILE=<deployer>
```

The sandbox cadence is `on_arrival`, so each seed batch fires the group's file-arrival job within about a minute.
About 2% of seeded rows deliberately violate a contract `examples` list, so `warn` expectation metrics, and the
expectation-failures alert, have something to show.

## 4. Clean up

```bash
make seed    sandbox PROFILE=<deployer> BATCH=reset
make destroy sandbox PROFILE=<deployer> CONFIRM=sandbox
```

## Other clouds and shared workspaces

- **AWS**: Unity Catalog on AWS cannot use ADLS. Use an S3 bucket plus an IAM role (UC trust policy and SNS/SQS
  permissions for automatic file events), and set `storage.cloud: aws` with `iam_role_arn`.
- **Shared workspaces**: storage created by someone else, or by a provisioning service, often has file events
  **off**, and you cannot enable them on a location you don't own. Bring your own storage, as above.
  Catalog names are metastore-wide, so the sandbox prefixes them (`sdpif_bronze_sbx`). Shared accounts may already
  use generic governed tag keys, so the sandbox also prefixes every tag key (`sdpif_`).
