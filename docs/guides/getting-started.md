# Getting started: run it end to end in a sandbox

The `sandbox` environment proves the whole framework on any workspace you control, using **synthetic Qlik
Replicate files** generated from the table contracts. No real source data is needed.

## 0. What you need

- **Azure.** A subscription where you can create resource groups **and role assignments** (Owner or User Access
  Administrator). It must be in the same Entra ID (Azure AD) tenant as your Databricks account.
- **Databricks.** **Account admin** on the Databricks account, so Terraform can attach the new workspace to the
  metastore. You also need these privileges on that region's Unity Catalog metastore: `CREATE STORAGE CREDENTIAL`,
  `CREATE EXTERNAL LOCATION` and `CREATE CATALOG`. The metastore is the account-level catalog store, one per
  region.
- **Tools.** Azure CLI (`az login`), Terraform ≥ 1.5, Databricks CLI ≥ 1.24, Python ≥ 3.11 and
  [uv](https://docs.astral.sh/uv/).

Where the four ids in `terraform.tfvars` come from:

| Key | Where to find it |
|---|---|
| `subscription_id`, `tenant_id` | `az account show --query '{subscription: id, tenant: tenantId}'` |
| `databricks_account_id` | account console (accounts.azuredatabricks.net), user menu, top right |
| `metastore_id` | account console → *Catalog* → the metastore in your region |

Nothing about Qlik is required. The sandbox **simulates Qlik Replicate's output**: the seed job writes the same
folder layout (`<TABLE>/`, `<TABLE>__ct/`), header columns (`header__change_seq`, `header__change_oper`
I/U/D/B) and `.dfm` metadata files from the table contracts. The pipelines cannot tell the difference.

## 1. Build the sandbox (`infra/`)

```bash
cp infra/azure-sandbox/terraform.tfvars.example infra/azure-sandbox/terraform.tfvars   # the four ids above
make infra-plan
make infra-sandbox
```

This creates:

- a dedicated resource group;
- a Premium workspace, attached to the metastore;
- an access connector, the managed identity Unity Catalog uses to reach storage;
- an ADLS Gen2 account with `landing/` and `catalogs/` containers;
- the role assignments for *file events*, the storage notifications that let Auto Loader and file-arrival
  triggers see new files without listing directories.

It writes the results (`workspace_host`, `landing.facets`, `storage.*`) into `conf/environments/sandbox.local.yml`,
which is gitignored. Add your own values there:

| Key | Value |
|---|---|
| `deployer.service_principal` | the deployer SP's application id (step 2) |
| `alerts.warehouse_name` | a SQL warehouse in the new workspace (e.g. `Serverless Starter Warehouse`) |

Leave `alerts.notify.emails` and `dqm.notify.emails` empty unless you want email. Every check still runs, and
the results are visible in the workspace.

Unity Catalog names are **metastore-wide**. On a metastore that other people also use, override the catalog names
(`catalogs.*`) and the `storage.*_name` entries in `sandbox.local.yml` so they don't collide with another sandbox.

## 2. The deployer identity

Every deploy runs as a service principal, never as a person. Create one in the new workspace as its admin:

```bash
DATABRICKS_AUTH_TYPE=azure-cli DATABRICKS_HOST=<workspace_host> \
  databricks service-principals create --display-name sdp-ingest-deployer-sandbox
```

Put its `applicationId` in `deployer.service_principal`. Then the workspace admin generates an OAuth secret for
it (*Settings → Identity and access → Service principals → Secrets*) and adds a CLI profile to `~/.databrickscfg`.
The secret stays in that file and never goes into the repo:

```ini
[sdpif-sandbox-deployer]
host          = <workspace_host>
client_id     = <applicationId>
client_secret = <the secret>
```

`make prereqs` (next step) grants this SP everything it needs in the sandbox. In dev/tst/prod some grants are
manual and owned by the platform team: [Prerequisites](../operations/prerequisites.md).

## 3. Prerequisites

```bash
make prereqs   sandbox PROFILE=azure-cli DRY_RUN=1   # review the exact CLI calls (as you, via az login)
make prereqs   sandbox PROFILE=azure-cli             # storage credential, external locations (file events on),
                                                     # catalogs, grants to the SP, tag policies
make preflight sandbox PROFILE=<deployer profile>          # must end "all required prerequisites met"
```

## 4. Deploy, seed, verify

```bash
make deploy sandbox PROFILE=<deployer>                     # preflight -> validate -> deploy -> governance -> start
make seed   sandbox PROFILE=<deployer>                     # full load for every table (+ .dfm files)
make seed   sandbox PROFILE=<deployer> BATCH=changes       # inserts, updates (+ before images), deletes
make verify sandbox PROFILE=<deployer>                     # silver == expected state, or the job fails
make trace  sandbox TABLE=cmc_meme_member PROFILE=<deployer>
```

The sandbox cadence is `on_arrival`: a file-arrival job starts the group's pipeline when new files land, so data
reaches silver about 3–4 minutes after a seed batch. Prod uses `continuous` (always-on), which takes about 30
seconds. [Environments & cadence](../operations/environments-and-cadence.md) lists every profile.
About 2% of seeded rows deliberately violate a contract `examples` list, so `warn` expectation metrics, and the
expectation-failures alert, have something to show.

## 5. The other end-to-end scenarios

Each scenario ends with `make verify`, which compares every silver table with the generator's expected state row
for row. All of these were run live in the sandbox.

| Scenario | Commands | What it proves |
|---|---|---|
| Tables with no files yet | right after `make deploy`, before the first seed | an update completes on missing landing folders |
| Qlik table reload | `make seed … BATCH=reload TABLE=cmc_clcl_claim`, wait for the triggered update, then the printed `make reload … SINCE=…`, then `make verify` | the snapshot repairs changes Qlik never delivered; a same-name `LOAD00000001` is re-read; old changes do not resurface |
| Always-on (`continuous`) group | set `cadence.overrides: {facets/claims: continuous}` in `sandbox.local.yml`, `make deploy`, seed changes, `make verify`; remove the override and deploy again | the one-line cadence switch, both ways |
| Onboard a table | `make scaffold …` into a group, `make lint`, `make deploy`, `make seed … BATCH=full_load TABLE=<t>`, `make verify` | a new table joins a running pipeline with no code change |

## 6. Clean up

```bash
make seed    sandbox PROFILE=<deployer> BATCH=reset            # synthetic landing files
```

Then follow [infra/README.md → Tear down](../../infra/README.md#tear-down-reverse-order). It removes the bundle,
the metastore-wide catalogs, external locations and credential, and finally the Azure resources.

## Other clouds and shared workspaces

- **AWS**: Unity Catalog on AWS cannot use ADLS. Use an S3 bucket plus an IAM role (UC trust policy and SNS/SQS
  permissions for automatic file events), and set `storage.cloud: aws` with `iam_role_arn`.
- **Shared workspaces**: storage created by someone else, or by a provisioning service, often has file events
  **off**, and you cannot enable them on a location you don't own. Bring your own storage, as above.
  Catalog names are metastore-wide, so the sandbox prefixes them (`sdpif_bronze_sbx`). They are fixed names, so a
  second sandbox on the same metastore must override them (see step 1). Shared accounts may already use generic
  governed tag keys, so the sandbox also prefixes every tag key (`sdpif_`).
