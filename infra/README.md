# infra/

A **dedicated sandbox, as code**, so it can be rebuilt at any time (sandbox subscriptions may be wiped on a
schedule) or torn down. Customer environments (dev/tst/prod) use workspaces and storage the platform team already
owns, so nothing here is used for them.

## `azure-sandbox/` — what it creates

| File | Resources |
|---|---|
| `main.tf` | resource group `<prefix>-rg`; Access Connector `<prefix>-connector` (managed identity = the Unity Catalog storage credential); its role assignments |
| `databricks.tf` | Premium workspace `<prefix>-dbw`; assignment to the regional Unity Catalog metastore |
| `storage.tf` | ADLS Gen2 account `<prefix>lz<suffix>` with `landing/` (synthetic Qlik-shaped files) and `catalogs/` (managed storage) |
| `outputs.tf` | `sandbox_env`: workspace URL, landing root and storage settings, written into `conf/environments/sandbox.local.yml` |

Unity Catalog objects (storage credential, external locations with file events, catalogs, grants, tag policies) are
**not** in Terraform. They live in the shared metastore and outlive the workspace, so `make prereqs sandbox` creates
them with the CLI and checks each one before creating it.

## Run it

> **One tenant.** The subscription, the Databricks account and the metastore must be in the same Entra tenant:
> Unity Catalog only accepts an access connector from the workspace's tenant. You need rights to create resource
> groups **and role assignments** (Owner or User Access Administrator), Databricks **account admin** (to attach
> the workspace to the metastore), and `CREATE STORAGE CREDENTIAL / EXTERNAL LOCATION / CATALOG` on the metastore.
> Where each id in `terraform.tfvars` comes from: [getting started, step 0](../docs/guides/getting-started.md#0-what-you-need).

```bash
az login --tenant <tenant id>
cp infra/azure-sandbox/terraform.tfvars.example infra/azure-sandbox/terraform.tfvars   # fill in
make infra-plan
make infra-sandbox                         # apply (asks to confirm; AUTO_APPROVE=1 skips) + writes sandbox.local.yml
make prereqs   sandbox PROFILE=azure-cli   # UC objects, as you (workspace + metastore admin), via az login
make preflight sandbox PROFILE=<deployer>  # then deploy as the deployer
```

Whoever runs Terraform becomes the workspace's admin and is granted Contributor on the access connector, which
is needed to register it as a storage credential.

## Role assignments, and why

| Role | Scope | Why |
|---|---|---|
| Storage Blob Data Contributor | storage account | read landing files; the seed job writes synthetic files; managed catalog storage |
| Storage Queue Data Contributor | storage account | consume file-event notifications |
| Storage Account Contributor | storage account | let Databricks create the event queue (automatic file events) |
| EventGrid EventSubscription Contributor + EventGrid Data Contributor | resource group | create the Event Grid subscription (Microsoft's docs name both, so both are granted) |
| Contributor (to you) | access connector | register it as a Unity Catalog storage credential |

## Tear down (reverse order)

```bash
make destroy sandbox PROFILE=<deployer> CONFIRM=sandbox                          # bundle resources

# UC objects from make prereqs, as the workspace admin (your az login)
export DATABRICKS_AUTH_TYPE=azure-cli DATABRICKS_HOST=<workspace_host>
databricks catalogs delete sdpif_bronze_sbx --force
databricks catalogs delete sdpif_silver_sbx --force
databricks external-locations delete sdpif-sbx-landing --force
databricks external-locations delete sdpif-sbx-catalogs --force
databricks storage-credentials delete sdpif-sbx-credential --force

make infra-destroy CONFIRM=infra                                                 # workspace, storage, identity
```

Use your own names if you overrode them in `sandbox.local.yml`.

Terraform state is local and gitignored. For a shared sandbox, move it to a remote backend.
