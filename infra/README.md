# infra/

A dedicated Azure sandbox, as code, so it can be rebuilt at any time (sandbox subscriptions may be wiped on a
schedule). Customer environments use workspaces and storage the platform team already owns; nothing here applies
to them.

| File | Creates |
|---|---|
| `azure-sandbox/main.tf` | resource group `<prefix>-rg`; access connector `<prefix>-connector` (the managed identity behind the storage credential); its role assignments |
| `azure-sandbox/databricks.tf` | Premium workspace `<prefix>-dbw`, attached to the regional Unity Catalog metastore |
| `azure-sandbox/storage.tf` | ADLS Gen2 account with containers `landing/` (Qlik-shaped files) and `catalogs/` (managed storage) |
| `azure-sandbox/unity_catalog.sh` | storage credential, external locations (landing with file events on), catalogs `lff_bronze_sbx` / `lff_silver_sbx` with schema `facets`, volume `lff_bronze_sbx.landing.qlik` |

Unity Catalog objects are created by a re-runnable CLI script, not by Terraform. They live in the shared metastore
and outlive the workspace, so Terraform state for them would go stale after a wipe.

## Run it

> **One tenant.** The subscription, the Databricks account and the metastore must be in the same Entra tenant.
> You need rights to create resource groups **and role assignments** (Owner or User Access Administrator),
> Databricks **account admin** (to attach the workspace to the metastore), and `CREATE STORAGE CREDENTIAL`,
> `CREATE EXTERNAL LOCATION` and `CREATE CATALOG` on the metastore.

```bash
az login --tenant <tenant id>
cp infra/azure-sandbox/terraform.tfvars.example infra/azure-sandbox/terraform.tfvars   # fill in
make infra                                                                             # Terraform, then unity_catalog.sh
```

`PREFIX` (default `lffsbx`) names the Azure resources.

## Role assignments, and why

| Role | Scope | Why |
|---|---|---|
| Storage Blob Data Contributor | storage account | read landing files; the generator writes them; managed catalog storage |
| Storage Queue Data Contributor | storage account | consume file-event notifications |
| Storage Account Contributor | storage account | let Databricks create the event queue (managed file events) |
| EventGrid EventSubscription Contributor + EventGrid Data Contributor | resource group | create the Event Grid subscription |
| Contributor (to you) | access connector | register it as a Unity Catalog storage credential |

## Tear down

```bash
make destroy                                    # this bundle's pipelines and jobs (and their tables)
terraform -chdir=infra/azure-sandbox destroy    # workspace, storage, connector
```

The Unity Catalog objects stay in the metastore until you drop them (`databricks catalogs delete <name> --force`,
then the external locations and the storage credential).
