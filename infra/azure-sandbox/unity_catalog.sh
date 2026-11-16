#!/usr/bin/env bash
# Create the sandbox's Unity Catalog objects if they are missing. Safe to re-run.
#
#   storage credential  lff-sbx-credential   the access connector's managed identity
#   external location   lff-sbx-landing      abfss://landing@<account>/   file events on (Auto Loader)
#   external location   lff-sbx-catalogs     abfss://catalogs@<account>/  managed storage, file events off
#   catalogs            lff_bronze_sbx, lff_silver_sbx, each with schema facets
#   volume              lff_bronze_sbx.landing.qlik  over abfss://landing@<account>/qlik  (where Qlik files land)
#
# Needs DATABRICKS_HOST (the workspace) and DATABRICKS_AUTH_TYPE=azure-cli; `make infra` sets both.
set -euo pipefail
cd "$(dirname "$0")"

out() { terraform output -raw "$1"; }
DFS="$(out storage_account_name).dfs.core.windows.net"
CONNECTOR="$(out access_connector_id)"
RG="$(out resource_group)"
SUB="$(out subscription_id)"
CRED=lff-sbx-credential
BRONZE=lff_bronze_sbx
SILVER=lff_silver_sbx

exists() { databricks "$@" >/dev/null 2>&1; }
create() { echo "+ databricks $1 $2 ${3:-}"; databricks "$@" >/dev/null; }

exists storage-credentials get "$CRED" || create storage-credentials create --json \
  "{\"name\": \"$CRED\", \"azure_managed_identity\": {\"access_connector_id\": \"$CONNECTOR\"}}"

exists external-locations get lff-sbx-landing || create external-locations create --json \
  "{\"name\": \"lff-sbx-landing\", \"url\": \"abfss://landing@$DFS/\", \"credential_name\": \"$CRED\",
    \"enable_file_events\": true,
    \"file_event_queue\": {\"managed_aqs\": {\"resource_group\": \"$RG\", \"subscription_id\": \"$SUB\"}}}"

exists external-locations get lff-sbx-catalogs || create external-locations create --json \
  "{\"name\": \"lff-sbx-catalogs\", \"url\": \"abfss://catalogs@$DFS/\", \"credential_name\": \"$CRED\",
    \"enable_file_events\": false}"

for catalog in "$BRONZE" "$SILVER"; do
  exists catalogs get "$catalog" || create catalogs create "$catalog" --storage-root "abfss://catalogs@$DFS/$catalog"
  exists schemas get "$catalog.facets" || create schemas create facets "$catalog"
done
exists schemas get "$BRONZE.landing" || create schemas create landing "$BRONZE"
exists volumes read "$BRONZE.landing.qlik" || create volumes create "$BRONZE" landing qlik EXTERNAL \
  --storage-location "abfss://landing@$DFS/qlik"

echo "Unity Catalog objects ready."
