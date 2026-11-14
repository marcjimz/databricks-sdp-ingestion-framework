output "workspace_url" {
  description = "Databricks workspace URL (the sandbox's workspace_host)."
  value       = "https://${azurerm_databricks_workspace.this.workspace_url}"
}

output "workspace_id" {
  value = azurerm_databricks_workspace.this.workspace_id
}

output "access_connector_id" {
  value = azurerm_databricks_access_connector.this.id
}

output "storage_account_name" {
  value = azurerm_storage_account.landing.name
}

locals {
  account_dns = "${azurerm_storage_account.landing.name}.dfs.core.windows.net"
}

# Consumed by `make infra-sandbox`, which writes it into conf/environments/sandbox.local.yml.
output "sandbox_env" {
  value = {
    workspace_host = "https://${azurerm_databricks_workspace.this.workspace_url}"
    account_id     = var.databricks_account_id
    landing_facets = "abfss://landing@${local.account_dns}/facets"
    storage = {
      create                         = true
      cloud                          = "azure"
      credential_name                = "sdpif-sbx-credential"
      external_location_name         = "sdpif-sbx-landing"
      catalog_external_location_name = "sdpif-sbx-catalogs"
      access_connector_id            = azurerm_databricks_access_connector.this.id
      resource_group                 = azurerm_resource_group.this.name
      subscription_id                = var.subscription_id
      url                            = "abfss://landing@${local.account_dns}/"
      catalog_storage_root           = "abfss://catalogs@${local.account_dns}/"
    }
  }
}
