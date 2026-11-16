output "workspace_url" {
  description = "Databricks workspace URL."
  value       = "https://${azurerm_databricks_workspace.this.workspace_url}"
}

output "workspace_id" {
  value = azurerm_databricks_workspace.this.workspace_id
}

# Read by unity_catalog.sh.
output "access_connector_id" {
  value = azurerm_databricks_access_connector.this.id
}

output "resource_group" {
  value = azurerm_resource_group.this.name
}

output "subscription_id" {
  value = var.subscription_id
}

output "storage_account_name" {
  value = azurerm_storage_account.landing.name
}
