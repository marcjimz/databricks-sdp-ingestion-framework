resource "azurerm_databricks_workspace" "this" {
  name                        = "${var.prefix}-dbw"
  location                    = azurerm_resource_group.this.location
  resource_group_name         = azurerm_resource_group.this.name
  sku                         = "premium" # Unity Catalog
  managed_resource_group_name = "databricks-rg-${var.prefix}-${var.location}"
  tags                        = local.tags
}

resource "databricks_metastore_assignment" "this" {
  provider     = databricks.account
  workspace_id = azurerm_databricks_workspace.this.workspace_id
  metastore_id = var.metastore_id
}
