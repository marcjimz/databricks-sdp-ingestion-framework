resource "azurerm_storage_account" "landing" {
  name                            = "${var.prefix}lz${random_string.suffix.result}"
  resource_group_name             = azurerm_resource_group.this.name
  location                        = azurerm_resource_group.this.location
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  account_kind                    = "StorageV2"
  is_hns_enabled                  = true
  min_tls_version                 = "TLS1_2"
  shared_access_key_enabled       = false
  allow_nested_items_to_be_public = false
  tags                            = local.tags
}

# Created through Azure Resource Manager (storage_account_id), so no data-plane role is needed.
resource "azurerm_storage_container" "landing" {
  name                  = "landing" # synthetic Qlik Replicate output: <source>/<group>/<TABLE>[__ct]/
  storage_account_id    = azurerm_storage_account.landing.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "catalogs" {
  name                  = "catalogs" # managed storage for the sandbox bronze/silver catalogs
  storage_account_id    = azurerm_storage_account.landing.id
  container_access_type = "private"
}
