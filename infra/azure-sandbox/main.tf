# Dedicated sandbox for the ingestion framework on Azure.
#
#   main.tf        resource group, access connector (the UC storage credential identity), roles
#   databricks.tf  Premium workspace + assignment to the regional Unity Catalog metastore
#   storage.tf     ADLS Gen2 account: landing/ (synthetic Qlik files), catalogs/ (managed storage)
#
# UC objects (credential, external locations with file events, catalogs, grants, tag policies) are
# created afterwards by `make prereqs sandbox`, from the values in outputs.tf.

locals {
  tags = merge(var.tags, { owner = var.owner })
}

resource "random_string" "suffix" {
  length  = 5
  upper   = false
  special = false
}

resource "azurerm_resource_group" "this" {
  name     = "${var.prefix}-rg"
  location = var.location
  tags     = local.tags
}

resource "azurerm_databricks_access_connector" "this" {
  name                = "${var.prefix}-connector"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  tags                = local.tags

  identity {
    type = "SystemAssigned"
  }
}

locals {
  connector_principal = azurerm_databricks_access_connector.this.identity[0].principal_id

  # Storage-account roles: read/write files, consume the event queue, and let Databricks create the
  # queue (AUTOMATIC file events). Resource-group roles: create the Event Grid subscription (Microsoft
  # documents both role names; granting both avoids the discrepancy).
  connector_roles = {
    blob_data    = { scope = azurerm_storage_account.landing.id, role = "Storage Blob Data Contributor" }
    queue_data   = { scope = azurerm_storage_account.landing.id, role = "Storage Queue Data Contributor" }
    account      = { scope = azurerm_storage_account.landing.id, role = "Storage Account Contributor" }
    eventgrid_es = { scope = azurerm_resource_group.this.id, role = "EventGrid EventSubscription Contributor" }
    eventgrid_dc = { scope = azurerm_resource_group.this.id, role = "EventGrid Data Contributor" }
  }
}

resource "azurerm_role_assignment" "connector" {
  for_each             = local.connector_roles
  scope                = each.value.scope
  role_definition_name = each.value.role
  principal_id         = local.connector_principal
  principal_type       = "ServicePrincipal"
}

# Registering the connector as a UC storage credential requires the REGISTERING user to hold
# Contributor on it directly (group-inherited roles are not enough): grant it to whoever runs
# Terraform, i.e. the admin who then runs `make prereqs sandbox`.
data "azurerm_client_config" "current" {}

resource "azurerm_role_assignment" "registrant" {
  scope                = azurerm_databricks_access_connector.this.id
  role_definition_name = "Contributor"
  principal_id         = data.azurerm_client_config.current.object_id
}
