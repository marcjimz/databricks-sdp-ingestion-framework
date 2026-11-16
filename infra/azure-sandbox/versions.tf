terraform {
  required_version = ">= 1.5"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.40"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.130"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
  # Local state. Sandbox subscriptions may be wiped on a schedule: `make infra` then sees the
  # resources gone and recreates them.
}

# Pinned to the sandbox tenant: the az CLI can otherwise hand out tokens for your home tenant.
provider "azurerm" {
  features {
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }
  subscription_id     = var.subscription_id
  tenant_id           = var.tenant_id
  storage_use_azuread = true # shared keys are disabled on the storage account
}

# Account level only (metastore assignment). Unity Catalog objects are created by unity_catalog.sh
# with the CLI: they live in the shared metastore and outlive the workspace, so Terraform state for them
# would go stale when the subscription is wiped.
provider "databricks" {
  alias           = "account"
  host            = "https://accounts.azuredatabricks.net"
  account_id      = var.databricks_account_id
  azure_tenant_id = var.tenant_id
  auth_type       = "azure-cli"
}
