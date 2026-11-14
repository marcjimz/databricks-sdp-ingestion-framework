variable "subscription_id" {
  description = "Azure subscription to deploy into."
  type        = string
}

variable "tenant_id" {
  description = "Entra tenant that owns the subscription AND the Databricks account (they must match)."
  type        = string
}

variable "databricks_account_id" {
  description = "Azure Databricks account ID (accounts.azuredatabricks.net)."
  type        = string
}

variable "metastore_id" {
  description = "Existing Unity Catalog metastore in var.location to assign the workspace to."
  type        = string
}

variable "location" {
  description = "One region for the workspace and storage (must match the metastore's region)."
  type        = string
  default     = "eastus2"
}

variable "prefix" {
  description = "Name prefix (lowercase letters and digits). Storage names are globally unique; a random suffix is added."
  type        = string
  default     = "sdpif"
  validation {
    condition     = can(regex("^[a-z][a-z0-9]{2,11}$", var.prefix))
    error_message = "prefix must be 3-12 lowercase letters/digits, starting with a letter."
  }
}

variable "owner" {
  description = "Owner email, applied as the `owner` tag (required by sandbox subscription policy)."
  type        = string
}

variable "tags" {
  description = "Tags applied to every resource."
  type        = map(string)
  default = {
    project = "sdp-ingestion-framework"
  }
}
