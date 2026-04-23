variable "catalog_name" {
  type        = string
  description = "Name of the platform catalog for Watchdog tables"
  default     = "platform"
}

variable "schema_name" {
  type        = string
  description = "Name of the schema within the platform catalog"
  default     = "watchdog"
}

variable "service_principal_client_id" {
  type        = string
  description = "Client ID (application ID) of the Watchdog service principal"
}

variable "service_principal_client_secret" {
  type        = string
  description = "Client secret of the Watchdog service principal"
  sensitive   = true
}

variable "tenant_id" {
  type        = string
  description = "Azure AD tenant ID"
}

variable "subscription_id" {
  type        = string
  description = "Azure subscription ID"
}

variable "scanned_catalog_names" {
  type        = list(string)
  description = "List of catalog names Watchdog should have SELECT access to for scanning"
  default     = []
}

variable "force_destroy" {
  type        = bool
  description = "Allow force destroy of catalog and schema. Set to false in production."
  default     = false
}
