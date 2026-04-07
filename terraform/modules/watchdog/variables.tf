variable "catalog_name" {
  type        = string
  description = "(Optional) Name of the platform catalog for Watchdog tables"
  default     = "platform"
}

variable "schema_name" {
  type        = string
  description = "(Optional) Name of the schema within the platform catalog"
  default     = "watchdog"
}

variable "service_principal_client_id" {
  type        = string
  description = "(Required) Client ID of the Watchdog service principal"
}

variable "service_principal_client_secret" {
  type        = string
  description = "(Required) Client secret of the Watchdog service principal"
  sensitive   = true
}

variable "tenant_id" {
  type        = string
  description = "(Required) Azure tenant ID"
}

variable "subscription_id" {
  type        = string
  description = "(Required) Azure subscription ID"
}

variable "force_destroy" {
  type        = bool
  description = "(Optional) Allow force destroy of catalog and schema. Set to false in production."
  default     = false
}

variable "ontos_service_principal_application_id" {
  type        = string
  description = "(Optional) Application ID of the Ontos service principal. When set, grants USE_CATALOG, USE_SCHEMA, and SELECT on the watchdog schema so Ontos can read governance data without manual SQL grants."
  default     = ""
}
