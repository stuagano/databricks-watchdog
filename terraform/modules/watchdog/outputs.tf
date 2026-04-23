output "service_principal_id" {
  description = "Databricks internal ID of the Watchdog service principal"
  value       = databricks_service_principal.watchdog.id
}

output "service_principal_application_id" {
  description = "Application (client) ID of the Watchdog service principal"
  value       = databricks_service_principal.watchdog.application_id
}

output "catalog_name" {
  description = "Name of the platform catalog"
  value       = databricks_catalog.platform.name
}

output "schema_fqn" {
  description = "Fully qualified schema name (catalog.schema)"
  value       = "${databricks_catalog.platform.name}.${databricks_schema.watchdog.name}"
}

output "secret_scope_name" {
  description = "Name of the secret scope containing Watchdog SP credentials"
  value       = databricks_secret_scope.watchdog.name
}
