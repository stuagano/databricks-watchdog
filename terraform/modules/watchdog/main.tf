# ==============================================================================
# Watchdog Infrastructure Module
# ==============================================================================
# Creates the Databricks resources for the Watchdog governance system:
#   1. Service principal registration in Databricks
#   2. Secret scope with SP credentials
#   3. Platform catalog + watchdog schema
#   4. UC grants: read on scanned catalogs, write on watchdog schema

# --------------------------------------------------------------------------
# 1. Service Principal — register in Databricks workspace
# --------------------------------------------------------------------------

resource "databricks_service_principal" "watchdog" {
  application_id       = var.service_principal_client_id
  display_name         = "SP for Watchdog Governance Scanner"
  allow_cluster_create = false
}

# --------------------------------------------------------------------------
# 2. Secret Scope — store SP credentials for the Watchdog job
# --------------------------------------------------------------------------

resource "databricks_secret_scope" "watchdog" {
  name = "watchdog"
}

resource "databricks_secret" "client_id" {
  key          = "client-id"
  string_value = var.service_principal_client_id
  scope        = databricks_secret_scope.watchdog.name
}

resource "databricks_secret" "client_secret" {
  key          = "client-secret"
  string_value = var.service_principal_client_secret
  scope        = databricks_secret_scope.watchdog.name
}

resource "databricks_secret" "tenant_id" {
  key          = "tenant-id"
  string_value = var.tenant_id
  scope        = databricks_secret_scope.watchdog.name
}

resource "databricks_secret" "subscription_id" {
  key          = "subscription-id"
  string_value = var.subscription_id
  scope        = databricks_secret_scope.watchdog.name
}

# --------------------------------------------------------------------------
# 3. Platform Catalog + Watchdog Schema
# --------------------------------------------------------------------------

resource "databricks_catalog" "platform" {
  name           = var.catalog_name
  comment        = "Platform tooling catalog — Watchdog governance, operational metadata"
  force_destroy  = var.force_destroy
  isolation_mode = "OPEN"
}

resource "databricks_schema" "watchdog" {
  catalog_name  = databricks_catalog.platform.name
  name          = var.schema_name
  comment       = "Watchdog governance scanner — resource inventory, policy evaluations, violations, audit trail"
  force_destroy = var.force_destroy
}

# --------------------------------------------------------------------------
# 4. UC Grants — minimal privilege
# --------------------------------------------------------------------------

resource "databricks_grants" "platform_catalog" {
  catalog = databricks_catalog.platform.name

  grant {
    principal  = databricks_service_principal.watchdog.application_id
    privileges = ["USE_CATALOG"]
  }
}

resource "databricks_grants" "watchdog_schema" {
  schema = "${databricks_catalog.platform.name}.${databricks_schema.watchdog.name}"

  grant {
    principal  = databricks_service_principal.watchdog.application_id
    privileges = ["USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE", "CREATE_FUNCTION"]
  }
}

# Read access on scanned catalogs — Watchdog needs SELECT to crawl resources
resource "databricks_grants" "scanned_catalogs" {
  for_each = toset(var.scanned_catalog_names)
  catalog  = each.value

  grant {
    principal  = databricks_service_principal.watchdog.application_id
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT"]
  }
}
