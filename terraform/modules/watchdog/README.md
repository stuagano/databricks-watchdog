# Watchdog Terraform Module

Provisions the Databricks infrastructure required to run the Watchdog governance scanner.

## What This Module Creates

- **Service principal** registered in the Databricks workspace
- **Secret scope** (`watchdog`) storing the SP's client ID, client secret, tenant ID, and subscription ID
- **Platform catalog** (default: `platform`) with a **watchdog schema** for scanner output tables
- **UC grants** giving the SP minimal privileges: write access on the watchdog schema, read access on scanned catalogs

## Azure-Specific Scope

This module stores Azure AD credentials (`tenant_id`, `subscription_id`) in the secret scope. These are required for the Watchdog job to authenticate as the service principal. If you are running on a non-Azure cloud, you will need to adapt the secret scope entries.

## Usage

```hcl
module "watchdog" {
  source = "./modules/watchdog"

  service_principal_client_id     = var.watchdog_sp_client_id
  service_principal_client_secret = var.watchdog_sp_client_secret
  tenant_id                       = var.tenant_id
  subscription_id                 = var.subscription_id

  catalog_name          = "platform"
  schema_name           = "watchdog"
  scanned_catalog_names = ["analytics", "raw", "curated"]
  force_destroy         = false
}
```

## Scanned Catalogs

The `scanned_catalog_names` variable controls which Unity Catalog catalogs Watchdog can crawl. The module grants `USE_CATALOG`, `USE_SCHEMA`, and `SELECT` on each listed catalog so the scanner can read resource metadata and table schemas. Add or remove catalog names as your environment changes.
