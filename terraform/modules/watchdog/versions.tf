terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "1.110.0"
    }
  }
  required_version = ">= 1.5.0"
}
