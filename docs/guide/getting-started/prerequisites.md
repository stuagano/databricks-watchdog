# Prerequisites

## Workspace Requirements

Watchdog requires a Databricks workspace with Unity Catalog enabled. The following resources must exist before deployment:

### Unity Catalog

- **Metastore** attached to the workspace.
- **Catalog** for Watchdog tables (e.g., `my_catalog`). The engine creates tables and views in this catalog.
- **Schema** within the catalog (e.g., `my_catalog.watchdog`). The engine creates all 8 tables and 14 views in this schema.
- **Unity Catalog grants**: the deploying identity needs `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, and `CREATE VIEW` on the target catalog and schema.

### SQL Warehouse

A SQL warehouse (serverless or pro) is required for:

- MCP server queries (statement execution API)
- Genie space operation
- Dashboard queries

The engine itself runs on a job cluster. A warehouse is not required for crawl and evaluate, but is required for the consumer-facing components.

### System Tables

The engine reads from UC system tables during crawl. These must be enabled in the workspace:

- `system.information_schema.tables`
- `system.information_schema.table_privileges` (and other `*_privileges` tables)
- `system.serving.endpoint_usage` (for AI agent governance)
- `system.serving.served_entities` (for agent metadata)

## Service Principal

A service principal is recommended for production deployments. The service principal needs:

| Permission | Scope | Purpose |
|---|---|---|
| `USE CATALOG` | Target catalog | Read/write Watchdog tables |
| `USE SCHEMA` | Target schema | Read/write Watchdog tables |
| `CREATE TABLE` | Target schema | Create Delta tables on first run |
| `CREATE VIEW` | Target schema | Create semantic views |
| `SELECT` on system tables | system catalog | Read information_schema and serving tables |
| Workspace-level access | Workspace | SDK access for jobs, clusters, warehouses, service principals, groups |

For multi-metastore deployments, the service principal needs access to each metastore that Watchdog will scan.

The service principal should **not** have workspace admin entitlements. Watchdog is read-only and does not need administrative privileges.

## Databricks Asset Bundles (DABs)

Watchdog deploys as a Databricks Asset Bundle. Requirements:

- **Databricks CLI** version 0.230.0 or later (with DABs support)
- A `databricks.yml` bundle configuration (provided in the repository)
- Authentication configured for the target workspace (token, OAuth, or OIDC)

Install the CLI:

```bash
# macOS
brew install databricks/tap/databricks

# pip
pip install databricks-cli
```

Verify the version:

```bash
databricks --version
# Must be >= 0.230.0
```

## Python Dependencies

The engine requires Python 3.10+ and the following packages (from `setup.py`):

| Package | Version | Purpose |
|---|---|---|
| `databricks-sdk` | >= 0.30.0 | Workspace API access for crawlers |
| `pyyaml` | >= 6.0 | YAML policy and ontology loading |
| `pyspark` | (provided by Databricks Runtime) | DataFrame operations, Delta writes |

These dependencies are installed automatically when the bundle deploys to a job cluster.

### Optional Dependencies

| Package | Install Extra | Purpose |
|---|---|---|
| `azure-communication-email` | `pip install watchdog[email]` | Email notifications via Azure Communication Services |

## Optional: Notifications

To enable email notifications for violation digests:

- **Azure Communication Services (ACS)**: An ACS resource with a configured sender address. Store the connection string and sender address in a Databricks secret scope:
  - Secret scope: `watchdog` (configurable)
  - Key: `acs_connection_string` -- the ACS connection string
  - Key: `acs_sender_address` -- the sender email address

Without ACS configuration, notifications still write to the `notification_queue` Delta table for consumption by external email pipelines. ACS is an optional direct-send path.

## Optional: Multi-Metastore

For organizations with multiple Unity Catalog metastores, Watchdog can scan all metastores and write results to the same Delta tables with a `metastore_id` discriminator.

Configuration:

- Set the `WATCHDOG_METASTORE_IDS` environment variable (comma-separated metastore IDs), or
- Pass `--metastore-ids` to the `crawl_all_metastores` entrypoint

The service principal must have access to each metastore. Cross-metastore views (`v_cross_metastore_compliance`, `v_cross_metastore_inventory`) aggregate across all scanned metastores.
