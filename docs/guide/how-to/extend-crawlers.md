# Extend Crawlers

This guide walks through adding a new resource type to Watchdog's crawler, from implementation through policy integration.

## When to Add a Crawler

Add a new crawler when:

- A resource type exists in the workspace that Watchdog does not currently discover (e.g., dashboards, notebooks, secrets).
- The resource type has governance-relevant properties (tags, owners, configurations) that policies should evaluate.
- The resource type can be enumerated via the Databricks SDK or system tables.

## Implementation Pattern

Every crawler follows the same pattern: enumerate resources from an API or system table, produce inventory rows via `_make_row()`, and return them as a list.

### Step 1: Add the Crawl Method

Add a new method to `ResourceCrawler` in `engine/src/watchdog/crawler.py`:

```python
def _crawl_dashboards(self) -> list:
    """Crawl Lakeview dashboards via the SDK.

    Captures dashboard name, creator, warehouse assignment, and
    publication status. Used by operational governance policies to
    enforce dashboard ownership and warehouse attribution.
    """
    rows = []
    for dash in self.w.lakeview.list():
        tags = {}
        if dash.warehouse_id:
            tags["warehouse_id"] = dash.warehouse_id

        rows.append(self._make_row(
            resource_type="dashboard",
            resource_id=f"dashboard:{dash.dashboard_id}",
            resource_name=dash.display_name or "",
            owner=getattr(dash, "creator", None) or "",
            tags=tags,
            metadata={
                "dashboard_id": dash.dashboard_id or "",
                "warehouse_id": dash.warehouse_id or "",
                "lifecycle_state": str(getattr(dash, "lifecycle_state", "")),
                "created_at": str(getattr(dash, "create_time", "")),
            },
        ))
    return rows
```

Key conventions:

- **resource_type:** A short, lowercase string (e.g., `dashboard`, `notebook`). This becomes the value in the `resource_type` column.
- **resource_id:** A namespaced unique identifier (e.g., `dashboard:<id>`). Must be stable across scans for violation deduplication.
- **resource_name:** A human-readable name shown in dashboards and violation reports.
- **_make_row():** Always use this helper. It stamps `scan_id`, `metastore_id`, and `discovered_at` consistently.
- **tags vs metadata:** Tags are governance-relevant key-value pairs used for ontology classification. Metadata is informational context used by rule evaluators and reports.

### Step 2: Register in crawl_all()

Add the new method to `crawl_all()` using `_safe_crawl()`:

```python
def crawl_all(self) -> list[CrawlResult]:
    # ... existing crawlers ...

    # Workspace resources via SDK
    for crawler_fn in [
        self._crawl_jobs,
        self._crawl_clusters,
        self._crawl_warehouses,
        self._crawl_pipelines,
        self._crawl_dashboards,   # <-- add here
    ]:
        result, rows = self._safe_crawl(crawler_fn)
        results.append(result)
        all_rows.extend(rows)
```

The `_safe_crawl()` wrapper catches all exceptions so one failing crawler never aborts the full scan. Errors are surfaced in the `CrawlResult.errors` list and printed by the entrypoint.

### Step 3: Add Ontology Classes

Add classes for the new resource type in `engine/ontologies/resource_classes.yml`:

```yaml
base_classes:
  # Add if it's a genuinely new base type:
  DashboardAsset:
    description: "Lakeview dashboards"
    matches_resource_types: [dashboard]

derived_classes:
  PublishedDashboard:
    parent: DashboardAsset
    description: "Dashboard in published state"
    classifier:
      metadata_equals:
        lifecycle_state: "ACTIVE"

  OrphanedDashboard:
    parent: DashboardAsset
    description: "Dashboard with no assigned warehouse"
    classifier:
      none_of:
        - tag_exists: [warehouse_id]
```

Alternatively, if the resource fits under an existing base class, add it to `matches_resource_types`:

```yaml
base_classes:
  ComputeAsset:
    description: "Any compute resource"
    matches_resource_types: [job, cluster, warehouse, pipeline, dashboard]
```

### Step 4: Add Policies

Create policies targeting the new classes:

```yaml
- id: POL-DASH-001
  name: "Dashboards must have an owner"
  applies_to: DashboardAsset
  domain: OperationalGovernance
  severity: medium
  description: "Unowned dashboards become stale and misleading"
  remediation: "Set the dashboard owner in the Lakeview settings"
  active: true
  rule:
    ref: has_owner
```

### Step 5: Update the Fallback Map

If the ontology files might not be present (MVP mode), add the new resource type to the `_CLASS_TYPE_FALLBACK` map in `engine/src/watchdog/policy_engine.py`:

```python
_CLASS_TYPE_FALLBACK: dict[str, set[str]] = {
    # ... existing entries ...
    "DashboardAsset": {"dashboard"},
    "PublishedDashboard": {"dashboard"},
}
```

### Step 6: Test

Run an ad-hoc scan and verify the new resources appear:

```sql
-- Check resource count
SELECT resource_type, COUNT(*) as count
FROM platform.watchdog.resource_inventory
WHERE scan_id = (SELECT MAX(scan_id) FROM platform.watchdog.resource_inventory)
  AND resource_type = 'dashboard'
GROUP BY resource_type

-- Check classification
SELECT class_name, COUNT(*) as count
FROM platform.watchdog.resource_classifications
WHERE scan_id = (SELECT MAX(scan_id) FROM platform.watchdog.resource_classifications)
  AND class_name LIKE '%Dashboard%'
GROUP BY class_name

-- Check violations
SELECT policy_id, COUNT(*) as count
FROM platform.watchdog.violations
WHERE resource_type = 'dashboard'
  AND status = 'open'
GROUP BY policy_id
```

## Error Handling

The `_safe_crawl()` wrapper ensures that individual crawler failures do not cascade. If `_crawl_dashboards()` raises an exception:

- The error is captured in `CrawlResult.errors`.
- An empty row list is returned.
- Other crawlers continue normally.
- The entrypoint prints the error but the scan completes.

For expected partial failures within a crawler (e.g., an API that may not be available on all workspaces), use a try/except inside the method and continue with partial results:

```python
def _crawl_dashboards(self) -> list:
    rows = []
    try:
        dashboards = self.w.lakeview.list()
    except Exception as e:
        print(f"  Dashboard crawl not available: {e}")
        return rows
    # ... process dashboards ...
    return rows
```

## Currently Crawled Resource Types

For reference, the crawler currently discovers these resource types:

| Resource Type | Source | Crawler Method |
|--------------|--------|---------------|
| `catalog` | SDK (catalogs.list) | `_crawl_catalogs` |
| `schema` | SDK (schemas.list) | `_crawl_schemas` |
| `table` | information_schema | `_crawl_tables` |
| `volume` | information_schema | `_crawl_volumes` |
| `grant` | information_schema + SDK | `_crawl_grants` |
| `group` | SDK (groups.list) | `_crawl_groups` |
| `service_principal` | SDK (service_principals.list) | `_crawl_service_principals` |
| `agent` | Apps API + serving endpoints | `_crawl_agents` |
| `agent_execution` | system.serving.endpoint_usage | `_crawl_agent_traces` |
| `job` | SDK (jobs.list) | `_crawl_jobs` |
| `cluster` | SDK (clusters.list) | `_crawl_clusters` |
| `warehouse` | SDK (warehouses.list) | `_crawl_warehouses` |
| `pipeline` | SDK (pipelines.list) | `_crawl_pipelines` |

Additionally, two enrichment crawlers update existing inventory rows with data quality metadata:

| Enrichment | Source | Method |
|-----------|--------|--------|
| DQM status | system.data_quality_monitoring | `_crawl_dqm_status` |
| LHM status | Profile table detection | `_crawl_lhm_status` |
