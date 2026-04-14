# Cross-Metastore Scanning

This guide covers configuring Watchdog to scan resources across multiple Unity Catalog metastores from a single deployment.

## When to Use

Multi-metastore scanning is needed when:

- The organization operates separate metastores for different regions, business units, or environments.
- A single compliance dashboard must show violations across all metastores.
- Policies and notifications should be centralized while data sovereignty is maintained.

## Configuration

### Environment Variable

Set the `WATCHDOG_METASTORE_IDS` environment variable with a comma-separated list of metastore IDs:

```bash
export WATCHDOG_METASTORE_IDS="abc123-def456,ghi789-jkl012,mno345-pqr678"
```

In a Databricks Workflow, set this as a task-level environment variable or cluster-level Spark configuration.

### CLI Argument

The `--metastore-ids` argument overrides the environment variable:

```bash
python -m watchdog.entrypoints crawl_all_metastores \
  --catalog platform \
  --schema watchdog \
  --secret-scope watchdog \
  --metastore-ids "abc123-def456,ghi789-jkl012"
```

### Fallback

When no metastore IDs are configured (neither environment variable nor CLI argument), `crawl_all_metastores` falls back to a single-metastore crawl using the workspace's current metastore.

## How the Metastore Discriminator Works

All metastores write to the same Delta tables (`resource_inventory`, `violations`, `policies`, etc.). The `metastore_id` column discriminates which metastore each row belongs to.

### Write Path

The `ResourceCrawler` stamps every inventory row with the metastore ID. When scanning multiple metastores, the `crawl_all_metastores` entrypoint creates a separate `ResourceCrawler` instance for each metastore ID and runs them sequentially:

```python
for metastore_id in metastore_ids:
    crawler = ResourceCrawler(spark, w, catalog, schema, metastore_id=metastore_id)
    results = crawler.crawl_all()
```

Each crawler instance uses the same `scan_id` timestamp format but produces distinct inventory rows tagged with its metastore ID.

### Read Path

All MCP tools, views, and queries support an optional `metastore` filter parameter. When provided, queries include a `WHERE metastore_id = '<id>'` clause. When omitted, results span all metastores.

## Cross-Metastore Views

Watchdog creates two cross-metastore views that aggregate compliance posture across metastores:

### v_cross_metastore_compliance

Compliance summary per metastore: total resources, open violations by severity, compliance percentage. Use this for the executive overview showing which metastores need attention.

### v_cross_metastore_inventory

Resource inventory per metastore: resource counts by type, latest scan timestamp. Use this to verify that all metastores are being scanned.

## Multi-Metastore Notifications

The notification queue includes a `metastore_id` column. When consuming notifications, enterprise email systems can route alerts based on metastore to different teams or distribution lists.

## Operational Considerations

### Sequential Scanning

The current implementation scans metastores sequentially. For deployments with five or more metastores, consider parallelizing into separate Databricks Workflow tasks (one task per metastore) that all write to the same Delta tables.

### Metastore Detection

When the `metastore_id` override is provided, the crawler uses it directly. When not provided, it auto-detects the current workspace's metastore via `w.metastores.current()`. Auto-detection only works for the workspace's default metastore; cross-metastore scanning always requires explicit IDs.

### State Isolation

Despite sharing tables, metastore data is logically isolated by the `metastore_id` column:

- Violations are scoped to their metastore during MERGE (the merge key includes `metastore_id`).
- Resolving a violation in one metastore does not affect the same resource in another.
- Policies are global (not metastore-scoped) unless explicitly filtered.
