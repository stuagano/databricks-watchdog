# CLI Reference

Watchdog provides four entrypoints that run as Databricks Workflow tasks. Each corresponds to a stage in the governance pipeline.

## watchdog-crawl

Discovers all workspace resources and writes them to the `resource_inventory` Delta table.

```bash
python -m watchdog.entrypoints crawl \
  --catalog <catalog> \
  --schema <schema> \
  --secret-scope <scope>
```

**Arguments:**

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--catalog` | Yes | | Unity Catalog name containing Watchdog tables |
| `--schema` | Yes | | Schema name (e.g., `watchdog`) |
| `--secret-scope` | Yes | | Databricks secret scope name |

**Behavior:**

1. Creates a `ResourceCrawler` with a timestamp-based `scan_id`.
2. Runs all crawler methods via `crawl_all()`, each wrapped in `_safe_crawl()` for error isolation.
3. Writes all discovered resources to `resource_inventory` in a single append.
4. Prints per-resource-type counts and any errors.

**Output:**

```
  catalogs: 4 resources (OK)
  schemas: 23 resources (OK)
  tables: 156 resources (OK)
  volumes: 8 resources (OK)
  groups: 12 resources (OK)
  service_principals: 5 resources (OK)
  agents: 18 resources (OK)
  agent_traces: 45 resources (OK)
  grants: 340 resources (OK)
  jobs: 67 resources (OK)
  clusters: 9 resources (OK)
  warehouses: 3 resources (OK)
  pipelines: 4 resources (OK)
  dqm_status: 0 resources (OK)
  lhm_status: 0 resources (OK)
```

---

## watchdog-evaluate

Evaluates all policies against the latest resource inventory and updates the violations table.

```bash
python -m watchdog.entrypoints evaluate \
  --catalog <catalog> \
  --schema <schema> \
  [--sync-policies]
```

**Arguments:**

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--catalog` | Yes | | Unity Catalog name |
| `--schema` | Yes | | Schema name |
| `--sync-policies` | No | false | Sync YAML policies to Delta before evaluating |

**Behavior:**

1. If `--sync-policies` is set, runs `sync_policies_to_delta()` to MERGE YAML policy definitions into the `policies` table.
2. Builds a `PolicyEngine` with ontology, rule engine, and all policies (YAML + user-created).
3. Runs `evaluate_all()`:
   - Pass 1: Classify resources into ontology classes.
   - Pass 2: Evaluate each policy against matching resources.
   - Writes to `scan_results` (append-only) and `violations` (MERGE).
   - Writes to `scan_summary` (append-only).
4. Refreshes all 14 semantic views.

**Output:**

```
Synced 42 policies from YAML to Delta
Watchdog: full mode - ontology (28 classes), rule engine (35 primitives), 42 policies (38 YAML + 4 user)
Ontology: 312 class assignments across 156 resources
Evaluated 42 policies
  Violations: 18 new, 3 resolved
Refreshed semantic views: v_resource_compliance, v_class_compliance, ...
```

---

## watchdog-notify

Sends violation notifications to resource owners via the dual-path notification system.

```bash
python -m watchdog.entrypoints notify \
  --catalog <catalog> \
  --schema <schema> \
  [--secret-scope <scope>] \
  [--dashboard-url <url>]
```

**Arguments:**

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--catalog` | Yes | | Unity Catalog name |
| `--schema` | Yes | | Schema name |
| `--secret-scope` | No | `watchdog` | Secret scope for ACS credentials |
| `--dashboard-url` | No | | Dashboard URL for deep links in emails |

**Behavior:**

1. Queries violations where `status = 'open'`, `notified_at IS NULL`, and `owner IS NOT NULL`.
2. Groups violations into per-owner digests.
3. **Path 1 (always):** Writes digests to `notification_queue` table.
4. **Path 2 (if configured):** Reads `acs_connection_string` and `acs_sender_address` from the secret scope. If present, sends emails via Azure Communication Services.
5. Stamps notified violations with `notified_at = current_timestamp()`.

**Output:**

```
Built digests for 8 owners (23 violations)
Path 1 (Delta queue): 8 entries written to notification_queue
Path 2 (ACS email): 8/8 emails sent
```

If ACS is not configured:

```
Path 2 (ACS email): skipped - acs_connection_string not in secret scope
```

---

## watchdog-adhoc

Ad-hoc scan that runs the full crawl-evaluate cycle and syncs policies. Designed for testing and one-off investigations.

```bash
python -m watchdog.entrypoints adhoc \
  --catalog <catalog> \
  --schema <schema> \
  --secret-scope <scope> \
  [--resource-type <type>] \
  [--resource-id <id>]
```

**Arguments:**

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--catalog` | Yes | | Unity Catalog name |
| `--schema` | Yes | | Schema name |
| `--secret-scope` | Yes | | Databricks secret scope name |
| `--resource-type` | No | `all` | Resource type to scan |
| `--resource-id` | No | | Specific resource to scan |

**Behavior:**

1. Runs `crawl_all()` to discover all workspace resources.
2. Syncs YAML policies to Delta via `sync_policies_to_delta()`.
3. Builds a `PolicyEngine` and runs `evaluate_all()`.
4. Refreshes all semantic views (called twice in current implementation).

**Output:**

```
  catalogs: 4 resources
  schemas: 23 resources
  ...
Synced 42 policies from YAML to Delta
Ontology: 312 classifications
Violations: 18 new, 3 resolved
Refreshed semantic views
```

---

## crawl-all-metastores

Cross-metastore crawler that scans resources across multiple Unity Catalog metastores.

```bash
python -m watchdog.entrypoints crawl_all_metastores \
  --catalog <catalog> \
  --schema <schema> \
  [--secret-scope <scope>] \
  [--metastore-ids <id1,id2,...>]
```

**Arguments:**

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--catalog` | Yes | | Unity Catalog name |
| `--schema` | Yes | | Schema name |
| `--secret-scope` | No | `watchdog` | Databricks secret scope name |
| `--metastore-ids` | No | | Comma-separated metastore IDs (overrides WATCHDOG_METASTORE_IDS env var) |

**Behavior:**

1. Reads metastore IDs from `--metastore-ids` argument or `WATCHDOG_METASTORE_IDS` environment variable.
2. If no IDs are configured, falls back to single-metastore `crawl()`.
3. For each metastore, creates a `ResourceCrawler` with the metastore ID and runs `crawl_all()`.
4. All results write to the same Delta tables with `metastore_id` discriminator.

**Output:**

```
Scanning metastore abc123-def456...
  catalogs: 4 resources (OK)
  ...
  Metastore abc123-def456: 234 resources
Scanning metastore ghi789-jkl012...
  ...
  Metastore ghi789-jkl012: 178 resources
Scanned 2 metastores, 412 resources
```
