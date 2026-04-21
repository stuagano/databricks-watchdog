# CLI Reference

Watchdog provides ten entrypoints that run as Databricks Workflow tasks. Each corresponds to a stage in the governance pipeline.

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
4. Refreshes all 14 compliance views.

**Output:**

```
Synced 42 policies from YAML to Delta
Watchdog: full mode - ontology (28 classes), rule engine (35 primitives), 42 policies (38 YAML + 4 user)
Ontology: 312 class assignments across 156 resources
Evaluated 42 policies
  Violations: 18 new, 3 resolved
Refreshed compliance views: v_resource_compliance, v_class_compliance, ...
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
4. Refreshes all compliance views (called twice in current implementation).

**Output:**

```
  catalogs: 4 resources
  schemas: 23 resources
  ...
Synced 42 policies from YAML to Delta
Ontology: 312 classifications
Violations: 18 new, 3 resolved
Refreshed compliance views
```

---

## Compile-Down Pipeline

### watchdog-compile

Compiles policies with `compile_to` blocks into runtime enforcement artifacts (UC tag policies, ABAC column masks, guardrails configs). Writes artifacts and manifest to the `compile_output/` directory, then runs drift detection on compiled artifacts.

```bash
python -m watchdog.entrypoints compile \
  --catalog <catalog> \
  --schema <schema>
```

**Arguments:**

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--catalog` | Yes | | Unity Catalog name |
| `--schema` | Yes | | Schema name |

**Behavior:**

1. Loads all policies (YAML + user-created from Delta).
2. Filters to policies with `compile_to` blocks.
3. Runs `compile_policies()` to emit artifacts for each target (UC tag policies, ABAC column masks, guardrails configs).
4. Writes artifacts and a `manifest.json` to the `compile_output/` directory.
5. Runs `check_drift()` against the manifest to detect in-sync, drifted, or missing artifacts.
6. Prints a compile summary.

**Output:**

```
Loaded 42 policies (6 with compile_to)
Compiled 6 policies -> 12 artifacts (4 uc_tag_policy, 4 uc_abac, 4 guardrails). Drift: 10 in_sync, 1 drifted, 1 missing.
```

---

### watchdog-deploy

Deploys compiled artifacts to the workspace. Reads the compile manifest, pushes each artifact to its target platform substrate (UC tag policies, ABAC column masks). Guardrails artifacts are skipped (deployed via disk).

```bash
python -m watchdog.entrypoints deploy \
  --catalog <catalog> \
  --schema <schema> \
  [--dry-run]
```

**Arguments:**

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--catalog` | Yes | | Unity Catalog name |
| `--schema` | Yes | | Schema name |
| `--dry-run` | No | false | Resolve targets but skip execution |

**Behavior:**

1. Reads the compile manifest from `compile_output/manifest.json`. Exits if no manifest found.
2. Loads artifact content from the compile output directory.
3. Calls `deploy_artifacts()` to push each artifact to its target platform substrate.
4. In `--dry-run` mode, resolves targets and reports what would be deployed without executing.
5. Prints per-artifact status and a summary with success/failure counts.

**Output:**

```
Deploying 12 artifacts...
  [OK] uc_tag_policy_pii_tagging: applied to catalog.schema
  [OK] uc_abac_pii_mask: column mask created
  [FAIL] guardrails_toxicity: skipped (disk-only)
Deployed 10/12 artifacts (2 failed).
```

With `--dry-run`:

```
(dry-run) Deploying 12 artifacts...
  [OK] uc_tag_policy_pii_tagging: would apply to catalog.schema
Deployed 12/12 artifacts (0 failed) (dry-run).
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

---

## Remediation Pipeline

### watchdog-remediate

Dispatches open violations to remediation agents. Reads violations with `status='open'`, dispatches each to the first agent whose `handles[]` matches its `policy_id`, and writes new proposals to `remediation_proposals` in status `pending_review`. Idempotent -- a violation that already has a proposal from the same agent version is skipped.

```bash
python -m watchdog.entrypoints remediate \
  --catalog <catalog> \
  --schema <schema> \
  [--limit <n>]
```

**Arguments:**

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--catalog` | Yes | | Unity Catalog name |
| `--schema` | Yes | | Schema name |
| `--limit` | No | `500` | Max open violations to consider in one run |

**Behavior:**

1. Ensures `remediation_agents` and `remediation_proposals` tables exist.
2. Registers all known agents (StewardAgent, ClusterTaggerAgent, DQMonitorScaffoldAgent, JobOwnerAgent).
3. Reads up to `--limit` open violations ordered by severity.
4. Loads existing proposal keys for idempotency checks.
5. Dispatches each violation to the first matching agent via `dispatch_remediations()`.
6. Writes new proposals to `remediation_proposals` table.
7. Refreshes remediation views.

**Output:**

```
Remediate: considered 45 violations -- dispatched 32 proposals, skipped 10, errors 3
```

---

### watchdog-apply

Applies approved remediation proposals by executing proposed SQL. Reads proposals with `status='approved'`, executes the SQL via Spark, and records each application in `remediation_applied`. Proposal status flips to `applied`.

```bash
python -m watchdog.entrypoints apply_approved_remediations \
  --catalog <catalog> \
  --schema <schema> \
  [--dry-run] \
  [--limit <n>]
```

**Arguments:**

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--catalog` | Yes | | Unity Catalog name |
| `--schema` | Yes | | Schema name |
| `--dry-run` | No | false | Preview what would be applied without executing SQL |
| `--limit` | No | `100` | Max approved proposals to apply in one run |

**Behavior:**

1. Ensures `remediation_applied` table exists.
2. Reads up to `--limit` approved proposals ordered by `created_at`.
3. For each proposal, calls `apply_proposal()` to execute the proposed SQL.
4. Records each application in `remediation_applied` with pre/post state and verify status.
5. Flips proposal status to `applied` (skipped in `--dry-run` mode).

**Output:**

```
Remediate-apply (applied): 18 proposals, 1 errors
```

With `--dry-run`:

```
Remediate-apply (dry-run): 18 proposals, 0 errors
```

---

### watchdog-verify

Verifies applied remediation proposals against the latest scan. Reads `remediation_applied` rows with `verify_status='pending'` and checks whether the corresponding violations resolved. Sets `verify_status` to `verified` or `verification_failed` and flips proposal status to `verified` when the violation is gone.

```bash
python -m watchdog.entrypoints verify_remediations \
  --catalog <catalog> \
  --schema <schema>
```

**Arguments:**

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--catalog` | Yes | | Unity Catalog name |
| `--schema` | Yes | | Schema name |

**Behavior:**

1. Reads `remediation_applied` rows where `verify_status = 'pending'`.
2. Looks up the corresponding `violation_id` for each proposal.
3. Checks whether those violations now have `status = 'resolved'` in the violations table.
4. Updates `verify_status` to `verified` or `verification_failed`.
5. Flips proposal status to `verified` for resolved violations.

**Output:**

```
Verify: 14 verified, 2 failed
```
