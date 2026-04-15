# Troubleshooting

Common issues with Watchdog and their solutions.

## 1. Zero Resources After Crawl

**Problem:** The crawl completes without errors but `resource_inventory` has zero rows for the latest scan.

**Cause:** The WorkspaceClient has no permissions to enumerate resources, or the UC metastore is not accessible from the compute. The `_safe_crawl()` wrapper catches exceptions silently; errors appear in `CrawlResult.errors` but an empty inventory still gets committed.

**Fix:**
1. Check the entrypoint output for error lines (e.g., `catalogs: 0 resources (ERROR: ...)`).
2. Verify the cluster or serverless compute has UC metastore access.
3. Verify the service principal or user running the job has `USE CATALOG` on at least one catalog and `LIST` on the Apps API.
4. Run a manual SDK check from a notebook:
   ```python
   from databricks.sdk import WorkspaceClient
   w = WorkspaceClient()
   print(list(w.catalogs.list()))
   ```

---

## 2. Resources Crawled but Not Classified

**Problem:** Resources appear in `resource_inventory` but the `resource_classifications` table has no entries for them.

**Cause:** The resources do not have the tags required by any derived class classifier. Base class assignment is automatic (based on `resource_type`), but derived classes require specific tag values. If a table has no `data_classification` tag, it will not be classified as `PiiAsset`, `ConfidentialAsset`, etc.

**Fix:**
1. Check the resource's tags:
   ```sql
   SELECT resource_name, tags
   FROM platform.watchdog.resource_inventory
   WHERE scan_id = (SELECT MAX(scan_id) FROM platform.watchdog.resource_inventory)
     AND resource_name = 'gold.finance.transactions'
   ```
2. Compare the tags against the classifier conditions in `engine/ontologies/resource_classes.yml`.
3. Add the required tags to the resource:
   ```sql
   ALTER TABLE gold.finance.transactions
   SET TAGS ('data_classification' = 'confidential', 'data_layer' = 'gold')
   ```
4. Re-run the scan. The resource should now appear in `resource_classifications`.

---

## 3. Policies Not Evaluating

**Problem:** Policies exist in the `policies` table but produce no scan results or violations.

**Cause:** The policy's `applies_to` class does not match any classified resources. If a policy targets `HipaaAsset` but no resources have been classified into that class, the policy evaluates against zero resources.

**Fix:**
1. Check which classes have resources:
   ```sql
   SELECT class_name, COUNT(*) as count
   FROM platform.watchdog.resource_classifications
   WHERE scan_id = (SELECT MAX(scan_id) FROM platform.watchdog.resource_classifications)
   GROUP BY class_name
   ORDER BY count DESC
   ```
2. Verify the policy's `applies_to` value appears in the list above.
3. If the class has no resources, the issue is classification (see issue 2 above).
4. If the policy uses `applies_to: "*"`, verify the policy is `active = true`.

---

## 4. Violations Not Resolving

**Problem:** A resource's tags have been fixed but the violation remains `open` after a re-scan.

**Cause:** The evaluate step reads the latest scan's resource inventory. If the crawl step ran before the tag was fixed, the inventory still reflects the old state. The violation resolves only when a scan discovers the resource with the corrected tags.

**Fix:**
1. Verify the tag was actually set on the resource (not just planned):
   ```sql
   SELECT tags FROM platform.watchdog.resource_inventory
   WHERE scan_id = (SELECT MAX(scan_id) FROM platform.watchdog.resource_inventory)
     AND resource_id = '<resource_id>'
   ```
2. If the tags are still old, run a fresh crawl + evaluate cycle:
   ```bash
   python -m watchdog.entrypoints adhoc --catalog platform --schema watchdog --secret-scope watchdog
   ```
3. If the tags are correct in the latest scan but the violation persists, check if the rule checks metadata (not tags). Some rules check `metadata` fields that come from the SDK, not UC tags.

---

## 5. Compliance Views Not Refreshing

**Problem:** Dashboard queries return stale data even after a successful evaluate run.

**Cause:** Views are created or replaced by `ensure_semantic_views()` at the end of the evaluate step. If the evaluate step fails partway through (e.g., OOM during `evaluate_all()`), the views are never refreshed.

**Fix:**
1. Check the evaluate entrypoint output for errors.
2. Views can be refreshed independently by calling `ensure_semantic_views()` from a notebook:
   ```python
   from watchdog.views import ensure_semantic_views
   ensure_semantic_views(spark, "platform", "watchdog")
   ```
3. Since views are regular (not materialized), they always reflect the current table state. If the underlying tables are current but the view returns old data, the issue is likely browser or dashboard caching.

---

## 6. Watchdog MCP Returns Empty Results

**Problem:** MCP tools like `get_violations` or `get_governance_summary` return empty results even though violations exist.

**Cause:** The MCP server runs SQL via the Statement Execution API as the calling user. If the user does not have `SELECT` grants on the `platform.watchdog` schema, queries return empty results (not an error).

**Fix:**
1. Verify the user has access:
   ```sql
   SHOW GRANTS ON SCHEMA platform.watchdog
   ```
2. Grant read access:
   ```sql
   GRANT USE_SCHEMA ON SCHEMA platform.watchdog TO `analytics-team`;
   GRANT SELECT ON SCHEMA platform.watchdog TO `analytics-team`;
   ```
3. If using multi-metastore, check whether the `metastore` parameter is filtering to a metastore with no data. Try omitting the parameter to query across all metastores.

---

## 7. Notifications Not Sending

**Problem:** The notify entrypoint runs but reports "No un-notified violations."

**Cause:** Either all open violations have already been notified (`notified_at IS NOT NULL`), or the violations have no `owner` value. The notification pipeline only processes violations where `status = 'open'`, `notified_at IS NULL`, and `owner IS NOT NULL`.

**Fix:**
1. Check for un-notified violations:
   ```sql
   SELECT COUNT(*), owner
   FROM platform.watchdog.violations
   WHERE status = 'open' AND notified_at IS NULL
   GROUP BY owner
   ```
2. If the count is zero but violations exist, they have already been notified. New violations from the next scan will trigger fresh notifications.
3. If violations exist but `owner` is NULL or empty, the crawler did not capture an owner. Set owners on the source resources in UC.
4. For ACS email issues, check that `acs_connection_string` and `acs_sender_address` are set in the secret scope and the `azure-communication-email` package is installed.

---

## 8. Multi-Metastore Scan Fails

**Problem:** `crawl_all_metastores` fails or produces partial results.

**Cause:** The compute may not have network access to all metastores, or the service principal lacks permissions in some metastores.

**Fix:**
1. Check the output for per-metastore error messages.
2. Verify that the metastore IDs in `WATCHDOG_METASTORE_IDS` or `--metastore-ids` are correct UUIDs.
3. Test each metastore individually:
   ```python
   from databricks.sdk import WorkspaceClient
   w = WorkspaceClient()
   summary = w.metastores.current()
   print(f"Current metastore: {summary.metastore_id}")
   ```
4. If some metastores are unreachable, remove them from the configuration and scan them from a different workspace that has access.

---

## 9. Stale Dashboard Data

**Problem:** The Lakeview dashboard shows data from old scans or does not reflect recent violations.

**Cause:** Dashboard queries typically filter on `scan_id = (SELECT MAX(scan_id) FROM ...)`. If the crawl ran but the evaluate did not, the latest scan_id in `resource_inventory` differs from the latest in `scan_results`.

**Fix:**
1. Verify that crawl and evaluate ran in sequence:
   ```sql
   SELECT MAX(scan_id) FROM platform.watchdog.resource_inventory;
   SELECT MAX(scan_id) FROM platform.watchdog.scan_results;
   ```
   These should match (or be close in time).
2. If only the crawl ran, execute the evaluate step.
3. Check that compliance views exist and are current:
   ```sql
   SHOW VIEWS IN platform.watchdog LIKE 'v_*'
   ```
4. If the dashboard is a published Lakeview dashboard, hard-refresh the browser page.

---

## 10. Version Comparison Returns Unexpected Results

**Problem:** The `metadata_gte` rule type (used for runtime version policies) produces incorrect pass/fail results.

**Cause:** The `metadata_gte` evaluator extracts leading numeric parts from version strings for comparison. It handles Databricks runtime formats like `15.4.x-scala2.12` but may behave unexpectedly with non-standard version strings or empty values.

**Fix:**
1. Check the actual metadata value:
   ```sql
   SELECT resource_name, metadata['spark_version'] as version
   FROM platform.watchdog.resource_inventory
   WHERE scan_id = (SELECT MAX(scan_id) FROM platform.watchdog.resource_inventory)
     AND resource_type = 'cluster'
   ```
2. The version comparison extracts up to three numeric parts: `15.4.x-scala2.12` becomes `(15, 4)`. The threshold `15.4` becomes `(15, 4)`. Comparison is tuple-based: `(15, 4) >= (15, 4)` passes.
3. If the metadata field is empty, the rule always fails with "field is empty."
4. If the version string has no numeric parts (e.g., `custom-runtime`), the evaluator falls back to lexicographic string comparison.
5. For non-standard runtime formats, consider using `metadata_matches` with a regex pattern instead of `metadata_gte`.
