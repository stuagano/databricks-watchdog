# `watchdog-deploy` — Push Compiled Artifacts to Workspace

**Status:** Approved
**Date:** 2026-04-21

---

## Problem

The compile-down framework emits runtime enforcement artifacts to disk, but nothing pushes them to the workspace. Tag policies and column masks exist as JSON specs in `compile_output/` — an operator must manually create the UC tag policy or run `ALTER TABLE SET COLUMN MASK`. The deployer closes this gap.

## Goal

A `watchdog-deploy` CLI entrypoint that reads the compile manifest, pushes artifacts to the workspace via platform APIs, records deployment results, and supports dry-run mode. Handles `uc_tag_policy` and `uc_abac` targets. Guardrails artifacts need no deployer (MCP server reads from disk).

## Non-Goals

- **Rollback on failure.** Deployment is one-way, matching the compile-down spec.
- **Guardrails deployment.** The MCP server reads guardrails artifacts from disk at startup — no API call needed.
- **Two-way sync.** Drift is detected and reported, not silently reconciled.

---

## CLI Interface

```
watchdog-deploy --catalog <catalog> --schema <schema> [--dry-run]
```

- `--catalog`, `--schema`: required, same pattern as other entrypoints
- `--dry-run`: resolve targets and build API calls / SQL, but skip execution

## Pipeline

1. Parse args
2. Create `SparkSession` + `WorkspaceClient`
3. Load manifest from `compile_output/manifest.json`
4. Read each artifact's JSON content from `compile_output/`
5. For each artifact, dispatch to the target-specific deployer:
   - `uc_tag_policy` → UC tag policy API (create-or-update)
   - `uc_abac` → resolve `applies_to` via `resource_classifications`, then `ALTER TABLE SET COLUMN MASK`
   - `guardrails` → skip (log "already deployed via disk")
6. Collect `DeployResult` per artifact
7. Update manifest with `deployed_at` per successful artifact
8. Print summary

## Target Deployers

### uc_tag_policy

Reads the artifact JSON (tag_key, policy_type, allowed_values, resource_types, scope) and calls the UC tag policy API. The API is declarative and idempotent — create-or-update semantics.

### uc_abac

1. Read artifact JSON (mask_function, applies_to)
2. Query `resource_classifications` to resolve `applies_to` to concrete tables:
   ```sql
   SELECT DISTINCT resource_id
   FROM {catalog}.{schema}.resource_classifications
   WHERE class_name = '{applies_to}'
   ```
3. For each matched table, get column list from `w.tables.get()`
4. Apply column mask via statement execution:
   ```sql
   ALTER TABLE {table} ALTER COLUMN {column} SET MASK {mask_function}
   ```
5. Requires a prior scan to have populated `resource_classifications`

### Deployer Protocol

```python
@dataclass
class DeployResult:
    artifact_id: str
    target: str
    success: bool
    error: str | None = None
    deployed_at: str | None = None
    details: str = ""  # human-readable action taken
```

Each deployer function returns a `DeployResult`. In dry-run mode, deployers resolve targets and build the action description but skip execution, returning `success=True` and `deployed_at=None`.

## Dry-Run Mode

`--dry-run` logs what would be deployed without making API calls or executing SQL. For each artifact, prints the target, action, and resolved tables (for ABAC). Summary includes `(dry-run)` suffix.

## Error Handling

Deploy all artifacts, collect errors, report at end. One broken artifact does not block others. Summary:

```
Deployed 8/10 artifacts (2 failed). Failures:
  uc_abac/POL-PII-001.json: PERMISSION_DENIED — cannot alter table gold.finance.gl
  uc_tag_policy/POL-STEWARD.json: tag policy API unavailable
```

## Manifest Update

On successful deployment, update the manifest entry with `deployed_at` timestamp. This lets drift detection distinguish "compiled but never deployed" from "compiled and deployed." The manifest update is written back to `compile_output/manifest.json`.

## Changes

| File | Action | Responsibility |
|------|--------|---------------|
| `engine/src/watchdog/deployer.py` | Create | `DeployResult`, `deploy_uc_tag_policy()`, `deploy_uc_abac()`, `deploy_artifacts()` |
| `engine/src/watchdog/entrypoints.py` | Modify | Add `deploy()` entrypoint |
| `engine/setup.py` | Modify | Register `watchdog-deploy` |
| `tests/unit/test_deployer.py` | Create | Tests for deployer logic with mocked SDK |

## Tests

| Test | Asserts |
|------|---------|
| `test_deploy_uc_tag_policy_calls_api` | Correct API call shape (mocked SDK) |
| `test_deploy_uc_tag_policy_dry_run` | No API call made, returns success with details |
| `test_deploy_uc_abac_resolves_tables` | Queries resource_classifications, applies mask per table |
| `test_deploy_uc_abac_no_matching_tables` | Returns success with "no tables matched" detail |
| `test_deploy_uc_abac_dry_run` | No SQL executed, returns action description |
| `test_deploy_artifacts_collects_errors` | Mixed success/failure, all attempted, summary correct |
| `test_deploy_artifacts_skips_guardrails` | Guardrails artifacts logged as skipped |
| `test_deploy_result_dataclass` | Fields populated correctly |
