# Drift Detection: `drift_check` Rule Type

**Status:** Design approved
**Date:** 2026-04-15
**Cycle:** 2 of 3 (Hub Integration → Drift Detection → Remediation Agents)

---

## Problem

Watchdog evaluates governance posture by checking resource properties (tags, metadata) against declarative rules. But some governance scenarios require comparing actual state against a **declared expected state** maintained outside Watchdog — for example, a permissions-as-code system that defines what grants should exist.

The architecture guide defines the drift detection pattern and expected state JSON schema, but the `drift_check` rule type is not yet implemented in the rule engine.

## Goals

- Add `drift_check` as a new rule type in the rule engine dispatch table
- Keep the rule engine pure Python (no Spark, no file access)
- Inject expected state into resource metadata via the existing pattern (like owner, resource_type)
- Support grants drift detection in v1; row_filters, column_masks, group_membership in future versions
- Produce violations through the same pipeline as every other rule type
- Test with synthetic expected state (no external producer required yet)

## Non-Goals

- Building an external system that produces expected state files
- Row filter, column mask, or group membership drift (v2)
- Drift remediation (that's the Remediation Agents cycle)
- UI for expected state management

---

## Design

### 1. The `drift_check` Rule Type

Added to the rule engine dispatch table at `rule_engine.py:75` alongside existing types.

**Rule YAML format:**

```yaml
rule:
  type: drift_check
  check: grants    # which section of expected state to compare
```

**What it expects in metadata (injected by the policy engine before evaluation):**

- `metadata["expected_grants"]` — JSON string of expected grant entries for this resource. Each entry has `catalog`, `schema`, `table` (nullable), `principal`, and `privileges` (list of strings).
- The grant resource's actual state is already in metadata from the grants crawler: `metadata["privilege"]`, `metadata["grantee"]`, `metadata["securable_type"]`, etc.

**Evaluation logic:**

1. If `expected_grants` is absent or empty in metadata, return PASS (no declared expectation = no drift, vacuously true)
2. Parse `expected_grants` from JSON string to list of dicts
3. Find expected entries matching this resource's grantee/securable
4. Compare expected privileges against actual privilege
5. Return FAIL with detail listing extra or missing privileges
6. Return PASS if actual matches expected

**RuleResult detail examples:**

- `FAIL: "Drift detected: grant 'MODIFY' on gold.finance.gl_balances for finance-analysts is not in expected state"`
- `FAIL: "Drift detected: expected grant 'SELECT' on gold.finance.gl_balances for finance-analysts not found in actual grants"`
- `PASS` (actual matches expected, or no expected state for this resource)

### 2. Expected State Loader

A new module `engine/src/watchdog/drift.py` provides a helper to load expected state from UC volumes.

**Function:** `load_expected_state(spark, catalog, schema, source_path) -> dict`

- `source_path` comes from the policy YAML's `rule.source` field (e.g., `expected_permissions/expected_state.json`)
- Reads from UC volume: `{catalog}.{schema}.{volume_name}/expected_state.json` where volume_name and file path are parsed from `source_path`
- Returns the parsed JSON dict, or empty dict on file-not-found (with a logged warning)

**Expected state JSON schema (from architecture guide):**

```json
{
  "generated_at": "2026-04-14T10:00:00Z",
  "environment": "production",
  "grants": [
    {
      "catalog": "gold",
      "schema": "finance",
      "table": null,
      "principal": "finance-analysts",
      "privileges": ["SELECT", "USE_CATALOG", "USE_SCHEMA"]
    }
  ]
}
```

### 3. Metadata Injection in the Policy Engine

The policy engine (`evaluate_all` in `policy_engine.py`) already injects `owner` and `resource_type` into resource metadata before calling the rule engine. Expected state injection follows the same pattern.

**Added between Pass 1 (classification) and Pass 2 (evaluation):**

1. Scan active policies for any with `rule.type == "drift_check"` and a `rule.source` field
2. For each unique `source`, call `load_expected_state()` once
3. Build a lookup from the grants section, keyed by `(catalog, schema, table, principal)`
4. During the evaluation loop, for each grant resource, check the lookup and inject `metadata["expected_grants"]` if a match exists

**Fallback behavior:**

- Expected state file doesn't exist → warning logged, no injection, drift_check passes vacuously
- File exists but no `grants` section → no injection, drift_check passes
- Grant resource has no matching expected entry → no injection, drift_check passes
- Multiple expected entries for the same principal/securable → all are injected as a JSON array

### 4. Sample Drift Policy

**File:** `engine/policies/drift_detection.yml`

```yaml
policies:
  - id: POL-DRIFT-001
    name: "Grant drift detection"
    applies_to: GrantAsset
    domain: AccessControl
    severity: critical
    description: "Detected grants that differ from the declared expected state. This indicates unauthorized manual grant changes."
    remediation: "Review the drift and either update the expected state file or revoke the unauthorized grant."
    active: true
    rule:
      type: drift_check
      source: expected_permissions/expected_state.json
      check: grants
```

**Policy namespace:** External systems use `POL-DRIFT-*` prefix to avoid collisions with Watchdog's built-in `POL-A*`, `POL-AGENT-*` policies (per architecture guide convention).

### 5. Tests

**File:** `tests/unit/test_drift.py`

Pure Python tests (no Spark), following the existing pattern in `test_rule_engine.py`.

**drift_check rule type tests:**

- Pass: actual privilege matches expected
- Fail: extra privilege not in expected state (detail includes the extra privilege)
- Fail: expected privilege not found in actual grants (detail includes the missing privilege)
- Pass: no `expected_grants` in metadata (vacuously true)
- Pass: empty expected grants list
- Handles malformed expected_grants JSON gracefully (returns FAIL with parse error detail)

**load_expected_state tests:**

- Returns parsed dict from valid JSON string
- Returns empty dict when source path references nonexistent volume
- Returns dict with empty grants list when grants section is missing

---

## Files Changed & Created

| Action | File | What |
|---|---|---|
| Edit | `engine/src/watchdog/rule_engine.py` | Add `drift_check` to dispatch table + `_eval_drift_check` method |
| Create | `engine/src/watchdog/drift.py` | `load_expected_state()` helper for reading UC volume JSON |
| Edit | `engine/src/watchdog/policy_engine.py` | Inject expected state into grant resource metadata before evaluation |
| Create | `engine/policies/drift_detection.yml` | Sample grant drift policy (POL-DRIFT-001) |
| Create | `tests/unit/test_drift.py` | Unit tests for drift_check rule type and expected state loader |

## Out of Scope

- Row filter, column mask, group membership drift (future versions)
- External producers of expected state files
- Drift remediation (Cycle 3: Remediation Agents)
- UI for managing expected state
- Changes to the crawler, views, MCP tools, or ontos-adapter
