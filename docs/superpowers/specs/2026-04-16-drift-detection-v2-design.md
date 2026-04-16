# Drift Detection v2: Row Filters, Column Masks, Group Membership

**Status:** Design approved
**Date:** 2026-04-16
**Builds on:** `docs/superpowers/specs/2026-04-15-drift-detection-design.md` (grants, v1)

---

## Problem

Drift detection v1 supports one check type: `grants`. The architecture guide identified three more: `row_filters`, `column_masks`, and `group_membership`. These cover the remaining ABAC and identity drift scenarios that grant-level detection misses — a permissions-as-code system may declare the correct row filters and column masks alongside grants, and unauthorized manual edits to either go undetected today.

## Goals

- Add `row_filters`, `column_masks`, and `group_membership` as first-class `drift_check` types
- Follow the grants pattern exactly: new crawler → new resource type → lookup builder → policy engine injection → rule engine evaluator
- Make the expected state JSON format compatible with OPA bundle data format (interoperability, no OPA runtime dependency)
- Keep the rule engine pure Python; keep `drift.py` responsible for all loading and lookup logic

## Non-Goals

- Checksum / UDF body verification (future — function name matching only in v2)
- OPA as the evaluation engine (future direction — see below)
- Row filter / column mask creation or modification (Watchdog is read-only)
- Group membership management

---

## Design

### 1. New Resource Types

Three new resource types, each following the grants pattern.

#### `row_filter`

Crawled from `{catalog}.information_schema.row_filters`.

```python
resource_type = "row_filter"
resource_id   = "row_filter:{catalog}.{schema}.{table}"
resource_name = "{table}"
metadata = {
    "table_full_name":  "gold.finance.gl_balances",
    "filter_function":  "gold.security.permissions_filter_gl_balances",
}
```

#### `column_mask`

Crawled from `{catalog}.information_schema.column_masks`.

```python
resource_type = "column_mask"
resource_id   = "column_mask:{catalog}.{schema}.{table}.{column}"
resource_name = "{table}.{column}"
metadata = {
    "table_full_name": "gold.finance.gl_balances",
    "column_name":     "cost_center_owner",
    "mask_function":   "gold.security.permissions_mask_cost_center",
}
```

#### `group_member`

Crawled by extending `_crawl_groups()` to emit one resource per member (members are already fetched via `w.groups.list(attributes="...,members,...")`).

```python
resource_type = "group_member"
resource_id   = "group_member:{group_name}:{member_value}"
resource_name = "{group_name}"
metadata = {
    "group_name":   "finance-analysts",
    "member_value": "user@company.com",
    "member_type":  "user",   # "user" | "service_principal" | "group"
}
```

### 2. New Ontology Classes

Added to `engine/ontologies/ontology.yml`:

```yaml
- name: RowFilterAsset
  kind: base
  description: A Unity Catalog row filter applied to a table
  matches_resource_types: [row_filter]

- name: ColumnMaskAsset
  kind: base
  description: A Unity Catalog column mask applied to a table column
  matches_resource_types: [column_mask]

- name: GroupMemberAsset
  kind: base
  description: A member of a workspace or account-level group
  matches_resource_types: [group_member]
```

### 3. Expected State JSON — Extended Schema

The existing schema gains three new sections. The full schema is OPA bundle `data.json` compatible (see OPA Interoperability below).

```json
{
  "generated_at": "2026-04-16T10:00:00Z",
  "environment": "production",
  "grants": [...],
  "row_filters": [
    {
      "table":    "gold.finance.gl_balances",
      "function": "gold.security.permissions_filter_gl_balances"
    }
  ],
  "column_masks": [
    {
      "table":    "gold.finance.gl_balances",
      "column":   "cost_center_owner",
      "function": "gold.security.permissions_mask_cost_center"
    }
  ],
  "group_membership": [
    {
      "group":   "finance-analysts",
      "members": ["user@company.com", "svc-etl@company.com"]
    }
  ]
}
```

### 4. OPA Bundle Compatibility

`load_expected_state()` is extended to support OPA bundle tarballs alongside plain JSON files:

| Source path | Behavior |
|---|---|
| `*.json` | Read directly (existing behavior, unchanged) |
| `*.tar.gz` | Extract archive, read `data.json` from root |

No OPA runtime dependency. Python stdlib `tarfile` module only.

An optional `data_path` field in the policy YAML handles systems that nest their data under a namespace (e.g., a bundle whose `data.json` has `{"permissions": {"grants": [...]}}`):

```yaml
rule:
  type: drift_check
  source: expected_permissions/bundle.tar.gz
  check: grants
  data_path: permissions   # navigate to this key before reading sections
```

If `data_path` is absent, sections are read from the root (no change for existing policies).

### 5. New Lookup Builders in `drift.py`

```python
def build_expected_row_filters_lookup(
    row_filters: list[dict]
) -> dict[str, dict]:
    """Keyed by table_full_name → {table, function}."""

def build_expected_column_masks_lookup(
    column_masks: list[dict]
) -> dict[str, dict]:
    """Keyed by "{table_full_name}.{column_name}" → {table, column, function}."""

def build_expected_group_membership_lookup(
    group_membership: list[dict]
) -> dict[str, set[str]]:
    """Keyed by group_name → set of expected member values."""
```

### 6. Policy Engine Injection

Extended alongside the existing grants injection (same pattern):

| Resource type | Lookup key | Injected field |
|---|---|---|
| `row_filter` | `table_full_name` | `expected_row_filters` |
| `column_mask` | `"{table_full_name}.{column_name}"` | `expected_column_masks` |
| `group_member` | `group_name` | `expected_group_members` |

### 7. Rule Engine — New Evaluator Branches

Three new branches in `_eval_drift_check`:

**`row_filters`**: reads `expected_row_filters` from metadata (JSON string `{table, function}`). Fails if actual `filter_function` ≠ expected `function`. Passes vacuously if no expected entry for this table.

**`column_masks`**: reads `expected_column_masks` from metadata (JSON string `{table, column, function}`). Fails if actual `mask_function` ≠ expected `function`. Passes vacuously if no expected entry for this table+column.

**`group_membership`**: reads `expected_group_members` from metadata (JSON string — list of member values). Fails if actual `member_value` not in the expected members set. Passes vacuously if no expected entry for this group.

**Drift semantics** (same direction as grants — flag unauthorized actuals):
- Extra row filter not in expected state → FAIL
- Extra column mask not in expected state → FAIL
- Group member not in expected members list → FAIL

### 8. Sample Policies

Added to `engine/policies/drift_detection.yml`:

```yaml
  - id: POL-DRIFT-002
    name: "Row filter drift detection"
    applies_to: RowFilterAsset
    domain: AccessControl
    severity: critical
    description: "Detected a row filter that differs from the declared expected state."
    remediation: "Review the drift and update the expected state file or remove the unauthorized row filter."
    active: true
    rule:
      type: drift_check
      source: expected_permissions/expected_state.json
      check: row_filters

  - id: POL-DRIFT-003
    name: "Column mask drift detection"
    applies_to: ColumnMaskAsset
    domain: AccessControl
    severity: critical
    description: "Detected a column mask that differs from the declared expected state."
    remediation: "Review the drift and update the expected state file or remove the unauthorized column mask."
    active: true
    rule:
      type: drift_check
      source: expected_permissions/expected_state.json
      check: column_masks

  - id: POL-DRIFT-004
    name: "Group membership drift detection"
    applies_to: GroupMemberAsset
    domain: AccessControl
    severity: high
    description: "Detected a group member not present in the declared expected state."
    remediation: "Review the drift and update the expected state file or remove the unexpected group member."
    active: true
    rule:
      type: drift_check
      source: expected_permissions/expected_state.json
      check: group_membership
```

### 9. Tests

All pure Python, appended to `tests/unit/test_drift.py`.

Per check type:
- Pass: actual matches expected
- Fail: actual not in expected, detail includes resource info (table/column/group + value)
- Pass: no expected state in metadata (vacuously true)
- Pass: no matching entry for this resource in expected state
- Malformed JSON: graceful FAIL with detail

Loader tests (appended to `TestLoadExpectedState`):
- Load from plain JSON (existing)
- Load from OPA bundle tarball: extracts `data.json` correctly
- Load from bundle with `data_path`: navigates nested structure
- Load from bundle missing `data.json`: returns empty dict

Lookup builder tests per type:
- Empty input → empty lookup
- Single entry → correct key mapping
- Multiple entries same key (column_masks, group_membership)

---

## Files Changed

| Action | File | What |
|---|---|---|
| Edit | `engine/src/watchdog/crawler.py` | `_crawl_row_filters()`, `_crawl_column_masks()`, extend `_crawl_groups()` |
| Edit | `engine/ontologies/ontology.yml` | Add `RowFilterAsset`, `ColumnMaskAsset`, `GroupMemberAsset` |
| Edit | `engine/src/watchdog/drift.py` | Bundle loader, 3 new lookup builders |
| Edit | `engine/src/watchdog/policy_engine.py` | Inject expected state for 3 new check types |
| Edit | `engine/src/watchdog/rule_engine.py` | 3 new branches in `_eval_drift_check` |
| Edit | `engine/policies/drift_detection.yml` | POL-DRIFT-002, POL-DRIFT-003, POL-DRIFT-004 |
| Edit | `tests/unit/test_drift.py` | Tests for all new check types + loader + lookup builders |

---

## Future Direction: OPA as Evaluation Engine

The current design treats OPA as a data source (interoperability). A natural next step is using OPA as the evaluation engine for drift policies — replacing the Python `_eval_drift_check` branches with Rego policies.

**What this would look like:**

```rego
# permissions/drift.rego
package permissions.drift

violation[msg] {
    input.resource_type == "group_member"
    not input.expected_group_members[input.member_value]
    msg := sprintf("Unexpected member %v in group %v", [input.member_value, input.group_name])
}
```

Watchdog would call `opa eval` (subprocess or embedded via `python-opa` / `opa-python-client`) with the resource metadata as `input` and the Rego policy as the evaluation target. Violations would surface through the same pipeline.

**Why this matters:**
- Teams already writing Rego for Conftest, OPA Gatekeeper, or AWS CloudFormation Guard get a single policy language across their stack
- Rego's set operations (`actual - expected`) are more expressive for complex drift scenarios than the current if-else branches
- OPA's decision log provides a separate audit trail complementary to Watchdog's violations table

**Prerequisites before pursuing:**
- Establish OPA runtime dependency policy (subprocess vs. embedded)
- Define how Rego policies are distributed (alongside YAML policies, or separate bundle)
- Evaluate `python-opa-client` or `opa` subprocess for local and Databricks compatibility

This is scoped out of v2 intentionally — the interoperability layer (bundle loading) is the low-risk first step that proves the integration point without committing to a runtime dependency.
