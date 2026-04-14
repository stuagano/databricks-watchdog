# Drift Detection

This guide explains Watchdog's expected-state drift detection pattern for comparing actual resource state against externally declared baselines.

## The Pattern

Watchdog's core model evaluates resources against declarative policies (tag checks, metadata rules). Drift detection extends this to compare actual state against an **expected state** maintained by an external system.

```
External System (permissions compiler, IaC pipeline, compliance team)
       |
  Generates expected_state.json
       |
  Uploads to UC volume
       |
Watchdog Scanner (daily scan)
       |
  drift_check rule type:
    1. Reads expected_state.json from volume
    2. Queries actual state
    3. Diffs expected vs actual
    4. Reports mismatches as violations
       |
  Same violations table, same notifications, same dashboards
```

Watchdog remains read-only throughout this process. It detects drift and reports it. Remediation is always a human action in the external system that owns the expected state.

## The drift_check Rule Type

The `drift_check` rule type is a planned extension to the rule engine. Unlike other rule types that evaluate resource properties (tags, metadata), `drift_check` compares a resource against an external declaration of what should exist.

> **Status:** The `drift_check` rule type is designed but not yet implemented in the rule engine dispatch table. The integration contract (expected state JSON schema, policy format, volume path convention) is stable and can be used by external systems today.

### Policy Schema

```yaml
- id: POL-DRIFT-001
  name: "Grant drift detection"
  applies_to: GrantAsset
  domain: AccessControl
  severity: critical
  active: true
  rule:
    type: drift_check
    source: expected_permissions/expected_state.json  # path within UC volume
    check: grants                                      # section of expected_state.json
```

The `source` field specifies the path within the Watchdog UC volume where the expected state file resides. The `check` field indicates which section of the JSON file to evaluate.

## Expected State JSON Structure

External systems produce a JSON file conforming to this schema:

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
  ],
  "row_filters": [
    {
      "table": "gold.finance.gl_balances",
      "function": "permissions_filter_finance_gl_balances",
      "enforcement": "uc_native",
      "checksum": "sha256:a1b2c3..."
    }
  ],
  "column_masks": [
    {
      "table": "gold.finance.gl_balances",
      "column": "cost_center_owner",
      "function": "permissions_mask_cost_center_owner",
      "enforcement": "uc_native",
      "checksum": "sha256:d4e5f6..."
    }
  ]
}
```

### Grants Section

Each entry describes a grant that should exist. The evaluator queries actual grants from the crawler output and reports:

- **Extra grants:** Grants that exist but are not in the expected state (unauthorized manual grants).
- **Missing grants:** Grants in the expected state that do not exist (revoked grants).
- **Modified grants:** Grants where the privilege set differs from expected.

### Row Filters Section

Each entry describes a row filter function that should be applied to a table. The evaluator checks:

- Whether the function exists.
- Whether it is applied to the correct table.
- Whether the function body matches the checksum (detects manual edits).

### Column Masks Section

Same structure as row filters. Each entry specifies a column mask function, target table and column, and a checksum for integrity verification.

## Evaluator Behavior

When implemented, the `drift_check` evaluator:

1. Loads expected state from the UC volume path specified in `rule.source`.
2. Reads the section specified by `rule.check` (grants, row_filters, column_masks, group_membership).
3. Queries the corresponding actual state (grants crawler output, `INFORMATION_SCHEMA`, SDK).
4. Returns `RuleResult(passed=False, detail="...")` listing extra, missing, or modified entries.
5. Uses checksums for integrity verification. A row filter function that was manually edited produces a checksum mismatch even if the function name is correct.

## Policy Namespace Convention

External systems should use distinct policy ID prefixes to avoid collisions with Watchdog's built-in policies:

| Prefix | Owner |
|--------|-------|
| `POL-A*` | Watchdog (access governance) |
| `POL-AGENT-*` | Watchdog (agent governance) |
| `POL-PERM-*` | Permissions enforcement (external) |
| `POL-DRIFT-*` | Generic drift detection (external) |

## Use Cases

### Permissions-as-Code

A YAML-based permissions compiler defines team grants, row filters, and column masks. At deploy time, the compiler generates `expected_state.json` and uploads it to the Watchdog volume. Watchdog detects unauthorized manual grants or modified UDFs between deployments.

### Infrastructure-as-Code

Terraform defines workspace configuration (cluster policies, init scripts, network settings). Expected state captures what Terraform applied. Watchdog detects manual overrides that drift from the declared infrastructure.

### Compliance Baselines

A compliance team defines required minimum grants for auditors as a static JSON file. Watchdog detects if grants are revoked or modified, ensuring auditor access remains intact between compliance reviews.

## Workflow

1. **External system generates** `expected_state.json` during its deploy/compile step.
2. **Upload to UC volume:**
   ```python
   w.files.upload(
       f"/Volumes/{catalog}/{schema}/{volume}/expected_permissions/expected_state.json",
       contents=json.dumps(expected_state).encode()
   )
   ```
3. **Add a drift policy** to `engine/policies/` referencing the volume path.
4. **Watchdog evaluates** on its next scan. Drift violations enter the standard lifecycle: open, notified, resolved when the external system corrects the state and re-uploads.
