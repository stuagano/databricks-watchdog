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

The `drift_check` rule type is registered in the rule engine dispatch table alongside other rule types (`has_tag`, `has_owner`, `all_of`, etc.). Unlike other rule types that evaluate resource properties (tags, metadata), `drift_check` compares a resource against an external declaration of what should exist.

The evaluator supports four check types:

| Check Type | Detects | Policy |
|---|---|---|
| `grants` | Unauthorized manual grants, revoked grants, modified privilege sets | POL-DRIFT-001 |
| `row_filters` | Row filter function mismatches on tables | POL-DRIFT-002 |
| `column_masks` | Column mask function mismatches on table columns | POL-DRIFT-003 |
| `group_membership` | Unauthorized group members, removed expected members | POL-DRIFT-004 |

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

- id: POL-DRIFT-002
  name: "Row filter drift detection"
  applies_to: RowFilterAsset
  domain: AccessControl
  severity: critical
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
  active: true
  rule:
    type: drift_check
    source: expected_permissions/expected_state.json
    check: column_masks

- id: POL-DRIFT-004
  name: "Group membership drift detection"
  applies_to: GroupMembershipAsset
  domain: AccessControl
  severity: high
  active: true
  rule:
    type: drift_check
    source: expected_permissions/expected_state.json
    check: group_membership
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
  ],
  "group_membership": [
    {
      "group": "finance-analysts",
      "members": ["alice@company.com", "bob@company.com"]
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

Each entry describes a row filter function that should be applied to a table. The evaluator checks whether the actual `filter_function` on the table matches the expected `function`. A mismatch (e.g., someone manually replaced the filter function) produces a drift violation identifying both the actual and expected function names.

### Column Masks Section

Each entry specifies a column mask function, target table and column. The evaluator checks whether the actual `mask_function` on the table/column pair matches the expected `function`. A mismatch produces a drift violation identifying the actual function, the expected function, and the affected column.

### Group Membership Section

Each entry declares the expected members of a group. The evaluator checks whether each actual group member appears in the expected members list. Members not in the expected list produce a drift violation identifying the unauthorized member and the group name. An empty expected members list means any actual member is unauthorized.

## Evaluator Behavior

The `drift_check` evaluator:

1. Loads expected state from the UC volume path specified in `rule.source`.
2. Reads the section specified by `rule.check` (grants, row_filters, column_masks, group_membership).
3. Injects the expected state into resource metadata before rule evaluation.
4. Compares actual state against expected state. Returns `RuleResult(passed=False, detail="...")` listing extra, missing, or modified entries when drift is detected.
5. If no expected state is present in metadata for a resource, the check passes vacuously (no declared expectation means no drift).
6. Malformed expected state JSON produces a failure with a detail message referencing the parse error.

The expected state is injected into metadata by the policy engine before evaluation, keeping the rule engine pure. The evaluator never loads files directly; it consumes pre-injected metadata keys:

| Check Type | Metadata Key | Value Format |
|---|---|---|
| `grants` | `expected_grants` | JSON array of grant entries |
| `row_filters` | `expected_row_filters` | JSON object with `table` and `function` |
| `column_masks` | `expected_column_masks` | JSON object with `table`, `column`, and `function` |
| `group_membership` | `expected_group_members` | JSON array of member strings |

## Expected State Loading

Watchdog supports two file formats for expected state:

- **Plain JSON** (`.json`): Standard JSON file as shown above.
- **OPA-style bundles** (`.tar.gz`): A tar.gz archive containing a `data.json` file at the root. The optional `data_path` parameter navigates into a top-level key (e.g., `"permissions"` extracts `data["permissions"]`).

Lookup builders transform the raw expected state into efficient structures for injection:

- `build_expected_grants_lookup`: Keyed by principal name.
- `build_expected_row_filters_lookup`: Keyed by `table` (full three-part name).
- `build_expected_column_masks_lookup`: Keyed by `{table}.{column}`.
- `build_expected_group_membership_lookup`: Keyed by group name, value is a set of expected members.

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

### Group Membership Governance

An identity team declares expected group memberships in their IDP sync pipeline. Watchdog detects when users are manually added to sensitive groups outside the approved provisioning flow, or when expected members are removed.

## Workflow

1. **External system generates** `expected_state.json` during its deploy/compile step.
2. **Upload to UC volume:**
   ```python
   w.files.upload(
       f"/Volumes/{catalog}/{schema}/{volume}/expected_permissions/expected_state.json",
       contents=json.dumps(expected_state).encode()
   )
   ```
3. **Add drift policies** to `engine/policies/` referencing the volume path and desired check types.
4. **Watchdog evaluates** on its next scan. Drift violations enter the standard lifecycle: open, notified, resolved when the external system corrects the state and re-uploads.
