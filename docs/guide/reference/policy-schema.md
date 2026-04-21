# Policy Schema Reference

This document describes the full YAML schema for Watchdog governance policies.

## Policy File Structure

Policies are organized in domain-scoped YAML files under `engine/policies/`:

```
engine/policies/
  access_governance.yml       # Least-privilege, grant hygiene
  agent_governance.yml        # AI agent behavior and compliance
  cost_governance.yml         # Cost attribution and chargeback
  data_quality.yml            # Documentation, naming, quality monitoring
  operational.yml             # Runtime compliance, alerting, SLA
  security_governance.yml     # Data protection, identity hygiene
  starter_policies.yml        # Minimal baseline (MVP deployment)
```

Each file contains a top-level `policies` key with a list of policy definitions:

```yaml
policies:
  - id: POL-S001
    name: "..."
    # ... fields ...
```

## Field Reference

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `id` | string | Yes | | Unique policy identifier. Must be unique across all policy files. |
| `name` | string | Yes | | Human-readable name. Shown in dashboards, violations, and notifications. |
| `applies_to` | string | Yes | | Ontology class name (e.g., `DataAsset`, `PiiTable`) or `*` for all resources. |
| `domain` | string | Yes | `Uncategorized` | Governance domain. Groups policies in views and dashboards. |
| `severity` | string | Yes | `medium` | One of: `critical`, `high`, `medium`, `low`. |
| `description` | string | Yes | | Why this policy exists and what risk it mitigates. |
| `remediation` | string | Yes | | Step-by-step instructions to fix a violation. |
| `active` | boolean | No | `true` | Set to `false` to disable evaluation without removing the policy. |
| `rule` | object | Yes | | Declarative rule tree. See Rule Tree Schema below. |
| `compile_to` | list\|object | No | `null` | Compile-down targets. See Compile-Down Configuration below. |

## Rule Tree Schema

The `rule` field accepts any valid rule definition. Rules can be nested arbitrarily deep using composite operators.

### Primitive Reference

```yaml
rule:
  ref: <primitive_name>
```

Resolves to the named primitive from `rule_primitives.yml`.

### Tag Check Rules

```yaml
# tag_exists
rule:
  type: tag_exists
  keys: [<key1>, <key2>]

# tag_equals
rule:
  type: tag_equals
  key: <key>
  value: <value>

# tag_in
rule:
  type: tag_in
  key: <key>
  allowed: [<v1>, <v2>, <v3>]

# tag_not_in
rule:
  type: tag_not_in
  key: <key>
  disallowed: [<v1>, <v2>]

# tag_matches
rule:
  type: tag_matches
  key: <key>
  pattern: <regex>
```

### Metadata Check Rules

```yaml
# metadata_equals
rule:
  type: metadata_equals
  field: <field>
  value: <value>

# metadata_matches
rule:
  type: metadata_matches
  field: <field>
  pattern: <regex>

# metadata_not_empty
rule:
  type: metadata_not_empty
  field: <field>

# metadata_gte
rule:
  type: metadata_gte
  field: <field>
  threshold: <value>

# has_owner (composite shorthand)
rule:
  type: has_owner
```

### Composite Rules

```yaml
# all_of (AND)
rule:
  type: all_of
  rules:
    - <rule_definition>
    - <rule_definition>

# any_of (OR)
rule:
  type: any_of
  rules:
    - <rule_definition>
    - <rule_definition>

# none_of (NOT)
rule:
  type: none_of
  rules:
    - <rule_definition>

# if_then (conditional)
rule:
  type: if_then
  condition: <rule_definition>
  then: <rule_definition>
```

### Shorthand Syntax

Policy YAML supports compact shorthand where the rule type key appears directly:

```yaml
# Shorthand
rule:
  tag_equals:
    dqx_enabled: "true"

# Equivalent typed form
rule:
  type: tag_equals
  key: dqx_enabled
  value: "true"

# Shorthand for if_then
rule:
  if_then:
    if:
      tag_equals:
        data_layer: "gold"
    then:
      tag_equals:
        dqm_enabled: "true"
```

## Complete Example

```yaml
policies:
  - id: POL-S001
    name: "PII assets must have a data steward and retention policy"
    applies_to: PiiAsset
    domain: SecurityGovernance
    severity: critical
    description: >
      PII data requires a named steward for accountability and
      a retention period for compliance. Without these, the
      organization cannot demonstrate data lifecycle management
      during audits.
    remediation: >
      Add 'data_steward' tag (email of responsible person) and
      'retention_days' tag (integer, days to retain) to the asset.
    active: true
    rule:
      type: all_of
      rules:
        - ref: has_data_steward
        - ref: has_retention_policy
```

## Hybrid Policy Management

Watchdog supports two policy sources that merge at evaluation time:

### YAML Policies (origin: yaml)

Version-controlled in git under `engine/policies/`. Synced to the Delta `policies` table via `--sync-policies`. These represent the SA-managed baseline.

The sync uses MERGE keyed on `policy_id`:
- Existing YAML policies are updated.
- New policies are inserted.
- YAML policies removed from the repo are deactivated (`active=false`).
- User-created policies are never touched.

### User Policies (origin: user)

Created directly in the Delta `policies` table via SQL or notebook. These represent platform-admin customizations for a specific environment.

```sql
INSERT INTO platform.watchdog.policies
(policy_id, policy_name, applies_to, domain, severity,
 description, remediation, active, rule_json, origin, updated_at)
VALUES (
  'POL-CUSTOM-001',
  'Analytics tables must have a team tag',
  'GoldTable',
  'CostGovernance',
  'medium',
  'Gold-layer tables need team attribution for chargeback',
  'Add a team tag to the table',
  true,
  '{"ref": "has_team_tag"}',
  'user',
  current_timestamp()
)
```

User policies use the `rule_json` column (JSON string) instead of the YAML `rule` field.

### Policy History

Every policy change (YAML sync or user edit) is recorded in the `policies_history` table as an append-only audit trail. Change detection uses the fields that affect evaluation behavior: `rule_json`, `severity`, `applies_to`, and `active`. Cosmetic edits to description or remediation do not generate history entries.

## Compile-Down Configuration

Policies can declare a `compile_to` block to emit runtime artifacts to external enforcement substrates. Policies without `compile_to` remain scan-only (evaluated by the Watchdog engine but not pushed to any runtime system).

The `compile_to` field accepts either a single entry (object) or a list of entries. A single object is automatically normalized to a one-element list by the policy loader. Each entry must include a `target` field identifying the substrate, plus target-specific configuration fields.

### Supported Targets

| Target | Description | Key Config Fields |
|--------|-------------|-------------------|
| `guardrails` | Guardrails MCP check definition | `kind` (advisory or blocking), `block_when` |
| `uc_tag_policy` | Unity Catalog tag policy spec | `tag_key`, `policy_type`, `allowed_values`, `resource_types`, `scope` |
| `uc_abac` | Unity Catalog ABAC column mask spec | `mask_function`, `apply_when` |

### Target: guardrails

Emits a JSON check definition the Guardrails MCP server can load at startup.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `target` | string | Yes | | Must be `guardrails` |
| `kind` | string | No | `advisory` | `advisory` (log only) or `blocking` (reject the action) |
| `block_when` | string | No | `null` | Predicate describing when the check fires |

### Target: uc_tag_policy

Emits a JSON spec the deployer turns into a Unity Catalog tag policy API call.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `target` | string | Yes | | Must be `uc_tag_policy` |
| `tag_key` | string | Yes | | Tag key to enforce (e.g., `data_steward`) |
| `policy_type` | string | No | `required` | `required` (tag must exist) or `allowed_values` (tag value constrained) |
| `allowed_values` | list | Conditional | | Required when `policy_type` is `allowed_values`. List of permitted tag values. |
| `resource_types` | list | No | `[table]` | Resource types the tag policy applies to |
| `scope` | object | No | `null` | Optional scope restriction with `catalog` and/or `schema` fields |

### Target: uc_abac

Emits a JSON spec the deployer turns into `ALTER TABLE ... SET COLUMN MASK` API calls.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `target` | string | Yes | | Must be `uc_abac` |
| `mask_function` | string | Yes | | Three-part UDF reference (`catalog.schema.function`) |
| `apply_when` | string | No | `null` | Human-readable scope note for deployer context |

### Examples

Single target (object shorthand):

```yaml
policies:
  - id: POL-G001
    name: "PII columns must be masked in production"
    applies_to: PiiTable
    domain: SecurityGovernance
    severity: critical
    description: >
      PII columns require column masks in production to prevent
      unauthorized access to sensitive data.
    remediation: >
      Apply column mask UDF main.governance.redact_pii to PII columns.
    rule:
      ref: has_column_mask
    compile_to:
      target: uc_abac
      mask_function: main.governance.redact_pii
      apply_when: environment = prod
```

Multiple targets (list form):

```yaml
policies:
  - id: POL-T001
    name: "Production tables must have a data steward"
    applies_to: GoldTable
    domain: SecurityGovernance
    severity: high
    description: >
      Gold-layer tables require a named data steward for accountability.
    remediation: >
      Add a 'data_steward' tag with the responsible person's email.
    rule:
      ref: has_data_steward
    compile_to:
      - target: uc_tag_policy
        tag_key: data_steward
        resource_types: [table, schema]
      - target: guardrails
        kind: blocking
        block_when: "table is in gold layer without data_steward tag"
```

### Drift Detection

The compiler writes a manifest file recording each emitted artifact and its content hash. On every scan, the policy engine checks deployed artifacts against the manifest:

- **in_sync** -- artifact on disk matches the manifest hash.
- **drifted** -- artifact exists but has been modified out-of-band.
- **missing** -- artifact was never emitted or has been deleted.

Drifted or missing artifacts emit meta-violations (severity high for missing, medium for drifted) so the compliance dashboard surfaces enforcement gaps.

## Severity Guidelines

| Severity | When to Use | Notification | Dashboard Color |
|----------|------------|--------------|-----------------|
| `critical` | Regulatory violations, data exposure risk, compliance blockers | Immediate digest | Red |
| `high` | Security best practices, missing required metadata, access control gaps | Daily digest | Orange |
| `medium` | Operational best practices, missing optional metadata | Weekly digest | Yellow |
| `low` | Style conventions, documentation gaps, nice-to-haves | None (dashboard only) | Blue |
