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

## Severity Guidelines

| Severity | When to Use | Notification | Dashboard Color |
|----------|------------|--------------|-----------------|
| `critical` | Regulatory violations, data exposure risk, compliance blockers | Immediate digest | Red |
| `high` | Security best practices, missing required metadata, access control gaps | Daily digest | Orange |
| `medium` | Operational best practices, missing optional metadata | Weekly digest | Yellow |
| `low` | Style conventions, documentation gaps, nice-to-haves | None (dashboard only) | Blue |
