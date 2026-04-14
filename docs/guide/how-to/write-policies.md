# Write Governance Policies

This guide walks through building a governance policy from scratch, testing it, and deploying it to production.

## Overview

A Watchdog policy is a declarative YAML rule that targets an ontology class and evaluates resources for compliance. Policies live in `engine/policies/` organized by governance domain.

## Step 1: Choose the Target Ontology Class

Every policy specifies an `applies_to` field that determines which resources the policy evaluates. This field references an ontology class from `resource_classes.yml`.

Common targets:

| Class | What It Matches |
|-------|----------------|
| `DataAsset` | All tables, volumes, catalogs, schemas |
| `ComputeAsset` | Jobs, clusters, warehouses, pipelines |
| `IdentityAsset` | Users, groups, service principals |
| `GrantAsset` | Permission assignments |
| `AgentAsset` | AI agents and agent executions |
| `GoldTable` | Tables tagged `data_layer=gold` |
| `PiiAsset` | Assets tagged `data_classification=pii` |
| `ConfidentialAsset` | Assets tagged `data_classification` in (confidential, restricted, pii) |
| `ProductionJob` | Jobs tagged `environment=prod` |
| `*` | All resources regardless of class |

Policies apply to the specified class and all its descendants. A policy on `ConfidentialAsset` automatically covers `PiiAsset` and `PiiTable` because they inherit from it.

## Step 2: Choose a Domain

The `domain` field groups policies for reporting and dashboard views. Use one of the standard domains:

| Domain | Purpose |
|--------|---------|
| `SecurityGovernance` | Access control, data protection, identity hygiene |
| `CostGovernance` | Cost attribution, chargeback, waste prevention |
| `DataQuality` | Documentation, naming, quality monitoring |
| `OperationalGovernance` | Runtime compliance, alerting, SLA management |
| `AgentGovernance` | AI agent behavior, data access, compliance |
| `RegulatoryCompliance` | Industry-specific regulatory policies |

## Step 3: Build the Rule

Rules are declarative checks composed from primitives. The simplest approach is to reference a named primitive using `ref:` syntax.

### Using a Named Primitive

Named primitives are defined in `engine/ontologies/rule_primitives.yml`. Reference them with the `ref` key:

```yaml
rule:
  ref: has_data_steward
```

This resolves to the `has_data_steward` primitive, which checks that the `data_steward` tag exists on the resource.

### Inline Rules

For rules not covered by existing primitives, define them inline:

```yaml
rule:
  type: tag_exists
  keys: [data_steward]
```

### Composing Rules

Combine multiple checks with `all_of` (AND), `any_of` (OR), or `none_of` (NOT):

```yaml
rule:
  type: all_of
  rules:
    - ref: has_data_steward
    - ref: has_retention_policy
```

### Conditional Rules

Use `if_then` when a check should only apply under certain conditions:

```yaml
rule:
  type: if_then
  condition:
    type: tag_equals
    key: environment
    value: "prod"
  then:
    type: all_of
    rules:
      - ref: has_on_call_team
      - ref: has_alert_channel
```

Resources that do not match the condition pass the policy automatically (vacuous truth).

### Shorthand Syntax

Policy YAML supports a compact shorthand for common patterns:

```yaml
# Shorthand (no explicit type field)
rule:
  tag_equals:
    dqx_enabled: "true"

# Equivalent typed form
rule:
  type: tag_equals
  key: dqx_enabled
  value: "true"
```

## Step 4: Full Example

Here is a complete policy requiring that confidential tables have both a data steward and a retention policy:

```yaml
# In engine/policies/security_governance.yml

policies:
  - id: POL-S010
    name: "Confidential tables must have data steward AND retention policy"
    applies_to: ConfidentialAsset
    domain: SecurityGovernance
    severity: critical
    description: >
      Confidential data requires a named steward for accountability
      and a retention period for compliance. Without these, the
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

### Policy Field Reference

| Field | Required | Description |
|-------|----------|-------------|
| `id` | Yes | Unique identifier (e.g., `POL-S010`). Use the domain prefix convention. |
| `name` | Yes | Human-readable name (shown in dashboards and violation reports). |
| `applies_to` | Yes | Ontology class name or `*` for all resources. |
| `domain` | Yes | Governance domain for grouping. |
| `severity` | Yes | One of: `critical`, `high`, `medium`, `low`. |
| `description` | Yes | Why this policy exists and what risk it mitigates. |
| `remediation` | Yes | Step-by-step instructions to fix a violation. |
| `active` | No | Set to `false` to disable without removing. Default: `true`. |
| `rule` | Yes | The declarative rule tree (inline, ref, or composite). |

## Step 5: Test with Ad-Hoc Scan

Before deploying, test the policy with the `watchdog-adhoc` entrypoint. This runs a full crawl-evaluate cycle and prints results immediately.

```bash
# From a Databricks notebook or job task
python -m watchdog.entrypoints adhoc \
  --catalog platform \
  --schema watchdog \
  --secret-scope watchdog
```

The ad-hoc entrypoint automatically syncs YAML policies to Delta before evaluating. Check the output for the new policy ID in the violations summary.

To verify a specific resource, query the scan results after the ad-hoc run:

```sql
SELECT resource_name, policy_id, result, details
FROM platform.watchdog.scan_results
WHERE scan_id = (SELECT MAX(scan_id) FROM platform.watchdog.scan_results)
  AND policy_id = 'POL-S010'
ORDER BY result
```

## Step 6: Deploy

Once the policy passes testing:

1. **Add the YAML** to the appropriate file in `engine/policies/` (or create a new domain file).

2. **Run with `--sync-policies`** to push YAML definitions to the Delta policies table:

   ```bash
   python -m watchdog.entrypoints evaluate \
     --catalog platform \
     --schema watchdog \
     --sync-policies
   ```

   The sync uses MERGE to upsert YAML policies. User-created policies (origin `user`) are never overwritten.

3. **Verify in the policies table:**

   ```sql
   SELECT policy_id, policy_name, active, origin, updated_at
   FROM platform.watchdog.policies
   WHERE policy_id = 'POL-S010'
   ```

The next scheduled scan picks up the policy automatically. Violations appear in dashboards and notification digests.

## Creating User Policies via SQL

Platform admins can create policies directly in the Delta table without modifying YAML. These policies have `origin='user'` and are never overwritten by YAML syncs.

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
  'Add a team tag: ALTER TABLE <table> SET TAGS (''team'' = ''your-team'')',
  true,
  '{"ref": "has_team_tag"}',
  'user',
  current_timestamp()
)
```

## Policy ID Conventions

| Prefix | Domain |
|--------|--------|
| `POL-S*` | Security governance |
| `POL-A*` | Access governance |
| `POL-C*` | Cost governance |
| `POL-Q*` | Data quality |
| `POL-O*` | Operational governance |
| `POL-AGENT-*` | Agent governance |
| `POL-EXEC-*` | Agent execution governance |
| `POL-PERM-*` | Permissions enforcement (external) |
| `POL-DRIFT-*` | Drift detection (external) |
| `POL-CUSTOM-*` | User-created policies |
