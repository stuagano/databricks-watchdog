# Policies

## What a Policy Is

A policy is a declarative rule evaluated against classified resources. Each policy specifies:

- **Which resources** it applies to (an ontology class or `*` for all resources)
- **What rule** must be satisfied (a composable expression of tag and metadata checks)
- **How severe** a failure is (critical, high, medium, low)
- **What to fix** when a resource fails (remediation text)

Policies are defined in YAML files organized by governance domain. The engine loads all active policies, matches each one to the resources classified under its `applies_to` class (including inherited descendants), and evaluates the rule. A failing evaluation produces a violation.

## Policy Domains

Policies are grouped into governance domains. Each domain represents a compliance concern:

| Domain | Description | Example |
|---|---|---|
| `SecurityGovernance` | Access control, least privilege, grant hygiene | No ALL PRIVILEGES on production data |
| `AgentGovernance` | AI agent behavior, data access, audit logging | PII-accessing agents must have audit logging |
| `OperationalGovernance` | Production readiness, alerting, error handling | Production agents must specify model endpoint |
| `CostGovernance` | Cost attribution, resource tagging | Compute resources must have cost center tag |
| `DataQuality` | Classification, stewardship, documentation | Data assets must have a classification label |

Domains are strings, not fixed enumerations. Custom domains can be introduced by setting the `domain` field in a policy YAML file.

## Severity Levels

Four severity levels indicate the urgency of a violation:

| Severity | Meaning | Example |
|---|---|---|
| `critical` | Immediate risk; security or compliance exposure | ALL PRIVILEGES granted on production catalog |
| `high` | Significant gap; should be addressed within days | Direct user grant bypasses group-based access |
| `medium` | Governance hygiene; address within a sprint | Group with MANAGE privilege has only one member |
| `low` | Best practice; address when convenient | Development cluster missing cost center tag |

Severity determines notification priority, dashboard ordering, and escalation thresholds. The `scan_summary` table tracks counts per severity level for trend reporting.

## Rule Composition

Every policy contains a `rule` field. Rules are composable expressions that the rule engine evaluates recursively. The simplest rule checks a single tag:

```yaml
rule:
  type: tag_exists
  keys: [data_classification]
```

Rules compose with boolean operators:

```yaml
rule:
  type: all_of
  rules:
    - type: tag_exists
      keys: [data_steward]
    - type: tag_exists
      keys: [retention_days]
```

Conditional rules use `if_then` to scope enforcement:

```yaml
rule:
  type: if_then
  condition:
    type: tag_equals
    key: data_classification
    value: "pii"
  then:
    type: all_of
    rules:
      - type: tag_exists
        keys: [data_steward]
      - type: tag_exists
        keys: [retention_days]
```

When the condition does not match (the resource is not PII), the rule is vacuously true -- it simply does not apply. When the condition matches, the `then` clause is enforced.

### Full Policy Example

This is a complete policy from `access_governance.yml`:

```yaml
- id: POL-A001
  name: "No ALL PRIVILEGES on production data"
  applies_to: OverprivilegedGrant
  domain: SecurityGovernance
  severity: critical
  description: >
    ALL PRIVILEGES and MANAGE grants on production assets
    violate least-privilege. Use specific grants (SELECT, MODIFY, etc.).
  remediation: >
    Replace ALL PRIVILEGES with specific required privileges.
    Use 'GRANT SELECT' or 'GRANT MODIFY' instead.
  active: true
  rule:
    ref: no_all_privileges
```

The policy targets the `OverprivilegedGrant` ontology class. Any resource classified as `OverprivilegedGrant` (a grant with `ALL PRIVILEGES` or `MANAGE`) is evaluated against the `no_all_privileges` rule primitive. A failure produces a critical-severity violation in the `SecurityGovernance` domain with the specified remediation text.

## Available Rule Types

The rule engine supports 14 rule types:

| Type | Description |
|---|---|
| `tag_exists` | Tag key(s) must be present |
| `tag_equals` | Tag must equal a specific value |
| `tag_in` | Tag value must be in an allowed set |
| `tag_not_in` | Tag value must not be in a disallowed set |
| `tag_matches` | Tag value must match a regex pattern |
| `metadata_equals` | Metadata field must equal a value |
| `metadata_matches` | Metadata field must match a regex |
| `metadata_not_empty` | Metadata field must exist and be non-empty |
| `metadata_gte` | Metadata field must be >= threshold (version-aware) |
| `has_owner` | Resource must have an owner (metadata or tag) |
| `all_of` | All child rules must pass (AND) |
| `any_of` | At least one child rule must pass (OR) |
| `none_of` | No child rules may pass (NOT / exclusion) |
| `if_then` | Conditional -- enforce `then` only when `condition` matches |

Composite rules (`all_of`, `any_of`, `none_of`, `if_then`) nest arbitrarily. A rule tree can combine conditions, exclusions, and requirements to any depth.

## Named Primitives

Named rule primitives are reusable building blocks defined in `engine/ontologies/rule_primitives.yml`. A policy references a primitive using the `ref` syntax:

```yaml
rule:
  ref: has_data_classification
```

The engine resolves the reference at evaluation time. The `has_data_classification` primitive expands to:

```yaml
type: tag_exists
keys: [data_classification]
```

Primitives reduce duplication across policies. Common checks -- ownership, classification, environment validation, cost attribution -- are defined once and referenced by every policy that needs them.

Selected built-in primitives:

| Primitive | What it checks |
|---|---|
| `has_owner` | Resource has an owner (metadata or tag) |
| `has_data_classification` | Data classification tag is present |
| `valid_data_classification` | Classification uses an approved value (public, internal, confidential, restricted, pii) |
| `has_data_steward` | Data steward tag is present |
| `has_cost_center` | Cost center tag is present |
| `is_production` | Environment tag equals `prod` |
| `runtime_current` | Compute runtime version >= 15.4 |
| `no_all_privileges` | Grant does not use ALL PRIVILEGES |
| `grant_uses_groups` | Grant assigned to a group, not an individual user |
| `pii_has_steward` | IF PII THEN must have steward AND retention |
| `production_has_alerting` | IF production THEN must have on-call team AND alert channel |
| `agent_has_audit_logging` | Agent has audit logging enabled |
| `has_agent_owner` | Agent has a designated owner |

## Hybrid Policy Management

Watchdog supports two policy sources that merge at evaluation time:

### YAML Policies (origin: yaml)

YAML policies live in `engine/policies/` and are version-controlled in git. They represent the SA-managed or platform-team baseline. When the engine deploys, `sync_policies_to_delta()` writes these policies to the `policies` Delta table using a MERGE keyed on `policy_id`. Only rows with `origin='yaml'` are updated; user-created policies are never overwritten.

YAML policies removed from the repository are automatically deactivated (not deleted) in the Delta table. All changes are recorded in `policies_history` for audit.

### User Policies (origin: user)

Platform administrators can create policies directly in the `policies` Delta table using SQL or a notebook. These rows have `origin='user'` and are loaded alongside YAML policies at evaluation time.

User policies allow governance teams to tune and extend the baseline for their environment without modifying the git-managed YAML. A platform admin might add a policy specific to their organization's naming conventions or regulatory requirements.

### Merge Behavior

At evaluation time, the engine loads both sources:

1. `load_yaml_policies()` reads YAML files from disk.
2. `load_delta_policies()` reads active user policies from Delta.
3. Both lists are concatenated. Policy IDs must be unique across sources.

YAML seeds best practices. Users tune and extend. The engine evaluates all active policies regardless of origin.
