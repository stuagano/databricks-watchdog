# Watchdog MCP Tools Reference

The Watchdog MCP server provides 13 tools for querying governance posture. These tools expose compliance data from the Watchdog Delta tables to AI assistants and agents via the Model Context Protocol.

All tools run as the calling user's identity. UC grants on the `platform.watchdog` schema control access.

## get_violations

Query open governance violations with filters.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `status` | string | No | `open` | Filter: open, resolved, exception |
| `severity` | string | No | | Filter: critical, high, medium, low |
| `resource_type` | string | No | | Filter by resource type (table, cluster, etc.) |
| `policy_id` | string | No | | Filter by policy ID |
| `owner` | string | No | | Filter by resource owner |
| `limit` | integer | No | 50 | Maximum results |
| `metastore` | string | No | | Filter to a specific metastore ID |

**Returns:** List of violations with resource_id, resource_name, policy_id, severity, domain, detail, remediation, first_detected, last_detected, status.

---

## get_governance_summary

High-level summary of the current governance state.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `metastore` | string | No | Filter to a specific metastore ID |

**Returns:** Total open violations by severity, recent trends, top offending resource types, coverage metrics.

---

## get_policies

List all governance policies with status and evaluation results.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `active_only` | boolean | No | `true` | Only show active policies |
| `metastore` | string | No | | Filter to a specific metastore ID |

**Returns:** List of policies with policy_id, name, applies_to, domain, severity, description, active status.

---

## get_scan_history

View recent Watchdog scan results.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `limit` | integer | No | 10 | Number of recent scans |
| `metastore` | string | No | | Filter to a specific metastore ID |

**Returns:** List of scans with scan_id, resources evaluated, violations found, violations resolved, timestamp.

---

## get_resource_violations

Get all violations for a specific resource.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `resource_id` | string | Yes | Resource identifier to look up |
| `metastore` | string | No | Filter to a specific metastore ID |

**Returns:** All violations (open, resolved, exception) for the resource with full compliance history.

---

## get_exceptions

List approved governance exceptions (waivers).

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `active_only` | boolean | No | `true` | Only show non-expired exceptions |
| `metastore` | string | No | | Filter to a specific metastore ID |

**Returns:** List of exceptions with resource_id, policy_id, approved_by, justification, expiration.

---

## explain_violation

Explain a violation in plain language with remediation steps.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `violation_id` | string | No | Violation UUID to explain |
| `resource_id` | string | No | Resource ID (alternative to violation_id) |
| `policy_id` | string | No | Policy ID (used with resource_id) |
| `metastore` | string | No | Filter to a specific metastore ID |

Accepts either `violation_id` alone or a `resource_id` + `policy_id` pair.

**Returns:** Plain-language explanation of the violation: what it means, why the policy exists, current resource state, and step-by-step remediation.

---

## what_if_policy

Simulate a proposed policy against the current inventory without creating violations.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `applies_to` | string | No | `*` | Ontology class the policy targets |
| `rule_type` | string | Yes | | Rule type to simulate (tag_exists, tag_equals, tag_in, metadata_equals, metadata_not_empty) |
| `rule_key` | string | Yes | | Tag key or metadata field to check |
| `rule_value` | string | No | | Expected value (for equals/in types) |
| `severity` | string | No | `medium` | Severity of the proposed policy |
| `metastore` | string | No | | Filter to a specific metastore ID |

**Returns:** List of resources that would violate the proposed policy, with counts and severity breakdown.

---

## list_metastores

List all metastores Watchdog has scanned.

**Parameters:** None.

**Returns:** Metastore IDs with latest scan timestamp and resource count.

---

## suggest_policies

Analyze the inventory and suggest new policies based on gaps.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `focus` | string | No | `all` | Focus area: gaps, classification, access, all |
| `resource_type` | string | No | | Limit analysis to a specific resource type |
| `limit` | integer | No | 10 | Max suggestions |
| `metastore` | string | No | | Filter to a specific metastore ID |

**Returns:** Suggested policies with YAML definitions ready to add to the engine.

---

## policy_impact_analysis

Analyze the impact of modifying an existing policy.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `policy_id` | string | Yes | Policy ID to analyze |
| `action` | string | Yes | deactivate, change_severity, change_scope |
| `new_severity` | string | No | New severity (for change_severity) |
| `new_applies_to` | string | No | New class scope (for change_scope) |
| `metastore` | string | No | Filter to a specific metastore ID |

**Returns:** Current violation count, projected change, affected resources.

---

## explore_governance

Run free-form SQL against Watchdog tables.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `query` | string | Yes | | SQL query (table names are unqualified) |
| `limit` | integer | No | 100 | Max rows (max: 1000) |

Available tables: `resource_inventory`, `violations`, `policies`, `exceptions`, `resource_classifications`, `scan_results`, `scan_summary`.

**Returns:** Query results as structured data.

**Example:**

```
query: "SELECT resource_type, COUNT(*) FROM resource_inventory
        WHERE scan_id = (SELECT MAX(scan_id) FROM resource_inventory)
        GROUP BY resource_type"
```

---

## suggest_classification

Suggest ontology class assignments for unclassified resources.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `resource_type` | string | No | | Focus on a specific resource type |
| `unclassified_only` | boolean | No | `true` | Only show unclassified resources |
| `limit` | integer | No | 50 | Max resources to analyze |
| `metastore` | string | No | | Filter to a specific metastore ID |

**Returns:** Resources with suggested class assignments and the tags needed to enable classification.
