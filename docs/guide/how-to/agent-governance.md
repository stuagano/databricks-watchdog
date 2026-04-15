# Govern AI Agents

This guide covers how Watchdog discovers, classifies, and governs AI agents deployed in a Databricks workspace.

## How Watchdog Discovers Agents

The crawler discovers agents from two sources:

### Source 1: Databricks Apps

The Apps API lists all deployed Databricks Apps. Watchdog applies a heuristic filter, including apps whose name or description contains keywords: `agent`, `mcp`, `assistant`, `bot`, or `ai`. Each matching app becomes an `agent` resource in the inventory.

Captured metadata includes app name, deployer, compute status, URL, and creation timestamp. The deployer is automatically tagged as `agent_owner` and `deployed_by`.

### Source 2: Model Serving Endpoints

All model serving endpoints are crawled. Each endpoint becomes an `agent` resource. Foundation Model API endpoints (names starting with `databricks-`) are automatically tagged as managed endpoints:

- `managed_endpoint=true`
- `agent_owner=databricks`
- `audit_logging_enabled=true`

This auto-classification ensures that platform-managed endpoints pass agent governance policies without manual tagging.

### Source 3: Agent Execution Traces

The crawler reads `system.serving.endpoint_usage` joined with `system.serving.served_entities` to discover per-requester usage patterns over the last 7 days. Each (endpoint, requester) pair becomes an `agent_execution` resource with usage metrics:

- Request count, input/output token totals
- Error count, rate-limiting events
- Entity type and task type from served entities
- High-volume and rate-limited flags

## Agent Ontology Classes

Watchdog classifies agents into seven derived classes based on their tags and metadata:

| Class | Classifies When | Description |
|-------|----------------|-------------|
| `ManagedModelEndpoint` | `managed_endpoint=true` | Databricks FMAPI endpoint (not a customer agent) |
| `AgentWithPiiAccess` | `accessed_pii=true` | Agent that has accessed PII data |
| `AgentWithExternalAccess` | `used_external_tool=true` | Agent that calls external APIs |
| `AgentWithDataExport` | `exported_data=true` | Agent that exports data outside the lakehouse |
| `UngovernedAgent` | No `agent_owner` AND no `audit_logging_enabled` | Agent with no governance metadata |
| `HighRiskExecution` | `resource_type=agent_execution` AND `accessed_pii=true` | Execution that accessed sensitive data |
| `ProductionAgent` | `environment=prod` | Agent deployed in production |

All agent classes inherit from `AgentAsset`, which matches resource types `agent` and `agent_execution`.

## Agent Governance Policies

### Agent-Level Policies

| Policy ID | Name | Severity | Target Class |
|-----------|------|----------|--------------|
| POL-AGENT-001 | Agents accessing PII must have audit logging enabled | Critical | AgentWithPiiAccess |
| POL-AGENT-002 | All agents must have a designated owner | High | AgentAsset |
| POL-AGENT-003 | Agents exporting data must have documented approval | Critical | AgentWithDataExport |
| POL-AGENT-004 | Agents calling external endpoints must be registered | High | AgentWithExternalAccess |
| POL-AGENT-005 | Ungoverned agents must not access production data | Critical | UngovernedAgent |
| POL-AGENT-006 | Production agents must specify their model endpoint | High | ProductionAgent |
| POL-AGENT-007 | Production agents must have error handling configured | Medium | ProductionAgent |

### Execution-Level Policies

| Policy ID | Name | Severity | Target Class |
|-----------|------|----------|--------------|
| POL-EXEC-001 | High-risk agent executions must have MLflow traces | Critical | HighRiskExecution |
| POL-EXEC-002 | Agent executions must complete within timeout | Medium | HighRiskExecution |

## Runtime Governance via Guardrails MCP

The Guardrails MCP server provides four runtime tools that agents call during execution for real-time governance:

### check_before_access

Call before an agent accesses a table. Returns allow/deny based on the table's classification, the agent's governance status, and applicable policies. When access is denied, it suggests alternative tables (e.g., a masked view).

```
Tool: check_before_access
Parameters:
  agent_id: "my-analytics-agent"
  table: "gold.finance.transactions"
  operation: "SELECT"
  columns: ["amount", "account_id"]
```

### log_agent_action

Log an agent action for the governance audit trail. Call after each significant action (data access, external API call, data export).

```
Tool: log_agent_action
Parameters:
  agent_id: "my-analytics-agent"
  action: "data_access"
  target: "gold.finance.transactions"
  details: {"columns": ["amount"], "row_count": 1500}
  classification: "confidential"
```

### get_agent_compliance

Returns the current compliance status of an agent: how many governance checks passed or failed in the current session, which data classifications were accessed, and the overall risk assessment.

### report_agent_execution

Generates a post-execution compliance report summarizing all governance checks, data accessed, policies triggered, and the overall compliance assessment. Call when the agent finishes its task.

## Agent Compliance Views

Watchdog creates four agent-specific compliance views after each evaluation:

### v_agent_inventory

One row per agent. Shows governance status, source (Apps or serving endpoint), owner, and violation counts. Use this as the primary agent roster.

### v_agent_execution_compliance

One row per agent execution. Includes usage metrics (request count, tokens), violation status, and risk flags.

### v_agent_risk_heatmap

One row per agent. Cross-tabulates data sensitivity against access frequency for risk scoring. High-sensitivity data accessed frequently by agents with governance gaps surfaces at the top.

### v_agent_remediation_priorities

One row per (policy_id, remediation action). Prioritized by impact: which single remediation action resolves the most violations? Includes affected agent lists and specific remediation steps.

### v_ai_gateway_cost_governance

One row per (endpoint, requester). Token consumption with estimated cost, entity type, task type, rate-limiting flags, and Watchdog governance cross-reference.

## Tagging Agents for Classification

Watchdog classifies agents by their tags. To bring an agent into a specific class, apply the corresponding tags:

```sql
-- Mark an agent as accessing PII (triggers POL-AGENT-001)
ALTER TABLE platform.watchdog.resource_inventory
SET TAGS ('accessed_pii' = 'true')
WHERE resource_id = 'agent:endpoint:my-agent'

-- Register external access (satisfies POL-AGENT-004)
ALTER TABLE platform.watchdog.resource_inventory
SET TAGS (
  'external_access_registered' = 'true',
  'external_endpoints_approved' = 'https://api.example.com'
)
WHERE resource_id = 'agent:endpoint:my-agent'
```

In practice, these tags are set through the agent's deployment configuration or by automation that monitors agent behavior.
