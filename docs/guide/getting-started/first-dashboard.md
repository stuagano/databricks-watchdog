# First Dashboard

After running a scan and evaluate, Watchdog's Delta tables and semantic views contain compliance data ready for visualization. Three options are available: a pre-built Lakeview dashboard, a Genie space for natural-language queries, or custom SQL against the views.

## Semantic Views

The evaluate step creates 14 semantic views in the Watchdog schema. These views join and aggregate across the core tables to provide ready-made analytical surfaces:

| View | Description |
|---|---|
| `v_resource_compliance` | One row per (resource, ontology class). Per-resource posture within each assigned class. |
| `v_class_compliance` | One row per ontology class. Violation counts and compliance rate per class. |
| `v_domain_compliance` | One row per governance domain. Executive posture summary. |
| `v_tag_policy_coverage` | One row per (resource, policy). Which policies are satisfied, violated, or not evaluated. |
| `v_data_classification_summary` | One row per catalog. Classification coverage, steward coverage, sensitive data percentage. |
| `v_dq_monitoring_coverage` | One row per table. DQ monitoring status (DQM, LHM, both, or neither) with anomaly counts. |
| `v_compliance_trend` | One row per scan. LAG-based deltas and direction indicators for trend dashboards. |
| `v_agent_inventory` | One row per agent. Governance status, source, owner, violation counts. |
| `v_agent_execution_compliance` | One row per agent execution. Usage metrics, violation status, risk flags. |
| `v_agent_risk_heatmap` | One row per agent. Data sensitivity cross-tabulated with access frequency for risk scoring. |
| `v_agent_remediation_priorities` | One row per (policy, remediation). Prioritized by impact -- which single action resolves the most violations. |
| `v_ai_gateway_cost_governance` | One row per (endpoint, requester). Token consumption, estimated cost, rate limiting flags. |
| `v_cross_metastore_compliance` | Aggregated compliance across all scanned metastores. |
| `v_cross_metastore_inventory` | Resource inventory across all scanned metastores. |

All views are regular (not materialized) and always reflect the current state of the underlying tables.

## Option A: Lakeview Dashboard

The repository includes pre-built Lakeview dashboard definitions that can be imported into the workspace. The dashboard has 10 pages covering:

1. **Domain Compliance** -- cross-domain posture summary with severity breakdown
2. **Resource Compliance** -- per-resource violation detail with ontology class context
3. **Owner Accountability** -- violations grouped by owner with remediation priorities
4. **Classification Coverage** -- data classification posture per catalog
5. **Agent Inventory** -- all discovered agents with governance status
6. **Agent Execution** -- execution-level compliance and risk flags
7. **Agent Risk** -- risk heatmap crossing data sensitivity with access volume
8. **Cost Governance** -- AI Gateway token consumption and cost attribution
9. **Compliance Trends** -- 30/60/90-day posture direction
10. **DQ Monitoring** -- monitoring coverage and anomaly summary

### Import Steps

1. Navigate to **SQL > Dashboards** in the workspace.
2. Select **Import from file** and upload the dashboard JSON from `engine/dashboards/`.
3. Update the catalog and schema references to match the deployment (e.g., `my_catalog.watchdog`).
4. Assign a SQL warehouse for query execution.

## Option B: Genie Space

A Genie space enables natural-language queries against Watchdog data. The space is configured with 27 tables: all 14 semantic views, the core Delta tables, and relevant UC system tables including `system.serving.endpoint_usage`.

### Deploy Steps

1. Run the Genie space deployment script (if provided in the repository) or create manually.
2. Add all 14 `v_*` views and core tables from the Watchdog schema.
3. Add `system.serving.endpoint_usage` for agent execution context.
4. Add instructions covering governance concepts, risk tiers, and common question patterns.

### Example Questions

Once deployed, the Genie space answers questions like:

- "What is our overall compliance percentage?"
- "Which owners have the most critical violations?"
- "Show me all ungoverned agents accessing production data."
- "How has our security governance posture changed over the last 30 days?"
- "Which AI agents have the highest token consumption?"

## Option C: Custom SQL

For teams that prefer building their own dashboards or integrating with external BI tools, the semantic views support direct SQL queries. Here are four starting queries:

### Domain Compliance Overview

```sql
SELECT
    domain,
    total_resources,
    open_violations,
    compliance_pct,
    critical_count,
    high_count
FROM my_catalog.watchdog.v_domain_compliance
ORDER BY compliance_pct ASC;
```

### Top Violation Owners

```sql
SELECT
    owner,
    COUNT(*) AS total_violations,
    COUNT(CASE WHEN severity = 'critical' THEN 1 END) AS critical,
    COUNT(CASE WHEN severity = 'high' THEN 1 END) AS high,
    COUNT(DISTINCT policy_id) AS policies_violated,
    COUNT(DISTINCT domain) AS domains_affected
FROM my_catalog.watchdog.violations
WHERE status = 'open'
GROUP BY owner
ORDER BY critical DESC, total_violations DESC
LIMIT 20;
```

### Compliance Trend (Last 10 Scans)

```sql
SELECT
    scanned_at,
    compliance_pct,
    open_violations,
    critical_open,
    new_violations,
    newly_resolved
FROM my_catalog.watchdog.v_compliance_trend
ORDER BY scanned_at DESC
LIMIT 10;
```

### Ungoverned Agents Accessing Production

```sql
SELECT
    ri.resource_name AS agent_name,
    ri.owner,
    v.policy_id,
    v.severity,
    v.detail,
    v.first_detected,
    DATEDIFF(CURRENT_DATE(), v.first_detected) AS days_open
FROM my_catalog.watchdog.violations v
JOIN my_catalog.watchdog.resource_inventory ri
    ON v.resource_id = ri.resource_id
WHERE v.status = 'open'
    AND v.domain = 'AgentGovernance'
    AND ri.resource_type IN ('agent', 'agent_execution')
ORDER BY v.severity, days_open DESC;
```

## Next Steps

- **Schedule daily refreshes.** Configure the Watchdog workflow to run on a cron schedule so dashboards always show current data.
- **Set up alerts.** Use Databricks SQL alerts on critical-severity violation counts to trigger notifications when posture degrades.
- **Deploy MCP servers.** Enable AI assistants and agents to query governance posture programmatically.
- **Add industry policies.** Import healthcare, financial, or defense policy packs to evaluate against regulatory frameworks.
